# failoverconnector as a spill switch (PoC)

This branch checks whether `failoverconnector` can act as the switch for a spill topic in Elastic Streams, and what it would take if it can't. It's a PoC on top of upstream main (`d63fa33`), and I'd split it into smaller PRs before sending anything upstream (see the end).

## Context

A Streams unit collector reads a Kafka topic, runs the customer's pipeline and fans the result out to several destinations, each ending in one exporter. The Kafka receiver only commits an offset once the whole fanout returns nil, and every exporter waits for the result, so an error blocks the record and it gets retried. That's loss-free, but with one consumer group per unit, a failing destination X (say a customer's remote Elasticsearch with a revoked key) holds back the healthy destinations in the same unit.

The spill option would have X's leaf write its already-processed output to its own Kafka topic once X has been unhealthy for a while, so the unit can commit and the healthy destinations keep flowing. A second consumer group drains the spill topic into X and blocks there until X recovers. With the existing connector, the leaf would look like this:

```yaml
connectors:
  failover/X:
    priority_levels: [[logs/X-primary], [logs/X-spill]]

service:
  pipelines:
    logs/X-primary: {receivers: [failover/X], exporters: [elasticsearch/X]}
    logs/X-spill:   {receivers: [failover/X], exporters: [kafka/spill-X]}
```

For this to be safe, the connector has to spill only on retryable errors that last, return permanent errors to the caller (they'd be counted as a drop, since replaying them later can't succeed), and return an error whenever neither level stored the data, so the record blocks instead of being dropped.

## What happens today

The connector keeps an index to the current level. When a call fails, it moves down one level and sends the same data there in the same call. After that, a ticker fires every `retry_interval` (10 minutes by default), and the next live call after a tick is sent to the higher levels first, moving back up if one of them accepts it.

The first commit ([`ba2fd8d`](https://github.com/marclop/opentelemetry-collector-contrib/commit/ba2fd8d8d1a)) adds `spill_poc_test.go`, which pins this down against unmodified upstream (run with `-race`). Where retries matter, X is a fake wrapped in a real `exporterhelper` with `WithRetry`, the same way the ES exporter behaves.

| Scenario | Test | What happens today |
| :---- | :---- | :---- |
| One transient error on X | `TestGap_SingleTransientErrorSpillsEverything` | Everything after it spills (6 of 6 batches, with X healthy again) until the next tick |
| X has a finite retry budget | `TestFailoverOnlyAfterExporterRetryBudget` | Failover waits for the budget, but the caller is held for all of it (300 ms budget, 299 ms blocked) |
| Recovery probe while X is down | `TestGap_RecoveryProbeStallsLiveCall` | Each tick holds a live call for X's whole retry budget. With a hung X and a caller deadline, the spill gets an expired context, fails, and is demoted too |
| Permanent error on X | `TestGap_PermanentErrorSpills` | It spills and demotes X, and `contains` can't express "everything except permanent errors" |
| X and spill both fail | `TestBothLevelsFailReturnsErrorToCaller`, `TestGap_StuckPastLastLevelUntilNextTick` | The caller gets an error (good), but every later call is rejected without trying anything until the next tick, even after both recover |
| Default `sending_queue` | `TestGap_DefaultConfigQueueAcksBeforeDelivery` | It returns nil before X or the spill is called, so `sending_queue: {enabled: false}` is required |
| Partial error from X | `TestPartialErrorSpillsWholeBatch` | The spill gets the whole batch, so the items X stored become duplicates, which item IDs catch |
| 32 concurrent callers while X fails | `TestConcurrentCallsWhileXFails` | All spilled, no races, and the `TryLock` sometimes lets one extra attempt through to X |
| Delivered or spilled? | `TestGap_NothingTellsCallerWhetherDataWasSpilled` | Both return nil, with nothing in the data or context to tell them apart |

## What the second commit changes

The second commit ([`bc0f05a`](https://github.com/marclop/opentelemetry-collector-contrib/commit/bc0f05abcad)) adds two settings and the `permanent` flag from [#51424](https://github.com/open-telemetry/opentelemetry-collector-contrib/pull/51424), and turns the `TestGap_*` tests into tests of the fixed behavior:

```yaml
connectors:
  failover/X:
    priority_levels: [[logs/X-primary], [logs/X-spill]]
    sending_queue: {enabled: false}
    failover_after: 5m          # X has to fail with no success in between for this long
    retry_timeout: 10s          # bounds each recovery probe on live data
    retry_interval: 1m
    condition:
      error: {permanent: false} # permanent errors go back to the caller
```

Until `failover_after` runs out, the connector returns X's error, so the receiver retries the record like any other block, and a success in between restarts the clock. I think this is better than relying on X's retry budget, since a budget applies per request (a slow or poison request looks the same as a dead destination) and holds the caller for all of it. With a short exporter budget and `failover_after` in minutes, the receiver gets control back often and the decision depends on how long X has been failing. In `TestFailoverAfterRidesOutTransientErrors`, intermittent failures never spill, and a sustained failure with `failover_after: 300ms` spills after 373 ms.

`retry_timeout` cuts the probe stall from X's whole retry budget (~290 ms in the test) to the timeout (~75 ms with 80 ms), and when it expires the data goes to the current level with the caller's original context, so a hung X no longer takes the spill down with it.

Two changes affect every user. The last level never fails over: its error goes back to the caller and the next call tries it again, which removes the stuck state and `errNoValidPipeline`. And `TryLock` is gone, since `HandleError` already takes its own mutex. The first one changes the error users see when everything fails, so it would be its own PR.

Production Go is +94/-55 across 8 files. Tests pass with `-race`, and `go vet` and the repo's `golangci-lint` config are clean.

## What a connector can't fix

Elastic Streams counts outcomes per Kafka record and per leaf, from what the leaf returns. With failover, the leaf returns nil for both delivered and spilled data, so spilled items would count as stored, and then again when the spill group delivers them. Counting after the connector in each level's pipeline, with `stored` and `spilled` both accepted as the record's outcome, would probably fix it, but that's on our side and unbuilt.

The level and the failure clock live in memory per connector instance, so each pod decides on its own and a restart resets both. That's loss-safe, but unit status would have to combine pods, which is why the active-level gauge in [#51685](https://github.com/open-telemetry/opentelemetry-collector-contrib/pull/51685) matters to us.

The collector's fanout calls leaves one after another, so until X is demoted it still holds the healthy leaves on the same record. Spill shortens the block to `failover_after` but doesn't remove it. Whether a long block inside one call collides with the Kafka poll interval is unverified.

## Upstreaming

The order I'd go in is `permanent` through #51424, then `failover_after` and `retry_timeout` together, then the last-level change on its own.
