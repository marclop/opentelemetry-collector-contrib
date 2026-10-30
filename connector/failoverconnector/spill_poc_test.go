// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package failoverconnector

// Characterization tests for using the failover connector as a two level
// "spill" switch: level 0 is the real destination X (an exporter), level 1 is
// a spill exporter that must never lose data. Tests named TestGap_* document
// behavior that does not fit the spill design. The others show behavior that
// does fit. With failover_after, retry_timeout and condition.error.permanent
// set, the connector closes most of those gaps.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pipeline"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/failoverconnector/internal/metadata"
)

var (
	errXDown     = errors.New("elasticsearch: connection refused")
	errSpillDown = errors.New("kafka: broker unavailable")
)

// consumeFn is the scripted behavior of a fake consumer.
type consumeFn func(ctx context.Context, ld plog.Logs) error

func alwaysFail(err error) consumeFn {
	return func(context.Context, plog.Logs) error { return err }
}

func alwaysSucceed() consumeFn {
	return func(context.Context, plog.Logs) error { return nil }
}

// fakeLogs is a consumer.Logs with swappable behavior that counts attempts and
// keeps the batches it accepted.
type fakeLogs struct {
	attempts  atomic.Int64
	delivered consumertest.LogsSink
	behavior  atomic.Pointer[consumeFn]
}

func newFakeLogs(fn consumeFn) *fakeLogs {
	f := &fakeLogs{}
	f.set(fn)
	return f
}

func (f *fakeLogs) set(fn consumeFn) { f.behavior.Store(&fn) }

func (*fakeLogs) Capabilities() consumer.Capabilities { return consumer.Capabilities{} }

func (f *fakeLogs) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	f.attempts.Add(1)
	if err := (*f.behavior.Load())(ctx, ld); err != nil {
		return err
	}
	return f.delivered.ConsumeLogs(ctx, ld)
}

func (f *fakeLogs) acceptedRecords() int { return f.delivered.LogRecordCount() }

// newExporter wraps next in a real exporterhelper exporter with a retry
// budget and no queue, the way an ES exporter behaves with
// sending_queue.wait_for_result: true.
func newExporter(t *testing.T, next consumer.Logs, maxElapsed time.Duration) consumer.Logs {
	t.Helper()
	retry := configretry.NewDefaultBackOffConfig()
	retry.InitialInterval = 5 * time.Millisecond
	retry.MaxInterval = 20 * time.Millisecond
	retry.MaxElapsedTime = maxElapsed
	exp, err := exporterhelper.NewLogs(t.Context(), exporter.Settings{
		ID:                component.MustNewID("fake_es"),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
		BuildInfo:         component.NewDefaultBuildInfo(),
	}, &struct{}{}, next.ConsumeLogs,
		exporterhelper.WithRetry(retry),
		exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{Timeout: 0}),
	)
	require.NoError(t, err)
	require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() { assert.NoError(t, exp.Shutdown(t.Context())) })
	return exp
}

var (
	xPrimaryID = pipeline.NewIDWithName(pipeline.SignalLogs, "X-primary")
	xSpillID   = pipeline.NewIDWithName(pipeline.SignalLogs, "X-spill")
)

func spillConfig(retryInterval time.Duration) *Config {
	return &Config{
		PipelinePriority: [][]pipeline.ID{{xPrimaryID}, {xSpillID}},
		RetryInterval:    retryInterval,
	}
}

// newSpillConnector builds the connector through the factory with the
// candidate wiring: level 0 is X, level 1 is the spill. The connector has no
// sending_queue, so ConsumeLogs is synchronous.
func newSpillConnector(t *testing.T, cfg *Config, x, spill consumer.Logs) *logsFailover {
	t.Helper()
	router := connector.NewLogsRouter(map[pipeline.ID]consumer.Logs{xPrimaryID: x, xSpillID: spill})
	conn, err := NewFactory().CreateLogsToLogs(t.Context(), connectortest.NewNopSettings(metadata.Type), cfg, router.(consumer.Logs))
	require.NoError(t, err)
	f, ok := conn.(*logsFailover)
	require.True(t, ok, "expected the raw connector, got %T", conn)
	t.Cleanup(func() { assert.NoError(t, f.Shutdown(t.Context())) })
	return f
}

func logsWithRecords(n int) plog.Logs {
	ld := plog.NewLogs()
	lrs := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for range n {
		lrs.AppendEmpty()
	}
	return ld
}

func timed(fn func() error) (time.Duration, error) {
	start := time.Now()
	err := fn()
	return time.Since(start), err
}

// Scenario 1. The default, failover_after unset, keeps failing over on the first error.
func TestSingleTransientErrorSpillsEverythingByDefault(t *testing.T) {
	var failNext atomic.Bool
	failNext.Store(true)
	x := newFakeLogs(func(context.Context, plog.Logs) error {
		if failNext.CompareAndSwap(true, false) {
			return errXDown
		}
		return nil
	})
	spill := newFakeLogs(alwaysSucceed())
	f := newSpillConnector(t, spillConfig(time.Hour), x, spill)

	// One failed request, then X is healthy again.
	for range 6 {
		require.NoError(t, f.ConsumeLogs(t.Context(), logsWithRecords(1)))
	}

	assert.Equal(t, int64(1), x.attempts.Load(), "X is never tried again after its first failure")
	assert.Equal(t, 6, spill.acceptedRecords(), "the failed call and the 5 after it all went to the spill")
	assert.Equal(t, 0, x.acceptedRecords())
	assert.Equal(t, 1, f.failover.TestGetCurrentConsumerIndex())
}

func TestFailoverAfterRidesOutTransientErrors(t *testing.T) {
	const failoverAfter = 300 * time.Millisecond

	t.Run("a success between failures restarts the clock", func(t *testing.T) {
		var down atomic.Bool
		down.Store(true)
		x := newFakeLogs(func(context.Context, plog.Logs) error {
			if down.Load() {
				return errXDown
			}
			return nil
		})
		spill := newFakeLogs(alwaysSucceed())
		cfg := spillConfig(time.Hour)
		cfg.FailoverAfter = failoverAfter
		f := newSpillConnector(t, cfg, x, spill)

		require.ErrorIs(t, f.ConsumeLogs(t.Context(), logsWithRecords(1)), errXDown, "error goes back so the receiver retries")
		time.Sleep(failoverAfter + 50*time.Millisecond)
		down.Store(false)
		require.NoError(t, f.ConsumeLogs(t.Context(), logsWithRecords(1)))
		down.Store(true)
		require.ErrorIs(t, f.ConsumeLogs(t.Context(), logsWithRecords(1)), errXDown, "the old failure no longer counts")

		assert.Equal(t, 0, f.failover.TestGetCurrentConsumerIndex())
		assert.Equal(t, 0, spill.acceptedRecords())
		assert.Equal(t, 1, x.acceptedRecords())
	})

	t.Run("sustained failure spills once failover_after has elapsed", func(t *testing.T) {
		const budget = 50 * time.Millisecond
		x := newFakeLogs(alwaysFail(errXDown))
		spill := newFakeLogs(alwaysSucceed())
		cfg := spillConfig(time.Hour)
		cfg.FailoverAfter = failoverAfter
		f := newSpillConnector(t, cfg, newExporter(t, x, budget), spill)

		start := time.Now()
		var blocked, rejected int
		for {
			elapsed, err := timed(func() error { return f.ConsumeLogs(t.Context(), logsWithRecords(1)) })
			if err == nil {
				break
			}
			require.ErrorIs(t, err, errXDown)
			assert.Less(t, elapsed, budget+200*time.Millisecond, "each call is held for the exporter retry budget only")
			rejected++
			blocked += int(elapsed.Milliseconds())
			require.Less(t, time.Since(start), 5*time.Second)
		}
		total := time.Since(start)
		t.Logf("spilled after %s, %d calls returned an error first, each held about %dms", total, rejected, blocked/rejected)
		assert.GreaterOrEqual(t, total, failoverAfter)
		assert.Less(t, total, failoverAfter+budget+300*time.Millisecond)
		assert.Equal(t, 1, f.failover.TestGetCurrentConsumerIndex())
		assert.Equal(t, 1, spill.acceptedRecords(), "only the call that crossed the threshold is spilled, earlier ones were retried by the caller")
	})
}

// Scenario 2.
func TestFailoverOnlyAfterExporterRetryBudget(t *testing.T) {
	const budget = 300 * time.Millisecond
	x := newFakeLogs(alwaysFail(errXDown))
	spill := newFakeLogs(alwaysSucceed())
	f := newSpillConnector(t, spillConfig(time.Hour), newExporter(t, x, budget), spill)

	first, err := timed(func() error { return f.ConsumeLogs(t.Context(), logsWithRecords(1)) })
	require.NoError(t, err)
	t.Logf("first call blocked %s (retry budget %s), X attempts=%d", first, budget, x.attempts.Load())
	assert.GreaterOrEqual(t, first, budget-50*time.Millisecond, "caller is blocked for about the retry budget before the connector sees an error")
	assert.Less(t, first, budget+500*time.Millisecond)
	assert.Greater(t, x.attempts.Load(), int64(3), "exporterhelper retried while the caller waited")
	assert.Equal(t, 1, spill.acceptedRecords())

	second, err := timed(func() error { return f.ConsumeLogs(t.Context(), logsWithRecords(1)) })
	require.NoError(t, err)
	t.Logf("second call took %s", second)
	assert.Less(t, second, 50*time.Millisecond, "once demoted, X is skipped")
}

// Scenario 3. Without retry_timeout the recovery attempt waits out X's retry budget.
func TestRecoveryProbe(t *testing.T) {
	const (
		budget   = 300 * time.Millisecond
		interval = 150 * time.Millisecond
	)

	t.Run("retry_timeout unset: every tick stalls one live call for the whole retry budget", func(t *testing.T) {
		x := newFakeLogs(alwaysFail(errXDown))
		spill := newFakeLogs(alwaysSucceed())
		f := newSpillConnector(t, spillConfig(interval), newExporter(t, x, budget), spill)

		_, err := timed(func() error { return f.ConsumeLogs(t.Context(), logsWithRecords(1)) })
		require.NoError(t, err)
		require.Equal(t, 1, f.failover.TestGetCurrentConsumerIndex())

		for i := range 3 {
			time.Sleep(interval + 50*time.Millisecond) // let the retry tick fire
			before := x.attempts.Load()
			probe, err := timed(func() error { return f.ConsumeLogs(t.Context(), logsWithRecords(1)) })
			require.NoError(t, err)
			t.Logf("tick %d: live call blocked %s, X attempts during probe=%d", i, probe, x.attempts.Load()-before)
			assert.GreaterOrEqual(t, probe, budget-50*time.Millisecond, "probe uses live data and waits out X's retry budget")
			assert.Greater(t, x.attempts.Load()-before, int64(3))
			assert.Equal(t, 1, f.failover.TestGetCurrentConsumerIndex(), "still on the spill level")
		}
		assert.Equal(t, 4, spill.acceptedRecords(), "no data lost, each probed batch still reached the spill")
	})

	t.Run("retry_timeout bounds the stall and the batch still reaches the spill", func(t *testing.T) {
		const retryTimeout = 80 * time.Millisecond
		x := newFakeLogs(alwaysFail(errXDown))
		spill := newFakeLogs(alwaysSucceed())
		cfg := spillConfig(interval)
		cfg.RetryTimeout = retryTimeout
		f := newSpillConnector(t, cfg, newExporter(t, x, budget), spill)
		_, err := timed(func() error { return f.ConsumeLogs(t.Context(), logsWithRecords(1)) })
		require.NoError(t, err)

		for i := range 3 {
			time.Sleep(interval + 50*time.Millisecond)
			probe, err := timed(func() error { return f.ConsumeLogs(t.Context(), logsWithRecords(1)) })
			require.NoError(t, err)
			t.Logf("tick %d: live call blocked %s (retry_timeout %s)", i, probe, retryTimeout)
			assert.Less(t, probe, retryTimeout+100*time.Millisecond)
			assert.Equal(t, 1, f.failover.TestGetCurrentConsumerIndex())
		}
		assert.Equal(t, 4, spill.acceptedRecords())
	})

	t.Run("a ctx deadline shorter than the retry budget makes the probe fail fast", func(t *testing.T) {
		x := newFakeLogs(alwaysFail(errXDown))
		spill := newFakeLogs(alwaysSucceed())
		f := newSpillConnector(t, spillConfig(interval), newExporter(t, x, budget), spill)
		_, err := timed(func() error { return f.ConsumeLogs(t.Context(), logsWithRecords(1)) })
		require.NoError(t, err)

		time.Sleep(interval + 50*time.Millisecond)
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		probe, err := timed(func() error { return f.ConsumeLogs(ctx, logsWithRecords(1)) })
		require.NoError(t, err)
		t.Logf("probe with 100ms deadline blocked %s", probe)
		assert.Less(t, probe, budget, "exporterhelper gives up when the next retry would pass the deadline")
	})

	for _, tt := range []struct {
		name         string
		retryTimeout time.Duration
		wantErr      bool
	}{
		{name: "retry_timeout unset: a hung X spends the caller's deadline and the spill sees an expired ctx", wantErr: true},
		{name: "retry_timeout set: a hung X is cut off and the spill still has the caller's ctx", retryTimeout: 30 * time.Millisecond},
	} {
		t.Run(tt.name, func(t *testing.T) {
			testHungProbe(t, interval, tt.retryTimeout, tt.wantErr)
		})
	}
}

func testHungProbe(t *testing.T, interval, retryTimeout time.Duration, wantErr bool) {
	t.Helper()
	x := newFakeLogs(alwaysFail(errXDown))
	ctxAwareSpill := newFakeLogs(func(ctx context.Context, _ plog.Logs) error { return ctx.Err() })
	cfg := spillConfig(interval)
	cfg.RetryTimeout = retryTimeout
	f := newSpillConnector(t, cfg, x, ctxAwareSpill)
	// Demote X with a live ctx, then let X hang like a request without a timeout.
	require.NoError(t, f.ConsumeLogs(t.Context(), logsWithRecords(1)))
	require.Equal(t, 1, f.failover.TestGetCurrentConsumerIndex())
	x.set(func(ctx context.Context, _ plog.Logs) error {
		<-ctx.Done()
		return ctx.Err()
	})

	time.Sleep(interval + 50*time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	probe, err := timed(func() error { return f.ConsumeLogs(ctx, logsWithRecords(1)) })
	t.Logf("probe blocked %s, err=%v", probe, err)
	if wantErr {
		require.ErrorIs(t, err, context.DeadlineExceeded, "the probe consumed the caller's budget and the spill call failed with the expired ctx")
		assert.Equal(t, 1, ctxAwareSpill.acceptedRecords(), "only the batch from before the probe was spilled")
	} else {
		require.NoError(t, err)
		assert.Equal(t, 2, ctxAwareSpill.acceptedRecords())
	}
	assert.Equal(t, 1, f.failover.TestGetCurrentConsumerIndex(), "the last level is never demoted")
}

// Scenario 4.
func TestPermanentErrors(t *testing.T) {
	permanentErr := consumererror.NewPermanent(errors.New("elasticsearch: 400 mapper_parsing_exception"))

	t.Run("condition.error.permanent false returns permanent errors as is and keeps X active", func(t *testing.T) {
		cfg := spillConfig(time.Hour)
		cfg.Condition = configoptional.Some(ConditionsConfig{ErrorCond: &ErrorCondition{Permanent: new(false)}})
		x := newFakeLogs(alwaysFail(permanentErr))
		spill := newFakeLogs(alwaysSucceed())
		f := newSpillConnector(t, cfg, x, spill)

		require.ErrorIs(t, f.ConsumeLogs(t.Context(), logsWithRecords(1)), permanentErr)
		assert.True(t, consumererror.IsPermanent(f.ConsumeLogs(t.Context(), logsWithRecords(1))))
		assert.Equal(t, 0, f.failover.TestGetCurrentConsumerIndex())
		assert.Equal(t, 0, spill.acceptedRecords())

		x.set(alwaysFail(errXDown))
		require.NoError(t, f.ConsumeLogs(t.Context(), logsWithRecords(1)), "any other error still spills, whatever its message")
		assert.Equal(t, 1, spill.acceptedRecords())
		assert.Equal(t, 1, f.failover.TestGetCurrentConsumerIndex())
	})

	t.Run("a permanent error whose message matches contains is still returned as is when permanent is false", func(t *testing.T) {
		matching := consumererror.NewPermanent(errors.New("mapping failed: connection refused"))
		cfg := spillConfig(time.Hour)
		cfg.Condition = configoptional.Some(ConditionsConfig{ErrorCond: &ErrorCondition{Contains: []string{"connection refused"}, Permanent: new(false)}})
		x := newFakeLogs(alwaysFail(matching))
		spill := newFakeLogs(alwaysSucceed())
		f := newSpillConnector(t, cfg, x, spill)

		require.ErrorIs(t, f.ConsumeLogs(t.Context(), logsWithRecords(1)), matching)
		assert.Equal(t, 0, spill.acceptedRecords())
	})

	t.Run("without a condition a permanent error spills and demotes X", func(t *testing.T) {
		x := newFakeLogs(alwaysFail(permanentErr))
		spill := newFakeLogs(alwaysSucceed())
		f := newSpillConnector(t, spillConfig(time.Hour), x, spill)

		require.NoError(t, f.ConsumeLogs(t.Context(), logsWithRecords(1)), "caller sees success, the permanent error is hidden")
		assert.Equal(t, 1, spill.acceptedRecords())
		assert.Equal(t, 1, f.failover.TestGetCurrentConsumerIndex(), "one poison record demotes X for everyone")
	})

	t.Run("contains alone is an allow list of retryable strings and cannot express not permanent", func(t *testing.T) {
		tests := []struct {
			name        string
			xErr        error
			wantSpilled bool
		}{
			{"permanent error with a message that does not match is returned as is", permanentErr, false},
			{"retryable error that matches spills", errXDown, true},
			{"retryable error that is not on the list is returned as is and never spills", errors.New("tls: handshake timeout"), false},
			{"permanent error whose message happens to match spills", consumererror.NewPermanent(errors.New("mapping failed: connection refused")), true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				x := newFakeLogs(alwaysFail(tt.xErr))
				spill := newFakeLogs(alwaysSucceed())
				cfg := spillConfig(time.Hour)
				cfg.Condition = configoptional.Some(ConditionsConfig{ErrorCond: &ErrorCondition{Contains: []string{"connection refused"}}})
				f := newSpillConnector(t, cfg, x, spill)

				err := f.ConsumeLogs(t.Context(), logsWithRecords(1))
				if tt.wantSpilled {
					require.NoError(t, err)
					assert.Equal(t, 1, spill.acceptedRecords())
					return
				}
				require.ErrorIs(t, err, tt.xErr)
				assert.Equal(t, 0, spill.acceptedRecords())
				assert.Equal(t, 0, f.failover.TestGetCurrentConsumerIndex())
			})
		}
	})
}

// Scenario 5.
func TestBothLevelsFailReturnsErrorToCaller(t *testing.T) {
	x := newFakeLogs(alwaysFail(errXDown))
	spill := newFakeLogs(alwaysFail(errSpillDown))
	f := newSpillConnector(t, spillConfig(time.Hour), x, spill)

	err := f.ConsumeLogs(t.Context(), logsWithRecords(1))
	require.ErrorIs(t, err, errSpillDown, "the spill's own error reaches the receiver, nothing is dropped")
	assert.Equal(t, int64(1), x.attempts.Load())
	assert.Equal(t, int64(1), spill.attempts.Load())
}

func TestSpillRecoveryDoesNotWaitForRetryTick(t *testing.T) {
	x := newFakeLogs(alwaysFail(errXDown))
	spill := newFakeLogs(alwaysFail(errSpillDown))
	f := newSpillConnector(t, spillConfig(time.Hour), x, spill)

	for range 3 {
		require.ErrorIs(t, f.ConsumeLogs(t.Context(), logsWithRecords(1)), errSpillDown)
	}
	require.Equal(t, 1, f.failover.TestGetCurrentConsumerIndex(), "the last level stays active, nothing sits past it")
	assert.Equal(t, int64(1), x.attempts.Load(), "X is skipped while the spill is the active level")
	assert.Equal(t, int64(3), spill.attempts.Load(), "every call retries the spill")

	spill.set(alwaysSucceed())
	require.NoError(t, f.ConsumeLogs(t.Context(), logsWithRecords(1)), "first call after the spill recovered, with retry_interval at 1h")
	assert.Equal(t, 1, spill.acceptedRecords())
}

// Scenario 6.
func TestGap_DefaultConfigQueueAcksBeforeDelivery(t *testing.T) {
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	require.True(t, cfg.QueueSettings.HasValue(), "sending_queue is on by default")
	assert.False(t, cfg.QueueSettings.Get().WaitForResult)
	cfg.PipelinePriority = [][]pipeline.ID{{xPrimaryID}, {xSpillID}}

	release := make(chan struct{})
	x := newFakeLogs(func(context.Context, plog.Logs) error {
		<-release
		return errXDown
	})
	spill := newFakeLogs(alwaysFail(errSpillDown))
	router := connector.NewLogsRouter(map[pipeline.ID]consumer.Logs{xPrimaryID: x, xSpillID: spill})
	conn, err := NewFactory().CreateLogsToLogs(t.Context(), connectortest.NewNopSettings(metadata.Type), cfg, router.(consumer.Logs))
	require.NoError(t, err)
	require.NoError(t, conn.Start(t.Context(), componenttest.NewNopHost()))
	defer func() {
		close(release)
		assert.NoError(t, conn.Shutdown(t.Context()))
	}()

	// X is blocked and then fails, and so does the spill: nothing is delivered,
	// yet the caller already got nil.
	require.NoError(t, conn.ConsumeLogs(t.Context(), logsWithRecords(1)), "nil returned before X or the spill were even tried")
	assert.Equal(t, int64(0), spill.attempts.Load())
}

func TestSendingQueueConfigForSynchronousErrors(t *testing.T) {
	tests := []struct {
		name          string
		yaml          map[string]any
		wantQueue     bool
		wantWaitFor   bool
		wantRawType   bool
		wantErrOnFail bool
	}{
		{
			name:          "default config queues and acks immediately",
			yaml:          map[string]any{},
			wantQueue:     true,
			wantErrOnFail: false,
		},
		{
			name:          "sending_queue.enabled false returns the raw connector",
			yaml:          map[string]any{"sending_queue": map[string]any{"enabled": false}},
			wantRawType:   true,
			wantErrOnFail: true,
		},
		{
			name:          "sending_queue.wait_for_result true propagates the error through the queue",
			yaml:          map[string]any{"sending_queue": map[string]any{"enabled": true, "wait_for_result": true}},
			wantQueue:     true,
			wantWaitFor:   true,
			wantErrOnFail: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewFactory().CreateDefaultConfig().(*Config)
			conf := confmap.NewFromStringMap(map[string]any{"priority_levels": [][]string{{"logs/X-primary"}, {"logs/X-spill"}}})
			require.NoError(t, conf.Merge(confmap.NewFromStringMap(tt.yaml)))
			require.NoError(t, conf.Unmarshal(cfg))
			require.Equal(t, tt.wantQueue, cfg.QueueSettings.HasValue())
			if tt.wantWaitFor {
				assert.True(t, cfg.QueueSettings.Get().WaitForResult)
			}

			x := newFakeLogs(alwaysFail(errXDown))
			spill := newFakeLogs(alwaysFail(errSpillDown))
			router := connector.NewLogsRouter(map[pipeline.ID]consumer.Logs{xPrimaryID: x, xSpillID: spill})
			conn, err := NewFactory().CreateLogsToLogs(t.Context(), connectortest.NewNopSettings(metadata.Type), cfg, router.(consumer.Logs))
			require.NoError(t, err)
			require.NoError(t, conn.Start(t.Context(), componenttest.NewNopHost()))
			defer func() { assert.NoError(t, conn.Shutdown(t.Context())) }()
			_, raw := conn.(*logsFailover)
			assert.Equal(t, tt.wantRawType, raw)

			err = conn.ConsumeLogs(t.Context(), logsWithRecords(1))
			if tt.wantErrOnFail {
				require.Error(t, err, "both levels fail, the caller must see it")
				assert.ErrorContains(t, err, errSpillDown.Error())
			} else {
				require.NoError(t, err, "queue acked data that nobody delivered")
			}
		})
	}
}

// Scenario 7.
func TestPartialErrorSpillsWholeBatch(t *testing.T) {
	failedSubset := logsWithRecords(1)
	x := newFakeLogs(func(context.Context, plog.Logs) error {
		return consumererror.NewLogs(errXDown, failedSubset)
	})
	spill := newFakeLogs(alwaysSucceed())
	f := newSpillConnector(t, spillConfig(time.Hour), x, spill)

	require.NoError(t, f.ConsumeLogs(t.Context(), logsWithRecords(3)))

	assert.Equal(t, 3, spill.acceptedRecords(), "the connector ignores the failed subset, so records X already accepted are spilled too (duplicates, not loss)")
}

// Scenario 8.
func TestConcurrentCallsWhileXFails(t *testing.T) {
	const callers = 32
	// Hold every caller inside X until all of them arrived, then fail them all at once.
	var arrived sync.WaitGroup
	arrived.Add(callers)
	var once sync.Once
	gate := make(chan struct{})
	x := newFakeLogs(func(context.Context, plog.Logs) error {
		select {
		case <-gate:
		case <-time.After(2 * time.Second):
		}
		return errXDown
	})
	spill := newFakeLogs(alwaysSucceed())
	f := newSpillConnector(t, spillConfig(time.Hour), x, spill)

	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			arrived.Done()
			arrived.Wait()
			once.Do(func() { time.AfterFunc(50*time.Millisecond, func() { close(gate) }) })
			errs <- f.ConsumeLogs(t.Context(), logsWithRecords(1))
		})
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	t.Logf("%d callers, X attempts=%d (extra=%d), spill accepted=%d", callers, x.attempts.Load(), x.attempts.Load()-callers, spill.acceptedRecords())
	assert.Equal(t, callers, spill.acceptedRecords(), "every record reached the spill exactly once")
	assert.GreaterOrEqual(t, x.attempts.Load(), int64(callers))
	assert.Equal(t, 1, f.failover.TestGetCurrentConsumerIndex())

	// Later traffic never touches X again.
	before := x.attempts.Load()
	for range callers {
		require.NoError(t, f.ConsumeLogs(t.Context(), logsWithRecords(1)))
	}
	assert.Equal(t, before, x.attempts.Load())
}

// Scenario 9.
func TestGap_NothingTellsCallerWhetherDataWasSpilled(t *testing.T) {
	var spilledCtx context.Context
	spillCtx := func(ctx context.Context, _ plog.Logs) error { spilledCtx = ctx; return nil }
	x := newFakeLogs(alwaysSucceed())
	spill := newFakeLogs(spillCtx)
	f := newSpillConnector(t, spillConfig(time.Hour), x, spill)

	ld := logsWithRecords(1)
	want, err := (&plog.JSONMarshaler{}).MarshalLogs(ld)
	require.NoError(t, err)

	require.NoError(t, f.ConsumeLogs(t.Context(), ld))
	assert.Equal(t, 1, x.acceptedRecords(), "delivered to X, nil")

	x.set(alwaysFail(errXDown))
	require.NoError(t, f.ConsumeLogs(t.Context(), ld))
	assert.Equal(t, 1, spill.acceptedRecords(), "spilled, also nil")
	got, err := (&plog.JSONMarshaler{}).MarshalLogs(spill.delivered.AllLogs()[0])
	require.NoError(t, err)
	assert.JSONEq(t, string(want), string(got), "the spilled batch carries no marker")
	assert.Equal(t, t.Context(), spilledCtx, "ctx is passed through untouched")
}
