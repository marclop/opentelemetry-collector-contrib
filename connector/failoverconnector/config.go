// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package failoverconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/failoverconnector"

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pipeline"
)

var (
	errNoPipelinePriority    = errors.New("No pipelines are defined in the priority list")
	errInvalidRetryIntervals = errors.New("Retry interval must be positive")
	errNegativeDuration      = errors.New("failover_after and retry_timeout must not be negative")
)

type Config struct {
	// QueueSettings use the exporterhelper sending_queue to move the queue to the connector to avoid data being stuck
	// in the queue of an unhealthy exporter
	QueueSettings configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`

	// PipelinePriority is the list of pipeline level priorities in a 1 - n configuration, multiple pipelines can
	// sit at a single priority level and will be routed in a fanout. If any pipeline at a level fails, the
	// level is considered unhealthy
	PipelinePriority [][]pipeline.ID `mapstructure:"priority_levels"`

	// RetryInterval is the frequency at which the pipeline levels will attempt to recover by going over
	// all levels below the current
	RetryInterval time.Duration `mapstructure:"retry_interval"`

	// FailoverAfter is how long a level must keep failing, without a single success, before the connector
	// moves to the next level. Until then errors are returned to the caller so upstream can retry. Zero
	// fails over on the first error.
	FailoverAfter time.Duration `mapstructure:"failover_after"`

	// RetryTimeout bounds each recovery attempt on a higher level, so a level that is still down cannot
	// hold up live data for longer. Zero means no bound beyond the caller's context.
	RetryTimeout time.Duration `mapstructure:"retry_timeout"`

	// Condition is an optional user-defined rule that controls which downstream
	// errors trigger failover. When omitted, all errors trigger failover.
	Condition configoptional.Optional[ConditionsConfig] `mapstructure:"condition"`

	// prevent unkeyed literal initialization
	_ struct{}
}

// Validate needs to ensure RetryInterval > # elements in PriorityList * RetryGap
func (c *Config) Validate() error {
	if len(c.PipelinePriority) == 0 {
		return errNoPipelinePriority
	}
	if c.RetryInterval <= 0 {
		return errInvalidRetryIntervals
	}
	if c.FailoverAfter < 0 || c.RetryTimeout < 0 {
		return errNegativeDuration
	}

	return nil
}
