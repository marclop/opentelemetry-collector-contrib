// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package failoverconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/failoverconnector"

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/pipeline"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/failoverconnector/internal/state"
)

var errConsumer = errors.New("Error registering consumer")

type consumerProvider[C any] func(...pipeline.ID) (C, error)

// baseFailoverRouter provides the common infrastructure for failover routing
type baseFailoverRouter[C any] struct {
	cfg       *Config
	pS        *state.PipelineSelector
	consumers []C

	notifyRetry chan struct{}
	done        chan struct{}
	conditions  Condition
	logger      *zap.Logger
}

// getCurrentConsumer returns the consumer for the current healthy level
func (f *baseFailoverRouter[C]) getCurrentConsumer() (C, int) {
	pl := f.pS.CurrentPipeline()
	return f.consumers[pl], pl
}

// getConsumerAtIndex returns the consumer at a specific index
func (f *baseFailoverRouter[C]) getConsumerAtIndex(idx int) C {
	return f.consumers[idx]
}

// reportConsumerError records a failure on level idx and reports whether the data should be
// retried on the current level. The last level never fails over, there is nothing to fail over to,
// so its error goes back to the caller and the next call tries the same level again.
func (f *baseFailoverRouter[C]) reportConsumerError(idx int, err error) bool {
	if idx == len(f.cfg.PipelinePriority)-1 {
		return false
	}
	if f.conditions != nil {
		f.logger.Debug("Downstream error matched failover condition", zap.Error(err), zap.Int("pipeline_level", idx))
	}
	return f.pS.HandleError(idx)
}

// retryContext bounds a recovery attempt by RetryTimeout.
func (f *baseFailoverRouter[C]) retryContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if f.cfg.RetryTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, f.cfg.RetryTimeout)
}

// shouldFailoverOnError goes through the user defined condition
// to check if given error should cause failover
func (f *baseFailoverRouter[C]) shouldFailoverOnError(err error) bool {
	if f.conditions != nil {
		return f.conditions.ShouldFailover(err)
	}
	return true
}

func (f *baseFailoverRouter[C]) Shutdown() {
	select {
	case <-f.done:
	default:
		close(f.done)
	}
}

func newBaseFailoverRouter[C any](provider consumerProvider[C], cfg *Config, logger *zap.Logger) (*baseFailoverRouter[C], error) {
	done := make(chan struct{})
	notifyRetry := make(chan struct{}, 1)
	pSConstants := state.PSConstants{
		RetryInterval: cfg.RetryInterval,
		FailoverAfter: cfg.FailoverAfter,
	}

	consumers := make([]C, 0)
	for _, pipelines := range cfg.PipelinePriority {
		baseConsumer, err := provider(pipelines...)
		if err != nil {
			return nil, errConsumer
		}
		consumers = append(consumers, baseConsumer)
	}

	selector := state.NewPipelineSelector(notifyRetry, done, pSConstants)
	return &baseFailoverRouter[C]{
		consumers:   consumers,
		cfg:         cfg,
		pS:          selector,
		done:        done,
		notifyRetry: notifyRetry,
		conditions:  buildCondition(cfg.Condition.Get()),
		logger:      logger,
	}, nil
}

// For Testing
func (f *baseFailoverRouter[C]) ModifyConsumerAtIndex(idx int, c C) {
	f.consumers[idx] = c
}

func (f *baseFailoverRouter[C]) TestGetCurrentConsumerIndex() int {
	return f.pS.CurrentPipeline()
}

func (f *baseFailoverRouter[C]) TestSetStableConsumerIndex(idx int) {
	f.pS.TestSetCurrentPipeline(idx)
}

func (f *baseFailoverRouter[C]) TestGetConsumerAtIndex(idx int) C {
	return f.consumers[idx]
}
