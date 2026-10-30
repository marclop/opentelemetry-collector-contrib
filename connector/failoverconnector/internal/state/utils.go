// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package state // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/failoverconnector/internal/state"

import (
	"context"
	"time"
)

type PSConstants struct {
	RetryInterval time.Duration
	FailoverAfter time.Duration
	RetryGap      time.Duration
	MaxRetries    int
}

type CancelManager struct {
	cancelFunc context.CancelFunc
}

func (c *CancelManager) Cancel() {
	if c.cancelFunc != nil {
		c.cancelFunc()
	}
}

func (c *CancelManager) UpdateFn(cancelFunc context.CancelFunc) {
	c.cancelFunc = cancelFunc
}
