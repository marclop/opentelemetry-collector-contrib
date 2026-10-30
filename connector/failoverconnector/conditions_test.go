// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package failoverconnector

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumererror"
)

func TestErrorConditionShouldFailover(t *testing.T) {
	cond := &ErrorCondition{Contains: []string{"network failure", "connection refused"}}

	assert.False(t, cond.ShouldFailover(nil), "nil error must never trigger failover")
	assert.True(t, cond.ShouldFailover(errors.New("connection reset: network failure while exporting")),
		"error containing the substring must trigger failover")
	assert.False(t, cond.ShouldFailover(errors.New("sending_queue is full: data dropped")),
		"error without the substring must not trigger failover")
	assert.True(t, cond.ShouldFailover(errors.New("Network Failure")),
		"matching must be case-insensitive")
	assert.True(t, cond.ShouldFailover(errors.New("CONNECTION REFUSED")),
		"any configured substring may match")
}

func TestErrorConditionPermanent(t *testing.T) {
	permanent := consumererror.NewPermanent(errors.New("400 mapper_parsing_exception"))
	retryable := errors.New("connection refused")

	tests := []struct {
		name             string
		cond             *ErrorCondition
		wantFailoverPerm bool
		wantFailoverRetr bool
	}{
		{"unset", &ErrorCondition{Contains: []string{"refused"}}, false, true},
		{"false without contains", &ErrorCondition{Permanent: new(false)}, false, true},
		{"true without contains", &ErrorCondition{Permanent: new(true)}, true, true},
		{"false with contains", &ErrorCondition{Permanent: new(false), Contains: []string{"mapper"}}, false, false},
		{"true with contains", &ErrorCondition{Permanent: new(true), Contains: []string{"refused"}}, true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantFailoverPerm, tt.cond.ShouldFailover(permanent), "permanent error")
			assert.Equal(t, tt.wantFailoverRetr, tt.cond.ShouldFailover(retryable), "retryable error")
		})
	}
}

func TestErrorConditionValidate(t *testing.T) {
	assert.NoError(t, (&ErrorCondition{Contains: []string{"network failure"}}).Validate())
	assert.NoError(t, (&ErrorCondition{Permanent: new(false)}).Validate())
	assert.ErrorIs(t, (&ErrorCondition{}).Validate(), errEmptyErrorContains)
	assert.ErrorIs(t, (&ErrorCondition{Contains: []string{""}}).Validate(), errEmptyErrorContains)
	assert.ErrorIs(t, (&ErrorCondition{Contains: []string{"network failure", ""}}).Validate(), errEmptyErrorContains)
}

func TestConditionsConfigValidate(t *testing.T) {
	assert.ErrorIs(t, (&ConditionsConfig{}).Validate(), errNoConditionDefined)
	assert.NoError(t, (&ConditionsConfig{ErrorCond: &ErrorCondition{}}).Validate())
	assert.NoError(t, (&ConditionsConfig{ErrorCond: &ErrorCondition{Contains: []string{"network failure"}}}).Validate())
}

func TestBuildCondition(t *testing.T) {
	assert.Nil(t, buildCondition(nil), "nil config must build no condition")

	cond := buildCondition(&ConditionsConfig{ErrorCond: &ErrorCondition{Contains: []string{"network failure"}}})
	require.NotNil(t, cond)
	assert.True(t, cond.ShouldFailover(errors.New("network failure")))
	assert.False(t, cond.ShouldFailover(errors.New("queue full")))
}
