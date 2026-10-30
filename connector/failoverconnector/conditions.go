// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package failoverconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/failoverconnector"

import (
	"errors"
	"slices"
	"strings"

	"go.opentelemetry.io/collector/consumer/consumererror"
)

var (
	errNoConditionDefined           = errors.New("no condition is defined")
	errTooManyConditions            = errors.New("only one failover condition can be applied")
	errEmptyErrorContains           = errors.New("error condition must define non-empty 'contains' strings or 'permanent'")
	_                     Condition = (*ErrorCondition)(nil)
)

// It plugs in all the available implementations of
// `Condition`. At most only one condition can be set
type ConditionsConfig struct {
	ErrorCond *ErrorCondition `mapstructure:"error"`

	// prevent unkeyed literal initialization
	_ struct{}
}

// Validate ensures exactly one condition is set and that the
// condition itself is valid.
func (c *ConditionsConfig) Validate() error {
	set := 0
	if c.ErrorCond != nil {
		set++
	}
	if set > 1 {
		return errTooManyConditions
	}

	if set == 0 {
		return errNoConditionDefined
	}

	return nil
}

// All conditions must implement this interface
type Condition interface {
	// ShouldFailover determines if the connector should failover based on current consumer error
	ShouldFailover(err error) bool
}

// ErrorCondition implements Condition
type ErrorCondition struct {
	// Contains lists case-insensitive substrings matched against downstream errors.
	// An error matching any substring triggers failover.
	Contains []string `mapstructure:"contains"`

	// Permanent controls whether permanent errors (consumererror.IsPermanent) trigger failover.
	// When unset they are treated like any other error. When false they are returned to the caller as is.
	Permanent *bool `mapstructure:"permanent"`

	// prevent unkeyed literal initialization
	_ struct{}
}

// Validate ensures the error condition has a usable match string.
func (c *ErrorCondition) Validate() error {
	if (len(c.Contains) == 0 && c.Permanent == nil) || slices.Contains(c.Contains, "") {
		return errEmptyErrorContains
	}
	return nil
}

// ShouldFailover reports whether err should trigger failover.
// A nil error never triggers failover. A permanent error follows Permanent when it is set. Otherwise
// failover happens when no substring is configured or the error message contains one.
func (c *ErrorCondition) ShouldFailover(err error) bool {
	if err == nil {
		return false
	}
	if c.Permanent != nil && consumererror.IsPermanent(err) {
		return *c.Permanent
	}
	if len(c.Contains) == 0 {
		return true
	}
	message := strings.ToLower(err.Error())
	for _, value := range c.Contains {
		if value != "" && strings.Contains(message, strings.ToLower(value)) {
			return true
		}
	}
	return false
}

func buildCondition(c *ConditionsConfig) Condition {
	if c == nil {
		return nil
	}
	if c.ErrorCond != nil {
		return c.ErrorCond
	}
	return nil
}
