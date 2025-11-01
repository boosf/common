package lock

import (
	"time"

	"github.com/boosf/common/internal/retrystrategy"
)

func NewRetriesOption(retries int64) lockOption {
	return &retriesLockOption{
		retries: retries,
	}
}

func NewRetryStrategyOption(retryStrategy retrystrategy.RetryStrategy) lockOption {
	return &retryStrategyLockOption{
		retryStrategy: retryStrategy,
	}
}

func newLockConfig() *lockConfig {
	return &lockConfig{
		Retries:       3,
		RetryStrategy: retrystrategy.NewExponentialBackoffWithJitterRetryStrategy(100*time.Millisecond, 3*time.Second),
	}
}

type lockConfig struct {
	Retries       int64
	RetryStrategy retrystrategy.RetryStrategy
}

type lockOption interface {
	Apply(config *lockConfig) *lockConfig
}

type retriesLockOption struct {
	retries int64
}

func (r *retriesLockOption) Apply(config *lockConfig) *lockConfig {
	config.Retries = r.retries
	return config
}

type retryStrategyLockOption struct {
	retryStrategy retrystrategy.RetryStrategy
}

func (r *retryStrategyLockOption) Apply(config *lockConfig) *lockConfig {
	config.RetryStrategy = r.retryStrategy
	return config
}
