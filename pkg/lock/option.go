package lock

import (
	"time"

	"github.com/boosf/common/internal/option"
	"github.com/boosf/common/internal/retrystrategy"
)

func NewRetriesOption(retries int64) option.Option[*lockOption] {
	return &retriesLockOption{
		retries: retries,
	}
}

func NewRetryStrategyOption(retryStrategy retrystrategy.RetryStrategy) option.Option[*lockOption] {
	return &retryStrategyLockOption{
		retryStrategy: retryStrategy,
	}
}

func newDefaultOption() *lockOption {
	return &lockOption{
		Retries:       3,
		RetryStrategy: retrystrategy.NewExponentialBackoffWithJitterRetryStrategy(100*time.Millisecond, 3*time.Second),
	}
}

type lockOption struct {
	Retries       int64
	RetryStrategy retrystrategy.RetryStrategy
}

type retriesLockOption struct {
	retries int64
}

func (r *retriesLockOption) Apply(option *lockOption) *lockOption {
	option.Retries = r.retries
	return option
}

type retryStrategyLockOption struct {
	retryStrategy retrystrategy.RetryStrategy
}

func (r *retryStrategyLockOption) Apply(option *lockOption) *lockOption {
	option.RetryStrategy = r.retryStrategy
	return option
}
