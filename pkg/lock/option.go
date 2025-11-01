package lock

import (
	"time"

	"github.com/boosf/common/internal/option"
	"github.com/boosf/common/internal/retrystrategy"
)

func NewRetriesOption(retries int64) option.Option[*Option] {
	return &retriesOption{
		retries: retries,
	}
}

func NewRetryStrategyOption(retryStrategy retrystrategy.RetryStrategy) option.Option[*Option] {
	return &retryStrategyOption{
		retryStrategy: retryStrategy,
	}
}

func newDefaultOption() *Option {
	return &Option{
		Retries:       3,
		RetryStrategy: retrystrategy.NewExponentialBackoffWithJitterRetryStrategy(100*time.Millisecond, 3*time.Second),
	}
}

type Option struct {
	Retries       int64
	RetryStrategy retrystrategy.RetryStrategy
}

type retriesOption struct {
	retries int64
}

func (r *retriesOption) Apply(option *Option) *Option {
	option.Retries = r.retries
	return option
}

type retryStrategyOption struct {
	retryStrategy retrystrategy.RetryStrategy
}

func (r *retryStrategyOption) Apply(option *Option) *Option {
	option.RetryStrategy = r.retryStrategy
	return option
}
