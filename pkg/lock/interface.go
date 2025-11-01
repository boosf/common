package lock

import (
	"context"
	"time"
)

type Client interface {
	Acquire(ctx context.Context, key string, duration time.Duration, options ...lockOption) (Lock, error)
}

type Lock interface {
	Extend(ctx context.Context, duration time.Duration) error
	Release(ctx context.Context) error
}
