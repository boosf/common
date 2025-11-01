package lock

import (
	"context"
	"time"

	"github.com/boosf/common/internal/option"
)

type Client interface {
	Acquire(ctx context.Context, key string, duration time.Duration, options ...option.Option[*lockOption]) (Lock, error)
}

type Lock interface {
	Extend(ctx context.Context, duration time.Duration) error
	Release(ctx context.Context) error
}
