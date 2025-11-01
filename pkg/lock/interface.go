package lock

import (
	"context"
)

type Lock interface {
	Acquire(ctx context.Context, key string) error
}
