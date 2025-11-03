package kvstore

import (
	"context"
	"time"
)

type Client interface {
	Get(ctx context.Context, key string) (string, error)
	Put(ctx context.Context, key string, value string) error
	PutWithExpiry(ctx context.Context, key string, value string, expiresAt time.Time) error
}
