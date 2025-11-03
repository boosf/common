package lock

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func New(template Template) Client {
	return &client{
		template: template,
	}
}

type client struct {
	template Template
}

func (c *client) Acquire(ctx context.Context, key string, duration time.Duration, options ...lockOption) (Lock, error) {
	config := newLockConfig()
	for _, option := range options {
		config = option.Apply(config)
	}
	errs := []error{}
	for attempt := range config.Retries {
		lock, err := c.template.Acquire(ctx, key, duration)
		if err != nil {
			errs = append(errs, err)
			config.RetryStrategy.WaitFor(attempt)
			continue
		}
		return lock, nil
	}
	return nil, fmt.Errorf("failed to acquire lock, key=%s, err=%w", key, errors.Join(errs...))
}
