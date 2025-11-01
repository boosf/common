package logs

import (
	"context"
	"fmt"
	"log"
)

func New() Client {
	return &client{}
}

type client struct {
}

func (c *client) Info(ctx context.Context, format string, args ...interface{}) {
	log.Printf("INFO: %s", fmt.Sprintf(format, args...))
}

func (c *client) Warn(ctx context.Context, format string, args ...interface{}) {
	log.Printf("WARN: %s", fmt.Sprintf(format, args...))
}

func (c *client) Error(ctx context.Context, format string, args ...interface{}) {
	log.Printf("ERROR: %s", fmt.Sprintf(format, args...))
}
