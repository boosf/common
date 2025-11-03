package messagebroker

import "context"

type Producer interface {
	Send(ctx context.Context, message *Message) error
}

type MessageHandler interface {
	Handle(ctx context.Context, message *Message) error
	SaveCheckpoint(ctx context.Context, checkpointKey string) error
	LoadCheckpoint(ctx context.Context, checkpointKey string) error
}

type Consumer interface {
	Consume(ctx context.Context, handler func() MessageHandler) error
}
