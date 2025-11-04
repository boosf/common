package messagebroker

import "context"

type Producer interface {
	Send(ctx context.Context, message *Message) error
}

type MessageHandler interface {
	Handle(ctx context.Context, message *Message) error
	Merge(handler MessageHandler) MessageHandler
	SaveCheckpoint(ctx context.Context, checkpoint *CheckpointMetadata) error
	LoadCheckpoint(ctx context.Context, checkpointKey string) (*CheckpointMetadata, error)
}

type MessageHandlerFactory func() MessageHandler

type Consumer interface {
	Consume(ctx context.Context, handlerFactory MessageHandlerFactory) error
}
