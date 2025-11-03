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

// For Kinesis, it does not store the offsets for us, meaning we need to store the checkpoint ID for each shard containing the most processed offset for our last checkpoint.
// If the shard is added, then the old shard will be deprecated and the shards will split, meaning we can read from the old parent shard ID to get its checkpoint, then continue reading for the new shards.
// We can guarantee the same messages within the new split shards will all be on the old checkpoint, which we use to create 2 new checkpoints for those old shards and deprecate the old checkpoint.

// For Kafka, it can automatically store the offsets for the partitions for us, so we don't need to store the offset for the partition and only the checkpoint .
// We
// We just need to commit each partition key atomically for if the partition key gets assigned a new partition upon rebalance so new consumer can takeover.

type Consumer interface {
	Consume(ctx context.Context, partitionMessageHandleCreator func() MessageHandler) error
}
