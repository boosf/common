package messagebroker

type Message struct {
	PartitionKey string
	Body         string
}

type CheckpointMetadata struct {
	CheckpointKey string // A unique string to represent the checkpoint for the consumer

	AppID     string // In Kafka represents consumer group, Kinesis some unique identifier
	Topic     string // In Kafka represents topic, Kinesis represents stream
	Partition string // In Kafka represents partition, Kinesis represents shard
	Offset    string // The current offset for the partition
}
