package messagebroker

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

func NewKinesisProducer(kinesisClient *kinesis.Client, streamName string) Producer {
	return &kinesisProducer{
		kinesisClient: kinesisClient,
		streamName:    streamName,
	}
}

func NewKinesisConsumer(kinesisClient *kinesis.Client, streamName string) Consumer {
	return &kinesisConsumer{
		kinesisClient: kinesisClient,
		streamName:    streamName,
	}
}

type kinesisProducer struct {
	kinesisClient *kinesis.Client
	streamName    string
}

func (k *kinesisProducer) Send(ctx context.Context, message *Message) error {
	input := &kinesis.PutRecordInput{
		StreamName:   aws.String(k.streamName),
		PartitionKey: aws.String(message.PartitionKey),
		Data:         []byte(message.Body),
	}
	_, err := k.kinesisClient.PutRecord(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to send message to kinesis, key=%s, body=%s, err=%w", message.PartitionKey, message.Body, err)
	}
	return nil
}

type kinesisConsumer struct {
	kinesisClient *kinesis.Client
	streamName    string
}

func (k *kinesisConsumer) Consume(ctx context.Context, handler func() MessageHandler) error {
	activeShards := map[string]context.CancelFunc{}
	for {
		select {
		case <-ctx.Done():
			return nil
		}
	}
}
