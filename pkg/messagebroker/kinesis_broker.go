package messagebroker

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

const (
	checkpointPrefix = "kinesis"
)

func NewKinesisProducer(kinesisClient *kinesis.Client, streamName string) Producer {
	return &kinesisProducer{
		kinesisClient: kinesisClient,
		streamName:    streamName,
	}
}

func NewKinesisConsumer(
	kinesisClient *kinesis.Client,
	appID string,
	streamName string,
	shardManager *kinesisShardManager,
	shardRefresh time.Duration,
	workerPoll time.Duration,
) Consumer {
	return &kinesisConsumer{
		kinesisClient:  kinesisClient,
		appID:          appID,
		streamName:     streamName,
		shardManager:   shardManager,
		shardRefresher: time.NewTicker(shardRefresh),
		workerPoller:   time.NewTicker(workerPoll),

		shardWorkers: map[string]context.CancelFunc{},
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
		return fmt.Errorf("failed to send message to kinesis, err=%w", err)
	}
	return nil
}

type kinesisConsumer struct {
	kinesisClient *kinesis.Client
	appID         string
	streamName    string
	shardManager  *kinesisShardManager

	shardRefresher *time.Ticker
	workerPoller   *time.Ticker
	shardWorkers   map[string]context.CancelFunc
}

type checkpoint struct {
	metadata *CheckpointMetadata
	handler  MessageHandler
}

func (k *kinesisConsumer) Consume(ctx context.Context, handlerFactory MessageHandlerFactory) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-k.shardRefresher.C:
			k.reloadWorker(ctx, handlerFactory)
		}
	}
}

func (k *kinesisConsumer) reloadWorker(ctx context.Context, handlerFactory MessageHandlerFactory) error {
	shards, err := k.shardManager.AcquireShards(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire shards, err=%w", err)
	}
	shardMap := k.buildShardMap(shards)
	for shardID, cancelFunc := range k.shardWorkers {
		if _, ok := shardMap[shardID]; !ok {
			cancelFunc()
			delete(k.shardWorkers, shardID)
		}
	}
	for shardID, shard := range shardMap {
		if _, ok := k.shardWorkers[shardID]; ok {
			continue
		}
		workerCtx, cancelFunc := context.WithCancel(ctx)
		go func() {
			if err := k.worker(workerCtx, shard, handlerFactory); err != nil {
				delete(k.shardWorkers, shardID)
			}
		}()
		k.shardWorkers[shardID] = cancelFunc
	}
	return nil
}

func (k *kinesisConsumer) buildShardMap(shards []*shardMetadata) map[string]*shardMetadata {
	out := map[string]*shardMetadata{}
	for _, shard := range shards {
		out[shard.id] = shard
	}
	return out
}

func (k *kinesisConsumer) worker(ctx context.Context, shard *shardMetadata, handlerFactory MessageHandlerFactory) error {
	checkpoint, err := k.loadCheckpoint(ctx, shard, handlerFactory)
	if err != nil {
		return fmt.Errorf("failed to load checkpoint, err=%w", err)
	}
	it, err := k.kinesisClient.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName:             aws.String(k.streamName),
		ShardId:                aws.String(shard.id),
		ShardIteratorType:      types.ShardIteratorTypeAfterSequenceNumber,
		StartingSequenceNumber: aws.String(checkpoint.metadata.Offset),
	})
	if err != nil {
		return fmt.Errorf("failed to get shard iterator, err=%w", err)
	}
	iter := it.ShardIterator
	for iter != nil {
		select {
		case <-ctx.Done():
			return nil
		case <-k.workerPoller.C:
			out, err := k.kinesisClient.GetRecords(ctx, &kinesis.GetRecordsInput{
				ShardIterator: iter,
			})
			if err != nil {
				return fmt.Errorf("failed to get records, err=%w", err)
			}
			var lastOffset *string
			for _, record := range out.Records {
				if record.PartitionKey == nil || record.SequenceNumber == nil {
					continue
				}
				message := &Message{
					PartitionKey: *record.PartitionKey,
					Body:         string(record.Data),
				}
				if err := checkpoint.handler.Handle(ctx, message); err != nil {
					return fmt.Errorf("failed to handle message, err=%w", err)
				}
				lastOffset = record.SequenceNumber
			}
			iter = out.NextShardIterator
			if lastOffset != nil {
				if err := checkpoint.handler.SaveCheckpoint(ctx, &CheckpointMetadata{
					CheckpointKey: k.buildCheckpointID(),
					AppID:         k.appID,
					Topic:         k.streamName,
					Partition:     shard.id,
					Offset:        *lastOffset,
				}); err != nil {
					return fmt.Errorf("failed to save checkpoint, err=%w", err)
				}
			}
		}
	}
	return nil
}

func (k *kinesisConsumer) loadCheckpoint(ctx context.Context, shard *shardMetadata, handlerFactory MessageHandlerFactory) (*checkpoint, error) {
	checkpointID := k.buildCheckpointID()
	handler := handlerFactory()
	checkpointMetadata, err := handler.LoadCheckpoint(ctx, checkpointID)
	if err != nil {
		return nil, fmt.Errorf("failed to load checkpoint, err=%w", err)
	}
	if checkpointMetadata != nil {
		return &checkpoint{
			metadata: checkpointMetadata,
			handler:  handler,
		}, nil
	}
	dependentHandler, err := k.loadDependentHandler(ctx, shard.dependent, handlerFactory)
	if err != nil {
		return nil, fmt.Errorf("failed to load dependent handler, err=%w", err)
	}
	offset, err := k.readShard(ctx, shard, dependentHandler, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read shard, err=%w", err)
	}
	return &checkpoint{
		metadata: &CheckpointMetadata{
			CheckpointKey: checkpointID,
			AppID:         k.appID,
			Topic:         k.streamName,
			Partition:     shard.id,
			Offset:        offset,
		},
		handler: dependentHandler,
	}, nil
}

func (k *kinesisConsumer) loadDependentHandler(ctx context.Context, dependentShards []*shardMetadata, handlerFactory MessageHandlerFactory) (MessageHandler, error) {
	handlers := []MessageHandler{}
	for _, dependentShard := range dependentShards {
		checkpoint, err := k.loadCheckpoint(ctx, dependentShard, handlerFactory)
		if err != nil {
			return nil, fmt.Errorf("failed to load parent checkpoint, err=%w", err)
		}
		handler := checkpoint.handler
		offset := checkpoint.metadata.Offset
		if _, err := k.readShard(ctx, dependentShard, handler, &offset); err != nil {
			return nil, fmt.Errorf("failed to read shard, err=%w", err)
		}
	}
	if len(handlers) == 0 {
		return handlerFactory(), nil
	}
	out := handlers[0]
	for i := 1; i <= len(handlers); i++ {
		out.Merge(handlers[i])
	}
	return out, nil
}

func (k *kinesisConsumer) readShard(ctx context.Context, shard *shardMetadata, handler MessageHandler, offset *string) (string, error) {
	input := k.buildShardIteratorInput(shard.id, offset)
	it, err := k.kinesisClient.GetShardIterator(ctx, input)
	if err != nil {
		return "", fmt.Errorf("failed to get shard iterator, err=%w", err)
	}
	iter := it.ShardIterator
	lastOffset := shard.startOffset
	if offset != nil {
		lastOffset = *offset
	}
	for iter != nil {
		out, err := k.kinesisClient.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: iter,
		})
		if err != nil {
			return "", fmt.Errorf("failed to get records, err=%w", err)
		}
		for _, record := range out.Records {
			if record.PartitionKey == nil || record.SequenceNumber == nil {
				continue
			}
			message := &Message{
				PartitionKey: *record.PartitionKey,
				Body:         string(record.Data),
			}
			if err := handler.Handle(ctx, message); err != nil {
				return "", fmt.Errorf("failed to handle message, err=%w", err)
			}
			lastOffset = *record.SequenceNumber
		}
		iter = out.NextShardIterator
	}
	return lastOffset, nil
}

func (k *kinesisConsumer) buildShardIteratorInput(shardID string, offset *string) *kinesis.GetShardIteratorInput {
	if offset != nil {
		return &kinesis.GetShardIteratorInput{
			StreamName:             aws.String(k.streamName),
			ShardId:                aws.String(shardID),
			ShardIteratorType:      types.ShardIteratorTypeAfterSequenceNumber,
			StartingSequenceNumber: aws.String(*offset),
		}
	}
	return &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(k.streamName),
		ShardId:           aws.String(shardID),
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	}
}

func (k *kinesisConsumer) buildCheckpointID() string {
	return fmt.Sprintf("checkpoint:%s:%s:%s", checkpointPrefix, k.appID, k.streamName)
}
