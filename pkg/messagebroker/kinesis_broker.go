package messagebroker

import (
	"context"
	"errors"
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
		kinesisClient: kinesisClient,
		appID:         appID,
		streamName:    streamName,
		shardManager:  shardManager,
		shardRefresh:  shardRefresh,
		workerPoll:    workerPoll,

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
	shardRefresh  time.Duration
	workerPoll    time.Duration

	shardWorkers map[string]context.CancelFunc
}

type checkpoint struct {
	metadata *CheckpointMetadata
	handler  MessageHandler
}

func (k *kinesisConsumer) Consume(ctx context.Context, handlerFactory MessageHandlerFactory) error {
	ticker := time.NewTicker(k.shardRefresh)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			k.consume(ctx, handlerFactory)
		}
	}
}

func (k *kinesisConsumer) consume(ctx context.Context, handlerFactory MessageHandlerFactory) error {
	shardConfig, err := k.shardManager.AcquireShards(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire shards, err=%w", err)
	}
	activeShardSet := map[string]struct{}{}
	for _, shard := range shardConfig.active {
		activeShardSet[shard] = struct{}{}
	}
	for shard, cancelFunc := range k.shardWorkers {
		if _, ok := activeShardSet[shard]; !ok {
			cancelFunc()
			delete(k.shardWorkers, shard)
		}
	}
	for shard := range activeShardSet {
		if _, ok := k.shardWorkers[shard]; ok {
			continue
		}
		workerCtx, cancelFunc := context.WithCancel(ctx)
		go func() {
			if err := k.worker(workerCtx, shardConfig, shard, handlerFactory); err != nil {
				delete(k.shardWorkers, shard)
			}
		}()
		k.shardWorkers[shard] = cancelFunc
	}
	return nil
}

func (k *kinesisConsumer) worker(ctx context.Context, shardConfig *shardConfiguration, shard string, handlerFactory MessageHandlerFactory) error {
	checkpoint, err := k.loadCheckpoint(ctx, shardConfig, shard, handlerFactory)
	if err != nil {
		return fmt.Errorf("failed to load checkpoint, err=%w", err)
	}
	it, err := k.kinesisClient.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName:             aws.String(k.streamName),
		ShardId:                aws.String(shard),
		ShardIteratorType:      types.ShardIteratorTypeAfterSequenceNumber,
		StartingSequenceNumber: aws.String(checkpoint.metadata.Offset),
	})
	if err != nil {
		return fmt.Errorf("failed to get shard iterator, err=%w", err)
	}
	iter := it.ShardIterator
	ticker := time.NewTicker(k.workerPoll)
	for iter != nil {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
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
					Partition:     shard,
					Offset:        *lastOffset,
				}); err != nil {
					return fmt.Errorf("failed to save checkpoint, err=%w", err)
				}
			}
		}
	}
	return nil
}

func (k *kinesisConsumer) loadCheckpoint(ctx context.Context, shardConfig *shardConfiguration, shard string, handlerFactory MessageHandlerFactory) (*checkpoint, error) {
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
	shardMetadata, ok := shardConfig.shards[shard]
	if !ok {
		return nil, errors.New("shard not in shards config")
	}
	dependentShards := k.buildDependentShards(shardMetadata)
	dependentHandler, err := k.loadDependentHandler(ctx, shardConfig, dependentShards, handlerFactory)
	if err != nil {
		return nil, fmt.Errorf("failed to load dependent handler, err=%w", err)
	}
	offset, err := k.readShard(ctx, shardConfig, shard, dependentHandler, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read shard, err=%w", err)
	}
	return &checkpoint{
		metadata: &CheckpointMetadata{
			CheckpointKey: checkpointID,
			AppID:         k.appID,
			Topic:         k.streamName,
			Partition:     shard,
			Offset:        offset,
		},
		handler: dependentHandler,
	}, nil
}

func (k *kinesisConsumer) buildDependentShards(shardMetadata *types.Shard) []string {
	dependentShards := []string{}
	parent := shardMetadata.ParentShardId
	if parent != nil {
		dependentShards = append(dependentShards, *parent)
	}
	adjacent := shardMetadata.AdjacentParentShardId
	if adjacent != nil {
		dependentShards = append(dependentShards, *adjacent)
	}
	return dependentShards
}

func (k *kinesisConsumer) loadDependentHandler(ctx context.Context, shardConfig *shardConfiguration, dependentShards []string, handlerFactory MessageHandlerFactory) (MessageHandler, error) {
	handlers := []MessageHandler{}
	for _, dependentShard := range dependentShards {
		checkpoint, err := k.loadCheckpoint(ctx, shardConfig, dependentShard, handlerFactory)
		if err != nil {
			return nil, fmt.Errorf("failed to load parent checkpoint, err=%w", err)
		}
		handler := checkpoint.handler
		offset := checkpoint.metadata.Offset
		if _, err := k.readShard(ctx, shardConfig, dependentShard, handler, &offset); err != nil {
			return nil, fmt.Errorf("failed to read shard, shard=%s, err=%w", dependentShard, err)
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

func (k *kinesisConsumer) readShard(ctx context.Context, shardConfig *shardConfiguration, shard string, handler MessageHandler, offset *string) (string, error) {
	input := k.buildShardIteratorInput(shard, offset)
	it, err := k.kinesisClient.GetShardIterator(ctx, input)
	if err != nil {
		return "", fmt.Errorf("failed to get shard iterator, err=%w", err)
	}
	iter := it.ShardIterator
	shardMetadata, ok := shardConfig.shards[shard]
	if !ok {
		return "", errors.New("shard does not exist")
	}
	if shardMetadata.SequenceNumberRange == nil || shardMetadata.SequenceNumberRange.StartingSequenceNumber == nil {
		return "", errors.New("shard metadata has no start sequence number")
	}
	lastOffset := *shardMetadata.SequenceNumberRange.StartingSequenceNumber
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

func (k *kinesisConsumer) buildShardIteratorInput(shard string, offset *string) *kinesis.GetShardIteratorInput {
	if offset != nil {
		return &kinesis.GetShardIteratorInput{
			StreamName:             aws.String(k.streamName),
			ShardId:                aws.String(shard),
			ShardIteratorType:      types.ShardIteratorTypeAfterSequenceNumber,
			StartingSequenceNumber: aws.String(*offset),
		}
	}
	return &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(k.streamName),
		ShardId:           aws.String(shard),
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	}
}

func (k *kinesisConsumer) buildCheckpointID() string {
	return fmt.Sprintf("checkpoint:%s:%s:%s", checkpointPrefix, k.appID, k.streamName)
}
