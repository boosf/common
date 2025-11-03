package messagebroker

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/boosf/common/pkg/kvstore"
	"github.com/boosf/common/pkg/lock"
)

const (
	lockPrefix   = "kinesis_shard_manager"
	lockDuration = 10 * time.Second
)

func newKinesisShardManager(
	kinesisClient *kinesis.Client,
	lockClient lock.Client,
	kvstoreClient kvstore.Client,
	appID string,
	nodeID string,
	streamName string,
) *kinesisShardManager {
	return &kinesisShardManager{
		kinesisClient: kinesisClient,
		lockClient:    lockClient,
		kvstoreClient: kvstoreClient,
		appID:         appID,
		nodeID:        nodeID,
		streamName:    streamName,
	}
}

type kinesisShardManager struct {
	kinesisClient *kinesis.Client
	lockClient    lock.Client
	kvstoreClient kvstore.Client
	appID         string
	nodeID        string
	streamName    string
}

type shardConfiguration struct {
	dependencyGraph map[string][]string
	active          []string
}

func (k *kinesisShardManager) AcquireShards(ctx context.Context) (*shardConfiguration, error) {
	// **** Here we will start the lock, lookup the consistent hash ring, find what active shards we should own and then hold them
	// **** Once we have our active shards, each worker will start their active shards, resolve their dependencies to get their current states (i.e. replay the old streams if necessary), then read from the horizon (or the stored offset)
	shards, err := k.getShards(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get shards")
	}
	shardConfiguration := k.buildShardConfiguration(shards)
	lockKey := k.getLockKey()
	lock, err := k.lockClient.Acquire(ctx, lockKey, lockDuration)
	if err != nil {
		return nil, fmt.Errorf("failed to get lock, err=%w", err)
	}
	defer lock.Release(ctx)
	return shardConfiguration, nil
}

func (k *kinesisShardManager) getShards(ctx context.Context) ([]*types.Shard, error) {
	shards := []*types.Shard{}
	var nextToken *string
	for {
		output, err := k.kinesisClient.ListShards(ctx, &kinesis.ListShardsInput{
			StreamName: aws.String(k.streamName),
			NextToken:  nextToken,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get shards, err=%w", err)
		}
		for _, shard := range output.Shards {
			tmp := shard
			shards = append(shards, &tmp)
		}
		if output.NextToken == nil {
			break
		}
		nextToken = output.NextToken
	}
	return shards, nil
}

func (k *kinesisShardManager) buildShardConfiguration(shards []*types.Shard) *shardConfiguration {
	graph := map[string][]string{}
	activeShards := []string{}
	for _, shard := range shards {
		if shard.ShardId == nil || shard.SequenceNumberRange == nil {
			continue
		}
		shardID := *shard.ShardId
		dependent := []string{}
		if shard.ParentShardId != nil {
			dependent = append(dependent, *shard.ParentShardId)
		}
		if shard.AdjacentParentShardId != nil {
			dependent = append(dependent, *shard.AdjacentParentShardId)
		}
		graph[shardID] = dependent
		if shard.SequenceNumberRange.EndingSequenceNumber == nil {
			activeShards = append(activeShards, shardID)
		}
	}
	return &shardConfiguration{
		dependencyGraph: graph,
		active:          activeShards,
	}
}

func (k *kinesisShardManager) getLockKey() string {
	return fmt.Sprintf("%s:%s:%s", lockPrefix, k.appID, k.streamName)
}
