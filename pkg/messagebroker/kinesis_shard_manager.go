package messagebroker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/boosf/common/pkg/clock"
	"github.com/boosf/common/pkg/kvstore"
	"github.com/boosf/common/pkg/lock"
	"github.com/boosf/common/pkg/utils"
)

const (
	keyPrefix    = "kinesis_shard_manager"
	lockDuration = 10 * time.Second
)

func NewKinesisShardManager(
	kinesisClient *kinesis.Client,
	lockClient lock.Client,
	kvstoreClient kvstore.Client,
	clockClient clock.Client,
	appID string,
	nodeID string,
	streamName string,
	lockDuration time.Duration,
	hashRingExpiry time.Duration,
	hashRingPartitionExpiry time.Duration,
	leaseDuration time.Duration,
) *kinesisShardManager {
	return &kinesisShardManager{
		kinesisClient:           kinesisClient,
		lockClient:              lockClient,
		kvstoreClient:           kvstoreClient,
		clockClient:             clockClient,
		appID:                   appID,
		nodeID:                  nodeID,
		streamName:              streamName,
		lockDuration:            lockDuration,
		hashRingExpiry:          hashRingExpiry,
		hashRingPartitionExpiry: hashRingPartitionExpiry,
		leaseDuration:           leaseDuration,

		leasedShards: map[string]lock.Lock{},
	}
}

type kinesisShardManager struct {
	kinesisClient           *kinesis.Client
	lockClient              lock.Client
	kvstoreClient           kvstore.Client
	clockClient             clock.Client
	appID                   string
	nodeID                  string
	streamName              string
	lockDuration            time.Duration
	hashRingExpiry          time.Duration
	hashRingPartitionExpiry time.Duration
	leaseDuration           time.Duration

	leasedShards map[string]lock.Lock
}

type shardConfiguration struct {
	dependencyGraph map[string][]string
	shards          map[string]*types.Shard
	active          []string
}

func (k *kinesisShardManager) AcquireShards(ctx context.Context) (*shardConfiguration, error) {
	shards, err := k.getShards(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get shards")
	}
	shardConfig := k.buildShardConfiguration(shards)
	leasedShards, err := k.leaseShards(ctx, shardConfig.active)
	if err != nil {
		return nil, fmt.Errorf("failed to lease shards")
	}
	return &shardConfiguration{
		dependencyGraph: shardConfig.dependencyGraph,
		shards:          shardConfig.shards,
		active:          leasedShards,
	}, nil
}

func (k *kinesisShardManager) leaseShards(ctx context.Context, activeShards []string) ([]string, error) {
	leaseLockKey := k.buildLeaseLockKey()
	leaseLock, err := k.lockClient.Acquire(ctx, leaseLockKey, lockDuration)
	if err != nil {
		return nil, fmt.Errorf("failed to get lock, err=%w", err)
	}
	defer leaseLock.Release(ctx)
	hashRing, err := k.loadHashRing(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load hash ring, err=%w", err)
	}
	hashRingPartitionExpires := k.clockClient.TimeFromNow(k.hashRingPartitionExpiry)
	hashRing.AddWithExpiry(k.nodeID, hashRingPartitionExpires)
	partitions, err := hashRing.Get(activeShards)
	if err != nil {
		return nil, fmt.Errorf("failed to get shards, err=%w", err)
	}
	if len(partitions) != len(activeShards) {
		return nil, fmt.Errorf("partition length different than active shards, expected=%d, actual=%d", len(activeShards), len(partitions))
	}
	shardCandidates := []string{}
	for i := range partitions {
		if partitions[i] == k.nodeID {
			shardCandidates = append(shardCandidates, activeShards[i])
		}
	}
	leasedShards := map[string]lock.Lock{}
	for _, shard := range shardCandidates {
		if lock, ok := k.leasedShards[shard]; ok {
			err := lock.Extend(ctx, k.leaseDuration)
			if err != nil {
				continue
			}
			leasedShards[shard] = lock
		}
		shardLockKey := k.buildShardLockKey()
		lock, err := k.lockClient.Acquire(ctx, shardLockKey, k.leaseDuration)
		if err != nil {
			continue
		}
		leasedShards[shard] = lock
	}
	for shard, lock := range k.leasedShards {
		if _, ok := leasedShards[shard]; !ok {
			lock.Release(ctx)
		}
	}
	k.leasedShards = leasedShards
	leasedShardValues := []string{}
	for shard := range leasedShards {
		leasedShardValues = append(leasedShardValues, shard)
	}

	return leasedShardValues, nil
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

func (k *kinesisShardManager) loadHashRing(ctx context.Context) (*utils.HashRing, error) {
	hashRing := utils.New(k.clockClient)
	hashRingKey := k.buildHashRingKey()
	hashRingPayload, err := k.kvstoreClient.Get(ctx, hashRingKey)
	if err != nil {
		if errors.As(err, kvstore.ErrNotFound) {
			return hashRing, nil
		}
		return nil, fmt.Errorf("failed to load value, err=%w", err)
	}
	if hashRing.UnmarshalJSON([]byte(hashRingPayload)); err != nil {
		return nil, fmt.Errorf("failed to unmarshal hash ring, err=%w", err)
	}
	return hashRing, nil
}

func (k *kinesisShardManager) saveHashRing(ctx context.Context, hashRing *utils.HashRing) error {
	hashRingKey := k.buildHashRingKey()
	hashRingPayload, err := hashRing.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal hash ring, err=%w", err)
	}
	expiresAt := k.clockClient.TimeFromNow(k.hashRingExpiry)
	if err := k.kvstoreClient.PutWithExpiry(ctx, hashRingKey, string(hashRingPayload), expiresAt); err != nil {
		return fmt.Errorf("failed to put value, err=%w", err)
	}
	return nil
}

func (k *kinesisShardManager) buildShardConfiguration(shards []*types.Shard) *shardConfiguration {
	graph := map[string][]string{}
	activeShards := []string{}
	shardMap := map[string]*types.Shard{}
	for _, shard := range shards {
		if shard.ShardId == nil || shard.SequenceNumberRange == nil {
			continue
		}
		shardID := *shard.ShardId
		shardMap[shardID] = shard
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
		shards:          shardMap,
		active:          activeShards,
	}
}

func (k *kinesisShardManager) buildLeaseLockKey() string {
	return fmt.Sprintf("lock:lease:%s:%s:%s", keyPrefix, k.appID, k.streamName)
}

func (k *kinesisShardManager) buildShardLockKey() string {
	return fmt.Sprintf("lock:shard:%s:%s:%s", keyPrefix, k.appID, k.streamName)
}

func (k *kinesisShardManager) buildHashRingKey() string {
	return fmt.Sprintf("hash_ring:%s:%s:%s", keyPrefix, k.appID, k.streamName)
}
