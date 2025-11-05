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

type shardMetadata struct {
	id          string
	dependent   []*shardMetadata
	startOffset string
	endOffset   *string
}

func (k *kinesisShardManager) AcquireShards(ctx context.Context) ([]*shardMetadata, error) {
	shards, err := k.getShards(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get shards, err=%w", err)
	}
	activeShards := k.buildActiveShards(shards)
	leasedShards, err := k.leaseShards(ctx, activeShards)
	if err != nil {
		return nil, fmt.Errorf("failed to lease shards, err=%w", err)
	}
	return leasedShards, nil
}

func (k *kinesisShardManager) leaseShards(ctx context.Context, shards []*shardMetadata) ([]*shardMetadata, error) {
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
	shardIDs := k.convertShardToShardID(shards)
	leasedShardIDs, err := k.updateLease(ctx, shardIDs, hashRing)
	if err != nil {
		return nil, fmt.Errorf("failed to update lease, err=%w", err)
	}
	if err := k.saveHashRing(ctx, hashRing); err != nil {
		return nil, fmt.Errorf("failed to save hash ring, err=%w", err)
	}
	return k.convertShardIDToShard(leasedShardIDs, shards), nil
}

func (k *kinesisShardManager) updateLease(ctx context.Context, shardIDs []string, hashRing *utils.HashRing) ([]string, error) {
	partitions, err := hashRing.Get(shardIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to get shards, err=%w", err)
	}
	leaseCandidates := []string{}
	for i := range partitions {
		if partitions[i] == k.nodeID {
			leaseCandidates = append(leaseCandidates, shardIDs[i])
		}
	}
	leasedShards := map[string]lock.Lock{}
	for _, shard := range leaseCandidates {
		if lock, ok := k.leasedShards[shard]; ok {
			err := lock.Extend(ctx, k.leaseDuration)
			if err != nil {
				continue
			}
			leasedShards[shard] = lock
		}
		shardLockKey := k.buildShardLockKey(shard)
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
	leasedShardKeys := []string{}
	for shard := range leasedShards {
		leasedShardKeys = append(leasedShardKeys, shard)
	}
	return leasedShardKeys, nil
}

func (k *kinesisShardManager) convertShardToShardID(shards []*shardMetadata) []string {
	out := []string{}
	for _, shard := range shards {
		out = append(out, shard.id)
	}
	return out
}

func (k *kinesisShardManager) convertShardIDToShard(shardIDs []string, shards []*shardMetadata) []*shardMetadata {
	shardMap := map[string]*shardMetadata{}
	for _, shard := range shards {
		shardMap[shard.id] = shard
	}
	out := []*shardMetadata{}
	for _, shardID := range shardIDs {
		if shardMetadata, ok := shardMap[shardID]; ok {
			out = append(out, shardMetadata)
		}
	}
	return out
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

func (k *kinesisShardManager) buildActiveShards(shards []*types.Shard) []*shardMetadata {
	activeShards := []*shardMetadata{}
	shardMap := k.buildShardMap(shards)
	shardMetadataMap := map[string]*shardMetadata{}
	for _, shard := range shards {
		shardMetadata := k.buildShardMetadata(shard, shardMap, shardMetadataMap)
		if shardMetadata.endOffset == nil {
			activeShards = append(activeShards, shardMetadata)
		}
	}
	return activeShards
}

func (k *kinesisShardManager) buildShardMap(shards []*types.Shard) map[string]*types.Shard {
	out := map[string]*types.Shard{}
	for _, shard := range shards {
		if shard.ShardId == nil {
			continue
		}
		out[*shard.ShardId] = shard
	}
	return out
}

func (k *kinesisShardManager) buildShardMetadata(shard *types.Shard, shardMap map[string]*types.Shard, shardMetadataMap map[string]*shardMetadata) *shardMetadata {
	if shard.ShardId == nil || shard.SequenceNumberRange == nil || shard.SequenceNumberRange.StartingSequenceNumber == nil {
		return nil
	}
	if shardMetadata, ok := shardMetadataMap[*shard.ShardId]; ok {
		return shardMetadata
	}
	out := &shardMetadata{
		id:          *shard.ShardId,
		dependent:   []*shardMetadata{},
		startOffset: *shard.SequenceNumberRange.StartingSequenceNumber,
		endOffset:   shard.SequenceNumberRange.EndingSequenceNumber,
	}
	if shard.ParentShardId != nil {
		parent, ok := shardMap[*shard.ParentShardId]
		if ok {
			out.dependent = append(out.dependent, k.buildShardMetadata(parent, shardMap, shardMetadataMap))
		}
	}
	if shard.AdjacentParentShardId != nil {
		adjacent, ok := shardMap[*shard.AdjacentParentShardId]
		if ok {
			out.dependent = append(out.dependent, k.buildShardMetadata(adjacent, shardMap, shardMetadataMap))
		}
	}
	shardMetadataMap[*shard.ShardId] = out
	return out
}

func (k *kinesisShardManager) loadHashRing(ctx context.Context) (*utils.HashRing, error) {
	hashRing := utils.New(k.clockClient)
	hashRingKey := k.buildHashRingKey()
	hashRingPayload, err := k.kvstoreClient.Get(ctx, hashRingKey)
	if err != nil {
		if errors.Is(err, kvstore.ErrNotFound) {
			return hashRing, nil
		}
		return nil, fmt.Errorf("failed to load value, err=%w", err)
	}
	if err := hashRing.UnmarshalJSON([]byte(hashRingPayload)); err != nil {
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

func (k *kinesisShardManager) buildLeaseLockKey() string {
	return fmt.Sprintf("lock:lease:%s:%s:%s", keyPrefix, k.appID, k.streamName)
}

func (k *kinesisShardManager) buildShardLockKey(shardID string) string {
	return fmt.Sprintf("lock:shard:%s:%s:%s:%s", keyPrefix, k.appID, k.streamName, shardID)
}

func (k *kinesisShardManager) buildHashRingKey() string {
	return fmt.Sprintf("hash_ring:%s:%s:%s", keyPrefix, k.appID, k.streamName)
}
