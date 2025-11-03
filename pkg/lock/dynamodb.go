package lock

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/boosf/common/pkg/clock"
)

func NewDynamoClient(
	dynamodbClient *dynamodb.Client,
	clockClient clock.Client,
	expiry time.Duration,
	table string,
	partitionKeyName string,
	idKeyName string,
	expiresAtKeyName string,
	ttlKeyName string,
) Client {
	return &dynamoClient{
		dynamodbClient:   dynamodbClient,
		clockClient:      clockClient,
		expiry:           expiry,
		table:            table,
		partitionKeyName: partitionKeyName,
		idKeyName:        idKeyName,
		expiresAtKeyName: expiresAtKeyName,
		ttlKeyName:       ttlKeyName,
	}
}

type dynamoClient struct {
	dynamodbClient   *dynamodb.Client
	clockClient      clock.Client
	expiry           time.Duration
	table            string
	partitionKeyName string
	idKeyName        string
	expiresAtKeyName string
	ttlKeyName       string
}

type lockExpiry struct {
	now       string
	expiresAt string
	ttl       string
}

func (d *dynamoClient) Acquire(ctx context.Context, key string, duration time.Duration, options ...lockOption) (Lock, error) {
	config := newLockConfig()
	for _, option := range options {
		config = option.Apply(config)
	}
	errs := []error{}
	for attempt := range config.Retries {
		lock, err := d.acquire(ctx, key, duration)
		if err != nil {
			errs = append(errs, err)
			config.RetryStrategy.WaitFor(attempt)
			continue
		}
		return lock, nil
	}
	return nil, fmt.Errorf("failed to acquire lock, key=%s, err=%w", key, errors.Join(errs...))
}

func (d *dynamoClient) acquire(ctx context.Context, key string, duration time.Duration) (Lock, error) {
	input := d.buildAcquireInput(key, duration)
	out, err := d.dynamodbClient.UpdateItem(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to update item, err=%w", err)
	}
	idAttr, ok := out.Attributes[d.idKeyName].(*types.AttributeValueMemberN)
	if !ok {
		return nil, errors.New("missing or invalid id")
	}
	return &dynamoLock{dynamoClient: d, key: key, id: idAttr.Value}, nil
}

func (d *dynamoClient) buildAcquireInput(key string, duration time.Duration) *dynamodb.UpdateItemInput {
	lockExpiry := d.getLockExpiry(duration)
	return &dynamodb.UpdateItemInput{
		TableName: aws.String(d.table),
		Key: map[string]types.AttributeValue{
			d.partitionKeyName: &types.AttributeValueMemberS{Value: key},
		},
		ConditionExpression: aws.String("attribute_not_exists(#pk) OR #expires < :now"),
		UpdateExpression:    aws.String("SET #id = if_not_exists(#id, :zero) + :one, #expires = :expires, #ttl = :ttl"),
		ExpressionAttributeNames: map[string]string{
			"#pk":      d.partitionKeyName,
			"#id":      d.idKeyName,
			"#expires": d.expiresAtKeyName,
			"#ttl":     d.ttlKeyName,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":zero":    &types.AttributeValueMemberN{Value: "0"},
			":one":     &types.AttributeValueMemberN{Value: "1"},
			":expires": &types.AttributeValueMemberN{Value: lockExpiry.expiresAt},
			":ttl":     &types.AttributeValueMemberN{Value: lockExpiry.ttl},
			":now":     &types.AttributeValueMemberN{Value: lockExpiry.now},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	}
}

func (d *dynamoClient) getLockExpiry(duration time.Duration) *lockExpiry {
	now := d.clockClient.UnixNow()
	expiresAt := now + int64(duration.Seconds())
	ttl := now + int64(d.expiry.Seconds())
	return &lockExpiry{
		now:       fmt.Sprint(now),
		expiresAt: fmt.Sprint(expiresAt),
		ttl:       fmt.Sprint(ttl),
	}
}

type dynamoLock struct {
	dynamoClient *dynamoClient
	key          string
	id           string
}

func (d *dynamoLock) Extend(ctx context.Context, duration time.Duration) error {
	dynamoClient := d.dynamoClient
	input := d.buildExtendInput(duration)
	_, err := dynamoClient.dynamodbClient.UpdateItem(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to update key, key=%s, err=%w", d.key, err)
	}
	return nil
}

func (d *dynamoLock) buildExtendInput(duration time.Duration) *dynamodb.UpdateItemInput {
	dynamoClient := d.dynamoClient
	lockExpiry := dynamoClient.getLockExpiry(duration)
	return &dynamodb.UpdateItemInput{
		TableName: aws.String(dynamoClient.table),
		Key: map[string]types.AttributeValue{
			dynamoClient.partitionKeyName: &types.AttributeValueMemberS{Value: d.key},
		},
		ConditionExpression: aws.String("#id = :id"),
		UpdateExpression:    aws.String("SET #ttl = :ttl, #expiry = :expiry"),
		ExpressionAttributeNames: map[string]string{
			"#id":     dynamoClient.idKeyName,
			"#ttl":    dynamoClient.ttlKeyName,
			"#expiry": dynamoClient.expiresAtKeyName,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":id":     &types.AttributeValueMemberN{Value: d.id},
			":ttl":    &types.AttributeValueMemberN{Value: lockExpiry.ttl},
			":expiry": &types.AttributeValueMemberS{Value: lockExpiry.expiresAt},
		},
	}
}

func (d *dynamoLock) Release(ctx context.Context) error {
	dynamoClient := d.dynamoClient
	input := d.buildReleaseInput()
	_, err := dynamoClient.dynamodbClient.UpdateItem(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to delete key, key=%s, err=%w", d.key, err)
	}
	return nil
}

func (d *dynamoLock) buildReleaseInput() *dynamodb.UpdateItemInput {
	dynamoClient := d.dynamoClient
	return &dynamodb.UpdateItemInput{
		TableName: aws.String(dynamoClient.table),
		Key: map[string]types.AttributeValue{
			dynamoClient.partitionKeyName: &types.AttributeValueMemberS{Value: d.key},
		},
		ConditionExpression: aws.String("#id = :id"),
		UpdateExpression:    aws.String("SET #expiry = :expiry"),
		ExpressionAttributeNames: map[string]string{
			"#id":     dynamoClient.idKeyName,
			"#expiry": dynamoClient.expiresAtKeyName,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":id":     &types.AttributeValueMemberN{Value: d.id},
			":expiry": &types.AttributeValueMemberS{Value: "0"},
		},
	}
}
