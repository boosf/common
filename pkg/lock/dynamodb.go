package lock

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/boosf/common/pkg/clock"
)

const (
	defaultSequenceID = "0"
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
	id, err := d.getSequenceID(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get sequence id, err=%w", err)
	}
	now := d.clockClient.Now()
	expiresAt := now + int64(duration.Seconds())
	ttl := now + int64(d.expiry.Seconds())
	_, err = d.dynamodbClient.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(d.table),
		Item: map[string]types.AttributeValue{
			d.partitionKeyName: &types.AttributeValueMemberS{Value: key},
			d.idKeyName:        &types.AttributeValueMemberN{Value: id},
			d.expiresAtKeyName: &types.AttributeValueMemberN{Value: fmt.Sprint(expiresAt)},
			d.ttlKeyName:       &types.AttributeValueMemberN{Value: fmt.Sprint(ttl)},
		},
		ConditionExpression: aws.String("attribute_not_exists(#pk) or (#id = :id - 1 and #expires < :now)"),
		ExpressionAttributeNames: map[string]string{
			"#pk":      d.partitionKeyName,
			"#id":      d.idKeyName,
			"#expires": d.expiresAtKeyName,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":id":  &types.AttributeValueMemberN{Value: id},
			":now": &types.AttributeValueMemberN{Value: fmt.Sprint(now)},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to put item, err=%w", err)
	}
	return &dynamoLock{dynamoClient: d, key: key, id: id}, nil
}

func (d *dynamoClient) getSequenceID(ctx context.Context, key string) (string, error) {
	item, err := d.dynamodbClient.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(d.table),
		Key: map[string]types.AttributeValue{
			d.partitionKeyName: &types.AttributeValueMemberS{Value: key},
		},
		ProjectionExpression: aws.String("#id"),
		ExpressionAttributeNames: map[string]string{
			"#id": d.idKeyName,
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get item, err=%w", err)
	}
	if item.Item == nil {
		return defaultSequenceID, nil
	}
	attributes, ok := item.Item[d.idKeyName]
	if !ok {
		return "", errors.New("id is missing from item")
	}
	currIDStr, ok := attributes.(*types.AttributeValueMemberN)
	if !ok {
		return "", errors.New("id is not type number")
	}
	currID, err := strconv.ParseInt(currIDStr.Value, 10, 64)
	if err != nil {
		return "", fmt.Errorf("failed to parse id, err=%w", err)
	}
	return fmt.Sprint(currID + 1), nil
}

type dynamoLock struct {
	dynamoClient *dynamoClient
	key          string
	id           string
}

func (d *dynamoLock) Extend(ctx context.Context, duration time.Duration) error {
	dynamoClient := d.dynamoClient
	ttl := dynamoClient.expiresAt(duration)
	_, err := dynamoClient.dynamodbClient.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(dynamoClient.table),
		Key: map[string]types.AttributeValue{
			dynamoClient.partitionKeyName: &types.AttributeValueMemberS{Value: d.key},
		},
		UpdateExpression:          aws.String("SET #ttl = :ttl"),
		ConditionExpression:       aws.String("#id = :id"),
		ExpressionAttributeNames:  map[string]string{"#ttl": dynamoClient.ttlKeyName, "#id": dynamoClient.idKeyName},
		ExpressionAttributeValues: map[string]types.AttributeValue{":ttl": &types.AttributeValueMemberN{Value: ttl}, ":id": &types.AttributeValueMemberS{Value: d.id}},
	})
	if err != nil {
		return fmt.Errorf("failed to update key, key=%s, err=%w", d.key, err)
	}
	return nil
}

func (d *dynamoLock) Release(ctx context.Context) error {
	dynamoClient := d.dynamoClient
	_, err := dynamoClient.dynamodbClient.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(dynamoClient.table),
		Key: map[string]types.AttributeValue{
			dynamoClient.partitionKeyName: &types.AttributeValueMemberS{Value: d.key},
		},
		ConditionExpression:       aws.String("#id = :id"),
		ExpressionAttributeNames:  map[string]string{"#id": dynamoClient.idKeyName},
		ExpressionAttributeValues: map[string]types.AttributeValue{":id": &types.AttributeValueMemberS{Value: d.id}},
	})
	if err != nil {
		return fmt.Errorf("failed to delete key, key=%s, err=%w", d.key, err)
	}
	return nil
}
