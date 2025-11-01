package lock

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/boosf/common/internal/option"
	"github.com/boosf/common/pkg/clock"
	"github.com/google/uuid"
)

func NewDynamoClient(
	dynamodbClient *dynamodb.Client,
	clockClient clock.Client,
	table string,
	partitionKeyName string,
	idKeyName string,
	ttlKeyName string,
) Client {
	return &dynamoClient{
		dynamodbClient:   dynamodbClient,
		clockClient:      clockClient,
		table:            table,
		partitionKeyName: partitionKeyName,
		idKeyName:        idKeyName,
		ttlKeyName:       ttlKeyName,
	}
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
		return fmt.Errorf("failed to update key, lock key=%s, lock id=%s, err=%w", d.key, d.id, err)
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
		return fmt.Errorf("failed to delete key, lock key=%s, lock id=%s, err=%w", d.key, d.id, err)
	}
	return nil
}

type dynamoClient struct {
	dynamodbClient   *dynamodb.Client
	clockClient      clock.Client
	table            string
	partitionKeyName string
	idKeyName        string
	ttlKeyName       string
}

func (d *dynamoClient) Acquire(ctx context.Context, key string, duration time.Duration, options ...option.Option[*lockOption]) (Lock, error) {
	ttl := d.expiresAt(duration)
	id := uuid.New().String()
	_, err := d.dynamodbClient.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(d.table),
		Item: map[string]types.AttributeValue{
			d.partitionKeyName: &types.AttributeValueMemberS{Value: key},
			d.ttlKeyName:       &types.AttributeValueMemberN{Value: ttl},
			d.idKeyName:        &types.AttributeValueMemberS{Value: id},
		},
		ConditionExpression:      aws.String("attribute_not_exists(#pk)"),
		ExpressionAttributeNames: map[string]string{"#pk": d.partitionKeyName},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to put item, lock key=%s, err=%w", key, err)
	}
	return &dynamoLock{dynamoClient: d, key: key, id: id}, nil
}

func (d *dynamoClient) expiresAt(duration time.Duration) string {
	ttl := d.clockClient.Now() + int64(duration.Seconds())
	return fmt.Sprint(ttl)
}
