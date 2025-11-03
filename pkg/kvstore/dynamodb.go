package kvstore

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func NewDynamoClient(db *dynamodb.Client, tableName string, partitionKey string, valueKey string, ttlKey string) Client {
	return &dynamoClient{
		db:           db,
		tableName:    tableName,
		partitionKey: partitionKey,
		valueKey:     valueKey,
		ttlKey:       ttlKey,
	}
}

type dynamoClient struct {
	db           *dynamodb.Client
	tableName    string
	partitionKey string
	valueKey     string
	ttlKey       string
}

func (d *dynamoClient) Get(ctx context.Context, key string) (string, error) {
	out, err := d.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]types.AttributeValue{
			d.partitionKey: &types.AttributeValueMemberS{Value: key},
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to get item, err=%w", err)
	}
	if out.Item == nil {
		return "", ErrNotFound
	}
	valAttr, ok := out.Item[d.valueKey].(*types.AttributeValueMemberS)
	if !ok {
		return "", fmt.Errorf("invalid value type")
	}
	return valAttr.Value, nil
}

func (d *dynamoClient) Put(ctx context.Context, key string, value string) error {
	_, err := d.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(d.tableName),
		Item: map[string]types.AttributeValue{
			d.partitionKey: &types.AttributeValueMemberS{Value: key},
			d.valueKey:     &types.AttributeValueMemberS{Value: value},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to put item, err=%w", err)
	}
	return nil
}

func (d *dynamoClient) PutWithExpiry(ctx context.Context, key string, value string, expiresAt time.Time) error {
	ttl := expiresAt.Unix()
	_, err := d.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(d.tableName),
		Item: map[string]types.AttributeValue{
			d.partitionKey: &types.AttributeValueMemberS{Value: key},
			d.valueKey:     &types.AttributeValueMemberS{Value: value},
			d.ttlKey:       &types.AttributeValueMemberN{Value: fmt.Sprint(ttl)},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to put item with expiry, err=%w", err)
	}
	return nil
}
