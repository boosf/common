package kvstore

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type s3Client struct {
	s3     *s3.Client
	bucket string
}

func NewS3Client(s3c *s3.Client, bucket string) Client {
	return &s3Client{
		s3:     s3c,
		bucket: bucket,
	}
}

func (s *s3Client) Get(ctx context.Context, key string) (string, error) {
	out, err := s.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get object, err=%w", err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read body, err=%w", err)
	}
	return string(b), nil
}

func (s *s3Client) Put(ctx context.Context, key string, value string) error {
	b := []byte(value)
	_, err := s.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(b),
	})
	if err != nil {
		return fmt.Errorf("failed to put object, err=%w", err)
	}
	return nil
}
