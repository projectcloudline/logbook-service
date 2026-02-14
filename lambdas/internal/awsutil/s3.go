package awsutil

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Client defines S3 operations used by Lambda handlers.
type S3Client interface {
	PresignPutObject(ctx context.Context, bucket, key, contentType string, expires time.Duration) (string, error)
	PresignGetObject(ctx context.Context, bucket, key string, expires time.Duration) (string, error)
	GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, error)
	PutObject(ctx context.Context, bucket, key, contentType string, body io.Reader) error
}

type s3Client struct {
	client  *s3.Client
	presign *s3.PresignClient
}

// NewS3Client creates an S3Client from an S3 service client.
func NewS3Client(client *s3.Client) S3Client {
	return &s3Client{
		client:  client,
		presign: s3.NewPresignClient(client),
	}
}

func (c *s3Client) PresignPutObject(ctx context.Context, bucket, key, contentType string, expires time.Duration) (string, error) {
	input := &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
	}
	resp, err := c.presign.PresignPutObject(ctx, input, func(opts *s3.PresignOptions) {
		opts.Expires = expires
	})
	if err != nil {
		return "", fmt.Errorf("presign put %s: %w", key, err)
	}
	return resp.URL, nil
}

func (c *s3Client) PresignGetObject(ctx context.Context, bucket, key string, expires time.Duration) (string, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	resp, err := c.presign.PresignGetObject(ctx, input, func(opts *s3.PresignOptions) {
		opts.Expires = expires
	})
	if err != nil {
		return "", fmt.Errorf("presign get %s: %w", key, err)
	}
	return resp.URL, nil
}

func (c *s3Client) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	resp, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get object %s: %w", key, err)
	}
	return resp.Body, nil
}

func (c *s3Client) PutObject(ctx context.Context, bucket, key, contentType string, body io.Reader) error {
	_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
		Body:        body,
	})
	if err != nil {
		return fmt.Errorf("put object %s: %w", key, err)
	}
	return nil
}
