package awsutil

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// SQSClient defines SQS operations used by Lambda handlers.
type SQSClient interface {
	SendMessage(ctx context.Context, queueURL, body string) error
}

// SQSAPI is the subset of the SQS client we use.
type SQSAPI interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

type sqsClient struct {
	client SQSAPI
}

// NewSQSClient creates an SQSClient from an SQS service client.
func NewSQSClient(client SQSAPI) SQSClient {
	return &sqsClient{client: client}
}

func (c *sqsClient) SendMessage(ctx context.Context, queueURL, body string) error {
	_, err := c.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(body),
	})
	if err != nil {
		return fmt.Errorf("send sqs message: %w", err)
	}
	return nil
}
