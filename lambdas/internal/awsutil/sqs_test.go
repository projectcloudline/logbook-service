package awsutil

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type mockSQSAPI struct {
	messages []string
}

func (m *mockSQSAPI) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	m.messages = append(m.messages, aws.ToString(params.MessageBody))
	return &sqs.SendMessageOutput{}, nil
}

func TestSQSClient_SendMessage(t *testing.T) {
	mock := &mockSQSAPI{}
	client := NewSQSClient(mock)

	err := client.SendMessage(context.Background(), "https://sqs.example.com/queue", "test message body")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mock.messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(mock.messages))
	}

	if mock.messages[0] != "test message body" {
		t.Errorf("message body = %q, want %q", mock.messages[0], "test message body")
	}
}

func TestSQSClient_SendMessage_Multiple(t *testing.T) {
	mock := &mockSQSAPI{}
	client := NewSQSClient(mock)

	messages := []string{"message 1", "message 2", "message 3"}
	for _, msg := range messages {
		err := client.SendMessage(context.Background(), "https://sqs.example.com/queue", msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if len(mock.messages) != len(messages) {
		t.Fatalf("expected %d messages, got %d", len(messages), len(mock.messages))
	}

	for i, msg := range messages {
		if mock.messages[i] != msg {
			t.Errorf("message[%d] = %q, want %q", i, mock.messages[i], msg)
		}
	}
}
