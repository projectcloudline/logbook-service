package awsutil

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

type mockSMClient struct {
	secrets   map[string]string
	callCount atomic.Int32
}

func (m *mockSMClient) GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	m.callCount.Add(1)
	id := aws.ToString(params.SecretId)
	val, ok := m.secrets[id]
	if !ok {
		return nil, fmt.Errorf("secret %s not found", id)
	}
	return &secretsmanager.GetSecretValueOutput{
		SecretString: aws.String(val),
	}, nil
}

func TestSecretsProvider_GetSecret(t *testing.T) {
	mock := &mockSMClient{
		secrets: map[string]string{
			"arn:db-creds": `{"host":"localhost","port":"5432"}`,
		},
	}
	provider := NewSecretsProvider(mock)

	val, err := provider.GetSecret(context.Background(), "arn:db-creds")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != `{"host":"localhost","port":"5432"}` {
		t.Errorf("unexpected value: %s", val)
	}
}

func TestSecretsProvider_Caching(t *testing.T) {
	mock := &mockSMClient{
		secrets: map[string]string{
			"arn:test": "secret-value",
		},
	}
	provider := NewSecretsProvider(mock)

	// First call
	_, _ = provider.GetSecret(context.Background(), "arn:test")
	// Second call — should hit cache
	_, _ = provider.GetSecret(context.Background(), "arn:test")

	if mock.callCount.Load() != 1 {
		t.Errorf("expected 1 API call, got %d", mock.callCount.Load())
	}
}

func TestSecretsProvider_NotFound(t *testing.T) {
	mock := &mockSMClient{
		secrets: map[string]string{},
	}
	provider := NewSecretsProvider(mock)

	_, err := provider.GetSecret(context.Background(), "arn:missing")
	if err == nil {
		t.Fatal("expected error for missing secret")
	}
}

func TestSecretsProvider_GetSecretJSON(t *testing.T) {
	mock := &mockSMClient{
		secrets: map[string]string{
			"arn:app": `{"GEMINI_API_KEY":"test-key-123","OTHER":"val"}`,
		},
	}
	provider := NewSecretsProvider(mock)

	result, err := provider.GetSecretJSON(context.Background(), "arn:app")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["GEMINI_API_KEY"] != "test-key-123" {
		t.Errorf("unexpected GEMINI_API_KEY: %s", result["GEMINI_API_KEY"])
	}
	if result["OTHER"] != "val" {
		t.Errorf("unexpected OTHER: %s", result["OTHER"])
	}
}

func TestSecretsProvider_GetSecretJSON_InvalidJSON(t *testing.T) {
	mock := &mockSMClient{
		secrets: map[string]string{
			"arn:bad": "not-json",
		},
	}
	provider := NewSecretsProvider(mock)

	_, err := provider.GetSecretJSON(context.Background(), "arn:bad")
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestSecretsProvider_MultipleDifferentSecrets(t *testing.T) {
	mock := &mockSMClient{
		secrets: map[string]string{
			"arn:secret1": "value1",
			"arn:secret2": "value2",
			"arn:secret3": "value3",
		},
	}
	provider := NewSecretsProvider(mock)

	// Get each secret once
	val1, err := provider.GetSecret(context.Background(), "arn:secret1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val1 != "value1" {
		t.Errorf("val1 = %q, want %q", val1, "value1")
	}

	val2, err := provider.GetSecret(context.Background(), "arn:secret2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val2 != "value2" {
		t.Errorf("val2 = %q, want %q", val2, "value2")
	}

	val3, err := provider.GetSecret(context.Background(), "arn:secret3")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val3 != "value3" {
		t.Errorf("val3 = %q, want %q", val3, "value3")
	}

	// Get secret1 again - should hit cache
	val1Again, _ := provider.GetSecret(context.Background(), "arn:secret1")
	if val1Again != "value1" {
		t.Errorf("val1Again = %q, want %q", val1Again, "value1")
	}

	// Should have made 3 API calls (one per unique secret)
	if mock.callCount.Load() != 3 {
		t.Errorf("expected 3 API calls, got %d", mock.callCount.Load())
	}
}
