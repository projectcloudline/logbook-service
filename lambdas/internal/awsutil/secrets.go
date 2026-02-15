// Package awsutil provides AWS client interfaces and helpers for Lambda functions.
package awsutil

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

// SecretsProvider retrieves and caches secrets from AWS Secrets Manager.
type SecretsProvider interface {
	GetSecret(ctx context.Context, secretARN string) (string, error)
	GetSecretJSON(ctx context.Context, secretARN string) (map[string]string, error)
}

// SecretsManagerAPI is the subset of the Secrets Manager client we use.
type SecretsManagerAPI interface {
	GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
}

type secretsProvider struct {
	client SecretsManagerAPI
	cache  map[string]string
	mu     sync.Mutex
}

// NewSecretsProvider creates a SecretsProvider backed by Secrets Manager.
func NewSecretsProvider(client SecretsManagerAPI) SecretsProvider {
	return &secretsProvider{
		client: client,
		cache:  make(map[string]string),
	}
}

func (s *secretsProvider) GetSecret(ctx context.Context, secretARN string) (string, error) {
	s.mu.Lock()
	if v, ok := s.cache[secretARN]; ok {
		s.mu.Unlock()
		return v, nil
	}
	s.mu.Unlock()

	out, err := s.client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretARN),
	})
	if err != nil {
		return "", fmt.Errorf("get secret %s: %w", secretARN, err)
	}

	val := aws.ToString(out.SecretString)

	s.mu.Lock()
	s.cache[secretARN] = val
	s.mu.Unlock()

	return val, nil
}

func (s *secretsProvider) GetSecretJSON(ctx context.Context, secretARN string) (map[string]string, error) {
	raw, err := s.GetSecret(ctx, secretARN)
	if err != nil {
		return nil, err
	}

	var result map[string]string
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return nil, fmt.Errorf("parse secret JSON: %w", err)
	}
	return result, nil
}
