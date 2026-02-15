package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"

	"github.com/projectcloudline/logbook-service/internal/awsutil"
	"github.com/projectcloudline/logbook-service/internal/db"
)

func main() {
	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("load AWS config: %v", err)
	}

	smClient := secretsmanager.NewFromConfig(cfg)
	secrets := awsutil.NewSecretsProvider(smClient)
	s3Client := awsutil.NewS3Client(s3.NewFromConfig(cfg))

	database := db.New(func(ctx context.Context) (map[string]string, error) {
		if host := os.Getenv("DB_HOST"); host != "" {
			return map[string]string{
				"host":     host,
				"port":     envOrDefault("DB_PORT", "5432"),
				"dbname":   envOrDefault("DB_NAME", "postgres"),
				"username": envOrDefault("DB_USER", "postgres"),
				"password": envOrDefault("DB_PASSWORD", "postgres"),
			}, nil
		}
		arn := os.Getenv("DB_SECRET_ARN")
		raw, err := secrets.GetSecret(ctx, arn)
		if err != nil {
			return nil, fmt.Errorf("get db secret: %w", err)
		}
		var creds map[string]string
		if err := json.Unmarshal([]byte(raw), &creds); err != nil {
			return nil, fmt.Errorf("parse db secret: %w", err)
		}
		return creds, nil
	})

	h := &Handler{
		db:      database,
		s3:      s3Client,
		secrets: secrets,
		bucket:  os.Getenv("BUCKET_NAME"),
	}

	lambda.Start(h.Handle)
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
