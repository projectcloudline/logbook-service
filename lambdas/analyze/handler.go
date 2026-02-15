package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/aws/aws-lambda-go/events"

	"github.com/projectcloudline/logbook-service/internal/awsutil"
	"github.com/projectcloudline/logbook-service/internal/db"
	"github.com/projectcloudline/logbook-service/internal/gemini"
)

// Handler holds dependencies for the Analyze Lambda.
type Handler struct {
	db      db.DB
	s3      awsutil.S3Client
	secrets awsutil.SecretsProvider
	gemini  gemini.Client
	bucket  string
}

// Handle processes SQS messages — one page per message.
func (h *Handler) Handle(ctx context.Context, event events.SQSEvent) error {
	for _, record := range event.Records {
		var msg pageMessage
		if err := json.Unmarshal([]byte(record.Body), &msg); err != nil {
			return fmt.Errorf("parse message: %w", err)
		}

		log.Printf("Analyzing page %d of upload %s: %s", msg.PageNumber, msg.UploadID, msg.S3Key)

		if err := h.processPage(ctx, msg); err != nil {
			log.Printf("ERROR processing page %s: %v", msg.PageID, err)
			h.markPageFailed(ctx, msg.PageID)
			return err
		}
	}
	return nil
}

type pageMessage struct {
	UploadID   string `json:"uploadId"`
	PageID     string `json:"pageId"`
	PageNumber int    `json:"pageNumber"`
	S3Key      string `json:"s3Key"`
}
