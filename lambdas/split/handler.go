package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/events"

	"github.com/projectcloudline/logbook-service/internal/awsutil"
	"github.com/projectcloudline/logbook-service/internal/db"
)

var imageExtensions = map[string]bool{
	".jpg": true, ".jpeg": true, ".png": true, ".gif": true,
	".bmp": true, ".tiff": true, ".tif": true, ".heic": true, ".heif": true,
}

// Handler holds dependencies for the Split Lambda.
type Handler struct {
	db       db.DB
	s3       awsutil.S3Client
	sqs      awsutil.SQSClient
	bucket   string
	queueURL string
	// mutoolPath overrides the default mutool binary path (for testing)
	mutoolPath string
}

// Handle processes S3 PUT events for uploaded logbook files.
func (h *Handler) Handle(ctx context.Context, event events.S3Event) error {
	for _, record := range event.Records {
		s3Key, _ := url.QueryUnescape(record.S3.Object.Key)
		bucket := record.S3.Bucket.Name

		log.Printf("Processing upload: s3://%s/%s", bucket, s3Key)

		parts := strings.Split(s3Key, "/")
		if len(parts) < 3 {
			log.Printf("Ignoring key %s — unexpected format", s3Key)
			continue
		}

		switch parts[0] {
		case "pages":
			if err := h.handlePageArrival(ctx, parts[1], s3Key); err != nil {
				return err
			}
		case "uploads":
			filename := strings.Join(parts[2:], "/")
			if err := h.handlePDFUpload(ctx, parts[1], filename, s3Key, bucket); err != nil {
				return err
			}
		default:
			log.Printf("Ignoring key %s — not in uploads/ or pages/ prefix", s3Key)
		}
	}
	return nil
}

func (h *Handler) handlePageArrival(ctx context.Context, batchID, s3Key string) error {
	// Parse page number from key: pages/{batchId}/page_XXXX.jpg
	filename := filepath.Base(s3Key)
	parts := strings.SplitN(filename, "_", 2)
	if len(parts) < 2 {
		log.Printf("Could not parse page number from %s", s3Key)
		return nil
	}
	numStr := strings.TrimSuffix(parts[1], filepath.Ext(parts[1]))
	pageNumber, err := strconv.Atoi(numStr)
	if err != nil {
		log.Printf("Could not parse page number from %s", s3Key)
		return nil
	}

	// Look up existing page record
	rows, err := h.db.Query(ctx,
		"SELECT id FROM upload_pages WHERE document_id = $1 AND page_number = $2",
		batchID, pageNumber)
	if err != nil {
		return fmt.Errorf("query page: %w", err)
	}
	if len(rows) == 0 {
		log.Printf("No page record found for batch %s page %d, skipping", batchID, pageNumber)
		return nil
	}

	pageID := fmt.Sprintf("%v", rows[0]["id"])

	// Set batch to processing
	_ = h.db.Exec(ctx,
		"UPDATE upload_batches SET processing_status = 'processing', updated_at = NOW() WHERE id = $1 AND processing_status = 'pending'",
		batchID)

	// Queue for analysis
	return h.sendAnalyzeMessage(ctx, batchID, pageID, pageNumber, s3Key)
}

func (h *Handler) handlePDFUpload(ctx context.Context, batchID, filename, s3Key, bucket string) error {
	ext := strings.ToLower(filepath.Ext(filename))

	// Mark as processing
	if err := h.db.Exec(ctx,
		"UPDATE upload_batches SET processing_status = 'processing', updated_at = NOW() WHERE id = $1",
		batchID); err != nil {
		return fmt.Errorf("update status: %w", err)
	}

	tmpdir, err := os.MkdirTemp("", "logbook-split-*")
	if err != nil {
		return fmt.Errorf("create tmpdir: %w", err)
	}
	defer os.RemoveAll(tmpdir)

	localFile := filepath.Join(tmpdir, filepath.Base(filename))

	// Download file from S3
	reader, err := h.s3.GetObject(ctx, bucket, s3Key)
	if err != nil {
		h.markFailed(ctx, batchID)
		return fmt.Errorf("download file: %w", err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		h.markFailed(ctx, batchID)
		return fmt.Errorf("read file: %w", err)
	}
	if err := os.WriteFile(localFile, data, 0644); err != nil {
		h.markFailed(ctx, batchID)
		return fmt.Errorf("write file: %w", err)
	}

	var pageKeys []string
	if ext == ".pdf" {
		pageKeys, err = h.splitPDF(ctx, localFile, batchID, tmpdir)
	} else if imageExtensions[ext] {
		pageKeys, err = h.handleSingleImage(ctx, localFile, batchID)
	} else {
		h.markFailed(ctx, batchID)
		return fmt.Errorf("unsupported file type: %s", ext)
	}
	if err != nil {
		h.markFailed(ctx, batchID)
		return err
	}

	// Update page count
	if err := h.db.Exec(ctx,
		"UPDATE upload_batches SET page_count = $1, updated_at = NOW() WHERE id = $2",
		len(pageKeys), batchID); err != nil {
		return fmt.Errorf("update page count: %w", err)
	}

	// Create page records and queue messages
	for i, pageKey := range pageKeys {
		pageNum := i + 1
		pageID, err := h.db.Insert(ctx,
			`INSERT INTO upload_pages (document_id, page_number, image_path, extraction_status)
			 VALUES ($1, $2, $3, 'pending') RETURNING id`,
			batchID, pageNum, pageKey)
		if err != nil {
			return fmt.Errorf("insert page: %w", err)
		}

		if err := h.sendAnalyzeMessage(ctx, batchID, pageID, pageNum, pageKey); err != nil {
			return fmt.Errorf("queue page: %w", err)
		}
	}

	log.Printf("Queued %d pages for analysis", len(pageKeys))
	return nil
}

func (h *Handler) splitPDF(ctx context.Context, pdfPath, batchID, tmpdir string) ([]string, error) {
	mutool := h.getMutoolPath()

	// mutool draw -o /tmp/pages/page-%04d.jpg -r 200 -F jpeg input.pdf
	outputPattern := filepath.Join(tmpdir, "page-%04d.jpg")
	cmd := exec.CommandContext(ctx, mutool, "draw", "-o", outputPattern, "-r", "200", "-F", "jpeg", pdfPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("mutool draw: %w", err)
	}

	// Find generated page files
	matches, err := filepath.Glob(filepath.Join(tmpdir, "page-*.jpg"))
	if err != nil {
		return nil, fmt.Errorf("glob pages: %w", err)
	}

	var pageKeys []string
	for i, match := range matches {
		pageFilename := fmt.Sprintf("page_%04d.jpg", i+1)
		s3Key := fmt.Sprintf("pages/%s/%s", batchID, pageFilename)

		fileData, err := os.ReadFile(match)
		if err != nil {
			return nil, fmt.Errorf("read page %d: %w", i+1, err)
		}

		if err := h.s3.PutObject(ctx, h.bucket, s3Key, "image/jpeg", bytes.NewReader(fileData)); err != nil {
			return nil, fmt.Errorf("upload page %d: %w", i+1, err)
		}

		pageKeys = append(pageKeys, s3Key)
		log.Printf("  Uploaded page %d/%d: %s", i+1, len(matches), s3Key)
	}

	return pageKeys, nil
}

func (h *Handler) handleSingleImage(ctx context.Context, localFile, batchID string) ([]string, error) {
	s3Key := fmt.Sprintf("pages/%s/page_0001.jpg", batchID)

	fileData, err := os.ReadFile(localFile)
	if err != nil {
		return nil, fmt.Errorf("read image: %w", err)
	}

	if err := h.s3.PutObject(ctx, h.bucket, s3Key, "image/jpeg", bytes.NewReader(fileData)); err != nil {
		return nil, fmt.Errorf("upload image: %w", err)
	}

	return []string{s3Key}, nil
}

func (h *Handler) markFailed(ctx context.Context, batchID string) {
	_ = h.db.Exec(ctx,
		"UPDATE upload_batches SET processing_status = 'failed', updated_at = NOW() WHERE id = $1",
		batchID)
}

func (h *Handler) sendAnalyzeMessage(ctx context.Context, batchID, pageID string, pageNumber int, s3Key string) error {
	msg, _ := json.Marshal(map[string]any{
		"uploadId":   batchID,
		"pageId":     pageID,
		"pageNumber": pageNumber,
		"s3Key":      s3Key,
	})
	return h.sqs.SendMessage(ctx, h.queueURL, string(msg))
}

func (h *Handler) getMutoolPath() string {
	if h.mutoolPath != "" {
		return h.mutoolPath
	}
	// Look for bundled binary relative to Lambda executable
	execDir, _ := os.Executable()
	bundled := filepath.Join(filepath.Dir(execDir), "bin", "mutool-arm64")
	if _, err := os.Stat(bundled); err == nil {
		return bundled
	}
	// Fall back to PATH
	return "mutool"
}
