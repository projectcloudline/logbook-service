package main

import (
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── Mock DB ────────────────────────────────────────────────────────────────

type mockDB struct {
	queryFn  func(ctx context.Context, sql string, args ...any) ([]map[string]any, error)
	insertFn func(ctx context.Context, sql string, args ...any) (string, error)
	execFn   func(ctx context.Context, sql string, args ...any) error
}

func (m *mockDB) Query(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
	if m.queryFn != nil {
		return m.queryFn(ctx, sql, args...)
	}
	return nil, nil
}

func (m *mockDB) Insert(ctx context.Context, sql string, args ...any) (string, error) {
	if m.insertFn != nil {
		return m.insertFn(ctx, sql, args...)
	}
	return "test-id", nil
}

func (m *mockDB) Exec(ctx context.Context, sql string, args ...any) error {
	if m.execFn != nil {
		return m.execFn(ctx, sql, args...)
	}
	return nil
}

func (m *mockDB) Pool() *pgxpool.Pool { return nil }

// ─── Mock S3 ────────────────────────────────────────────────────────────────

type mockS3 struct {
	putCalls []string
}

func (m *mockS3) PresignPutObject(ctx context.Context, bucket, key, contentType string, expires time.Duration) (string, error) {
	return "https://example.com/put", nil
}

func (m *mockS3) PresignGetObject(ctx context.Context, bucket, key string, expires time.Duration) (string, error) {
	return "https://example.com/get", nil
}

func (m *mockS3) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("fake-file-data")), nil
}

func (m *mockS3) PutObject(ctx context.Context, bucket, key, contentType string, body io.Reader) error {
	m.putCalls = append(m.putCalls, key)
	return nil
}

// ─── Mock SQS ───────────────────────────────────────────────────────────────

type mockSQS struct {
	messages []string
}

func (m *mockSQS) SendMessage(ctx context.Context, queueURL, body string) error {
	m.messages = append(m.messages, body)
	return nil
}

// ─── Tests ──────────────────────────────────────────────────────────────────

func TestHandlePageArrival(t *testing.T) {
	tests := []struct {
		name        string
		s3Key       string
		queryRows   []map[string]any
		wantMessage bool
	}{
		{
			name:        "page record found — queues message",
			s3Key:       "pages/batch-1/page_0001.jpg",
			queryRows:   []map[string]any{{"id": "page-id-1"}},
			wantMessage: true,
		},
		{
			name:        "no page record — skips",
			s3Key:       "pages/batch-1/page_0002.jpg",
			queryRows:   nil,
			wantMessage: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqs := &mockSQS{}
			db := &mockDB{
				queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
					return tt.queryRows, nil
				},
			}
			h := &Handler{
				db:       db,
				s3:       &mockS3{},
				sqs:      sqs,
				bucket:   "test-bucket",
				queueURL: "https://sqs.example.com/queue",
			}

			err := h.Handle(context.Background(), events.S3Event{
				Records: []events.S3EventRecord{{
					S3: events.S3Entity{
						Bucket: events.S3Bucket{Name: "test-bucket"},
						Object: events.S3Object{Key: tt.s3Key},
					},
				}},
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.wantMessage && len(sqs.messages) == 0 {
				t.Error("expected SQS message but none sent")
			}
			if !tt.wantMessage && len(sqs.messages) > 0 {
				t.Error("expected no SQS message but one was sent")
			}

			if tt.wantMessage {
				var msg map[string]any
				json.Unmarshal([]byte(sqs.messages[0]), &msg)
				if msg["uploadId"] != "batch-1" {
					t.Errorf("uploadId = %v, want batch-1", msg["uploadId"])
				}
			}
		})
	}
}

func TestHandleSingleImage(t *testing.T) {
	s3Mock := &mockS3{}
	sqsMock := &mockSQS{}
	db := &mockDB{
		insertFn: func(ctx context.Context, sql string, args ...any) (string, error) {
			return "page-id-1", nil
		},
	}

	h := &Handler{
		db:       db,
		s3:       s3Mock,
		sqs:      sqsMock,
		bucket:   "test-bucket",
		queueURL: "https://sqs.example.com/queue",
	}

	err := h.Handle(context.Background(), events.S3Event{
		Records: []events.S3EventRecord{{
			S3: events.S3Entity{
				Bucket: events.S3Bucket{Name: "test-bucket"},
				Object: events.S3Object{Key: "uploads/batch-1/photo.jpg"},
			},
		}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(s3Mock.putCalls) == 0 {
		t.Error("expected S3 put call for single image")
	}
	if len(sqsMock.messages) != 1 {
		t.Errorf("expected 1 SQS message, got %d", len(sqsMock.messages))
	}
}

func TestHandleIgnoresUnknownPrefix(t *testing.T) {
	h := &Handler{
		db:  &mockDB{},
		s3:  &mockS3{},
		sqs: &mockSQS{},
	}

	err := h.Handle(context.Background(), events.S3Event{
		Records: []events.S3EventRecord{{
			S3: events.S3Entity{
				Bucket: events.S3Bucket{Name: "test-bucket"},
				Object: events.S3Object{Key: "unknown/batch-1/file.pdf"},
			},
		}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHandleIgnoresShortKey(t *testing.T) {
	h := &Handler{
		db:  &mockDB{},
		s3:  &mockS3{},
		sqs: &mockSQS{},
	}

	err := h.Handle(context.Background(), events.S3Event{
		Records: []events.S3EventRecord{{
			S3: events.S3Entity{
				Bucket: events.S3Bucket{Name: "test-bucket"},
				Object: events.S3Object{Key: "short"},
			},
		}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSendAnalyzeMessage(t *testing.T) {
	sqs := &mockSQS{}
	h := &Handler{sqs: sqs, queueURL: "https://sqs.example.com/queue"}

	err := h.sendAnalyzeMessage(context.Background(), "batch-1", "page-1", 3, "pages/batch-1/page_0003.jpg")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sqs.messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(sqs.messages))
	}

	var msg map[string]any
	json.Unmarshal([]byte(sqs.messages[0]), &msg)
	if msg["uploadId"] != "batch-1" {
		t.Errorf("uploadId = %v", msg["uploadId"])
	}
	if fmt.Sprintf("%v", msg["pageNumber"]) != "3" {
		t.Errorf("pageNumber = %v", msg["pageNumber"])
	}
}

func TestMarkFailed(t *testing.T) {
	execCalled := false
	db := &mockDB{
		execFn: func(ctx context.Context, sql string, args ...any) error {
			execCalled = true
			if !strings.Contains(sql, "failed") {
				t.Error("expected 'failed' in SQL")
			}
			return nil
		},
	}
	h := &Handler{db: db}
	h.markFailed(context.Background(), "batch-1")
	if !execCalled {
		t.Error("expected exec to be called")
	}
}

// ─── Tests: HandlePDFUpload ─────────────────────────────────────────────

func TestHandlePDFUpload_UnsupportedExtension(t *testing.T) {
	db := &mockDB{
		execFn: func(ctx context.Context, sql string, args ...any) error {
			return nil
		},
	}

	h := &Handler{
		db:     db,
		s3:     &mockS3{},
		bucket: "test-bucket",
	}

	err := h.handlePDFUpload(context.Background(), "batch-1", "document.txt", "uploads/batch-1/document.txt", "test-bucket")
	if err == nil {
		t.Fatal("expected error for unsupported file type")
	}
	if !strings.Contains(err.Error(), "unsupported file type") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestHandlePageArrival_URLEncoded(t *testing.T) {
	sqs := &mockSQS{}
	db := &mockDB{
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			return []map[string]any{{"id": "page-id-1"}}, nil
		},
		execFn: func(ctx context.Context, sql string, args ...any) error {
			return nil
		},
	}

	h := &Handler{
		db:       db,
		s3:       &mockS3{},
		sqs:      sqs,
		bucket:   "test-bucket",
		queueURL: "https://sqs.example.com/queue",
	}

	// URL-encoded S3 key with spaces (must still have proper page number format)
	err := h.Handle(context.Background(), events.S3Event{
		Records: []events.S3EventRecord{{
			S3: events.S3Entity{
				Bucket: events.S3Bucket{Name: "test-bucket"},
				Object: events.S3Object{Key: "pages/batch%2D1/page_0001.jpg"},
			},
		}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sqs.messages) != 1 {
		t.Errorf("expected 1 SQS message, got %d", len(sqs.messages))
	}
}

type mockFailingS3 struct{}

func (m *mockFailingS3) PresignPutObject(ctx context.Context, bucket, key, contentType string, expires time.Duration) (string, error) {
	return "", fmt.Errorf("s3 error")
}

func (m *mockFailingS3) PresignGetObject(ctx context.Context, bucket, key string, expires time.Duration) (string, error) {
	return "", fmt.Errorf("s3 error")
}

func (m *mockFailingS3) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("s3 download failed")
}

func (m *mockFailingS3) PutObject(ctx context.Context, bucket, key, contentType string, body io.Reader) error {
	return fmt.Errorf("s3 upload failed")
}

func TestHandlePDFUpload_S3Error(t *testing.T) {
	db := &mockDB{
		execFn: func(ctx context.Context, sql string, args ...any) error {
			return nil
		},
	}

	h := &Handler{
		db:     db,
		s3:     &mockFailingS3{},
		bucket: "test-bucket",
	}

	err := h.handlePDFUpload(context.Background(), "batch-1", "document.pdf", "uploads/batch-1/document.pdf", "test-bucket")
	if err == nil {
		t.Fatal("expected error from S3 download")
	}
	if !strings.Contains(err.Error(), "download file") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestGetMutoolPath(t *testing.T) {
	h := &Handler{}

	// Default should return "mutool"
	path := h.getMutoolPath()
	if path != "mutool" {
		t.Errorf("expected 'mutool', got %q", path)
	}

	// Custom path should be used
	h.mutoolPath = "/custom/path/mutool"
	path = h.getMutoolPath()
	if path != "/custom/path/mutool" {
		t.Errorf("expected custom path, got %q", path)
	}
}

func TestHandlePageArrival_ParseErrors(t *testing.T) {
	h := &Handler{
		db:  &mockDB{},
		s3:  &mockS3{},
		sqs: &mockSQS{},
	}

	tests := []struct {
		name string
		key  string
	}{
		{"no underscore", "pages/batch-1/page0001.jpg"},
		{"invalid number", "pages/batch-1/page_abcd.jpg"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := h.Handle(context.Background(), events.S3Event{
				Records: []events.S3EventRecord{{
					S3: events.S3Entity{
						Bucket: events.S3Bucket{Name: "test-bucket"},
						Object: events.S3Object{Key: tt.key},
					},
				}},
			})
			// Should not error, just log and skip
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestHandlePDFUpload_DBError(t *testing.T) {
	db := &mockDB{
		execFn: func(ctx context.Context, sql string, args ...any) error {
			return fmt.Errorf("db connection failed")
		},
	}

	h := &Handler{
		db:     db,
		s3:     &mockS3{},
		bucket: "test-bucket",
	}

	err := h.handlePDFUpload(context.Background(), "batch-1", "doc.pdf", "uploads/batch-1/doc.pdf", "test-bucket")
	if err == nil {
		t.Fatal("expected error from DB")
	}
	if !strings.Contains(err.Error(), "update status") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestHandleSingleImage_ReadError(t *testing.T) {
	h := &Handler{
		db:     &mockDB{},
		s3:     &mockS3{},
		bucket: "test-bucket",
	}

	// Try to read a file that doesn't exist
	_, err := h.handleSingleImage(context.Background(), "/nonexistent/file.jpg", "batch-1")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
	if !strings.Contains(err.Error(), "read image") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestSendAnalyzeMessage_MultiplePages(t *testing.T) {
	sqs := &mockSQS{}
	h := &Handler{sqs: sqs, queueURL: "https://sqs.example.com/queue"}

	// Send multiple messages
	for i := 1; i <= 3; i++ {
		err := h.sendAnalyzeMessage(context.Background(), "batch-1", fmt.Sprintf("page-%d", i), i, fmt.Sprintf("pages/batch-1/page_%04d.jpg", i))
		if err != nil {
			t.Fatalf("unexpected error on message %d: %v", i, err)
		}
	}

	if len(sqs.messages) != 3 {
		t.Errorf("expected 3 messages, got %d", len(sqs.messages))
	}

	for i, msg := range sqs.messages {
		var parsed map[string]any
		json.Unmarshal([]byte(msg), &parsed)
		if parsed["uploadId"] != "batch-1" {
			t.Errorf("message %d: uploadId = %v", i, parsed["uploadId"])
		}
		expectedPageNum := float64(i + 1)
		if parsed["pageNumber"] != expectedPageNum {
			t.Errorf("message %d: pageNumber = %v, want %v", i, parsed["pageNumber"], expectedPageNum)
		}
	}
}

func TestHandlePDFUpload_ImageFullPath(t *testing.T) {
	s3Mock := &mockS3{}
	sqsMock := &mockSQS{}
	db := &mockDB{
		execFn: func(ctx context.Context, sql string, args ...any) error {
			return nil
		},
		insertFn: func(ctx context.Context, sql string, args ...any) (string, error) {
			return "page-id-1", nil
		},
	}

	h := &Handler{
		db:       db,
		s3:       s3Mock,
		sqs:      sqsMock,
		bucket:   "test-bucket",
		queueURL: "https://sqs.example.com/queue",
	}

	err := h.handlePDFUpload(context.Background(), "batch-1", "photo.png", "uploads/batch-1/photo.png", "test-bucket")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(s3Mock.putCalls) != 1 {
		t.Errorf("expected 1 S3 put call, got %d", len(s3Mock.putCalls))
	}
	if len(sqsMock.messages) != 1 {
		t.Errorf("expected 1 SQS message, got %d", len(sqsMock.messages))
	}
}

func TestHandlePDFUpload_InsertError(t *testing.T) {
	s3Mock := &mockS3{}
	db := &mockDB{
		execFn: func(ctx context.Context, sql string, args ...any) error {
			return nil
		},
		insertFn: func(ctx context.Context, sql string, args ...any) (string, error) {
			return "", fmt.Errorf("insert failed")
		},
	}

	h := &Handler{
		db:       db,
		s3:       s3Mock,
		sqs:      &mockSQS{},
		bucket:   "test-bucket",
		queueURL: "https://sqs.example.com/queue",
	}

	err := h.handlePDFUpload(context.Background(), "batch-1", "photo.jpg", "uploads/batch-1/photo.jpg", "test-bucket")
	if err == nil {
		t.Fatal("expected error from db insert")
	}
	if !strings.Contains(err.Error(), "insert page") {
		t.Errorf("unexpected error: %v", err)
	}
}

type mockS3PutFails struct{}

func (m *mockS3PutFails) PresignPutObject(ctx context.Context, bucket, key, contentType string, expires time.Duration) (string, error) {
	return "", nil
}
func (m *mockS3PutFails) PresignGetObject(ctx context.Context, bucket, key string, expires time.Duration) (string, error) {
	return "", nil
}
func (m *mockS3PutFails) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("fake-image-data")), nil
}
func (m *mockS3PutFails) PutObject(ctx context.Context, bucket, key, contentType string, body io.Reader) error {
	return fmt.Errorf("s3 put failed")
}

func TestHandlePDFUpload_PutObjectFails(t *testing.T) {
	db := &mockDB{
		execFn: func(ctx context.Context, sql string, args ...any) error {
			return nil
		},
	}

	h := &Handler{
		db:     db,
		s3:     &mockS3PutFails{},
		bucket: "test-bucket",
	}

	err := h.handlePDFUpload(context.Background(), "batch-1", "photo.jpg", "uploads/batch-1/photo.jpg", "test-bucket")
	if err == nil {
		t.Fatal("expected error from S3 PutObject")
	}
}

func TestHandlePageArrival_QueryError(t *testing.T) {
	db := &mockDB{
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			return nil, fmt.Errorf("query failed")
		},
	}

	h := &Handler{
		db:  db,
		s3:  &mockS3{},
		sqs: &mockSQS{},
	}

	err := h.Handle(context.Background(), events.S3Event{
		Records: []events.S3EventRecord{{
			S3: events.S3Entity{
				Bucket: events.S3Bucket{Name: "test-bucket"},
				Object: events.S3Object{Key: "pages/batch-1/page_0001.jpg"},
			},
		}},
	})
	if err == nil {
		t.Fatal("expected error from DB query")
	}
}

func TestHandle_MultipleRecords(t *testing.T) {
	sqs := &mockSQS{}
	db := &mockDB{
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			return []map[string]any{{"id": "page-id-1"}}, nil
		},
	}
	h := &Handler{
		db:       db,
		s3:       &mockS3{},
		sqs:      sqs,
		bucket:   "test-bucket",
		queueURL: "https://sqs.example.com/queue",
	}

	err := h.Handle(context.Background(), events.S3Event{
		Records: []events.S3EventRecord{
			{
				S3: events.S3Entity{
					Bucket: events.S3Bucket{Name: "test-bucket"},
					Object: events.S3Object{Key: "pages/batch-1/page_0001.jpg"},
				},
			},
			{
				S3: events.S3Entity{
					Bucket: events.S3Bucket{Name: "test-bucket"},
					Object: events.S3Object{Key: "pages/batch-1/page_0002.jpg"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sqs.messages) != 2 {
		t.Errorf("expected 2 SQS messages, got %d", len(sqs.messages))
	}
}

// ─── Tests: normalizeImage ──────────────────────────────────────────────

// createTestImage creates a small test image file in the given format.
func createTestImage(t *testing.T, dir, name string, encoder func(*os.File, image.Image)) string {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 10, 10))
	for y := 0; y < 10; y++ {
		for x := 0; x < 10; x++ {
			img.Set(x, y, color.RGBA{R: 255, G: 0, B: 0, A: 255})
		}
	}
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	encoder(f, img)
	f.Close()
	return path
}

func TestNormalizeImage_JPEG(t *testing.T) {
	dir := t.TempDir()
	imgPath := createTestImage(t, dir, "test.jpg", func(f *os.File, img image.Image) {
		jpeg.Encode(f, img, nil)
	})

	h := &Handler{}
	result, cleanup, err := h.normalizeImage(imgPath, ".jpg")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cleanup != nil {
		t.Error("expected nil cleanup for JPEG passthrough")
	}
	if result != imgPath {
		t.Errorf("expected same path %q, got %q", imgPath, result)
	}
}

func TestNormalizeImage_PNG(t *testing.T) {
	dir := t.TempDir()
	imgPath := createTestImage(t, dir, "test.png", func(f *os.File, img image.Image) {
		png.Encode(f, img)
	})

	h := &Handler{}
	result, cleanup, err := h.normalizeImage(imgPath, ".png")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cleanup != nil {
		t.Error("expected nil cleanup for PNG passthrough")
	}
	if result != imgPath {
		t.Errorf("expected same path %q, got %q", imgPath, result)
	}
}

func TestNormalizeImage_GIF(t *testing.T) {
	dir := t.TempDir()
	imgPath := createTestImage(t, dir, "test.gif", func(f *os.File, img image.Image) {
		gif.Encode(f, img, nil)
	})

	h := &Handler{}
	result, cleanup, err := h.normalizeImage(imgPath, ".gif")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cleanup == nil {
		t.Error("expected non-nil cleanup for GIF conversion")
	}
	defer cleanup()

	if result == imgPath {
		t.Error("expected different output path for GIF conversion")
	}
	if filepath.Ext(result) != ".jpg" {
		t.Errorf("expected .jpg extension, got %q", filepath.Ext(result))
	}

	// Verify the output is valid JPEG
	f, err := os.Open(result)
	if err != nil {
		t.Fatalf("open result: %v", err)
	}
	defer f.Close()
	_, err = jpeg.Decode(f)
	if err != nil {
		t.Fatalf("result is not valid JPEG: %v", err)
	}
}

func TestNormalizeImage_HEIC(t *testing.T) {
	dir := t.TempDir()
	// Create a dummy HEIC file (not a real HEIC, so conversion will fail)
	heicPath := filepath.Join(dir, "test.heic")
	os.WriteFile(heicPath, []byte("not-a-real-heic"), 0644)

	// Use a fake converter that always fails
	h := &Handler{heifConvertPath: "/nonexistent/heif-convert"}
	_, _, err := h.normalizeImage(heicPath, ".heic")
	if err == nil {
		t.Fatal("expected error when heif-convert is not available")
	}
	if !strings.Contains(err.Error(), "heif-convert") {
		t.Errorf("expected heif-convert error, got: %v", err)
	}
}

func TestNormalizeImage_HEIC_WithConverter(t *testing.T) {
	// Create a shell script that acts as heif-convert: copies a valid JPEG to output
	dir := t.TempDir()

	// Create a real JPEG to use as "converted" output
	jpegPath := createTestImage(t, dir, "source.jpg", func(f *os.File, img image.Image) {
		jpeg.Encode(f, img, nil)
	})

	// Create a fake heif-convert script that copies the JPEG
	scriptPath := filepath.Join(dir, "fake-heif-convert")
	os.WriteFile(scriptPath, []byte(fmt.Sprintf("#!/bin/sh\ncp %s \"$2\"\n", jpegPath)), 0755)

	heicPath := filepath.Join(dir, "test.heic")
	os.WriteFile(heicPath, []byte("fake-heic-data"), 0644)

	h := &Handler{heifConvertPath: scriptPath}
	result, cleanup, err := h.normalizeImage(heicPath, ".heic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cleanup == nil {
		t.Error("expected non-nil cleanup for HEIC conversion")
	}
	defer cleanup()

	if filepath.Ext(result) != ".jpg" {
		t.Errorf("expected .jpg extension, got %q", filepath.Ext(result))
	}

	// Verify the output is valid JPEG
	f, err := os.Open(result)
	if err != nil {
		t.Fatalf("open result: %v", err)
	}
	defer f.Close()
	_, err = jpeg.Decode(f)
	if err != nil {
		t.Fatalf("result is not valid JPEG: %v", err)
	}
}

func TestGetHeifConvertPath(t *testing.T) {
	h := &Handler{}

	// Default should return "heif-convert" (falls through to PATH)
	path := h.getHeifConvertPath()
	if path != "heif-convert" {
		t.Errorf("expected 'heif-convert', got %q", path)
	}

	// Custom path should be used
	h.heifConvertPath = "/custom/path/heif-convert"
	path = h.getHeifConvertPath()
	if path != "/custom/path/heif-convert" {
		t.Errorf("expected custom path, got %q", path)
	}
}

func TestHandleSingleImage_GIF(t *testing.T) {
	sqsMock := &mockSQS{}
	db := &mockDB{
		execFn: func(ctx context.Context, sql string, args ...any) error {
			return nil
		},
		insertFn: func(ctx context.Context, sql string, args ...any) (string, error) {
			return "page-id-1", nil
		},
	}

	// Create a real GIF file and serve it from mock S3
	tmpDir := t.TempDir()
	gifImg := image.NewRGBA(image.Rect(0, 0, 10, 10))
	for y := 0; y < 10; y++ {
		for x := 0; x < 10; x++ {
			gifImg.Set(x, y, color.RGBA{R: 0, G: 255, B: 0, A: 255})
		}
	}
	var gifBuf strings.Builder
	gif.Encode(&gifBuf, gifImg, nil)
	gifData := gifBuf.String()

	_ = tmpDir

	// Mock S3 that returns GIF data
	gifS3 := &mockS3WithData{data: gifData}

	h := &Handler{
		db:       db,
		s3:       gifS3,
		sqs:      sqsMock,
		bucket:   "test-bucket",
		queueURL: "https://sqs.example.com/queue",
	}

	err := h.handlePDFUpload(context.Background(), "batch-1", "photo.gif", "uploads/batch-1/photo.gif", "test-bucket")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(gifS3.putCalls) != 1 {
		t.Errorf("expected 1 S3 put call, got %d", len(gifS3.putCalls))
	}
	if len(sqsMock.messages) != 1 {
		t.Errorf("expected 1 SQS message, got %d", len(sqsMock.messages))
	}
}

// mockS3WithData returns specific data from GetObject.
type mockS3WithData struct {
	data     string
	putCalls []string
}

func (m *mockS3WithData) PresignPutObject(ctx context.Context, bucket, key, contentType string, expires time.Duration) (string, error) {
	return "", nil
}
func (m *mockS3WithData) PresignGetObject(ctx context.Context, bucket, key string, expires time.Duration) (string, error) {
	return "", nil
}
func (m *mockS3WithData) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(m.data)), nil
}
func (m *mockS3WithData) PutObject(ctx context.Context, bucket, key, contentType string, body io.Reader) error {
	m.putCalls = append(m.putCalls, key)
	return nil
}
