package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/projectcloudline/logbook-service/internal/gemini"
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
	presignPutFn func(ctx context.Context, bucket, key, contentType string, expires time.Duration) (string, error)
	presignGetFn func(ctx context.Context, bucket, key string, expires time.Duration) (string, error)
}

func (m *mockS3) PresignPutObject(ctx context.Context, bucket, key, contentType string, expires time.Duration) (string, error) {
	if m.presignPutFn != nil {
		return m.presignPutFn(ctx, bucket, key, contentType, expires)
	}
	return "https://s3.example.com/presigned-put", nil
}

func (m *mockS3) PresignGetObject(ctx context.Context, bucket, key string, expires time.Duration) (string, error) {
	if m.presignGetFn != nil {
		return m.presignGetFn(ctx, bucket, key, expires)
	}
	return "https://s3.example.com/presigned-get", nil
}

func (m *mockS3) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("data")), nil
}

func (m *mockS3) PutObject(ctx context.Context, bucket, key, contentType string, body io.Reader) error {
	return nil
}

// ─── Mock Secrets ───────────────────────────────────────────────────────────

type mockSecrets struct {
	secrets map[string]string
}

func (m *mockSecrets) GetSecret(ctx context.Context, arn string) (string, error) {
	if v, ok := m.secrets[arn]; ok {
		return v, nil
	}
	return "", fmt.Errorf("secret not found: %s", arn)
}

func (m *mockSecrets) GetSecretJSON(ctx context.Context, arn string) (map[string]string, error) {
	raw, err := m.GetSecret(ctx, arn)
	if err != nil {
		return nil, err
	}
	var result map[string]string
	json.Unmarshal([]byte(raw), &result)
	return result, nil
}

// ─── Test Helpers ───────────────────────────────────────────────────────────

func newTestHandler(db *mockDB) *Handler {
	return &Handler{
		db: db,
		s3: &mockS3{},
		secrets: &mockSecrets{
			secrets: map[string]string{
				"faa-secret": "test-api-key",
			},
		},
		bucket: "test-bucket",
	}
}

func makeEvent(method, resource, body string, pathParams map[string]string, queryParams map[string]string) json.RawMessage {
	event := events.APIGatewayProxyRequest{
		HTTPMethod:            method,
		Resource:              resource,
		Body:                  body,
		PathParameters:        pathParams,
		QueryStringParameters: queryParams,
	}
	b, _ := json.Marshal(event)
	return b
}

func parseBody(t *testing.T, body string) map[string]any {
	t.Helper()
	var result map[string]any
	if err := json.Unmarshal([]byte(body), &result); err != nil {
		t.Fatalf("failed to parse response body: %v\nbody: %s", err, body)
	}
	return result
}

// ─── Tests ──────────────────────────────────────────────────────────────────

func TestWarmerEvent(t *testing.T) {
	h := newTestHandler(&mockDB{})
	event := json.RawMessage(`{"source":"logbook.warmer"}`)
	resp, err := h.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if resp.Body != "warm" {
		t.Errorf("body = %q, want %q", resp.Body, "warm")
	}
}

func TestNotFoundRoute(t *testing.T) {
	h := newTestHandler(&mockDB{})
	event := makeEvent("GET", "/nonexistent", "", nil, nil)
	resp, err := h.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 404 {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHandleUpload(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantErr    string
	}{
		{
			name:       "missing tailNumber",
			body:       `{"files":[{"filename":"a.pdf"}]}`,
			wantStatus: 400,
			wantErr:    "tailNumber is required",
		},
		{
			name:       "missing files",
			body:       `{"tailNumber":"N123"}`,
			wantStatus: 400,
			wantErr:    "files array is required",
		},
		{
			name:       "mixed file types",
			body:       `{"tailNumber":"N123","files":[{"filename":"a.pdf"},{"filename":"b.jpg"}]}`,
			wantStatus: 400,
			wantErr:    "Cannot mix",
		},
		{
			name:       "multiple PDFs",
			body:       `{"tailNumber":"N123","files":[{"filename":"a.pdf"},{"filename":"b.pdf"}]}`,
			wantStatus: 400,
			wantErr:    "Only one PDF",
		},
		{
			name:       "unsupported file type",
			body:       `{"tailNumber":"N123","files":[{"filename":"a.docx"}]}`,
			wantStatus: 400,
			wantErr:    "Files must be PDF",
		},
		{
			name:       "successful pdf upload",
			body:       `{"tailNumber":"N123","logType":"airframe","files":[{"filename":"log.pdf"}]}`,
			wantStatus: 200,
		},
		{
			name:       "successful image upload",
			body:       `{"tailNumber":"N456","logType":"engine","files":[{"filename":"page1.jpg"},{"filename":"page2.jpg"}]}`,
			wantStatus: 200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &mockDB{
				insertFn: func(ctx context.Context, sql string, args ...any) (string, error) {
					return "test-uuid-123", nil
				},
			}
			h := newTestHandler(db)

			event := makeEvent("POST", "/uploads", tt.body, nil, nil)
			resp, err := h.Handle(context.Background(), event)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if resp.StatusCode != tt.wantStatus {
				body := parseBody(t, resp.Body)
				t.Errorf("status = %d, want %d, body: %v", resp.StatusCode, tt.wantStatus, body)
			}

			if tt.wantErr != "" {
				body := parseBody(t, resp.Body)
				errMsg, _ := body["error"].(string)
				if !strings.Contains(errMsg, tt.wantErr) {
					t.Errorf("error = %q, want to contain %q", errMsg, tt.wantErr)
				}
			}

			if tt.wantStatus == 200 {
				body := parseBody(t, resp.Body)
				if _, ok := body["uploadId"]; !ok {
					t.Error("missing uploadId in response")
				}
				if _, ok := body["files"]; !ok {
					t.Error("missing files in response")
				}
			}
		})
	}
}

func TestHandleStatus(t *testing.T) {
	tests := []struct {
		name       string
		batchID    string
		queryRows  []map[string]any
		wantStatus int
	}{
		{
			name:       "not found",
			batchID:    "nonexistent",
			queryRows:  nil,
			wantStatus: 404,
		},
		{
			name:    "found",
			batchID: "batch-123",
			queryRows: []map[string]any{{
				"id":                "batch-123",
				"processing_status": "completed",
				"page_count":        int64(5),
				"source_filename":   "logbook.pdf",
				"logbook_type":      "airframe",
				"upload_type":       "pdf",
				"created_at":        "2024-01-01T00:00:00Z",
				"completed_pages":   int64(5),
				"failed_pages":      int64(0),
				"needs_review_pages": int64(1),
				"total_pages":       int64(5),
			}},
			wantStatus: 200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &mockDB{
				queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
					return tt.queryRows, nil
				},
			}
			h := newTestHandler(db)

			event := makeEvent("GET", "/uploads/{id}/status", "",
				map[string]string{"id": tt.batchID}, nil)
			resp, err := h.Handle(context.Background(), event)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
		})
	}
}

func TestHandlePageImage(t *testing.T) {
	tests := []struct {
		name       string
		queryRows  []map[string]any
		wantStatus int
	}{
		{
			name:       "page not found",
			queryRows:  nil,
			wantStatus: 404,
		},
		{
			name:       "page found",
			queryRows:  []map[string]any{{"image_path": "pages/batch-1/page_0001.jpg"}},
			wantStatus: 200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &mockDB{
				queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
					return tt.queryRows, nil
				},
			}
			h := newTestHandler(db)

			event := makeEvent("GET", "/uploads/{id}/pages/{pageNumber}/image", "",
				map[string]string{"id": "batch-1", "pageNumber": "1"}, nil)
			resp, err := h.Handle(context.Background(), event)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
		})
	}
}

func TestHandleListUploads(t *testing.T) {
	db := &mockDB{
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			return []map[string]any{
				{"id": "upload-1", "logbook_type": "airframe"},
				{"id": "upload-2", "logbook_type": "engine"},
			}, nil
		},
	}
	h := newTestHandler(db)

	event := makeEvent("GET", "/aircraft/{tailNumber}/uploads", "",
		map[string]string{"tailNumber": "N123AB"}, nil)
	resp, err := h.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	body := parseBody(t, resp.Body)
	if body["tailNumber"] != "N123AB" {
		t.Errorf("tailNumber = %v, want N123AB", body["tailNumber"])
	}
}

func TestHandleSummary(t *testing.T) {
	tests := []struct {
		name       string
		queryRows  []map[string]any
		wantStatus int
	}{
		{
			name:       "aircraft not found",
			queryRows:  nil,
			wantStatus: 404,
		},
		{
			name:       "aircraft found",
			queryRows:  []map[string]any{{"id": "aircraft-1", "registration": "N123AB"}},
			wantStatus: 200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callCount := 0
			db := &mockDB{
				queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
					callCount++
					if callCount == 1 {
						return tt.queryRows, nil
					}
					return nil, nil
				},
			}
			h := newTestHandler(db)

			event := makeEvent("GET", "/aircraft/{tailNumber}/summary", "",
				map[string]string{"tailNumber": "N123AB"}, nil)
			resp, err := h.Handle(context.Background(), event)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
		})
	}
}

func TestHandleEntries(t *testing.T) {
	tests := []struct {
		name       string
		tailNumber string
		queryParams map[string]string
		hasAircraft bool
		wantStatus int
	}{
		{
			name:        "aircraft not found",
			tailNumber:  "N999",
			hasAircraft: false,
			wantStatus:  404,
		},
		{
			name:        "success with defaults",
			tailNumber:  "N123",
			hasAircraft: true,
			wantStatus:  200,
		},
		{
			name:        "with filters",
			tailNumber:  "N123",
			queryParams: map[string]string{"type": "inspection", "page": "2", "limit": "10"},
			hasAircraft: true,
			wantStatus:  200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callCount := 0
			db := &mockDB{
				queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
					callCount++
					if callCount == 1 { // aircraft lookup
						if !tt.hasAircraft {
							return nil, nil
						}
						return []map[string]any{{"id": "aid-1"}}, nil
					}
					if strings.Contains(sql, "COUNT") {
						return []map[string]any{{"total": int64(42)}}, nil
					}
					return []map[string]any{
						{"id": "entry-1", "entry_type": "maintenance"},
					}, nil
				},
			}
			h := newTestHandler(db)

			event := makeEvent("GET", "/aircraft/{tailNumber}/entries", "",
				map[string]string{"tailNumber": tt.tailNumber}, tt.queryParams)
			resp, err := h.Handle(context.Background(), event)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d, body: %s", resp.StatusCode, tt.wantStatus, resp.Body)
			}
		})
	}
}

func TestHandleEntryDetail(t *testing.T) {
	tests := []struct {
		name       string
		hasAircraft bool
		hasEntry   bool
		wantStatus int
	}{
		{"aircraft not found", false, false, 404},
		{"entry not found", true, false, 404},
		{"success", true, true, 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callCount := 0
			db := &mockDB{
				queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
					callCount++
					if callCount == 1 { // aircraft lookup
						if !tt.hasAircraft {
							return nil, nil
						}
						return []map[string]any{{"id": "aid-1"}}, nil
					}
					if callCount == 2 { // entry lookup
						if !tt.hasEntry {
							return nil, nil
						}
						return []map[string]any{{"id": "entry-1", "entry_type": "maintenance"}}, nil
					}
					return nil, nil
				},
			}
			h := newTestHandler(db)

			event := makeEvent("GET", "/aircraft/{tailNumber}/entries/{entryId}", "",
				map[string]string{"tailNumber": "N123", "entryId": "entry-1"}, nil)
			resp, err := h.Handle(context.Background(), event)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
		})
	}
}

func TestHandleUpdateEntry(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantErr    string
	}{
		{
			name:       "empty body",
			body:       "{}",
			wantStatus: 400,
			wantErr:    "Request body is required",
		},
		{
			name:       "invalid review status",
			body:       `{"reviewStatus":"invalid"}`,
			wantStatus: 400,
			wantErr:    "reviewStatus must be",
		},
		{
			name:       "successful update",
			body:       `{"shopName":"New Shop","reviewStatus":"approved","reviewedBy":"user1"}`,
			wantStatus: 200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callCount := 0
			db := &mockDB{
				queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
					callCount++
					if callCount == 1 { // aircraft lookup
						return []map[string]any{{"id": "aid-1"}}, nil
					}
					// UPDATE RETURNING or subsequent queries
					return []map[string]any{{"id": "entry-1", "entry_type": "maintenance"}}, nil
				},
			}
			h := newTestHandler(db)

			event := makeEvent("PATCH", "/aircraft/{tailNumber}/entries/{entryId}", tt.body,
				map[string]string{"tailNumber": "N123", "entryId": "entry-1"}, nil)
			resp, err := h.Handle(context.Background(), event)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d, body: %s", resp.StatusCode, tt.wantStatus, resp.Body)
			}
			if tt.wantErr != "" {
				body := parseBody(t, resp.Body)
				errMsg, _ := body["error"].(string)
				if !strings.Contains(errMsg, tt.wantErr) {
					t.Errorf("error = %q, want to contain %q", errMsg, tt.wantErr)
				}
			}
		})
	}
}

func TestHandleInspections(t *testing.T) {
	callCount := 0
	db := &mockDB{
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			callCount++
			if callCount == 1 { // aircraft lookup
				return []map[string]any{{"id": "aid-1"}}, nil
			}
			if strings.Contains(sql, "COUNT") {
				return []map[string]any{{"total": int64(3)}}, nil
			}
			return []map[string]any{
				{"id": "insp-1", "inspection_type": "annual"},
			}, nil
		},
	}
	h := newTestHandler(db)

	event := makeEvent("GET", "/aircraft/{tailNumber}/inspections", "",
		map[string]string{"tailNumber": "N123"}, nil)
	resp, err := h.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandleAds(t *testing.T) {
	callCount := 0
	db := &mockDB{
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			callCount++
			if callCount == 1 { // aircraft lookup
				return []map[string]any{{"id": "aid-1"}}, nil
			}
			if strings.Contains(sql, "COUNT") {
				return []map[string]any{{"total": int64(2)}}, nil
			}
			return []map[string]any{
				{"id": "ad-1", "ad_number": "AD-2024-001"},
			}, nil
		},
	}
	h := newTestHandler(db)

	event := makeEvent("GET", "/aircraft/{tailNumber}/ads", "",
		map[string]string{"tailNumber": "N123"}, nil)
	resp, err := h.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandleParts(t *testing.T) {
	tests := []struct {
		name        string
		queryParams map[string]string
		wantStatus  int
	}{
		{"default active parts", nil, 200},
		{"all parts", map[string]string{"status": "all"}, 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callCount := 0
			db := &mockDB{
				queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
					callCount++
					if callCount == 1 { // aircraft lookup
						return []map[string]any{{"id": "aid-1"}}, nil
					}
					return []map[string]any{
						{"id": "part-1", "part_name": "Propeller"},
					}, nil
				},
			}
			h := newTestHandler(db)

			event := makeEvent("GET", "/aircraft/{tailNumber}/parts", "",
				map[string]string{"tailNumber": "N123"}, tt.queryParams)
			resp, err := h.Handle(context.Background(), event)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
		})
	}
}

func TestHandleQuery(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "missing question",
			body:       `{}`,
			wantStatus: 400,
		},
		{
			name:       "aircraft not found",
			body:       `{"question":"When was the last oil change?"}`,
			wantStatus: 404,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &mockDB{
				queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
					return nil, nil // no aircraft found
				},
			}
			h := newTestHandler(db)
			h.gemini = &gemini.MockClient{}

			event := makeEvent("POST", "/aircraft/{tailNumber}/query", tt.body,
				map[string]string{"tailNumber": "N123"}, nil)
			resp, err := h.Handle(context.Background(), event)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
		})
	}
}

func TestHandleQuery_WithResults(t *testing.T) {
	callCount := 0
	db := &mockDB{
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			callCount++
			if callCount == 1 { // aircraft lookup
				return []map[string]any{{"id": "aid-1"}}, nil
			}
			// vector search results
			return []map[string]any{
				{
					"chunk_text":             "Oil changed",
					"chunk_type":             "narrative",
					"entry_date":             "2024-01-15",
					"entry_type":             "maintenance",
					"maintenance_narrative":  "Changed oil and filter",
					"inspection_type":        nil,
					"similarity":             0.95,
				},
			}, nil
		},
	}

	h := newTestHandler(db)
	h.gemini = &gemini.MockClient{
		EmbedContentFn: func(ctx context.Context, model string, text string) ([]float32, error) {
			return make([]float32, 768), nil
		},
		GenerateContentFn: func(ctx context.Context, model string, parts []gemini.Part, config *gemini.GenerateConfig) (string, error) {
			return "The last oil change was performed on January 15, 2024.", nil
		},
	}

	event := makeEvent("POST", "/aircraft/{tailNumber}/query",
		`{"question":"When was the last oil change?"}`,
		map[string]string{"tailNumber": "N123"}, nil)
	resp, err := h.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200, body: %s", resp.StatusCode, resp.Body)
	}

	body := parseBody(t, resp.Body)
	if body["answer"] == nil || body["answer"] == "" {
		t.Error("missing answer in response")
	}
	sources, ok := body["sources"].([]any)
	if !ok || len(sources) == 0 {
		t.Error("missing sources in response")
	}
}

func TestHandleStatus_WithFailedPages(t *testing.T) {
	callCount := 0
	db := &mockDB{
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			callCount++
			if callCount == 1 {
				return []map[string]any{{
					"id":                 "batch-123",
					"processing_status":  "completed_with_errors",
					"page_count":         int64(5),
					"source_filename":    "logbook.pdf",
					"logbook_type":       "airframe",
					"upload_type":        "pdf",
					"created_at":         "2024-01-01T00:00:00Z",
					"completed_pages":    int64(3),
					"failed_pages":       int64(2),
					"needs_review_pages": int64(0),
					"total_pages":        int64(5),
				}}, nil
			}
			// failed page numbers query
			return []map[string]any{
				{"page_number": int64(2)},
				{"page_number": int64(4)},
			}, nil
		},
	}
	h := newTestHandler(db)

	event := makeEvent("GET", "/uploads/{id}/status", "",
		map[string]string{"id": "batch-123"}, nil)
	resp, err := h.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	body := parseBody(t, resp.Body)
	fpn, ok := body["failedPageNumbers"].([]any)
	if !ok || len(fpn) != 2 {
		t.Errorf("expected 2 failed page numbers, got %v", body["failedPageNumbers"])
	}
}

func TestHandleStatus_NilPageCount(t *testing.T) {
	db := &mockDB{
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			return []map[string]any{{
				"id":                 "batch-123",
				"processing_status":  "processing",
				"page_count":         nil,
				"source_filename":    "logbook.pdf",
				"logbook_type":       "airframe",
				"upload_type":        "pdf",
				"created_at":         "2024-01-01T00:00:00Z",
				"completed_pages":    int64(0),
				"failed_pages":       int64(0),
				"needs_review_pages": int64(0),
				"total_pages":        int64(3),
			}}, nil
		},
	}
	h := newTestHandler(db)

	event := makeEvent("GET", "/uploads/{id}/status", "",
		map[string]string{"id": "batch-123"}, nil)
	resp, err := h.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	body := parseBody(t, resp.Body)
	// pageCount should fall back to total_pages
	if body["pageCount"] != float64(3) {
		t.Errorf("pageCount = %v, want 3", body["pageCount"])
	}
}

func TestHandleStatus_DBError(t *testing.T) {
	db := &mockDB{
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			return nil, fmt.Errorf("db error")
		},
	}
	h := newTestHandler(db)

	event := makeEvent("GET", "/uploads/{id}/status", "",
		map[string]string{"id": "batch-123"}, nil)
	_, err := h.Handle(context.Background(), event)
	if err == nil {
		t.Fatal("expected error from DB")
	}
}

func TestHandleUpload_InvalidJSON(t *testing.T) {
	h := newTestHandler(&mockDB{})
	event := makeEvent("POST", "/uploads", "not json", nil, nil)
	resp, err := h.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 400 {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleEntries_WithNeedsReview(t *testing.T) {
	callCount := 0
	db := &mockDB{
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			callCount++
			if callCount == 1 {
				return []map[string]any{{"id": "aid-1"}}, nil
			}
			if strings.Contains(sql, "COUNT") {
				return []map[string]any{{"total": int64(5)}}, nil
			}
			return []map[string]any{
				{"id": "entry-1", "entry_type": "maintenance", "needs_review": true},
			}, nil
		},
	}
	h := newTestHandler(db)

	event := makeEvent("GET", "/aircraft/{tailNumber}/entries", "",
		map[string]string{"tailNumber": "N123"},
		map[string]string{"needsReview": "true", "dateFrom": "2024-01-01", "dateTo": "2024-12-31"})
	resp, err := h.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandleUpdateEntry_NoFieldsToUpdate(t *testing.T) {
	callCount := 0
	db := &mockDB{
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			callCount++
			if callCount == 1 {
				return []map[string]any{{"id": "aid-1"}}, nil
			}
			return []map[string]any{{"id": "entry-1"}}, nil
		},
	}
	h := newTestHandler(db)

	// Send a body with unknown fields (no patchable fields and no reviewStatus)
	event := makeEvent("PATCH", "/aircraft/{tailNumber}/entries/{entryId}",
		`{"unknownField":"value"}`,
		map[string]string{"tailNumber": "N123", "entryId": "entry-1"}, nil)
	resp, err := h.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 400 {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
	body := parseBody(t, resp.Body)
	if !strings.Contains(body["error"].(string), "No fields") {
		t.Errorf("error = %v, want 'No fields to update'", body["error"])
	}
}

func TestHandleUpdateEntry_NotFound(t *testing.T) {
	callCount := 0
	db := &mockDB{
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			callCount++
			if callCount == 1 {
				return []map[string]any{{"id": "aid-1"}}, nil
			}
			// UPDATE RETURNING returns empty — not found
			return nil, nil
		},
	}
	h := newTestHandler(db)

	event := makeEvent("PATCH", "/aircraft/{tailNumber}/entries/{entryId}",
		`{"shopName":"Test Shop"}`,
		map[string]string{"tailNumber": "N123", "entryId": "entry-999"}, nil)
	resp, err := h.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 404 {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHandleInspections_WithTypeFilter(t *testing.T) {
	callCount := 0
	db := &mockDB{
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			callCount++
			if callCount == 1 {
				return []map[string]any{{"id": "aid-1"}}, nil
			}
			if strings.Contains(sql, "COUNT") {
				return []map[string]any{{"total": int64(2)}}, nil
			}
			return []map[string]any{
				{"id": "insp-1", "inspection_type": "annual"},
			}, nil
		},
	}
	h := newTestHandler(db)

	event := makeEvent("GET", "/aircraft/{tailNumber}/inspections", "",
		map[string]string{"tailNumber": "N123"},
		map[string]string{"type": "annual"})
	resp, err := h.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestToInt64(t *testing.T) {
	tests := []struct {
		input any
		want  int64
		ok    bool
	}{
		{int64(42), 42, true},
		{int32(42), 42, true},
		{int(42), 42, true},
		{float64(42.0), 42, true},
		{"not a number", 0, false},
		{nil, 0, false},
	}
	for _, tt := range tests {
		got, ok := toInt64(tt.input)
		if got != tt.want || ok != tt.ok {
			t.Errorf("toInt64(%v) = (%d, %v), want (%d, %v)", tt.input, got, ok, tt.want, tt.ok)
		}
	}
}

func TestFirstOrNil(t *testing.T) {
	if firstOrNil(nil) != nil {
		t.Error("expected nil for empty slice")
	}
	if firstOrNil([]map[string]any{}) != nil {
		t.Error("expected nil for empty slice")
	}
	rows := []map[string]any{{"key": "val"}}
	result := firstOrNil(rows)
	if result == nil {
		t.Error("expected non-nil for non-empty slice")
	}
}

func TestGetGeminiClient_Cached(t *testing.T) {
	h := newTestHandler(&mockDB{})
	mock := &gemini.MockClient{}
	h.gemini = mock

	client, err := h.getGeminiClient(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client != mock {
		t.Error("expected cached client to be returned")
	}
}

func TestHandleQuery_NoResults(t *testing.T) {
	callCount := 0
	db := &mockDB{
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			callCount++
			if callCount == 1 {
				return []map[string]any{{"id": "aid-1"}}, nil
			}
			return []map[string]any{}, nil
		},
	}
	h := newTestHandler(db)
	h.gemini = &gemini.MockClient{
		EmbedContentFn: func(ctx context.Context, model string, text string) ([]float32, error) {
			return make([]float32, 768), nil
		},
	}

	event := makeEvent("POST", "/aircraft/{tailNumber}/query",
		`{"question":"test?"}`,
		map[string]string{"tailNumber": "N123"}, nil)
	resp, err := h.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	body := parseBody(t, resp.Body)
	if body["answer"] != "No maintenance records found for this aircraft." {
		t.Errorf("unexpected answer: %v", body["answer"])
	}
}

func TestFormatEmbedding(t *testing.T) {
	result := formatEmbedding([]float32{0.1, 0.2, 0.3})
	if result != "[0.1,0.2,0.3]" {
		t.Errorf("got %q, want %q", result, "[0.1,0.2,0.3]")
	}
}

func TestNewUUID(t *testing.T) {
	id := newUUID()
	if len(id) != 36 {
		t.Errorf("UUID length = %d, want 36", len(id))
	}
	// Check format: 8-4-4-4-12
	parts := strings.Split(id, "-")
	if len(parts) != 5 {
		t.Errorf("UUID parts = %d, want 5", len(parts))
	}

	// Ensure uniqueness
	id2 := newUUID()
	if id == id2 {
		t.Error("UUIDs should be unique")
	}
}
