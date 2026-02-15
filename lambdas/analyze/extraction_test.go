package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/jpeg"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/projectcloudline/logbook-service/internal/anthropic"
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

type putObjectCall struct {
	key         string
	contentType string
}

type mockS3 struct {
	getObjectFn func(ctx context.Context, bucket, key string) (io.ReadCloser, error)
	putCalls    []putObjectCall
}

func (m *mockS3) PresignPutObject(ctx context.Context, bucket, key, contentType string, expires time.Duration) (string, error) {
	return "https://example.com/put", nil
}

func (m *mockS3) PresignGetObject(ctx context.Context, bucket, key string, expires time.Duration) (string, error) {
	return "https://example.com/get", nil
}

func (m *mockS3) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	if m.getObjectFn != nil {
		return m.getObjectFn(ctx, bucket, key)
	}
	return io.NopCloser(strings.NewReader("fake-image-data")), nil
}

func (m *mockS3) PutObject(ctx context.Context, bucket, key, contentType string, body io.Reader) error {
	m.putCalls = append(m.putCalls, putObjectCall{key: key, contentType: contentType})
	return nil
}

// makeTestJPEG creates a JPEG with dark bands for testing the slicer.
func makeTestJPEG(width, height int, bands [][2]int) []byte {
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	draw.Draw(img, img.Bounds(), &image.Uniform{color.White}, image.Point{}, draw.Src)
	for _, b := range bands {
		for y := b[0]; y < b[1] && y < height; y++ {
			for x := 0; x < width; x++ {
				img.Set(x, y, color.Black)
			}
		}
	}
	var buf bytes.Buffer
	jpeg.Encode(&buf, img, &jpeg.Options{Quality: 85})
	return buf.Bytes()
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

// ─── Tests: NormalizeEntryType ──────────────────────────────────────────────

func TestNormalizeEntryType(t *testing.T) {
	tests := []struct {
		name              string
		entryType         string
		inspectionType    string
		wantEntryType     string
		wantInspectionType string
	}{
		{
			name:              "annual legacy type",
			entryType:         "annual",
			wantEntryType:     "inspection",
			wantInspectionType: "annual",
		},
		{
			name:              "100hr legacy type",
			entryType:         "100hr",
			wantEntryType:     "inspection",
			wantInspectionType: "100hr",
		},
		{
			name:              "progressive legacy type",
			entryType:         "progressive",
			wantEntryType:     "inspection",
			wantInspectionType: "progressive",
		},
		{
			name:              "altimeter_check legacy type",
			entryType:         "altimeter_check",
			wantEntryType:     "inspection",
			wantInspectionType: "altimeter_static",
		},
		{
			name:              "transponder_check legacy type",
			entryType:         "transponder_check",
			wantEntryType:     "inspection",
			wantInspectionType: "transponder",
		},
		{
			name:              "inspection without subtype",
			entryType:         "inspection",
			wantEntryType:     "inspection",
			wantInspectionType: "other",
		},
		{
			name:              "inspection with subtype",
			entryType:         "inspection",
			inspectionType:    "annual",
			wantEntryType:     "inspection",
			wantInspectionType: "annual",
		},
		{
			name:              "maintenance stays",
			entryType:         "maintenance",
			wantEntryType:     "maintenance",
			wantInspectionType: "",
		},
		{
			name:              "unknown becomes other",
			entryType:         "unknown_type",
			wantEntryType:     "other",
			wantInspectionType: "",
		},
		{
			name:              "empty defaults to maintenance",
			entryType:         "",
			wantEntryType:     "maintenance",
			wantInspectionType: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := &extractedEntry{
				EntryType:      tt.entryType,
				InspectionType: tt.inspectionType,
			}
			normalizeEntryType(entry)
			if entry.EntryType != tt.wantEntryType {
				t.Errorf("entryType = %q, want %q", entry.EntryType, tt.wantEntryType)
			}
			if entry.InspectionType != tt.wantInspectionType {
				t.Errorf("inspectionType = %q, want %q", entry.InspectionType, tt.wantInspectionType)
			}
		})
	}
}

// ─── Tests: CheckAircraftIdentity ───────────────────────────────────────────

func TestCheckAircraftIdentity(t *testing.T) {
	tests := []struct {
		name        string
		entry       extractedEntry
		expected    expectedIdentity
		wantReview  bool
		wantMissing bool
	}{
		{
			name:     "no expected serial — no check",
			entry:    extractedEntry{AircraftSerial: "12345"},
			expected: expectedIdentity{},
			wantReview: false,
		},
		{
			name:     "no extracted serial — no check",
			entry:    extractedEntry{},
			expected: expectedIdentity{serialNumber: "12345"},
			wantReview: false,
		},
		{
			name:     "serial matches",
			entry:    extractedEntry{AircraftSerial: "12345"},
			expected: expectedIdentity{serialNumber: "12345"},
			wantReview: false,
		},
		{
			name:     "serial mismatch — flags review",
			entry:    extractedEntry{AircraftSerial: "99999"},
			expected: expectedIdentity{serialNumber: "12345"},
			wantReview: true,
			wantMissing: true,
		},
		{
			name:  "serial matches but make+model both fail — flags review",
			entry: extractedEntry{
				AircraftSerial: "12345",
				AircraftMake:   "Piper",
				AircraftModel:  "Cherokee",
			},
			expected: expectedIdentity{
				serialNumber: "12345",
				make:         "Cessna",
				model:        "172N",
			},
			wantReview: true,
			wantMissing: true,
		},
		{
			name:  "serial matches, model fails but make matches — OK",
			entry: extractedEntry{
				AircraftSerial: "12345",
				AircraftMake:   "Cessna",
				AircraftModel:  "182",
			},
			expected: expectedIdentity{
				serialNumber: "12345",
				make:         "Cessna",
				model:        "172N",
			},
			wantReview: false,
		},
		{
			name:  "fuzzy match with dashes and spaces",
			entry: extractedEntry{
				AircraftSerial: "172-84765",
				AircraftMake:   "CESSNA",
			},
			expected: expectedIdentity{
				serialNumber: "17284765",
				make:         "Cessna",
			},
			wantReview: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := tt.entry
			checkAircraftIdentity(&entry, tt.expected)
			if entry.NeedsReview != tt.wantReview {
				t.Errorf("needsReview = %v, want %v", entry.NeedsReview, tt.wantReview)
			}
			hasMissing := len(entry.MissingData) > 0
			if hasMissing != tt.wantMissing {
				t.Errorf("hasMissingData = %v, want %v", hasMissing, tt.wantMissing)
			}
		})
	}
}

// ─── Tests: CleanMarkdownFences ─────────────────────────────────────────────

func TestCleanMarkdownFences(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"no fences", `{"key":"value"}`, `{"key":"value"}`},
		{"json fences", "```json\n{\"key\":\"value\"}\n```", `{"key":"value"}`},
		{"plain fences", "```\n{\"key\":\"value\"}\n```", `{"key":"value"}`},
		{"empty", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cleanMarkdownFences(tt.in)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// ─── Tests: ProcessPage ─────────────────────────────────────────────────────

func TestProcessPage(t *testing.T) {
	execCalls := 0
	insertCalls := 0

	db := &mockDB{
		execFn: func(ctx context.Context, sql string, args ...any) error {
			execCalls++
			return nil
		},
		insertFn: func(ctx context.Context, sql string, args ...any) (string, error) {
			insertCalls++
			return "entry-id-1", nil
		},
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			if strings.Contains(sql, "upload_batches") {
				return []map[string]any{{
					"aircraft_id":   "aircraft-1",
					"registration":  "N123AB",
					"serial_number": "12345",
					"make":          "Cessna",
					"model":         "172N",
				}}, nil
			}
			// batch completion check
			return []map[string]any{{
				"total":  int64(1),
				"done":   int64(1),
				"failed": int64(0),
			}}, nil
		},
	}

	h := &Handler{
		db:     db,
		s3:     &mockS3{},
		bucket: "test-bucket",
		gemini: &gemini.MockClient{
			GenerateContentFn: func(ctx context.Context, model string, parts []gemini.Part, config *gemini.GenerateConfig) (string, error) {
				for _, p := range parts {
					if strings.Contains(p.Text, "QA specialist") {
						return `{"results":[{"entryIndex":0,"verdict":"pass","issues":[],"summary":"OK"}]}`, nil
					}
				}
				return `{"pageType":"maintenance_entry","entries":[{"date":"2024-01-15","entryType":"maintenance","maintenanceNarrative":"Changed oil and filter","confidence":0.95}]}`, nil
			},
			EmbedContentFn: func(ctx context.Context, model string, text string) ([]float32, error) {
				return make([]float32, 768), nil
			},
		},
		secrets: &mockSecrets{},
	}

	err := h.processPage(context.Background(), pageMessage{
		UploadID:   "batch-1",
		PageID:     "page-1",
		PageNumber: 1,
		S3Key:      "pages/batch-1/page_0001.jpg",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if insertCalls == 0 {
		t.Error("expected at least one insert call for the entry")
	}
}

// ─── Tests: Handle SQS Event ───────────────────────────────────────────────

func TestHandle(t *testing.T) {
	db := &mockDB{
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			if strings.Contains(sql, "upload_batches") {
				return []map[string]any{{
					"aircraft_id":   "aircraft-1",
					"registration":  "N123AB",
					"serial_number": nil,
					"make":          nil,
					"model":         nil,
				}}, nil
			}
			return []map[string]any{{
				"total":  int64(1),
				"done":   int64(1),
				"failed": int64(0),
			}}, nil
		},
	}

	h := &Handler{
		db:     db,
		s3:     &mockS3{},
		bucket: "test-bucket",
		gemini: &gemini.MockClient{
			GenerateContentFn: func(ctx context.Context, model string, parts []gemini.Part, config *gemini.GenerateConfig) (string, error) {
				return `{"pageType":"cover","entries":[]}`, nil
			},
		},
		secrets: &mockSecrets{},
	}

	err := h.Handle(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{
			{Body: `{"uploadId":"batch-1","pageId":"page-1","pageNumber":1,"s3Key":"pages/batch-1/page_0001.jpg"}`},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHandle_InvalidJSON(t *testing.T) {
	h := &Handler{
		db: &mockDB{},
	}

	err := h.Handle(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{
			{Body: `invalid json{{{`},
		},
	})

	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
	if !strings.Contains(err.Error(), "parse message") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestHandle_ProcessPageError(t *testing.T) {
	// With slicing, per-slice Gemini failures are non-fatal warnings.
	// The process succeeds with 0 entries extracted.
	db := &mockDB{
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			if strings.Contains(sql, "upload_batches") {
				return []map[string]any{{
					"aircraft_id":   "aircraft-1",
					"registration":  "N123AB",
					"serial_number": nil,
					"make":          nil,
					"model":         nil,
				}}, nil
			}
			return []map[string]any{{
				"total":  int64(1),
				"done":   int64(1),
				"failed": int64(0),
			}}, nil
		},
		execFn: func(ctx context.Context, sql string, args ...any) error {
			return nil
		},
	}

	h := &Handler{
		db:     db,
		s3:     &mockS3{},
		bucket: "test-bucket",
		gemini: &gemini.MockClient{
			GenerateContentFn: func(ctx context.Context, model string, parts []gemini.Part, config *gemini.GenerateConfig) (string, error) {
				return "", fmt.Errorf("gemini error")
			},
		},
		secrets: &mockSecrets{},
	}

	err := h.Handle(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{
			{Body: `{"uploadId":"batch-1","pageId":"page-1","pageNumber":1,"s3Key":"pages/batch-1/page_0001.jpg"}`},
		},
	})

	// Per-slice Gemini errors are warnings, not fatal — process completes with 0 entries.
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ─── Tests: Normalize/Fuzzy ─────────────────────────────────────────────────

func TestNormalize(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"N-123AB", "N123AB"},
		{"cessna 172", "CESSNA172"},
		{"  hello  ", "HELLO"},
	}
	for _, tt := range tests {
		got := normalize(tt.in)
		if got != tt.want {
			t.Errorf("normalize(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestFuzzyMatch(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{"Cessna", "CESSNA", true},
		{"172N", "172", true},
		{"Piper", "Cessna", false},
	}
	for _, tt := range tests {
		got := fuzzyMatch(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("fuzzyMatch(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestFormatEmbedding(t *testing.T) {
	result := formatEmbedding([]float32{0.1, 0.2, 0.3})
	if result != "[0.1,0.2,0.3]" {
		t.Errorf("got %q, want %q", result, "[0.1,0.2,0.3]")
	}
}

// ─── Tests: SaveEntry ────────────────────────────────────────────────────

func TestSaveEntry(t *testing.T) {
	tests := []struct {
		name           string
		entry          extractedEntry
		wantInsertSQL  string
		wantPartCalls  int
		wantADCalls    int
		wantInspection bool
	}{
		{
			name: "entry with parts actions",
			entry: extractedEntry{
				Date:                 "2024-01-15",
				EntryType:            "maintenance",
				MaintenanceNarrative: "Oil change and parts replaced",
				Confidence:           0.95,
				PartsActions: []partsActionRec{
					{
						Action:     "replaced",
						PartName:   "Oil Filter",
						PartNumber: "OH-123",
						Quantity:   1,
					},
					{
						Action:       "replaced",
						PartName:     "Spark Plug",
						PartNumber:   "SP-456",
						OldPartNumber: "SP-123",
						Quantity:     4,
					},
				},
			},
			wantPartCalls: 2,
		},
		{
			name: "entry with AD compliance",
			entry: extractedEntry{
				Date:                 "2024-02-20",
				EntryType:            "ad_compliance",
				MaintenanceNarrative: "Complied with AD 2024-01-01",
				ADCompliance: []adComplianceRec{
					{
						ADNumber: "2024-01-01",
						Method:   "inspection",
						Notes:    "Inspected per AD",
					},
					{
						ADNumber: "2023-05-10",
						Method:   "replacement",
						Notes:    "Replaced part",
					},
				},
			},
			wantADCalls: 2,
		},
		{
			name: "inspection entry",
			entry: extractedEntry{
				Date:                "2024-03-15",
				EntryType:           "inspection",
				InspectionType:      "annual",
				MaintenanceNarrative: "Annual inspection completed",
				FARReference:        "14 CFR 91.409",
				FlightTime:          1234.5,
			},
			wantInspection: true,
		},
		{
			name: "entry with no date - should skip",
			entry: extractedEntry{
				EntryType:            "maintenance",
				MaintenanceNarrative: "No date entry",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			insertCalls := 0
			execCalls := 0
			partCalls := 0
			adCalls := 0
			inspectionCalls := 0

			db := &mockDB{
				insertFn: func(ctx context.Context, sql string, args ...any) (string, error) {
					insertCalls++
					return "entry-id-1", nil
				},
				execFn: func(ctx context.Context, sql string, args ...any) error {
					execCalls++
					if strings.Contains(sql, "parts_actions") {
						partCalls++
					}
					if strings.Contains(sql, "ad_compliance") {
						adCalls++
					}
					if strings.Contains(sql, "inspection_records") {
						inspectionCalls++
					}
					return nil
				},
			}

			h := &Handler{
				db: db,
				gemini: &gemini.MockClient{
					EmbedContentFn: func(ctx context.Context, model string, text string) ([]float32, error) {
						return make([]float32, 768), nil
					},
				},
			}

			err := h.saveEntry(context.Background(), "aircraft-1", "page-1", &tt.entry)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// No date entries should be skipped
			if tt.entry.Date == "" {
				if insertCalls > 0 {
					t.Error("expected no insert for entry with no date")
				}
				return
			}

			if insertCalls != 1 {
				t.Errorf("expected 1 insert call, got %d", insertCalls)
			}

			if partCalls != tt.wantPartCalls {
				t.Errorf("expected %d parts actions, got %d", tt.wantPartCalls, partCalls)
			}

			if adCalls != tt.wantADCalls {
				t.Errorf("expected %d AD compliance records, got %d", tt.wantADCalls, adCalls)
			}

			if tt.wantInspection && inspectionCalls != 1 {
				t.Error("expected inspection record to be created")
			}
		})
	}
}

// ─── Tests: CheckBatchCompletion ─────────────────────────────────────────

func TestCheckBatchCompletion(t *testing.T) {
	tests := []struct {
		name           string
		total          int64
		done           int64
		failed         int64
		wantStatus     string
		wantExecCalled bool
	}{
		{
			name:           "all completed - no failures",
			total:          5,
			done:           5,
			failed:         0,
			wantStatus:     "completed",
			wantExecCalled: true,
		},
		{
			name:           "all failed",
			total:          3,
			done:           0,
			failed:         3,
			wantStatus:     "failed",
			wantExecCalled: true,
		},
		{
			name:           "mixed success and failure",
			total:          10,
			done:           7,
			failed:         3,
			wantStatus:     "completed_with_errors",
			wantExecCalled: true,
		},
		{
			name:           "still processing - not all done",
			total:          5,
			done:           3,
			failed:         0,
			wantExecCalled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			execCalled := false
			var capturedStatus string

			db := &mockDB{
				queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
					return []map[string]any{{
						"total":  tt.total,
						"done":   tt.done,
						"failed": tt.failed,
					}}, nil
				},
				execFn: func(ctx context.Context, sql string, args ...any) error {
					execCalled = true
					if strings.Contains(sql, "UPDATE upload_batches") {
						capturedStatus = fmt.Sprintf("%v", args[0])
					}
					return nil
				},
			}

			h := &Handler{db: db}
			h.checkBatchCompletion(context.Background(), "batch-1")

			if execCalled != tt.wantExecCalled {
				t.Errorf("execCalled = %v, want %v", execCalled, tt.wantExecCalled)
			}

			if tt.wantExecCalled && capturedStatus != tt.wantStatus {
				t.Errorf("status = %q, want %q", capturedStatus, tt.wantStatus)
			}
		})
	}
}

// ─── Tests: ProcessPage Error Paths ──────────────────────────────────────

func TestProcessPage_Errors(t *testing.T) {
	// With slicing, per-slice Gemini errors and invalid JSON are non-fatal
	// warnings (the slice is skipped). Only infrastructure errors (DB, S3 download)
	// are fatal.
	tests := []struct {
		name        string
		setupDB     func() *mockDB
		setupS3     func() *mockS3
		setupGemini func() *gemini.MockClient
		wantError   bool
	}{
		{
			name: "gemini error per slice — non-fatal",
			setupDB: func() *mockDB {
				return &mockDB{
					queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
						if strings.Contains(sql, "upload_batches") {
							return []map[string]any{{
								"aircraft_id":   "aircraft-1",
								"registration":  "N123AB",
								"serial_number": nil,
								"make":          nil,
								"model":         nil,
							}}, nil
						}
						return []map[string]any{{
							"total": int64(1), "done": int64(1), "failed": int64(0),
						}}, nil
					},
					execFn: func(ctx context.Context, sql string, args ...any) error {
						return nil
					},
				}
			},
			setupS3: func() *mockS3 {
				return &mockS3{}
			},
			setupGemini: func() *gemini.MockClient {
				return &gemini.MockClient{
					GenerateContentFn: func(ctx context.Context, model string, parts []gemini.Part, config *gemini.GenerateConfig) (string, error) {
						return "", fmt.Errorf("gemini api error")
					},
				}
			},
			wantError: false,
		},
		{
			name: "invalid json response per slice — non-fatal",
			setupDB: func() *mockDB {
				return &mockDB{
					queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
						if strings.Contains(sql, "upload_batches") {
							return []map[string]any{{
								"aircraft_id":   "aircraft-1",
								"registration":  "N123AB",
								"serial_number": nil,
								"make":          nil,
								"model":         nil,
							}}, nil
						}
						return []map[string]any{{
							"total": int64(1), "done": int64(1), "failed": int64(0),
						}}, nil
					},
					execFn: func(ctx context.Context, sql string, args ...any) error {
						return nil
					},
				}
			},
			setupS3: func() *mockS3 {
				return &mockS3{}
			},
			setupGemini: func() *gemini.MockClient {
				return &gemini.MockClient{
					GenerateContentFn: func(ctx context.Context, model string, parts []gemini.Part, config *gemini.GenerateConfig) (string, error) {
						return "not valid json{{{", nil
					},
				}
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Handler{
				db:      tt.setupDB(),
				s3:      tt.setupS3(),
				gemini:  tt.setupGemini(),
				bucket:  "test-bucket",
				secrets: &mockSecrets{},
			}

			err := h.processPage(context.Background(), pageMessage{
				UploadID:   "batch-1",
				PageID:     "page-1",
				PageNumber: 1,
				S3Key:      "pages/batch-1/page_0001.jpg",
			})

			if tt.wantError && err == nil {
				t.Error("expected error but got nil")
			}
			if !tt.wantError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// ─── Tests: Helper Functions ─────────────────────────────────────────────

func TestStrVal(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want string
	}{
		{"nil", nil, ""},
		{"string", "hello", "hello"},
		{"int", 123, "123"},
		{"bool", true, "true"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := strVal(tt.in)
			if got != tt.want {
				t.Errorf("strVal(%v) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestToInt64(t *testing.T) {
	tests := []struct {
		name   string
		in     any
		want   int64
		wantOk bool
	}{
		{"int64", int64(123), 123, true},
		{"int32", int32(456), 456, true},
		{"int", int(789), 789, true},
		{"float64", float64(100.5), 100, true},
		{"string", "not a number", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := toInt64(tt.in)
			if got != tt.want {
				t.Errorf("toInt64(%v) = %d, want %d", tt.in, got, tt.want)
			}
			if ok != tt.wantOk {
				t.Errorf("toInt64(%v) ok = %v, want %v", tt.in, ok, tt.wantOk)
			}
		})
	}
}

func TestMarkPageFailed(t *testing.T) {
	execCalled := false
	db := &mockDB{
		execFn: func(ctx context.Context, sql string, args ...any) error {
			execCalled = true
			if !strings.Contains(sql, "failed") {
				t.Error("expected 'failed' in SQL")
			}
			if args[0] != "page-123" {
				t.Errorf("expected page-123, got %v", args[0])
			}
			return nil
		},
	}

	h := &Handler{db: db}
	h.markPageFailed(context.Background(), "page-123")

	if !execCalled {
		t.Error("expected exec to be called")
	}
}

func TestSaveEntry_PartsActionNormalization(t *testing.T) {
	tests := []struct {
		name           string
		action         string
		wantAction     string
	}{
		{"valid action", "installed", "installed"},
		{"reinstalled maps to installed", "reinstalled", "installed"},
		{"serviced maps to repaired", "serviced", "repaired"},
		{"unknown action defaults", "unknown_action", "installed"},
		{"empty action defaults", "", "installed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedAction string
			db := &mockDB{
				insertFn: func(ctx context.Context, sql string, args ...any) (string, error) {
					return "entry-id-1", nil
				},
				execFn: func(ctx context.Context, sql string, args ...any) error {
					if strings.Contains(sql, "parts_actions") {
						capturedAction = fmt.Sprintf("%v", args[1])
					}
					return nil
				},
			}

			h := &Handler{
				db: db,
				gemini: &gemini.MockClient{
					EmbedContentFn: func(ctx context.Context, model string, text string) ([]float32, error) {
						return make([]float32, 768), nil
					},
				},
			}

			entry := &extractedEntry{
				Date:                 "2024-01-15",
				EntryType:            "maintenance",
				MaintenanceNarrative: "Test",
				PartsActions: []partsActionRec{
					{
						Action:   tt.action,
						PartName: "Test Part",
					},
				},
			}

			err := h.saveEntry(context.Background(), "aircraft-1", "page-1", entry)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if capturedAction != tt.wantAction {
				t.Errorf("action = %q, want %q", capturedAction, tt.wantAction)
			}
		})
	}
}

func TestSaveEntry_ADComplianceMethodNormalization(t *testing.T) {
	var capturedMethod string
	db := &mockDB{
		insertFn: func(ctx context.Context, sql string, args ...any) (string, error) {
			return "entry-id-1", nil
		},
		execFn: func(ctx context.Context, sql string, args ...any) error {
			if strings.Contains(sql, "ad_compliance") {
				capturedMethod = fmt.Sprintf("%v", args[4])
			}
			return nil
		},
	}

	h := &Handler{
		db: db,
		gemini: &gemini.MockClient{
			EmbedContentFn: func(ctx context.Context, model string, text string) ([]float32, error) {
				return make([]float32, 768), nil
			},
		},
	}

	entry := &extractedEntry{
		Date:                 "2024-01-15",
		EntryType:            "ad_compliance",
		MaintenanceNarrative: "Test",
		ADCompliance: []adComplianceRec{
			{
				ADNumber: "2024-01-01",
				Method:   "invalid_method",
			},
		},
	}

	err := h.saveEntry(context.Background(), "aircraft-1", "page-1", entry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedMethod != "other" {
		t.Errorf("method = %q, want %q", capturedMethod, "other")
	}
}

func TestProcessPage_EmptyGeminiResponse(t *testing.T) {
	db := &mockDB{
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			if strings.Contains(sql, "upload_batches") {
				return []map[string]any{{
					"aircraft_id":   "aircraft-1",
					"registration":  "N123AB",
					"serial_number": nil,
					"make":          nil,
					"model":         nil,
				}}, nil
			}
			return []map[string]any{{
				"total":  int64(1),
				"done":   int64(1),
				"failed": int64(0),
			}}, nil
		},
		execFn: func(ctx context.Context, sql string, args ...any) error {
			return nil
		},
	}

	h := &Handler{
		db:     db,
		s3:     &mockS3{},
		bucket: "test-bucket",
		gemini: &gemini.MockClient{
			GenerateContentFn: func(ctx context.Context, model string, parts []gemini.Part, config *gemini.GenerateConfig) (string, error) {
				return "", nil // Empty response
			},
		},
		secrets: &mockSecrets{},
	}

	err := h.processPage(context.Background(), pageMessage{
		UploadID:   "batch-1",
		PageID:     "page-1",
		PageNumber: 1,
		S3Key:      "pages/batch-1/page_0001.jpg",
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGenerateEmbedding_Error(t *testing.T) {
	db := &mockDB{
		execFn: func(ctx context.Context, sql string, args ...any) error {
			return nil
		},
	}

	h := &Handler{
		db: db,
		gemini: &gemini.MockClient{
			EmbedContentFn: func(ctx context.Context, model string, text string) ([]float32, error) {
				return nil, fmt.Errorf("embedding api error")
			},
		},
	}

	err := h.generateEmbedding(context.Background(), "entry-123", "test narrative")
	if err == nil {
		t.Fatal("expected error from embedding API")
	}
	if !strings.Contains(err.Error(), "embed content") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestProcessPage_UploadBatchNotFound(t *testing.T) {
	db := &mockDB{
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			// Return empty result
			return []map[string]any{}, nil
		},
		execFn: func(ctx context.Context, sql string, args ...any) error {
			return nil
		},
	}

	h := &Handler{
		db:     db,
		s3:     &mockS3{},
		bucket: "test-bucket",
		gemini: &gemini.MockClient{
			GenerateContentFn: func(ctx context.Context, model string, parts []gemini.Part, config *gemini.GenerateConfig) (string, error) {
				return `{"pageType":"maintenance_entry","entries":[]}`, nil
			},
		},
		secrets: &mockSecrets{},
	}

	err := h.processPage(context.Background(), pageMessage{
		UploadID:   "batch-999",
		PageID:     "page-1",
		PageNumber: 1,
		S3Key:      "pages/batch-999/page_0001.jpg",
	})

	if err == nil {
		t.Fatal("expected error for missing upload batch")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestSaveEntry_InspectionTypeNormalization(t *testing.T) {
	var capturedType string
	db := &mockDB{
		insertFn: func(ctx context.Context, sql string, args ...any) (string, error) {
			return "entry-id-1", nil
		},
		execFn: func(ctx context.Context, sql string, args ...any) error {
			if strings.Contains(sql, "inspection_records") {
				capturedType = fmt.Sprintf("%v", args[2])
			}
			return nil
		},
	}

	h := &Handler{
		db: db,
		gemini: &gemini.MockClient{
			EmbedContentFn: func(ctx context.Context, model string, text string) ([]float32, error) {
				return make([]float32, 768), nil
			},
		},
	}

	entry := &extractedEntry{
		Date:                 "2024-01-15",
		EntryType:            "inspection",
		InspectionType:       "invalid_type",
		MaintenanceNarrative: "Test",
	}

	err := h.saveEntry(context.Background(), "aircraft-1", "page-1", entry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedType != "other" {
		t.Errorf("inspection type = %q, want %q", capturedType, "other")
	}
}

func TestCheckBatchCompletion_QueryError(t *testing.T) {
	db := &mockDB{
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			return nil, fmt.Errorf("database error")
		},
	}

	h := &Handler{db: db}
	// Should not panic or return error, just log
	h.checkBatchCompletion(context.Background(), "batch-1")
}

func TestCheckBatchCompletion_EmptyResult(t *testing.T) {
	db := &mockDB{
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			return []map[string]any{}, nil
		},
	}

	h := &Handler{db: db}
	// Should handle empty result gracefully
	h.checkBatchCompletion(context.Background(), "batch-1")
}

func TestProcessPage_DBUpdateError(t *testing.T) {
	db := &mockDB{
		execFn: func(ctx context.Context, sql string, args ...any) error {
			return fmt.Errorf("db update failed")
		},
	}

	h := &Handler{
		db:     db,
		s3:     &mockS3{},
		bucket: "test-bucket",
	}

	err := h.processPage(context.Background(), pageMessage{
		UploadID:   "batch-1",
		PageID:     "page-1",
		PageNumber: 1,
		S3Key:      "pages/batch-1/page_0001.jpg",
	})

	if err == nil {
		t.Fatal("expected error from DB update")
	}
	if !strings.Contains(err.Error(), "mark processing") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestProcessPage_S3GetObjectError(t *testing.T) {
	db := &mockDB{
		execFn: func(ctx context.Context, sql string, args ...any) error {
			return nil
		},
	}

	h := &Handler{
		db:     db,
		s3:     &mockS3{},
		bucket: "test-bucket",
		gemini: &gemini.MockClient{
			GenerateContentFn: func(ctx context.Context, model string, parts []gemini.Part, config *gemini.GenerateConfig) (string, error) {
				return "", fmt.Errorf("simulated error")
			},
		},
		secrets: &mockSecrets{},
	}

	// This will fail at Gemini step
	err := h.processPage(context.Background(), pageMessage{
		UploadID:   "batch-1",
		PageID:     "page-1",
		PageNumber: 1,
		S3Key:      "pages/batch-1/page_0001.jpg",
	})

	// Should get an error from Gemini
	if err == nil {
		t.Fatal("expected error from processing")
	}
}

func TestSaveEntry_MissingDataHandling(t *testing.T) {
	db := &mockDB{
		insertFn: func(ctx context.Context, sql string, args ...any) (string, error) {
			// Check that missing_data is properly passed
			return "entry-id-1", nil
		},
		execFn: func(ctx context.Context, sql string, args ...any) error {
			return nil
		},
	}

	h := &Handler{
		db: db,
		gemini: &gemini.MockClient{
			EmbedContentFn: func(ctx context.Context, model string, text string) ([]float32, error) {
				return make([]float32, 768), nil
			},
		},
	}

	entry := &extractedEntry{
		Date:                 "2024-01-15",
		EntryType:            "maintenance",
		MaintenanceNarrative: "Test",
		MissingData:          []string{"aircraft_hours", "mechanic_cert"},
	}

	err := h.saveEntry(context.Background(), "aircraft-1", "page-1", entry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSaveEntry_ShortNarrative(t *testing.T) {
	insertCalled := false
	db := &mockDB{
		insertFn: func(ctx context.Context, sql string, args ...any) (string, error) {
			insertCalled = true
			return "entry-id-1", nil
		},
		execFn: func(ctx context.Context, sql string, args ...any) error {
			return nil
		},
	}

	h := &Handler{
		db: db,
		gemini: &gemini.MockClient{
			EmbedContentFn: func(ctx context.Context, model string, text string) ([]float32, error) {
				t.Error("EmbedContent should not be called for short narrative")
				return nil, nil
			},
		},
	}

	entry := &extractedEntry{
		Date:                 "2024-01-15",
		EntryType:            "maintenance",
		MaintenanceNarrative: "Short", // Less than 10 characters
	}

	err := h.saveEntry(context.Background(), "aircraft-1", "page-1", entry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !insertCalled {
		t.Error("expected insert to be called")
	}
}

// ─── Tests: Slicing Integration ──────────────────────────────────────────

func TestProcessPage_WithSlicing(t *testing.T) {
	// Create a JPEG with 3 dark bands → slicer should produce 3 slices.
	testJPEG := makeTestJPEG(200, 600, [][2]int{
		{50, 130},
		{230, 330},
		{430, 530},
	})

	extractCalls := 0
	qaCalls := 0
	insertCalls := 0
	s3Mock := &mockS3{
		getObjectFn: func(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(testJPEG)), nil
		},
	}

	db := &mockDB{
		execFn: func(ctx context.Context, sql string, args ...any) error {
			return nil
		},
		insertFn: func(ctx context.Context, sql string, args ...any) (string, error) {
			insertCalls++
			return fmt.Sprintf("entry-id-%d", insertCalls), nil
		},
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			if strings.Contains(sql, "upload_batches") {
				return []map[string]any{{
					"aircraft_id":   "aircraft-1",
					"registration":  "N123AB",
					"serial_number": "12345",
					"make":          "Cessna",
					"model":         "172N",
				}}, nil
			}
			return []map[string]any{{
				"total":  int64(1),
				"done":   int64(1),
				"failed": int64(0),
			}}, nil
		},
	}

	h := &Handler{
		db:     db,
		s3:     s3Mock,
		bucket: "test-bucket",
		gemini: &gemini.MockClient{
			GenerateContentFn: func(ctx context.Context, model string, parts []gemini.Part, config *gemini.GenerateConfig) (string, error) {
				// Detect QA calls by checking if the prompt contains the QA marker
				for _, p := range parts {
					if strings.Contains(p.Text, "QA specialist") {
						qaCalls++
						return `{"results":[{"entryIndex":0,"verdict":"pass","issues":[],"summary":"All fields match"}]}`, nil
					}
				}
				extractCalls++
				return fmt.Sprintf(`{"pageType":"maintenance_entry","entries":[{"date":"2024-01-%02d","entryType":"maintenance","maintenanceNarrative":"Entry %d oil change and filter replacement","confidence":0.95}]}`, extractCalls, extractCalls), nil
			},
			EmbedContentFn: func(ctx context.Context, model string, text string) ([]float32, error) {
				return make([]float32, 768), nil
			},
		},
		secrets: &mockSecrets{},
	}

	err := h.processPage(context.Background(), pageMessage{
		UploadID:   "batch-1",
		PageID:     "page-1",
		PageNumber: 1,
		S3Key:      "pages/batch-1/page_0001.jpg",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Gemini extraction called once per slice (3 slices).
	if extractCalls != 3 {
		t.Errorf("extractCalls = %d, want 3", extractCalls)
	}

	// QA called once per slice (3 slices, all pass on first attempt).
	if qaCalls != 3 {
		t.Errorf("qaCalls = %d, want 3", qaCalls)
	}

	// Each slice returns 1 entry → 3 inserts.
	if insertCalls != 3 {
		t.Errorf("insertCalls = %d, want 3", insertCalls)
	}

	// Slices should be uploaded to S3.
	if len(s3Mock.putCalls) != 3 {
		t.Errorf("s3 putCalls = %d, want 3", len(s3Mock.putCalls))
	}
	for _, call := range s3Mock.putCalls {
		if !strings.HasPrefix(call.key, "slices/batch-1/page_0001/slice_") {
			t.Errorf("unexpected s3 key: %s", call.key)
		}
		if call.contentType != "image/jpeg" {
			t.Errorf("unexpected content type: %s", call.contentType)
		}
	}
}

func TestProcessPage_SlicerFallback(t *testing.T) {
	// Invalid image bytes → slicer fails → fallback to full image → 1 extract + 1 QA call.
	extractCalls := 0
	qaCalls := 0
	s3Mock := &mockS3{}

	db := &mockDB{
		execFn: func(ctx context.Context, sql string, args ...any) error {
			return nil
		},
		insertFn: func(ctx context.Context, sql string, args ...any) (string, error) {
			return "entry-id-1", nil
		},
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			if strings.Contains(sql, "upload_batches") {
				return []map[string]any{{
					"aircraft_id":   "aircraft-1",
					"registration":  "N123AB",
					"serial_number": nil,
					"make":          nil,
					"model":         nil,
				}}, nil
			}
			return []map[string]any{{
				"total":  int64(1),
				"done":   int64(1),
				"failed": int64(0),
			}}, nil
		},
	}

	h := &Handler{
		db:     db,
		s3:     s3Mock,
		bucket: "test-bucket",
		gemini: &gemini.MockClient{
			GenerateContentFn: func(ctx context.Context, model string, parts []gemini.Part, config *gemini.GenerateConfig) (string, error) {
				for _, p := range parts {
					if strings.Contains(p.Text, "QA specialist") {
						qaCalls++
						return `{"results":[{"entryIndex":0,"verdict":"pass","issues":[],"summary":"OK"}]}`, nil
					}
				}
				extractCalls++
				return `{"pageType":"maintenance_entry","entries":[{"date":"2024-01-15","entryType":"maintenance","maintenanceNarrative":"Changed oil","confidence":0.9}]}`, nil
			},
			EmbedContentFn: func(ctx context.Context, model string, text string) ([]float32, error) {
				return make([]float32, 768), nil
			},
		},
		secrets: &mockSecrets{},
	}

	err := h.processPage(context.Background(), pageMessage{
		UploadID:   "batch-1",
		PageID:     "page-1",
		PageNumber: 1,
		S3Key:      "pages/batch-1/page_0001.jpg",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Slicer fails on "fake-image-data" → falls back to 1 slice → 1 extract + 1 QA.
	if extractCalls != 1 {
		t.Errorf("extractCalls = %d, want 1 (fallback to full image)", extractCalls)
	}
	if qaCalls != 1 {
		t.Errorf("qaCalls = %d, want 1", qaCalls)
	}
}

// ─── Tests: QA Verification ──────────────────────────────────────────────

func TestExtractAndVerifySlice_QAPass(t *testing.T) {
	// QA passes on first attempt — entries saved without review flag.
	extractCalls := 0
	qaCalls := 0

	mockGemini := &gemini.MockClient{
		GenerateContentFn: func(ctx context.Context, model string, parts []gemini.Part, config *gemini.GenerateConfig) (string, error) {
			for _, p := range parts {
				if strings.Contains(p.Text, "QA specialist") {
					qaCalls++
					return `{"results":[{"entryIndex":0,"verdict":"pass","issues":[],"summary":"All fields verified"}]}`, nil
				}
			}
			extractCalls++
			return `{"pageType":"maintenance_entry","entries":[{"date":"2024-01-15","entryType":"maintenance","maintenanceNarrative":"Changed oil and filter","confidence":0.95}]}`, nil
		},
	}

	h := &Handler{secrets: &mockSecrets{}}

	entries, pageType, err := h.extractAndVerifySlice(context.Background(), []byte("img"), "image/jpeg", mockGemini, 0, "page-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].NeedsReview {
		t.Error("entry should not need review when QA passes")
	}
	if pageType != "maintenance_entry" {
		t.Errorf("pageType = %q, want %q", pageType, "maintenance_entry")
	}
	if extractCalls != 1 {
		t.Errorf("extractCalls = %d, want 1", extractCalls)
	}
	if qaCalls != 1 {
		t.Errorf("qaCalls = %d, want 1", qaCalls)
	}
}

func TestExtractAndVerifySlice_QAFail_RetrySucceeds(t *testing.T) {
	// QA fails on first attempt with critical issue, retry extraction passes QA.
	extractCalls := 0
	qaCalls := 0

	mockGemini := &gemini.MockClient{
		GenerateContentFn: func(ctx context.Context, model string, parts []gemini.Part, config *gemini.GenerateConfig) (string, error) {
			for _, p := range parts {
				if strings.Contains(p.Text, "QA specialist") {
					qaCalls++
					if qaCalls == 1 {
						return `{"results":[{"entryIndex":0,"verdict":"fail","issues":[{"field":"maintenanceNarrative","issue":"truncated","expected":"full text here","extracted":"partial","severity":"critical"}],"summary":"Narrative truncated"}]}`, nil
					}
					return `{"results":[{"entryIndex":0,"verdict":"pass","issues":[],"summary":"All fields match after retry"}]}`, nil
				}
			}
			extractCalls++
			if extractCalls == 1 {
				return `{"pageType":"maintenance_entry","entries":[{"date":"2024-01-15","entryType":"maintenance","maintenanceNarrative":"partial","confidence":0.9}]}`, nil
			}
			return `{"pageType":"maintenance_entry","entries":[{"date":"2024-01-15","entryType":"maintenance","maintenanceNarrative":"full text here","confidence":0.95}]}`, nil
		},
	}

	h := &Handler{secrets: &mockSecrets{}}

	entries, _, err := h.extractAndVerifySlice(context.Background(), []byte("img"), "image/jpeg", mockGemini, 0, "page-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].NeedsReview {
		t.Error("entry should not need review after successful retry")
	}
	if entries[0].MaintenanceNarrative != "full text here" {
		t.Errorf("narrative = %q, want corrected version", entries[0].MaintenanceNarrative)
	}
	if extractCalls != 2 {
		t.Errorf("extractCalls = %d, want 2", extractCalls)
	}
	if qaCalls != 2 {
		t.Errorf("qaCalls = %d, want 2", qaCalls)
	}
}

func TestExtractAndVerifySlice_QAFail_MaxRetries(t *testing.T) {
	// QA fails on both attempts — entries flagged for review.
	mockGemini := &gemini.MockClient{
		GenerateContentFn: func(ctx context.Context, model string, parts []gemini.Part, config *gemini.GenerateConfig) (string, error) {
			for _, p := range parts {
				if strings.Contains(p.Text, "QA specialist") {
					return `{"results":[{"entryIndex":0,"verdict":"fail","issues":[{"field":"date","issue":"incorrect","expected":"2024-02-15","extracted":"2024-01-15","severity":"critical"}],"summary":"Wrong date"}]}`, nil
				}
			}
			return `{"pageType":"maintenance_entry","entries":[{"date":"2024-01-15","entryType":"maintenance","maintenanceNarrative":"Oil change","confidence":0.9}]}`, nil
		},
	}

	h := &Handler{secrets: &mockSecrets{}}

	entries, _, err := h.extractAndVerifySlice(context.Background(), []byte("img"), "image/jpeg", mockGemini, 0, "page-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if !entries[0].NeedsReview {
		t.Error("entry should be flagged for review after max retries")
	}
}

func TestExtractAndVerifySlice_QANeedsReview(t *testing.T) {
	// QA returns needs_review — accepted without retry, flagged for review.
	mockGemini := &gemini.MockClient{
		GenerateContentFn: func(ctx context.Context, model string, parts []gemini.Part, config *gemini.GenerateConfig) (string, error) {
			for _, p := range parts {
				if strings.Contains(p.Text, "QA specialist") {
					return `{"results":[{"entryIndex":0,"verdict":"needs_review","issues":[{"field":"mechanicCertificate","issue":"incorrect","expected":"unclear","extracted":"12345","severity":"minor"}],"summary":"Certificate number ambiguous"}]}`, nil
				}
			}
			return `{"pageType":"maintenance_entry","entries":[{"date":"2024-01-15","entryType":"maintenance","maintenanceNarrative":"Oil change","mechanicCertificate":"12345","confidence":0.85}]}`, nil
		},
	}

	h := &Handler{secrets: &mockSecrets{}}

	entries, _, err := h.extractAndVerifySlice(context.Background(), []byte("img"), "image/jpeg", mockGemini, 0, "page-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if !entries[0].NeedsReview {
		t.Error("entry should be flagged for review with needs_review verdict")
	}
	if !strings.Contains(entries[0].ExtractionNotes, "Certificate number ambiguous") {
		t.Errorf("extraction notes should contain QA summary, got: %q", entries[0].ExtractionNotes)
	}
}

func TestExtractAndVerifySlice_ClaudeError(t *testing.T) {
	// Claude client fails — falls back to Gemini for QA.
	qaCalls := 0

	mockGemini := &gemini.MockClient{
		GenerateContentFn: func(ctx context.Context, model string, parts []gemini.Part, config *gemini.GenerateConfig) (string, error) {
			for _, p := range parts {
				if strings.Contains(p.Text, "QA specialist") {
					qaCalls++
					return `{"results":[{"entryIndex":0,"verdict":"pass","issues":[],"summary":"OK"}]}`, nil
				}
			}
			return `{"pageType":"maintenance_entry","entries":[{"date":"2024-01-15","entryType":"maintenance","maintenanceNarrative":"Oil change","confidence":0.95}]}`, nil
		},
	}

	mockClaude := &anthropic.MockClient{
		CreateMessageFn: func(ctx context.Context, model string, maxTokens int64, messages []anthropic.Message) (string, error) {
			return "", fmt.Errorf("claude API error")
		},
	}

	h := &Handler{
		claude:  mockClaude,
		secrets: &mockSecrets{},
	}

	entries, _, err := h.extractAndVerifySlice(context.Background(), []byte("img"), "image/jpeg", mockGemini, 0, "page-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	// Should have fallen back to Gemini QA
	if qaCalls != 1 {
		t.Errorf("gemini QA calls = %d, want 1 (fallback from Claude)", qaCalls)
	}
}

func TestExtractAndVerifySlice_NoClaude(t *testing.T) {
	// No Claude client configured — Gemini used for QA.
	qaCalls := 0

	mockGemini := &gemini.MockClient{
		GenerateContentFn: func(ctx context.Context, model string, parts []gemini.Part, config *gemini.GenerateConfig) (string, error) {
			for _, p := range parts {
				if strings.Contains(p.Text, "QA specialist") {
					qaCalls++
					return `{"results":[{"entryIndex":0,"verdict":"pass","issues":[],"summary":"OK"}]}`, nil
				}
			}
			return `{"pageType":"maintenance_entry","entries":[{"date":"2024-01-15","entryType":"maintenance","maintenanceNarrative":"Oil change","confidence":0.95}]}`, nil
		},
	}

	h := &Handler{
		// No claude client set — should use Gemini fallback
		secrets: &mockSecrets{},
	}

	entries, _, err := h.extractAndVerifySlice(context.Background(), []byte("img"), "image/jpeg", mockGemini, 0, "page-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if qaCalls != 1 {
		t.Errorf("gemini QA calls = %d, want 1", qaCalls)
	}
}

func TestExtractAndVerifySlice_EmptyExtraction(t *testing.T) {
	// Empty extraction (blank/header slice) — QA skipped entirely.
	qaCalls := 0

	mockGemini := &gemini.MockClient{
		GenerateContentFn: func(ctx context.Context, model string, parts []gemini.Part, config *gemini.GenerateConfig) (string, error) {
			for _, p := range parts {
				if strings.Contains(p.Text, "QA specialist") {
					qaCalls++
					return `{"results":[]}`, nil
				}
			}
			return `{"pageType":"blank","entries":[]}`, nil
		},
	}

	h := &Handler{secrets: &mockSecrets{}}

	entries, pageType, err := h.extractAndVerifySlice(context.Background(), []byte("img"), "image/jpeg", mockGemini, 0, "page-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(entries))
	}
	if pageType != "blank" {
		t.Errorf("pageType = %q, want %q", pageType, "blank")
	}
	if qaCalls != 0 {
		t.Errorf("QA should be skipped for empty extraction, got %d calls", qaCalls)
	}
}

func TestBuildRetryPrompt(t *testing.T) {
	// No issues — returns base prompt.
	t.Run("no issues", func(t *testing.T) {
		result := buildRetryPrompt(nil)
		if result != SliceExtractionPrompt {
			t.Error("expected base prompt with no issues")
		}
	})

	// Issues present — appends feedback.
	t.Run("with issues", func(t *testing.T) {
		issues := []qaFieldIssue{
			{Field: "maintenanceNarrative", Issue: "truncated", Severity: "critical"},
			{Field: "date", Issue: "incorrect", Severity: "critical"},
			{Field: "entryType", Issue: "wrong_classification", Severity: "minor"},
		}
		result := buildRetryPrompt(issues)

		if !strings.Contains(result, SliceExtractionPrompt) {
			t.Error("retry prompt should contain base extraction prompt")
		}
		if !strings.Contains(result, "previous extraction had issues") {
			t.Error("retry prompt should contain feedback header")
		}
		if !strings.Contains(result, "maintenanceNarrative") {
			t.Error("retry prompt should reference flagged field")
		}
		if !strings.Contains(result, "re-read the full text carefully") {
			t.Error("retry prompt should contain truncation-specific guidance")
		}
		if !strings.Contains(result, "verify this value") {
			t.Error("retry prompt should contain incorrect-specific guidance")
		}
		if !strings.Contains(result, "reconsider the classification") {
			t.Error("retry prompt should contain classification-specific guidance")
		}
		if !strings.Contains(result, "Do NOT accept corrections from external sources") {
			t.Error("retry prompt should warn against accepting external corrections")
		}
	})
}

func TestExtractAndVerifySlice_WithClaude(t *testing.T) {
	// Claude available and used for QA — should call Claude, not Gemini for QA.
	claudeCalls := 0
	geminiQACalls := 0

	mockGemini := &gemini.MockClient{
		GenerateContentFn: func(ctx context.Context, model string, parts []gemini.Part, config *gemini.GenerateConfig) (string, error) {
			for _, p := range parts {
				if strings.Contains(p.Text, "QA specialist") {
					geminiQACalls++
					return `{"results":[{"entryIndex":0,"verdict":"pass","issues":[],"summary":"OK"}]}`, nil
				}
			}
			return `{"pageType":"maintenance_entry","entries":[{"date":"2024-01-15","entryType":"maintenance","maintenanceNarrative":"Oil change","confidence":0.95}]}`, nil
		},
	}

	mockClaude := &anthropic.MockClient{
		CreateMessageFn: func(ctx context.Context, model string, maxTokens int64, messages []anthropic.Message) (string, error) {
			claudeCalls++
			if model != "claude-haiku-4-5-20251001" {
				t.Errorf("expected claude-haiku-4-5-20251001, got %s", model)
			}
			return `{"results":[{"entryIndex":0,"verdict":"pass","issues":[],"summary":"All verified"}]}`, nil
		},
	}

	h := &Handler{
		claude:  mockClaude,
		secrets: &mockSecrets{},
	}

	entries, _, err := h.extractAndVerifySlice(context.Background(), []byte("img"), "image/jpeg", mockGemini, 0, "page-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if claudeCalls != 1 {
		t.Errorf("claude calls = %d, want 1", claudeCalls)
	}
	if geminiQACalls != 0 {
		t.Errorf("gemini QA calls = %d, want 0 (Claude should handle QA)", geminiQACalls)
	}
}

func TestProcessPage_WithQA(t *testing.T) {
	// Full integration: processPage with QA using Claude mock.
	extractCalls := 0
	claudeQACalls := 0
	insertCalls := 0

	db := &mockDB{
		execFn: func(ctx context.Context, sql string, args ...any) error {
			return nil
		},
		insertFn: func(ctx context.Context, sql string, args ...any) (string, error) {
			insertCalls++
			return fmt.Sprintf("entry-id-%d", insertCalls), nil
		},
		queryFn: func(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
			if strings.Contains(sql, "upload_batches") {
				return []map[string]any{{
					"aircraft_id":   "aircraft-1",
					"registration":  "N123AB",
					"serial_number": "12345",
					"make":          "Cessna",
					"model":         "172N",
				}}, nil
			}
			return []map[string]any{{
				"total":  int64(1),
				"done":   int64(1),
				"failed": int64(0),
			}}, nil
		},
	}

	h := &Handler{
		db:     db,
		s3:     &mockS3{},
		bucket: "test-bucket",
		gemini: &gemini.MockClient{
			GenerateContentFn: func(ctx context.Context, model string, parts []gemini.Part, config *gemini.GenerateConfig) (string, error) {
				extractCalls++
				return `{"pageType":"maintenance_entry","entries":[{"date":"2024-01-15","entryType":"maintenance","maintenanceNarrative":"Changed oil and filter per SB 1234","confidence":0.95}]}`, nil
			},
			EmbedContentFn: func(ctx context.Context, model string, text string) ([]float32, error) {
				return make([]float32, 768), nil
			},
		},
		claude: &anthropic.MockClient{
			CreateMessageFn: func(ctx context.Context, model string, maxTokens int64, messages []anthropic.Message) (string, error) {
				claudeQACalls++
				return `{"results":[{"entryIndex":0,"verdict":"pass","issues":[],"summary":"Verified"}]}`, nil
			},
		},
		secrets: &mockSecrets{},
	}

	err := h.processPage(context.Background(), pageMessage{
		UploadID:   "batch-1",
		PageID:     "page-1",
		PageNumber: 1,
		S3Key:      "pages/batch-1/page_0001.jpg",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if extractCalls != 1 {
		t.Errorf("extractCalls = %d, want 1", extractCalls)
	}
	if claudeQACalls != 1 {
		t.Errorf("claudeQACalls = %d, want 1", claudeQACalls)
	}
	if insertCalls != 1 {
		t.Errorf("insertCalls = %d, want 1", insertCalls)
	}
}

// TestQAWithRealLLMs sends an image through extraction + QA with real APIs.
//
// Usage:
//
//	GEMINI_API_KEY=... ANTHROPIC_API_KEY=... TEST_IMAGE_PATH=/path/to/slice.jpg go test ./analyze/ -run TestQAWithRealLLMs -v -count=1
func TestQAWithRealLLMs(t *testing.T) {
	geminiKey := os.Getenv("GEMINI_API_KEY")
	imgPath := os.Getenv("TEST_IMAGE_PATH")
	if geminiKey == "" || imgPath == "" {
		t.Skip("set GEMINI_API_KEY and TEST_IMAGE_PATH to run this test")
	}

	ctx := context.Background()
	geminiClient, err := gemini.New(ctx, geminiKey)
	if err != nil {
		t.Fatalf("create gemini client: %v", err)
	}

	data, err := os.ReadFile(imgPath)
	if err != nil {
		t.Fatalf("read image: %v", err)
	}
	t.Logf("Image: %s (%d bytes)", imgPath, len(data))

	h := &Handler{secrets: &mockSecrets{}}

	// Set up Claude if key is available
	anthropicKey := os.Getenv("ANTHROPIC_API_KEY")
	if anthropicKey != "" {
		h.claude = anthropic.New(anthropicKey)
		t.Log("Using Claude for QA")
	} else {
		t.Log("No ANTHROPIC_API_KEY set, using Gemini for QA")
	}

	entries, pageType, err := h.extractAndVerifySlice(ctx, data, "image/jpeg", geminiClient, 0, "test-page")
	if err != nil {
		t.Fatalf("extract+verify failed: %v", err)
	}

	t.Logf("pageType=%q, entries=%d", pageType, len(entries))
	for i, e := range entries {
		t.Logf("  Entry %d: date=%s type=%s needsReview=%v", i, e.Date, e.EntryType, e.NeedsReview)
		if e.ExtractionNotes != "" {
			t.Logf("    Notes: %s", e.ExtractionNotes)
		}
		if len(e.MaintenanceNarrative) > 100 {
			t.Logf("    Narrative: %.100s...", e.MaintenanceNarrative)
		} else {
			t.Logf("    Narrative: %s", e.MaintenanceNarrative)
		}
	}
}

// TestExtractionWithRealLLM sends an image through the actual Gemini API with
// the SliceExtractionPrompt and prints the response. Use this to verify LLM
// behavior on specific images (e.g., scanner backgrounds, blank pages).
//
// Usage:
//
//	GEMINI_API_KEY=... TEST_IMAGE_PATH=/tmp/slicer-pdf-batch/.../slice_001.jpg go test ./analyze/ -run TestExtractionWithRealLLM -v -count=1
func TestExtractionWithRealLLM(t *testing.T) {
	apiKey := os.Getenv("GEMINI_API_KEY")
	imgPath := os.Getenv("TEST_IMAGE_PATH")
	if apiKey == "" || imgPath == "" {
		t.Skip("set GEMINI_API_KEY and TEST_IMAGE_PATH to run this test")
	}

	ctx := context.Background()
	client, err := gemini.New(ctx, apiKey)
	if err != nil {
		t.Fatalf("create gemini client: %v", err)
	}

	data, err := os.ReadFile(imgPath)
	if err != nil {
		t.Fatalf("read image: %v", err)
	}
	t.Logf("Image: %s (%d bytes)", imgPath, len(data))

	temp := float32(0.1)
	resp, err := client.GenerateContent(ctx, "gemini-2.5-flash", []gemini.Part{
		{Text: SliceExtractionPrompt},
		{Data: data, MIMEType: "image/jpeg"},
	}, &gemini.GenerateConfig{
		Temperature:      &temp,
		ResponseMIMEType: "application/json",
	})
	if err != nil {
		t.Fatalf("gemini call failed: %v", err)
	}

	// Pretty-print the JSON response.
	var parsed json.RawMessage
	if err := json.Unmarshal([]byte(resp), &parsed); err != nil {
		t.Logf("Raw response (not JSON): %s", resp)
	} else {
		pretty, _ := json.MarshalIndent(parsed, "", "  ")
		t.Logf("Response:\n%s", pretty)
	}

	// Parse and check entries.
	var result extractionResult
	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		t.Fatalf("parse response: %v", err)
	}
	t.Logf("pageType=%q, entries=%d", result.PageType, len(result.Entries))
}

func TestExtractBatchID(t *testing.T) {
	tests := []struct {
		key  string
		want string
	}{
		{"pages/batch-123/page_0001.jpg", "batch-123"},
		{"pages/abc-def-ghi/page_0003.jpg", "abc-def-ghi"},
		{"singlepart", "unknown"},
		{"a/b/c/d", "b"},
	}
	for _, tt := range tests {
		got := extractBatchID(tt.key)
		if got != tt.want {
			t.Errorf("extractBatchID(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}
