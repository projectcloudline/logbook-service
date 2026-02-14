package main

import (
	"context"
	cryptoRand "crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"

	"github.com/projectcloudline/logbook-service/internal/awsutil"
	"github.com/projectcloudline/logbook-service/internal/db"
	"github.com/projectcloudline/logbook-service/internal/gemini"
	"github.com/projectcloudline/logbook-service/internal/models"
)

// Handler holds dependencies for all API endpoints.
type Handler struct {
	db      db.DB
	s3      awsutil.S3Client
	secrets awsutil.SecretsProvider
	gemini  gemini.Client
	bucket  string
}

var pdfExtensions = map[string]bool{".pdf": true}

var imageExtensions = map[string]bool{
	".jpg": true, ".jpeg": true, ".png": true, ".gif": true,
	".bmp": true, ".tiff": true, ".tif": true, ".heic": true, ".heif": true,
}

var contentTypeMap = map[string]string{
	".jpg": "image/jpeg", ".jpeg": "image/jpeg",
	".png": "image/png", ".gif": "image/gif",
	".bmp": "image/bmp", ".tiff": "image/tiff", ".tif": "image/tiff",
	".heic": "image/heic", ".heif": "image/heif",
	".pdf": "application/pdf",
}

// Handle routes incoming events to the appropriate handler.
func (h *Handler) Handle(ctx context.Context, rawEvent json.RawMessage) (events.APIGatewayProxyResponse, error) {
	// Check for EventBridge warmer
	var warmer struct {
		Source string `json:"source"`
	}
	if json.Unmarshal(rawEvent, &warmer) == nil && warmer.Source == "logbook.warmer" {
		return events.APIGatewayProxyResponse{StatusCode: 200, Body: "warm"}, nil
	}

	var event events.APIGatewayProxyRequest
	if err := json.Unmarshal(rawEvent, &event); err != nil {
		return errResponse(400, "invalid request")
	}

	method := event.HTTPMethod
	path := event.Resource
	pathParams := event.PathParameters

	switch {
	case path == "/uploads" && method == "POST":
		return h.handleUpload(ctx, event)
	case path == "/uploads/{id}/status" && method == "GET":
		return h.handleStatus(ctx, pathParams["id"])
	case path == "/uploads/{id}/pages/{pageNumber}/image" && method == "GET":
		return h.handlePageImage(ctx, pathParams["id"], pathParams["pageNumber"])
	case path == "/aircraft/{tailNumber}/uploads" && method == "GET":
		return h.handleListUploads(ctx, pathParams["tailNumber"])
	case path == "/aircraft/{tailNumber}/summary" && method == "GET":
		return h.handleSummary(ctx, pathParams["tailNumber"])
	case path == "/aircraft/{tailNumber}/query" && method == "POST":
		return h.handleQuery(ctx, pathParams["tailNumber"], event)
	case path == "/aircraft/{tailNumber}/entries" && method == "GET":
		return h.handleEntries(ctx, pathParams["tailNumber"], event)
	case path == "/aircraft/{tailNumber}/entries/{entryId}" && method == "GET":
		return h.handleEntryDetail(ctx, pathParams["tailNumber"], pathParams["entryId"])
	case path == "/aircraft/{tailNumber}/entries/{entryId}" && method == "PATCH":
		return h.handleUpdateEntry(ctx, pathParams["tailNumber"], pathParams["entryId"], event)
	case path == "/aircraft/{tailNumber}/inspections" && method == "GET":
		return h.handleInspections(ctx, pathParams["tailNumber"], event)
	case path == "/aircraft/{tailNumber}/ads" && method == "GET":
		return h.handleAds(ctx, pathParams["tailNumber"], event)
	case path == "/aircraft/{tailNumber}/parts" && method == "GET":
		return h.handleParts(ctx, pathParams["tailNumber"], event)
	default:
		return errResponse(404, "Not found")
	}
}

func errResponse(status int, msg string) (events.APIGatewayProxyResponse, error) {
	return models.APIResponse(status, map[string]string{"error": msg})
}

// getAircraftID looks up the aircraft ID by registration, returning an error response if not found.
func (h *Handler) getAircraftID(ctx context.Context, tailNumber string) (string, *events.APIGatewayProxyResponse, error) {
	tail := strings.ToUpper(tailNumber)
	rows, err := h.db.Query(ctx, "SELECT id FROM aircraft WHERE registration = $1", tail)
	if err != nil {
		return "", nil, err
	}
	if len(rows) == 0 {
		resp, _ := errResponse(404, fmt.Sprintf("Aircraft %s not found", tail))
		return "", &resp, nil
	}
	return fmt.Sprintf("%v", rows[0]["id"]), nil, nil
}

func (h *Handler) enrichAircraftFromFAA(ctx context.Context, aircraftID, tailNumber string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("WARNING: FAA enrichment panic for %s: %v", tailNumber, r)
		}
	}()

	apiKey, err := h.secrets.GetSecret(ctx, os.Getenv("FAA_REGISTRY_SECRET_ARN"))
	if err != nil {
		log.Printf("WARNING: FAA enrichment failed for %s: %v", tailNumber, err)
		return
	}

	url := fmt.Sprintf("%s/registry/%s", os.Getenv("FAA_REGISTRY_URL"), tailNumber)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	req.Header.Set("x-api-key", apiKey)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("WARNING: FAA enrichment failed for %s: %v", tailNumber, err)
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var data map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
		log.Printf("WARNING: FAA enrichment parse failed for %s: %v", tailNumber, err)
		return
	}

	_ = h.db.Exec(ctx,
		"UPDATE aircraft SET make = $1, model = $2, serial_number = $3, updated_at = NOW() WHERE id = $4",
		data["manufacturer"], data["model"], data["serialNumber"], aircraftID,
	)
}

// ─── POST /uploads ──────────────────────────────────────────────────────────

type uploadRequest struct {
	TailNumber string       `json:"tailNumber"`
	LogType    string       `json:"logType"`
	Files      []uploadFile `json:"files"`
}

type uploadFile struct {
	Filename string `json:"filename"`
}

func (h *Handler) handleUpload(ctx context.Context, event events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var req uploadRequest
	if err := json.Unmarshal([]byte(event.Body), &req); err != nil {
		return errResponse(400, "invalid request body")
	}

	tail := strings.ToUpper(strings.TrimSpace(req.TailNumber))
	if tail == "" {
		return errResponse(400, "tailNumber is required")
	}
	if len(req.Files) == 0 {
		return errResponse(400, "files array is required")
	}
	if len(req.Files) > 500 {
		return errResponse(400, "Maximum 500 files per upload")
	}

	// Classify files
	var pdfFiles, imgFiles []uploadFile
	for _, f := range req.Files {
		ext := strings.ToLower(filepath.Ext(f.Filename))
		if pdfExtensions[ext] {
			pdfFiles = append(pdfFiles, f)
		} else if imageExtensions[ext] {
			imgFiles = append(imgFiles, f)
		}
	}

	if len(pdfFiles) > 0 && len(imgFiles) > 0 {
		return errResponse(400, "Cannot mix PDF and image files in one upload")
	}
	if len(pdfFiles) == 0 && len(imgFiles) == 0 {
		return errResponse(400, "Files must be PDF (.pdf) or images (.jpg, .jpeg, .png, etc.)")
	}
	if len(pdfFiles) > 1 {
		return errResponse(400, "Only one PDF per upload")
	}

	// Upsert aircraft
	aircraftID, err := h.db.Insert(ctx,
		`INSERT INTO aircraft (registration) VALUES ($1)
		 ON CONFLICT (registration) DO UPDATE SET updated_at = NOW()
		 RETURNING id`, tail)
	if err != nil {
		return events.APIGatewayProxyResponse{}, fmt.Errorf("upsert aircraft: %w", err)
	}

	// Enrich with FAA data (non-blocking)
	h.enrichAircraftFromFAA(ctx, aircraftID, tail)

	batchID := newUUID()

	if len(pdfFiles) > 0 {
		return h.handlePDFUpload(ctx, batchID, aircraftID, req.LogType, pdfFiles[0])
	}
	return h.handleMultiImageUpload(ctx, batchID, aircraftID, req.LogType, imgFiles)
}

func (h *Handler) handlePDFUpload(ctx context.Context, batchID, aircraftID, logType string, file uploadFile) (events.APIGatewayProxyResponse, error) {
	filename := file.Filename
	if filename == "" {
		filename = "logbook.pdf"
	}
	s3Key := fmt.Sprintf("uploads/%s/%s", batchID, filename)

	_, err := h.db.Insert(ctx,
		`INSERT INTO upload_batches (id, aircraft_id, logbook_type, upload_type, source_filename, s3_key, processing_status)
		 VALUES ($1, $2, $3, 'pdf', $4, $5, 'pending') RETURNING id`,
		batchID, aircraftID, logType, filename, s3Key)
	if err != nil {
		return events.APIGatewayProxyResponse{}, fmt.Errorf("insert batch: %w", err)
	}

	uploadURL, err := h.s3.PresignPutObject(ctx, h.bucket, s3Key, "application/pdf", time.Hour)
	if err != nil {
		return events.APIGatewayProxyResponse{}, fmt.Errorf("presign: %w", err)
	}

	return models.APIResponse(200, map[string]any{
		"uploadId":   batchID,
		"uploadType": "pdf",
		"files": []map[string]any{
			{"filename": filename, "uploadUrl": uploadURL, "s3Key": s3Key},
		},
	})
}

func (h *Handler) handleMultiImageUpload(ctx context.Context, batchID, aircraftID, logType string, files []uploadFile) (events.APIGatewayProxyResponse, error) {
	pageCount := len(files)
	sourceName := files[0].Filename
	if pageCount > 1 {
		sourceName = fmt.Sprintf("%d images", pageCount)
	}

	_, err := h.db.Insert(ctx,
		`INSERT INTO upload_batches (id, aircraft_id, logbook_type, upload_type, source_filename, page_count, processing_status)
		 VALUES ($1, $2, $3, 'multi_image', $4, $5, 'pending') RETURNING id`,
		batchID, aircraftID, logType, sourceName, pageCount)
	if err != nil {
		return events.APIGatewayProxyResponse{}, fmt.Errorf("insert batch: %w", err)
	}

	var resultFiles []map[string]any
	for i, f := range files {
		pageNum := i + 1
		filename := f.Filename
		if filename == "" {
			filename = fmt.Sprintf("page_%04d.jpg", pageNum)
		}
		ext := strings.ToLower(filepath.Ext(filename))
		ct := contentTypeMap[ext]
		if ct == "" {
			ct = "image/jpeg"
		}
		pageKey := fmt.Sprintf("pages/%s/page_%04d%s", batchID, pageNum, ext)

		_, err := h.db.Insert(ctx,
			`INSERT INTO upload_pages (document_id, page_number, image_path, extraction_status)
			 VALUES ($1, $2, $3, 'pending') RETURNING id`,
			batchID, pageNum, pageKey)
		if err != nil {
			return events.APIGatewayProxyResponse{}, fmt.Errorf("insert page: %w", err)
		}

		url, err := h.s3.PresignPutObject(ctx, h.bucket, pageKey, ct, time.Hour)
		if err != nil {
			return events.APIGatewayProxyResponse{}, fmt.Errorf("presign: %w", err)
		}

		resultFiles = append(resultFiles, map[string]any{
			"filename":   filename,
			"pageNumber": pageNum,
			"uploadUrl":  url,
			"s3Key":      pageKey,
		})
	}

	return models.APIResponse(200, map[string]any{
		"uploadId":   batchID,
		"uploadType": "multi_image",
		"pageCount":  pageCount,
		"files":      resultFiles,
	})
}

// ─── GET /uploads/{id}/status ───────────────────────────────────────────────

func (h *Handler) handleStatus(ctx context.Context, batchID string) (events.APIGatewayProxyResponse, error) {
	rows, err := h.db.Query(ctx,
		`SELECT ub.id, ub.processing_status, ub.page_count, ub.source_filename,
		        ub.logbook_type, ub.upload_type, ub.created_at,
		        COUNT(up.id) FILTER (WHERE up.extraction_status = 'completed') AS completed_pages,
		        COUNT(up.id) FILTER (WHERE up.extraction_status = 'failed') AS failed_pages,
		        COUNT(up.id) FILTER (WHERE up.needs_review = TRUE) AS needs_review_pages,
		        COUNT(up.id) AS total_pages
		 FROM upload_batches ub
		 LEFT JOIN upload_pages up ON up.document_id = ub.id
		 WHERE ub.id = $1
		 GROUP BY ub.id`, batchID)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}
	if len(rows) == 0 {
		return errResponse(404, "Upload not found")
	}

	row := rows[0]
	pageCount := row["page_count"]
	if pageCount == nil || pageCount == int64(0) {
		pageCount = row["total_pages"]
	}

	result := map[string]any{
		"uploadId":         fmt.Sprintf("%v", row["id"]),
		"status":           row["processing_status"],
		"filename":         row["source_filename"],
		"logType":          row["logbook_type"],
		"uploadType":       row["upload_type"],
		"pageCount":        pageCount,
		"completedPages":   row["completed_pages"],
		"failedPages":      row["failed_pages"],
		"needsReviewPages": row["needs_review_pages"],
		"createdAt":        row["created_at"],
	}

	failedPages, _ := toInt64(row["failed_pages"])
	if failedPages > 0 {
		failedRows, err := h.db.Query(ctx,
			`SELECT page_number FROM upload_pages
			 WHERE document_id = $1 AND extraction_status = 'failed'
			 ORDER BY page_number`, batchID)
		if err == nil {
			var nums []any
			for _, r := range failedRows {
				nums = append(nums, r["page_number"])
			}
			result["failedPageNumbers"] = nums
		}
	}

	return models.APIResponse(200, result)
}

// ─── GET /uploads/{id}/pages/{pageNumber}/image ────────────────────────────

func (h *Handler) handlePageImage(ctx context.Context, batchID, pageNumber string) (events.APIGatewayProxyResponse, error) {
	rows, err := h.db.Query(ctx,
		`SELECT image_path FROM upload_pages WHERE document_id = $1 AND page_number = $2`,
		batchID, pageNumber)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}
	if len(rows) == 0 {
		return errResponse(404, "Page not found")
	}

	imagePath := fmt.Sprintf("%v", rows[0]["image_path"])
	imageURL, err := h.s3.PresignGetObject(ctx, h.bucket, imagePath, time.Hour)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}

	return models.APIResponse(200, map[string]any{
		"uploadId":   batchID,
		"pageNumber": pageNumber,
		"imageUrl":   imageURL,
	})
}

// ─── GET /aircraft/{tailNumber}/uploads ─────────────────────────────────────

func (h *Handler) handleListUploads(ctx context.Context, tailNumber string) (events.APIGatewayProxyResponse, error) {
	tail := strings.ToUpper(tailNumber)
	rows, err := h.db.Query(ctx,
		`SELECT ub.id, ub.logbook_type, ub.upload_type, ub.source_filename,
		        ub.processing_status, ub.page_count, ub.date_range_start,
		        ub.date_range_end, ub.created_at
		 FROM upload_batches ub
		 JOIN aircraft a ON ub.aircraft_id = a.id
		 WHERE a.registration = $1
		 ORDER BY ub.created_at DESC`, tail)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}

	return models.APIResponse(200, map[string]any{
		"tailNumber": tail,
		"uploads":    rows,
	})
}

// ─── GET /aircraft/{tailNumber}/summary ─────────────────────────────────────

func (h *Handler) handleSummary(ctx context.Context, tailNumber string) (events.APIGatewayProxyResponse, error) {
	tail := strings.ToUpper(tailNumber)

	aircraft, err := h.db.Query(ctx, "SELECT * FROM aircraft WHERE registration = $1", tail)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}
	if len(aircraft) == 0 {
		return errResponse(404, fmt.Sprintf("Aircraft %s not found", tail))
	}
	aid := fmt.Sprintf("%v", aircraft[0]["id"])

	annual, _ := h.db.Query(ctx,
		`SELECT me.entry_date, me.flight_time
		 FROM inspection_records ir
		 JOIN maintenance_entries me ON ir.entry_id = me.id
		 WHERE ir.aircraft_id = $1 AND ir.inspection_type = 'annual'
		 ORDER BY ir.inspection_date DESC LIMIT 1`, aid)

	hundredhr, _ := h.db.Query(ctx,
		`SELECT me.entry_date, me.flight_time
		 FROM inspection_records ir
		 JOIN maintenance_entries me ON ir.entry_id = me.id
		 WHERE ir.aircraft_id = $1 AND ir.inspection_type = '100hr'
		 ORDER BY ir.inspection_date DESC LIMIT 1`, aid)

	oil, _ := h.db.Query(ctx,
		`SELECT entry_date, flight_time FROM maintenance_entries
		 WHERE aircraft_id = $1
		   AND (lower(maintenance_narrative) LIKE '%%oil change%%'
		        OR lower(maintenance_narrative) LIKE '%%oil filter%%')
		 ORDER BY entry_date DESC LIMIT 1`, aid)

	tt, _ := h.db.Query(ctx,
		`SELECT flight_time FROM maintenance_entries
		 WHERE aircraft_id = $1 AND flight_time IS NOT NULL
		 ORDER BY entry_date DESC LIMIT 1`, aid)

	expirations, _ := h.db.Query(ctx,
		`SELECT 'life_limited_part' AS type, part_name AS name, expiration_date
		 FROM life_limited_parts WHERE aircraft_id = $1 AND is_active = TRUE
		   AND expiration_date IS NOT NULL AND expiration_date <= CURRENT_DATE + INTERVAL '90 days'
		 UNION ALL
		 SELECT inspection_type AS type, inspection_type || ' inspection' AS name, next_due_date AS expiration_date
		 FROM inspection_records WHERE aircraft_id = $1
		   AND next_due_date IS NOT NULL AND next_due_date <= CURRENT_DATE + INTERVAL '90 days'
		 ORDER BY expiration_date`, aid, aid)

	result := map[string]any{
		"tailNumber":          tail,
		"aircraft":            aircraft[0],
		"lastAnnual":          firstOrNil(annual),
		"last100hr":           firstOrNil(hundredhr),
		"lastOilChange":       firstOrNil(oil),
		"totalTime":           nil,
		"upcomingExpirations": expirations,
	}

	if len(tt) > 0 {
		result["totalTime"] = tt[0]["flight_time"]
	}

	return models.APIResponse(200, result)
}

// ─── POST /aircraft/{tailNumber}/query ──────────────────────────────────────

func (h *Handler) handleQuery(ctx context.Context, tailNumber string, event events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var body struct {
		Question string `json:"question"`
	}
	if err := json.Unmarshal([]byte(event.Body), &body); err != nil || strings.TrimSpace(body.Question) == "" {
		return errResponse(400, "question is required")
	}

	tail := strings.ToUpper(tailNumber)
	aid, notFound, err := h.getAircraftID(ctx, tail)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}
	if notFound != nil {
		return *notFound, nil
	}

	geminiClient, err := h.getGeminiClient(ctx)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}

	// Generate embedding for the question
	embedding, err := geminiClient.EmbedContent(ctx, "gemini-embedding-001", body.Question)
	if err != nil {
		return events.APIGatewayProxyResponse{}, fmt.Errorf("embed question: %w", err)
	}

	embeddingStr := formatEmbedding(embedding)

	results, err := h.db.Query(ctx,
		`SELECT me.chunk_text, me.chunk_type,
		        m.entry_date, m.entry_type, m.maintenance_narrative,
		        ir.inspection_type,
		        1 - (me.embedding <=> $1::halfvec) AS similarity
		 FROM maintenance_embeddings me
		 JOIN maintenance_entries m ON me.entry_id = m.id
		 LEFT JOIN inspection_records ir ON ir.entry_id = m.id
		 WHERE m.aircraft_id = $2
		 ORDER BY me.embedding <=> $1::halfvec
		 LIMIT 10`, embeddingStr, aid)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}

	if len(results) == 0 {
		return models.APIResponse(200, map[string]any{
			"tailNumber": tail,
			"question":   body.Question,
			"answer":     "No maintenance records found for this aircraft.",
			"sources":    []any{},
		})
	}

	// Build context for Gemini
	var contextParts []string
	for _, r := range results {
		label := fmt.Sprintf("%v", r["entry_type"])
		if it, ok := r["inspection_type"]; ok && it != nil {
			label = fmt.Sprintf("%s/%v", label, it)
		}
		contextParts = append(contextParts,
			fmt.Sprintf("[%v] (%s) %v", r["entry_date"], label, r["maintenance_narrative"]))
	}
	contextText := strings.Join(contextParts, "\n---\n")

	ragPrompt := fmt.Sprintf(`You are an aircraft maintenance expert assistant. Answer the question based ONLY on the maintenance records provided below.

Aircraft: %s

MAINTENANCE RECORDS:
%s

QUESTION: %s

Provide a clear, accurate answer. Cite specific dates and entries. If the records don't contain enough information, say so.`, tail, contextText, body.Question)

	temp := float32(0.2)
	answer, err := geminiClient.GenerateContent(ctx, "gemini-2.5-flash", []gemini.Part{
		{Text: ragPrompt},
	}, &gemini.GenerateConfig{Temperature: &temp})
	if err != nil {
		return events.APIGatewayProxyResponse{}, fmt.Errorf("generate answer: %w", err)
	}

	// Build sources (top 5)
	limit := 5
	if len(results) < limit {
		limit = len(results)
	}
	var sources []map[string]any
	for _, r := range results[:limit] {
		source := map[string]any{
			"date":           fmt.Sprintf("%v", r["entry_date"]),
			"type":           r["entry_type"],
			"inspectionType": r["inspection_type"],
		}
		if sim, ok := r["similarity"]; ok {
			source["similarity"] = sim
		}
		sources = append(sources, source)
	}

	return models.APIResponse(200, map[string]any{
		"tailNumber": tail,
		"question":   body.Question,
		"answer":     answer,
		"sources":    sources,
	})
}

// ─── GET /aircraft/{tailNumber}/entries ──────────────────────────────────────

func (h *Handler) handleEntries(ctx context.Context, tailNumber string, event events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	aid, notFound, err := h.getAircraftID(ctx, tailNumber)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}
	if notFound != nil {
		return *notFound, nil
	}

	qp := models.ParseQueryParams(event)
	entryType := qp.Params["type"]
	dateFrom := qp.Params["dateFrom"]
	dateTo := qp.Params["dateTo"]
	needsReview := qp.Params["needsReview"]

	whereClauses := []string{"me.aircraft_id = $1"}
	args := []any{aid}
	argIdx := 2

	if entryType != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("me.entry_type = $%d", argIdx))
		args = append(args, entryType)
		argIdx++
	}
	if dateFrom != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("me.entry_date >= $%d", argIdx))
		args = append(args, dateFrom)
		argIdx++
	}
	if dateTo != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("me.entry_date <= $%d", argIdx))
		args = append(args, dateTo)
		argIdx++
	}
	if strings.EqualFold(needsReview, "true") {
		whereClauses = append(whereClauses, "me.needs_review = TRUE")
	}

	whereSQL := strings.Join(whereClauses, " AND ")

	countRows, err := h.db.Query(ctx,
		fmt.Sprintf("SELECT COUNT(*) AS total FROM maintenance_entries me WHERE %s", whereSQL),
		args...)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}
	total, _ := toInt(countRows[0]["total"])

	queryArgs := append(args, qp.Limit, qp.Offset)
	entries, err := h.db.Query(ctx,
		fmt.Sprintf(`SELECT me.id, me.entry_type, me.entry_date, me.hobbs_time, me.tach_time,
		        me.flight_time, me.shop_name, me.mechanic_name,
		        me.maintenance_narrative, me.confidence_score, me.needs_review,
		        me.review_status, me.missing_data,
		        ir.inspection_type
		 FROM maintenance_entries me
		 LEFT JOIN inspection_records ir ON ir.entry_id = me.id
		 WHERE %s
		 ORDER BY me.entry_date DESC
		 LIMIT $%d OFFSET $%d`, whereSQL, argIdx, argIdx+1),
		queryArgs...)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}

	return models.APIResponse(200, map[string]any{
		"tailNumber": strings.ToUpper(tailNumber),
		"entries":    entries,
		"pagination": models.NewPagination(total, qp.Page, qp.Limit),
	})
}

// ─── GET /aircraft/{tailNumber}/entries/{entryId} ───────────────────────────

func (h *Handler) handleEntryDetail(ctx context.Context, tailNumber, entryID string) (events.APIGatewayProxyResponse, error) {
	aid, notFound, err := h.getAircraftID(ctx, tailNumber)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}
	if notFound != nil {
		return *notFound, nil
	}

	entries, err := h.db.Query(ctx,
		"SELECT me.* FROM maintenance_entries me WHERE me.id = $1 AND me.aircraft_id = $2",
		entryID, aid)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}
	if len(entries) == 0 {
		return errResponse(404, "Entry not found")
	}

	entry := entries[0]

	parts, _ := h.db.Query(ctx,
		"SELECT * FROM parts_actions WHERE entry_id = $1 ORDER BY created_at", entryID)
	ads, _ := h.db.Query(ctx,
		"SELECT * FROM ad_compliance WHERE entry_id = $1 ORDER BY compliance_date", entryID)
	inspections, _ := h.db.Query(ctx,
		"SELECT * FROM inspection_records WHERE entry_id = $1", entryID)

	entry["partsActions"] = parts
	entry["adCompliance"] = ads
	if len(inspections) > 0 {
		entry["inspectionRecord"] = inspections[0]
	} else {
		entry["inspectionRecord"] = nil
	}

	return models.APIResponse(200, map[string]any{
		"tailNumber": strings.ToUpper(tailNumber),
		"entry":      entry,
	})
}

// ─── PATCH /aircraft/{tailNumber}/entries/{entryId} ─────────────────────────

var patchableFields = map[string]string{
	"entryDate":           "entry_date",
	"entryType":           "entry_type",
	"hobbsTime":           "hobbs_time",
	"tachTime":            "tach_time",
	"flightTime":          "flight_time",
	"timeSinceOverhaul":   "time_since_overhaul",
	"shopName":            "shop_name",
	"shopAddress":         "shop_address",
	"shopPhone":           "shop_phone",
	"repairStationNumber": "repair_station_number",
	"mechanicName":        "mechanic_name",
	"mechanicCertificate": "mechanic_certificate",
	"workOrderNumber":     "work_order_number",
	"maintenanceNarrative": "maintenance_narrative",
}

func (h *Handler) handleUpdateEntry(ctx context.Context, tailNumber, entryID string, event events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	aid, notFound, err := h.getAircraftID(ctx, tailNumber)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}
	if notFound != nil {
		return *notFound, nil
	}

	var body map[string]any
	if err := json.Unmarshal([]byte(event.Body), &body); err != nil || len(body) == 0 {
		return errResponse(400, "Request body is required")
	}

	reviewStatus, _ := body["reviewStatus"].(string)
	reviewedBy, _ := body["reviewedBy"].(string)

	if reviewStatus != "" && reviewStatus != "approved" && reviewStatus != "corrected" && reviewStatus != "rejected" {
		return errResponse(400, "reviewStatus must be approved, corrected, or rejected")
	}

	var setClauses []string
	var values []any
	argIdx := 1

	for camel, col := range patchableFields {
		if v, ok := body[camel]; ok {
			setClauses = append(setClauses, fmt.Sprintf("%s = $%d", col, argIdx))
			values = append(values, v)
			argIdx++
		}
	}

	if reviewStatus != "" {
		setClauses = append(setClauses, fmt.Sprintf("review_status = $%d", argIdx))
		values = append(values, reviewStatus)
		argIdx++
		setClauses = append(setClauses, "reviewed_at = NOW()")
		if reviewedBy != "" {
			setClauses = append(setClauses, fmt.Sprintf("reviewed_by = $%d", argIdx))
			values = append(values, reviewedBy)
			argIdx++
		}
		if reviewStatus == "approved" || reviewStatus == "rejected" {
			setClauses = append(setClauses, "needs_review = FALSE")
		}
	}

	if len(setClauses) == 0 {
		return errResponse(400, "No fields to update")
	}

	setClauses = append(setClauses, "updated_at = NOW()")
	values = append(values, entryID, aid)

	rows, err := h.db.Query(ctx,
		fmt.Sprintf(`UPDATE maintenance_entries SET %s WHERE id = $%d AND aircraft_id = $%d RETURNING id`,
			strings.Join(setClauses, ", "), argIdx, argIdx+1),
		values...)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}
	if len(rows) == 0 {
		return errResponse(404, "Entry not found")
	}

	return h.handleEntryDetail(ctx, tailNumber, entryID)
}

// ─── GET /aircraft/{tailNumber}/inspections ─────────────────────────────────

func (h *Handler) handleInspections(ctx context.Context, tailNumber string, event events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	aid, notFound, err := h.getAircraftID(ctx, tailNumber)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}
	if notFound != nil {
		return *notFound, nil
	}

	qp := models.ParseQueryParams(event)
	inspectionType := qp.Params["type"]

	whereClauses := []string{"ir.aircraft_id = $1"}
	args := []any{aid}
	argIdx := 2

	if inspectionType != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("ir.inspection_type = $%d", argIdx))
		args = append(args, inspectionType)
		argIdx++
	}

	whereSQL := strings.Join(whereClauses, " AND ")

	countRows, err := h.db.Query(ctx,
		fmt.Sprintf("SELECT COUNT(*) AS total FROM inspection_records ir WHERE %s", whereSQL),
		args...)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}
	total, _ := toInt(countRows[0]["total"])

	queryArgs := append(args, qp.Limit, qp.Offset)
	inspections, err := h.db.Query(ctx,
		fmt.Sprintf(`SELECT ir.id, ir.inspection_type, ir.inspection_date, ir.aircraft_hours,
		        ir.next_due_date, ir.next_due_hours, ir.far_reference,
		        ir.inspector_name, ir.inspector_certificate, ir.notes,
		        me.maintenance_narrative, me.shop_name
		 FROM inspection_records ir
		 LEFT JOIN maintenance_entries me ON ir.entry_id = me.id
		 WHERE %s
		 ORDER BY ir.inspection_date DESC
		 LIMIT $%d OFFSET $%d`, whereSQL, argIdx, argIdx+1),
		queryArgs...)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}

	latestByType, _ := h.db.Query(ctx,
		`SELECT DISTINCT ON (ir.inspection_type)
		        ir.inspection_type, ir.inspection_date, ir.next_due_date, ir.next_due_hours
		 FROM inspection_records ir
		 WHERE ir.aircraft_id = $1
		 ORDER BY ir.inspection_type, ir.inspection_date DESC`, aid)

	return models.APIResponse(200, map[string]any{
		"tailNumber":   strings.ToUpper(tailNumber),
		"inspections":  inspections,
		"latestByType": latestByType,
		"pagination":   models.NewPagination(total, qp.Page, qp.Limit),
	})
}

// ─── GET /aircraft/{tailNumber}/ads ─────────────────────────────────────────

func (h *Handler) handleAds(ctx context.Context, tailNumber string, event events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	aid, notFound, err := h.getAircraftID(ctx, tailNumber)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}
	if notFound != nil {
		return *notFound, nil
	}

	qp := models.ParseQueryParams(event)

	countRows, err := h.db.Query(ctx,
		"SELECT COUNT(*) AS total FROM ad_compliance WHERE aircraft_id = $1", aid)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}
	total, _ := toInt(countRows[0]["total"])

	ads, err := h.db.Query(ctx,
		`SELECT ad.id, ad.ad_number, ad.compliance_date, ad.compliance_method,
		        ad.next_due_date, ad.next_due_hours, ad.notes,
		        me.entry_date, me.maintenance_narrative, me.shop_name
		 FROM ad_compliance ad
		 LEFT JOIN maintenance_entries me ON ad.entry_id = me.id
		 WHERE ad.aircraft_id = $1
		 ORDER BY ad.compliance_date DESC
		 LIMIT $2 OFFSET $3`, aid, qp.Limit, qp.Offset)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}

	return models.APIResponse(200, map[string]any{
		"tailNumber": strings.ToUpper(tailNumber),
		"ads":        ads,
		"pagination": models.NewPagination(total, qp.Page, qp.Limit),
	})
}

// ─── GET /aircraft/{tailNumber}/parts ───────────────────────────────────────

func (h *Handler) handleParts(ctx context.Context, tailNumber string, event events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	aid, notFound, err := h.getAircraftID(ctx, tailNumber)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}
	if notFound != nil {
		return *notFound, nil
	}

	params := event.QueryStringParameters
	status := "active"
	if params != nil {
		if v, ok := params["status"]; ok {
			status = v
		}
	}

	whereClauses := []string{"aircraft_id = $1"}
	args := []any{aid}

	if status != "all" {
		whereClauses = append(whereClauses, "is_active = TRUE")
	}
	whereSQL := strings.Join(whereClauses, " AND ")

	parts, err := h.db.Query(ctx,
		fmt.Sprintf(`SELECT id, part_name, part_number, serial_number,
		        install_date, install_hours, life_limit_hours, life_limit_months,
		        expiration_date, is_active, removal_date, notes
		 FROM life_limited_parts
		 WHERE %s
		 ORDER BY expiration_date ASC NULLS LAST`, whereSQL),
		args...)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}

	return models.APIResponse(200, map[string]any{
		"tailNumber": strings.ToUpper(tailNumber),
		"parts":      parts,
		"total":      len(parts),
	})
}

// ─── Helpers ────────────────────────────────────────────────────────────────

func (h *Handler) getGeminiClient(ctx context.Context) (gemini.Client, error) {
	if h.gemini != nil {
		return h.gemini, nil
	}

	apiKey := os.Getenv("GEMINI_API_KEY")
	if apiKey == "" {
		secretJSON, err := h.secrets.GetSecretJSON(ctx, os.Getenv("GEMINI_SECRET_ARN"))
		if err != nil {
			return nil, fmt.Errorf("get gemini secret: %w", err)
		}
		apiKey = secretJSON["GEMINI_API_KEY"]
	}

	client, err := gemini.New(ctx, apiKey)
	if err != nil {
		return nil, err
	}
	h.gemini = client
	return client, nil
}

func formatEmbedding(embedding []float32) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, v := range embedding {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%g", v)
	}
	b.WriteByte(']')
	return b.String()
}

func firstOrNil(rows []map[string]any) any {
	if len(rows) > 0 {
		return rows[0]
	}
	return nil
}

func toInt64(v any) (int64, bool) {
	switch val := v.(type) {
	case int64:
		return val, true
	case int32:
		return int64(val), true
	case int:
		return int64(val), true
	case float64:
		return int64(val), true
	default:
		return 0, false
	}
}

func toInt(v any) (int, bool) {
	i, ok := toInt64(v)
	return int(i), ok
}

func newUUID() string {
	var uuid [16]byte
	_, _ = cryptoRand.Read(uuid[:])
	uuid[6] = (uuid[6] & 0x0f) | 0x40 // version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
}
