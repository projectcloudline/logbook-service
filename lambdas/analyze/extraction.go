package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/projectcloudline/logbook-service/internal/gemini"
	"github.com/projectcloudline/logbook-service/internal/slicer"
)

var mimeTypeMap = map[string]string{
	".jpg": "image/jpeg", ".jpeg": "image/jpeg",
	".png": "image/png", ".gif": "image/gif",
	".bmp": "image/bmp", ".tiff": "image/tiff", ".tif": "image/tiff",
	".heic": "image/heic", ".heif": "image/heif",
}

func (h *Handler) processPage(ctx context.Context, msg pageMessage) error {
	// Mark page as processing
	if err := h.db.Exec(ctx,
		"UPDATE upload_pages SET extraction_status = 'processing' WHERE id = $1",
		msg.PageID); err != nil {
		return fmt.Errorf("mark processing: %w", err)
	}

	// Download image from S3
	ext := strings.ToLower(filepath.Ext(msg.S3Key))
	if ext == "" {
		ext = ".jpg"
	}
	mimeType := mimeTypeMap[ext]
	if mimeType == "" {
		mimeType = "image/jpeg"
	}

	reader, err := h.s3.GetObject(ctx, h.bucket, msg.S3Key)
	if err != nil {
		return fmt.Errorf("download image: %w", err)
	}
	defer reader.Close()

	imageBytes, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("read image: %w", err)
	}

	// Slice image into individual entry strips
	slices, sliceErr := slicer.SliceImage(imageBytes, slicer.DefaultOptions())
	if sliceErr != nil {
		// Fallback: use the full image as a single slice
		log.Printf("WARNING: slicer failed for page %s, using full image: %v", msg.PageID, sliceErr)
		slices = []slicer.Slice{{Index: 0, ImageData: imageBytes, Y0: 0, Y1: 0}}
	}
	log.Printf("Page %s: sliced into %d strips", msg.PageID, len(slices))

	// Call Gemini for each slice and collect entries
	geminiClient, err := h.getGeminiClient(ctx)
	if err != nil {
		return fmt.Errorf("get gemini client: %w", err)
	}

	batchID := extractBatchID(msg.S3Key)
	var allEntries []extractedEntry
	var lastPageType string

	temp := float32(0.1)
	for _, sl := range slices {
		// Upload slice to S3 for debugging/audit (non-fatal)
		sliceKey := fmt.Sprintf("slices/%s/page_%04d/slice_%03d.jpg", batchID, msg.PageNumber, sl.Index)
		if putErr := h.s3.PutObject(ctx, h.bucket, sliceKey, "image/jpeg", bytes.NewReader(sl.ImageData)); putErr != nil {
			log.Printf("WARNING: failed to upload slice %s: %v", sliceKey, putErr)
		}

		// Determine which image data and MIME type to send.
		// For fallback (slicer failed), slices contain the original bytes which may be PNG/etc.
		sliceMIME := "image/jpeg"
		sliceData := sl.ImageData
		if sliceErr != nil {
			// Fallback: use original image bytes and MIME type
			sliceMIME = mimeType
			sliceData = imageBytes
		}

		responseText, geminiErr := geminiClient.GenerateContent(ctx, "gemini-2.5-flash", []gemini.Part{
			{Text: SliceExtractionPrompt},
			{Data: sliceData, MIMEType: sliceMIME},
		}, &gemini.GenerateConfig{
			Temperature:      &temp,
			ResponseMIMEType: "application/json",
		})
		if geminiErr != nil {
			log.Printf("WARNING: gemini failed for slice %d of page %s: %v", sl.Index, msg.PageID, geminiErr)
			continue
		}

		responseText = cleanMarkdownFences(responseText)
		if responseText == "" {
			log.Printf("WARNING: empty Gemini response for slice %d of page %s", sl.Index, msg.PageID)
			continue
		}

		var sliceExtraction extractionResult
		if err := json.Unmarshal([]byte(responseText), &sliceExtraction); err != nil {
			log.Printf("WARNING: parse failed for slice %d of page %s: %v", sl.Index, msg.PageID, err)
			continue
		}

		allEntries = append(allEntries, sliceExtraction.Entries...)
		if sliceExtraction.PageType != "" {
			lastPageType = sliceExtraction.PageType
		}
	}

	// Build combined extraction result
	extraction := extractionResult{
		PageType: lastPageType,
		Entries:  allEntries,
	}
	if extraction.PageType == "" {
		extraction.PageType = "other"
	}

	// Store raw extraction
	rawJSON, _ := json.Marshal(extraction)
	if err := h.db.Exec(ctx,
		`UPDATE upload_pages SET raw_extraction = $1, page_type = $2,
		 extraction_model = 'gemini-2.5-flash', extraction_timestamp = NOW()
		 WHERE id = $3`,
		string(rawJSON), extraction.PageType, msg.PageID); err != nil {
		return fmt.Errorf("store extraction: %w", err)
	}

	// Get aircraft identity for validation
	rows, err := h.db.Query(ctx,
		`SELECT ub.aircraft_id, a.registration, a.serial_number, a.make, a.model
		 FROM upload_batches ub
		 JOIN aircraft a ON ub.aircraft_id = a.id
		 WHERE ub.id = $1`, msg.UploadID)
	if err != nil {
		return fmt.Errorf("get aircraft: %w", err)
	}
	if len(rows) == 0 {
		return fmt.Errorf("upload batch %s not found", msg.UploadID)
	}

	aircraftID := fmt.Sprintf("%v", rows[0]["aircraft_id"])
	expected := expectedIdentity{
		registration: strVal(rows[0]["registration"]),
		serialNumber: strVal(rows[0]["serial_number"]),
		make:         strVal(rows[0]["make"]),
		model:        strVal(rows[0]["model"]),
	}

	// Process each entry
	for i := range extraction.Entries {
		checkAircraftIdentity(&extraction.Entries[i], expected)
		if err := h.saveEntry(ctx, aircraftID, msg.PageID, &extraction.Entries[i]); err != nil {
			log.Printf("WARNING: save entry failed: %v", err)
		}
	}

	// Mark page complete
	needsReview := false
	for _, e := range extraction.Entries {
		if e.NeedsReview {
			needsReview = true
			break
		}
	}
	if err := h.db.Exec(ctx,
		"UPDATE upload_pages SET extraction_status = 'completed', needs_review = $1 WHERE id = $2",
		needsReview, msg.PageID); err != nil {
		return fmt.Errorf("mark complete: %w", err)
	}

	// Check batch completion
	h.checkBatchCompletion(ctx, msg.UploadID)

	log.Printf("Page %s: extracted %d entries from %d slices", msg.PageID, len(extraction.Entries), len(slices))
	return nil
}

// extractBatchID parses the batch ID from an S3 key like "pages/{batchId}/page_0001.jpg".
func extractBatchID(s3Key string) string {
	parts := strings.Split(s3Key, "/")
	if len(parts) >= 2 {
		return parts[1]
	}
	return "unknown"
}

func (h *Handler) markPageFailed(ctx context.Context, pageID string) {
	_ = h.db.Exec(ctx,
		"UPDATE upload_pages SET extraction_status = 'failed' WHERE id = $1", pageID)
}

func (h *Handler) checkBatchCompletion(ctx context.Context, batchID string) {
	rows, err := h.db.Query(ctx,
		`SELECT COUNT(*) AS total,
		        COUNT(*) FILTER (WHERE extraction_status IN ('completed', 'skipped')) AS done,
		        COUNT(*) FILTER (WHERE extraction_status = 'failed') AS failed
		 FROM upload_pages WHERE document_id = $1`, batchID)
	if err != nil {
		log.Printf("WARNING: check batch completion failed: %v", err)
		return
	}
	if len(rows) == 0 {
		return
	}

	total, _ := toInt64(rows[0]["total"])
	done, _ := toInt64(rows[0]["done"])
	failed, _ := toInt64(rows[0]["failed"])

	if total > 0 && done+failed == total {
		var status string
		switch {
		case failed == 0:
			status = "completed"
		case done == 0:
			status = "failed"
		default:
			status = "completed_with_errors"
		}
		_ = h.db.Exec(ctx,
			"UPDATE upload_batches SET processing_status = $1, updated_at = NOW() WHERE id = $2",
			status, batchID)
	}
}

func (h *Handler) getGeminiClient(ctx context.Context) (gemini.Client, error) {
	if h.gemini != nil {
		return h.gemini, nil
	}

	apiKey, err := h.secrets.GetSecret(ctx, fmt.Sprintf("%s", mustEnv("GEMINI_SECRET_ARN")))
	if err != nil {
		return nil, fmt.Errorf("get gemini secret: %w", err)
	}

	var secretMap map[string]string
	if err := json.Unmarshal([]byte(apiKey), &secretMap); err != nil {
		return nil, fmt.Errorf("parse gemini secret: %w", err)
	}

	client, err := gemini.New(ctx, secretMap["GEMINI_API_KEY"])
	if err != nil {
		return nil, err
	}
	h.gemini = client
	return client, nil
}

// ─── Entry Normalization & Saving ───────────────────────────────────────────

type extractionResult struct {
	PageType string           `json:"pageType"`
	Entries  []extractedEntry `json:"entries"`
}

type extractedEntry struct {
	Date                 string            `json:"date"`
	AircraftRegistration string            `json:"aircraftRegistration"`
	AircraftSerial       string            `json:"aircraftSerial"`
	AircraftMake         string            `json:"aircraftMake"`
	AircraftModel        string            `json:"aircraftModel"`
	HobbsTime            any               `json:"hobbsTime"`
	TachTime             any               `json:"tachTime"`
	FlightTime           any               `json:"flightTime"`
	TimeSinceOverhaul    any               `json:"timeSinceOverhaul"`
	ShopName             string            `json:"shopName"`
	ShopAddress          string            `json:"shopAddress"`
	ShopPhone            string            `json:"shopPhone"`
	RepairStationNumber  string            `json:"repairStationNumber"`
	MechanicName         string            `json:"mechanicName"`
	MechanicCertificate  string            `json:"mechanicCertificate"`
	WorkOrderNumber      string            `json:"workOrderNumber"`
	MaintenanceNarrative string            `json:"maintenanceNarrative"`
	EntryType            string            `json:"entryType"`
	InspectionType       string            `json:"inspectionType"`
	FARReference         string            `json:"farReference"`
	Confidence           any               `json:"confidence"`
	NeedsReview          bool              `json:"needsReview"`
	MissingData          []string          `json:"missingData"`
	ExtractionNotes      string            `json:"extractionNotes"`
	ADCompliance         []adComplianceRec `json:"adCompliance"`
	PartsActions         []partsActionRec  `json:"partsActions"`
}

type adComplianceRec struct {
	ADNumber string `json:"adNumber"`
	Method   string `json:"method"`
	Notes    string `json:"notes"`
}

type partsActionRec struct {
	Action          string `json:"action"`
	PartName        string `json:"partName"`
	PartNumber      string `json:"partNumber"`
	SerialNumber    string `json:"serialNumber"`
	OldPartNumber   string `json:"oldPartNumber"`
	OldSerialNumber string `json:"oldSerialNumber"`
	Quantity        any    `json:"quantity"`
	Notes           string `json:"notes"`
}

var legacyInspectionMap = map[string]string{
	"annual":            "annual",
	"100hr":             "100hr",
	"progressive":       "progressive",
	"altimeter_check":   "altimeter_static",
	"transponder_check": "transponder",
}

var validEntryTypes = map[string]bool{
	"maintenance": true, "inspection": true, "ad_compliance": true, "other": true,
}

var validActionTypes = map[string]bool{
	"installed": true, "removed": true, "replaced": true,
	"repaired": true, "inspected": true, "overhauled": true,
}

var actionTypeMap = map[string]string{
	"reinstalled": "installed",
	"serviced":    "repaired",
	"applied":     "installed",
	"adjusted":    "repaired",
	"cleaned":     "repaired",
	"tested":      "inspected",
	"calibrated":  "inspected",
	"lubricated":  "repaired",
}

var validComplianceMethods = map[string]bool{
	"inspection": true, "replacement": true, "modification": true,
	"terminating_action": true, "recurring": true, "not_applicable": true, "other": true,
}

var validInspectionTypes = map[string]bool{
	"annual": true, "100hr": true, "50hr": true, "progressive": true,
	"altimeter_static": true, "transponder": true, "elt": true, "other": true,
}

func normalizeEntryType(entry *extractedEntry) {
	if entry.EntryType == "" {
		entry.EntryType = "maintenance"
	}

	if mapped, ok := legacyInspectionMap[entry.EntryType]; ok {
		entry.InspectionType = mapped
		entry.EntryType = "inspection"
	} else if entry.EntryType == "inspection" && entry.InspectionType == "" {
		entry.InspectionType = "other"
	}

	if !validEntryTypes[entry.EntryType] {
		entry.EntryType = "other"
	}
}

func (h *Handler) saveEntry(ctx context.Context, aircraftID, pageID string, entry *extractedEntry) error {
	normalizeEntryType(entry)

	// Skip entries with no date
	if entry.Date == "" {
		log.Printf("  Skipping entry with no date (narrative: %.80s...)", entry.MaintenanceNarrative)
		return nil
	}

	// Insert maintenance_entries
	var missingData any
	if len(entry.MissingData) > 0 {
		missingData = entry.MissingData
	}

	entryID, err := h.db.Insert(ctx,
		`INSERT INTO maintenance_entries
		 (aircraft_id, page_id, entry_type, entry_date, hobbs_time, tach_time,
		  flight_time, time_since_overhaul, shop_name, shop_address, shop_phone,
		  repair_station_number, mechanic_name, mechanic_certificate,
		  work_order_number, maintenance_narrative, confidence_score,
		  needs_review, missing_data)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
		 RETURNING id`,
		aircraftID, pageID,
		entry.EntryType,
		entry.Date,
		entry.HobbsTime,
		entry.TachTime,
		entry.FlightTime,
		entry.TimeSinceOverhaul,
		entry.ShopName,
		entry.ShopAddress,
		entry.ShopPhone,
		entry.RepairStationNumber,
		entry.MechanicName,
		entry.MechanicCertificate,
		entry.WorkOrderNumber,
		entry.MaintenanceNarrative,
		entry.Confidence,
		entry.NeedsReview,
		missingData,
	)
	if err != nil {
		return fmt.Errorf("insert entry: %w", err)
	}

	// Parts actions
	for _, part := range entry.PartsActions {
		action := part.Action
		if action == "" {
			action = "installed"
		}
		if !validActionTypes[action] {
			if mapped, ok := actionTypeMap[action]; ok {
				action = mapped
			} else {
				action = "installed"
			}
		}
		quantity := part.Quantity
		if quantity == nil {
			quantity = 1
		}
		if err := h.db.Exec(ctx,
			`INSERT INTO parts_actions
			 (entry_id, action_type, part_name, part_number, serial_number,
			  old_part_number, old_serial_number, quantity, notes)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			entryID, action,
			part.PartName, part.PartNumber,
			part.SerialNumber, part.OldPartNumber,
			part.OldSerialNumber, quantity,
			part.Notes,
		); err != nil {
			log.Printf("WARNING: insert parts action failed: %v", err)
		}
	}

	// AD compliance
	for _, ad := range entry.ADCompliance {
		method := ad.Method
		if method != "" && !validComplianceMethods[method] {
			method = "other"
		}
		if err := h.db.Exec(ctx,
			`INSERT INTO ad_compliance
			 (entry_id, aircraft_id, ad_number, compliance_date, compliance_method, notes)
			 VALUES ($1,$2,$3,$4,$5,$6)`,
			entryID, aircraftID, ad.ADNumber,
			entry.Date, method, ad.Notes,
		); err != nil {
			log.Printf("WARNING: insert ad compliance failed: %v", err)
		}
	}

	// Inspection record
	if entry.InspectionType != "" {
		if !validInspectionTypes[entry.InspectionType] {
			entry.InspectionType = "other"
		}
		if err := h.db.Exec(ctx,
			`INSERT INTO inspection_records
			 (aircraft_id, entry_id, inspection_type, inspection_date,
			  aircraft_hours, far_reference, inspector_name, inspector_certificate)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
			aircraftID, entryID, entry.InspectionType,
			entry.Date, entry.FlightTime,
			entry.FARReference, entry.MechanicName,
			entry.MechanicCertificate,
		); err != nil {
			log.Printf("WARNING: insert inspection record failed: %v", err)
		}
	}

	// Generate embedding
	if len(entry.MaintenanceNarrative) > 10 {
		if err := h.generateEmbedding(ctx, entryID, entry.MaintenanceNarrative); err != nil {
			log.Printf("WARNING: embedding generation failed for entry %s: %v", entryID, err)
		}
	}

	return nil
}

func (h *Handler) generateEmbedding(ctx context.Context, entryID, text string) error {
	geminiClient, err := h.getGeminiClient(ctx)
	if err != nil {
		return err
	}

	embedding, err := geminiClient.EmbedContent(ctx, "gemini-embedding-001", text)
	if err != nil {
		return fmt.Errorf("embed content: %w", err)
	}

	embeddingStr := formatEmbedding(embedding)
	return h.db.Exec(ctx,
		`INSERT INTO maintenance_embeddings (entry_id, embedding, chunk_text, chunk_type)
		 VALUES ($1, $2::halfvec, $3, 'narrative')
		 ON CONFLICT (entry_id, chunk_type) DO UPDATE SET embedding = EXCLUDED.embedding, chunk_text = EXCLUDED.chunk_text`,
		entryID, embeddingStr, text)
}

// ─── Identity Checks ────────────────────────────────────────────────────────

type expectedIdentity struct {
	registration string
	serialNumber string
	make         string
	model        string
}

func normalize(s string) string {
	s = strings.ToUpper(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ReplaceAll(s, " ", "")
	return s
}

func fuzzyMatch(extracted, expected string) bool {
	a := normalize(extracted)
	b := normalize(expected)
	return strings.Contains(a, b) || strings.Contains(b, a)
}

func checkAircraftIdentity(entry *extractedEntry, expected expectedIdentity) {
	if expected.serialNumber == "" {
		return // No FAA data to compare against
	}
	if entry.AircraftSerial == "" {
		return // Gemini didn't extract a serial
	}

	serialMatch := normalize(entry.AircraftSerial) == normalize(expected.serialNumber)

	makeMatch := true
	modelMatch := true
	if entry.AircraftMake != "" && expected.make != "" {
		makeMatch = fuzzyMatch(entry.AircraftMake, expected.make)
	}
	if entry.AircraftModel != "" && expected.model != "" {
		modelMatch = fuzzyMatch(entry.AircraftModel, expected.model)
	}

	if !serialMatch || (!makeMatch && !modelMatch) {
		var reasons []string
		if !serialMatch {
			reasons = append(reasons, fmt.Sprintf("serial %q != %q", entry.AircraftSerial, expected.serialNumber))
		}
		if !makeMatch {
			reasons = append(reasons, fmt.Sprintf("make %q !~ %q", entry.AircraftMake, expected.make))
		}
		if !modelMatch {
			reasons = append(reasons, fmt.Sprintf("model %q !~ %q", entry.AircraftModel, expected.model))
		}

		entry.NeedsReview = true
		note := fmt.Sprintf("Aircraft identity mismatch: %s", strings.Join(reasons, ", "))
		entry.ExtractionNotes += note
		entry.MissingData = append(entry.MissingData, "aircraft_identity_mismatch")
		log.Printf("  WARNING: %s", note)
	}
}

// ─── Helpers ────────────────────────────────────────────────────────────────

func cleanMarkdownFences(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "```json") {
		s = s[7:]
	} else if strings.HasPrefix(s, "```") {
		s = s[3:]
	}
	if strings.HasSuffix(s, "```") {
		s = s[:len(s)-3]
	}
	return strings.TrimSpace(s)
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

func strVal(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
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

func mustEnv(key string) string {
	return os.Getenv(key)
}
