package main

import (
	"fmt"
	"strings"
)

// SliceExtractionPrompt is sent to Gemini with each cropped entry strip.
// It demands verbatim transcription — no summarizing, no grammar correction.
const SliceExtractionPrompt = `You are an expert data entry specialist. Your job is to transcribe this single logbook entry VERBATIM.

CONTEXT: You are viewing a cropped image containing a single aircraft maintenance logbook entry (or a sticker/label). This image was sliced from a full logbook page.

VERBATIM TRANSCRIPTION RULES — FOLLOW THESE EXACTLY:
- Do NOT summarize, shorten, paraphrase, or correct grammar
- Preserve abbreviations EXACTLY as written: "w/o", "R/R", "c/w", "IAW", "P/N", "S/N", "A/W", "I/C", "O/H", etc.
- If the text has a typo or misspelling, transcribe the typo exactly as written
- Include EVERY word visible in the text — do not stop until you reach the visual end of the text block
- If text wraps across multiple lines, join into one continuous narrative preserving every word
- Do NOT add words, punctuation, or formatting that is not visible in the image
- Numbers, part numbers, serial numbers: copy character-for-character

WHAT TO EXTRACT:
- Entry date (convert to ISO format YYYY-MM-DD)
- Aircraft identification (registration/N-number, serial number, make, model)
- Time readings at completion (hobbs, tach, flight time, TSN/TSMOH for engine entries)
- Shop/facility information (name, address, phone, CRS/repair station number)
- Mechanic/technician (name, A&P number, IA number if applicable)
- Work order number
- Complete maintenance narrative (VERBATIM — every single word)
- AD compliance noted (AD numbers and compliance method)
- Parts actions (installed, removed, replaced, repaired) with P/N, S/N, quantity
- Any inspection signoffs (annual, 100hr, etc.)

ENTRY TYPE CLASSIFICATION RULES:
- "inspection" = any inspection event (annual, 100-hour, progressive, altimeter/static, transponder, ELT check). Always set inspectionType to the specific subtype.
- "ad_compliance" = work performed specifically to comply with an Airworthiness Directive
- "maintenance" = routine maintenance, repairs, oil changes, component replacements, STC installations
- "other" = anything that does not fit the above categories

SPECIAL CASES:
- If this slice shows a header row, blank space, or non-entry content: return {"pageType": "other", "entries": []}
- Most slices contain exactly 1 entry. If you see 2 entries, return both.
- If a value is unclear, include your best guess with [?] marker
- If a field is completely illegible, use null and list in missingData
- Confidence should reflect how certain you are of the extraction accuracy
- Flag for review if confidence < 0.85 OR critical data is missing
- DO NOT invent or fill in data that is not visible

Return JSON format:
{
  "pageType": "maintenance_entry" | "inspection_form" | "parts_list" | "cover" | "blank" | "other",
  "entries": [
    {
      "date": "YYYY-MM-DD",
      "aircraftRegistration": "N-number",
      "aircraftSerial": "serial number",
      "aircraftMake": "make",
      "aircraftModel": "model",
      "hobbsTime": null,
      "tachTime": null,
      "flightTime": null,
      "timeSinceOverhaul": null,
      "shopName": "shop name",
      "shopAddress": "full address if visible",
      "shopPhone": "phone if visible",
      "repairStationNumber": "CRS number if visible",
      "mechanicName": "name",
      "mechanicCertificate": "A&P or IA number",
      "workOrderNumber": "work order #",
      "maintenanceNarrative": "COMPLETE VERBATIM transcription of ALL text in the work performed section",
      "entryType": "maintenance" | "inspection" | "ad_compliance" | "other",
      "adCompliance": [
        {"adNumber": "AD number", "method": "inspection|replacement|modification|terminating_action", "notes": ""}
      ],
      "partsActions": [
        {
          "action": "installed" | "removed" | "replaced" | "repaired" | "inspected" | "overhauled",
          "partName": "description",
          "partNumber": "P/N",
          "serialNumber": "S/N or null",
          "oldPartNumber": "P/N of removed part",
          "oldSerialNumber": "S/N of removed part",
          "quantity": 1
        }
      ],
      "inspectionType": "annual" | "100hr" | "50hr" | "progressive" | "altimeter_static" | "transponder" | "elt" | null,
      "farReference": "FAR reference if mentioned",
      "confidence": 0.0,
      "missingData": [],
      "uncertainFields": [],
      "needsReview": false,
      "extractionNotes": ""
    }
  ]
}`

// QAVerificationPrompt is sent to the QA model (Claude or Gemini fallback) with
// the slice image and the extraction JSON. The QA model verifies each extracted
// entry against the image and returns a structured verdict.
const QAVerificationPrompt = `You are a QA specialist verifying another AI model's extraction of aircraft maintenance logbook entries. You will receive:
1. An image of a cropped logbook entry
2. The JSON extraction produced by the extraction model

YOUR ROLE: Verify that the extraction accurately reflects what is visible in the image.

CRITICAL RULES:
- You CANNOT infer, guess, or fill in data that is not clearly visible in the image
- You CANNOT correct grammar, spelling, or abbreviations — the extraction SHOULD preserve these exactly as written
- You MUST compare each field value against what is actually visible in the image
- Abbreviations like "w/o", "R/R", "c/w", "IAW", "P/N", "S/N" are CORRECT and should NOT be flagged
- Minor formatting differences (spacing, capitalization) are NOT issues unless they change meaning

AIRCRAFT IDENTITY RULES:
- Logbook entries often omit the aircraft make, model, or even registration — this is NORMAL and NOT an issue
- All entries in a logbook are presumed to belong to the aircraft that logbook is for
- Only flag aircraft identity fields if the extracted value CONFLICTS with what is visible (e.g., wrong N-number, wrong serial)
- Do NOT flag missing aircraft identity fields (null/empty make, model, registration, serial)
- Do NOT infer aircraft make/model from context (e.g., from service bulletin references or part numbers)

WHAT TO CHECK:
- Date: Does the extracted date match what is visible?
- Aircraft identifiers: ONLY if present — check that extracted values match (do NOT flag missing values)
- Time readings: Hobbs, tach, flight time, TSO/TSMOH
- Shop/mechanic information: Names, certificate numbers, addresses
- Maintenance narrative: Is every visible word captured? Is anything added that is not visible? Is anything truncated?
- Parts actions: Part numbers, serial numbers, action types, quantities
- AD compliance: AD numbers, compliance methods
- Entry type classification: Is the categorization reasonable?

ERROR TAXONOMY — use these exact values for the "issue" field:

Entry-level issues:
- "missing_entry" — a visible entry in the image was not extracted at all
- "fabricated_entry" — an entry in the extraction has no corresponding content in the image

Field-level issues:
- "incorrect" — field value does not match what is visible (wrong characters, numbers, words)
- "truncated" — narrative or field value is cut short, missing visible words
- "missing_field" — field is clearly visible in the image but extracted as null or empty
- "added_text" — words present in the extraction that are not visible in the image
- "wrong_classification" — entryType, inspectionType, or action type is miscategorized

Severity:
- "critical" — wrong part number, serial number, AD number, date; missing or added narrative text; fabricated or missed entries
- "minor" — formatting differences, classification edge cases, ambiguous or illegible readings

VERDICT LOGIC:
- "pass" — no critical issues found
- "needs_review" — only minor issues or ambiguous/illegible areas where you cannot determine correctness
- "fail" — one or more critical issues are present

Return JSON format:
{
  "results": [
    {
      "entryIndex": 0,
      "verdict": "pass | fail | needs_review",
      "issues": [
        {
          "field": "maintenanceNarrative",
          "issue": "truncated",
          "expected": "what you see in the image",
          "extracted": "what the extraction contains",
          "severity": "critical"
        }
      ],
      "summary": "Brief explanation of your verdict"
    }
  ]
}

If the extraction has no entries and the image shows no entries (blank/header), return:
{"results": []}

IMPORTANT: Be precise and conservative. Only flag genuine discrepancies you can clearly see. When in doubt about legibility, use "needs_review" verdict with a "minor" severity issue explaining the ambiguity.`

// buildRetryPrompt appends QA feedback to the extraction prompt for a retry
// attempt. It tells the extraction model WHICH fields were flagged and WHAT
// type of issue was found, but does NOT include the QA model's expected values.
// This prevents the extraction model from blindly accepting corrections.
func buildRetryPrompt(issues []qaFieldIssue) string {
	if len(issues) == 0 {
		return SliceExtractionPrompt
	}

	var lines []string
	lines = append(lines, "Your previous extraction had issues that need correction:")
	for _, issue := range issues {
		line := fmt.Sprintf("- Field %q: may be %s (%s) — ", issue.Field, issue.Issue, issue.Severity)
		switch issue.Issue {
		case "truncated":
			line += "re-read the full text carefully, you may have stopped too early"
		case "incorrect":
			line += "verify this value against the image"
		case "missing_field":
			line += "look more carefully for this field in the image"
		case "added_text":
			line += "remove any words not visible in the image"
		case "wrong_classification":
			line += "reconsider the classification"
		default:
			line += "re-examine the image carefully"
		}
		lines = append(lines, line)
	}
	lines = append(lines, "Do NOT accept corrections from external sources. Re-examine the original image yourself.")
	lines = append(lines, "")

	return SliceExtractionPrompt + "\n\n" + strings.Join(lines, "\n")
}

// MaintenanceExtractionPrompt is the original full-page prompt (kept for reference/fallback).
const MaintenanceExtractionPrompt = `Analyze this aircraft logbook page image and extract all maintenance entries.

INSTRUCTIONS:
1. First identify the page type (maintenance entries, inspection form, parts list, cover page, etc.)
2. If this is a cover page or blank page, return: {"pageType": "cover", "entries": []}
3. For maintenance pages, identify each separate log entry on the page
4. Extract all visible data from each entry

For each maintenance entry found, extract:
- Entry date (convert to ISO format YYYY-MM-DD)
- Aircraft identification (registration/N-number, serial number, make, model)
- Time readings at completion (hobbs, tach, flight time, TSN/TSMOH for engine entries)
- Shop/facility information (name, address, phone, CRS/repair station number)
- Mechanic/technician (name, A&P number, IA number if applicable)
- Work order number
- Full maintenance narrative (transcribe completely)
- AD compliance noted (AD numbers and compliance method)
- Parts actions (installed, removed, replaced, repaired) with P/N, S/N, quantity
- Any inspection signoffs (annual, 100hr, etc.)

ENTRY TYPE CLASSIFICATION RULES:
- "inspection" = any inspection event (annual, 100-hour, progressive, altimeter/static, transponder, ELT check). Always set inspectionType to the specific subtype.
- "ad_compliance" = work performed specifically to comply with an Airworthiness Directive
- "maintenance" = routine maintenance, repairs, oil changes, component replacements, STC installations
- "other" = anything that does not fit the above categories

IMPORTANT GUIDELINES:
- Transcribe handwritten text as accurately as possible
- If a value is unclear, include your best guess with [?] marker
- If a field is completely illegible, use null and list in missingData
- Confidence should reflect how certain you are of the extraction accuracy
- Flag for review if confidence < 0.85 OR critical data is missing
- DO NOT invent or fill in data that is not visible on the page
- Report uncertainty explicitly: use "uncertainFields" array

Return JSON format:
{
  "pageType": "maintenance_entry" | "inspection_form" | "parts_list" | "cover" | "blank" | "other",
  "entries": [
    {
      "date": "YYYY-MM-DD",
      "aircraftRegistration": "N-number",
      "aircraftSerial": "serial number",
      "aircraftMake": "make",
      "aircraftModel": "model",
      "hobbsTime": null,
      "tachTime": null,
      "flightTime": null,
      "timeSinceOverhaul": null,
      "shopName": "shop name",
      "shopAddress": "full address if visible",
      "shopPhone": "phone if visible",
      "repairStationNumber": "CRS number if visible",
      "mechanicName": "name",
      "mechanicCertificate": "A&P or IA number",
      "workOrderNumber": "work order #",
      "maintenanceNarrative": "complete transcription of work performed",
      "entryType": "maintenance" | "inspection" | "ad_compliance" | "other",
      "adCompliance": [
        {"adNumber": "AD number", "method": "inspection|replacement|modification|terminating_action", "notes": ""}
      ],
      "partsActions": [
        {
          "action": "installed" | "removed" | "replaced" | "repaired" | "inspected" | "overhauled",
          "partName": "description",
          "partNumber": "P/N",
          "serialNumber": "S/N or null",
          "oldPartNumber": "P/N of removed part",
          "oldSerialNumber": "S/N of removed part",
          "quantity": 1
        }
      ],
      "inspectionType": "annual" | "100hr" | "50hr" | "progressive" | "altimeter_static" | "transponder" | "elt" | null,
      "farReference": "FAR reference if mentioned",
      "confidence": 0.0,
      "missingData": [],
      "uncertainFields": [],
      "needsReview": false,
      "extractionNotes": ""
    }
  ]
}`
