package main

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
