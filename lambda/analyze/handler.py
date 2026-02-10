"""Analyze Lambda — triggered by SQS, processes a single page with Gemini."""

import json
import os
import tempfile
import boto3

from google import genai
from google.genai import types

from shared.db import execute_query, execute_insert, get_connection
from prompts import MAINTENANCE_EXTRACTION_PROMPT

s3 = boto3.client('s3')
sm = boto3.client('secretsmanager', region_name='us-west-2')

BUCKET = os.environ['BUCKET_NAME']

# Cached Gemini client
_gemini_client = None


def get_gemini_client() -> genai.Client:
    global _gemini_client
    if _gemini_client:
        return _gemini_client
    secret = json.loads(
        sm.get_secret_value(SecretId=os.environ['GEMINI_SECRET_ARN'])['SecretString']
    )
    _gemini_client = genai.Client(api_key=secret['api_key'])
    return _gemini_client


def handler(event, context):
    """Process SQS messages — one page per message."""
    for record in event.get('Records', []):
        msg = json.loads(record['body'])
        logbook_id = msg['logbookId']
        page_id = msg['pageId']
        page_number = msg['pageNumber']
        s3_key = msg['s3Key']

        print(f'Analyzing page {page_number} of logbook {logbook_id}: {s3_key}')

        try:
            process_page(logbook_id, page_id, page_number, s3_key)
        except Exception as e:
            print(f'ERROR processing page {page_id}: {e}')
            mark_page_failed(page_id)
            raise


def process_page(logbook_id: str, page_id: str, page_number: int, s3_key: str):
    """Download page image, extract with Gemini, write results to DB."""
    conn = get_connection()

    # Mark page as processing
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE logbook_pages SET extraction_status = 'processing' WHERE id = %s",
            (page_id,)
        )
        conn.commit()

    # Download image
    with tempfile.NamedTemporaryFile(suffix='.jpg') as tmp:
        s3.download_file(BUCKET, s3_key, tmp.name)
        image_bytes = open(tmp.name, 'rb').read()

    # Call Gemini
    client = get_gemini_client()
    image_part = types.Part.from_bytes(data=image_bytes, mime_type='image/jpeg')

    response = client.models.generate_content(
        model='gemini-2.5-flash',
        contents=[MAINTENANCE_EXTRACTION_PROMPT, image_part],
        config=types.GenerateContentConfig(
            temperature=0.1,
            response_mime_type='application/json',
        ),
    )

    response_text = response.text if hasattr(response, 'text') else ''
    # Clean markdown fences
    if response_text.startswith('```json'):
        response_text = response_text[7:]
    if response_text.startswith('```'):
        response_text = response_text[3:]
    if response_text.endswith('```'):
        response_text = response_text[:-3]
    response_text = response_text.strip()

    extraction = json.loads(response_text)

    # Store raw extraction
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE logbook_pages SET raw_extraction = %s, page_type = %s,
               extraction_model = 'gemini-2.5-flash', extraction_timestamp = NOW()
               WHERE id = %s""",
            (json.dumps(extraction), extraction.get('pageType', 'other'), page_id)
        )
        conn.commit()

    # Get aircraft_id from logbook
    rows = execute_query(
        "SELECT aircraft_id FROM logbook_documents WHERE id = %s", (logbook_id,)
    )
    if not rows:
        raise ValueError(f'Logbook {logbook_id} not found')
    aircraft_id = rows[0]['aircraft_id']

    # Process each entry
    entries = extraction.get('entries', [])
    for entry in entries:
        save_entry(conn, aircraft_id, page_id, entry)

    # Mark page complete
    needs_review = any(e.get('needsReview', False) for e in entries)
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE logbook_pages SET extraction_status = 'completed', needs_review = %s WHERE id = %s""",
            (needs_review, page_id)
        )
        conn.commit()

    # Check if all pages are done → mark logbook complete
    check_logbook_completion(conn, logbook_id)

    print(f'Page {page_id}: extracted {len(entries)} entries')


def save_entry(conn, aircraft_id: str, page_id: str, entry: dict):
    """Save a single extracted maintenance entry and its sub-records."""
    # Insert maintenance_entries
    entry_id = None
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO maintenance_entries
               (aircraft_id, page_id, entry_type, entry_date, hobbs_time, tach_time,
                flight_time, time_since_overhaul, shop_name, shop_address, shop_phone,
                repair_station_number, mechanic_name, mechanic_certificate,
                work_order_number, maintenance_narrative, confidence_score,
                needs_review, missing_data)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               RETURNING id""",
            (
                aircraft_id, page_id,
                entry.get('entryType', 'maintenance'),
                entry.get('date'),
                entry.get('hobbsTime'),
                entry.get('tachTime'),
                entry.get('flightTime'),
                entry.get('timeSinceOverhaul'),
                entry.get('shopName'),
                entry.get('shopAddress'),
                entry.get('shopPhone'),
                entry.get('repairStationNumber'),
                entry.get('mechanicName'),
                entry.get('mechanicCertificate'),
                entry.get('workOrderNumber'),
                entry.get('maintenanceNarrative', ''),
                entry.get('confidence'),
                entry.get('needsReview', False),
                entry.get('missingData'),
            )
        )
        result = cur.fetchone()
        entry_id = str(result[0])
        conn.commit()

    # Parts actions
    for part in entry.get('partsActions', []):
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO parts_actions
                   (entry_id, action_type, part_name, part_number, serial_number,
                    old_part_number, old_serial_number, quantity, notes)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    entry_id, part.get('action', 'installed'),
                    part.get('partName'), part.get('partNumber'),
                    part.get('serialNumber'), part.get('oldPartNumber'),
                    part.get('oldSerialNumber'), part.get('quantity', 1),
                    part.get('notes'),
                )
            )
            conn.commit()

    # AD compliance
    for ad in entry.get('adCompliance', []):
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO ad_compliance
                   (entry_id, aircraft_id, ad_number, compliance_date, compliance_method, notes)
                   VALUES (%s,%s,%s,%s,%s,%s)""",
                (
                    entry_id, aircraft_id, ad.get('adNumber'),
                    entry.get('date'), ad.get('method'), ad.get('notes'),
                )
            )
            conn.commit()

    # Inspection record
    if entry.get('inspectionType'):
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO inspection_records
                   (aircraft_id, entry_id, inspection_type, inspection_date,
                    aircraft_hours, far_reference, inspector_name, inspector_certificate)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    aircraft_id, entry_id, entry['inspectionType'],
                    entry.get('date'), entry.get('flightTime'),
                    entry.get('farReference'), entry.get('mechanicName'),
                    entry.get('mechanicCertificate'),
                )
            )
            conn.commit()

    # Generate embedding for the maintenance narrative
    narrative = entry.get('maintenanceNarrative', '')
    if narrative and len(narrative) > 10:
        try:
            generate_embedding(entry_id, narrative)
        except Exception as e:
            print(f'WARNING: embedding generation failed for entry {entry_id}: {e}')


def generate_embedding(entry_id: str, text: str):
    """Generate and store a vector embedding for a maintenance narrative."""
    client = get_gemini_client()
    resp = client.models.embed_content(
        model='gemini-embedding-001',
        contents=text,
    )
    embedding = resp.embeddings[0].values  # 3072 dims

    embedding_str = '[' + ','.join(str(v) for v in embedding) + ']'

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO maintenance_embeddings (entry_id, embedding, chunk_text, chunk_type)
               VALUES (%s, %s::vector, %s, 'narrative')
               ON CONFLICT (entry_id, chunk_type) DO UPDATE SET embedding = EXCLUDED.embedding, chunk_text = EXCLUDED.chunk_text""",
            (entry_id, embedding_str, text)
        )
        conn.commit()


def check_logbook_completion(conn, logbook_id: str):
    """If all pages are done, mark the logbook as completed."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT COUNT(*) AS total,
                      COUNT(*) FILTER (WHERE extraction_status IN ('completed', 'skipped')) AS done,
                      COUNT(*) FILTER (WHERE extraction_status = 'failed') AS failed
               FROM logbook_pages WHERE document_id = %s""",
            (logbook_id,)
        )
        row = cur.fetchone()
        total, done, failed = row

        if total > 0 and done + failed == total:
            status = 'completed' if failed == 0 else 'failed'
            cur.execute(
                "UPDATE logbook_documents SET processing_status = %s, updated_at = NOW() WHERE id = %s",
                (status, logbook_id)
            )
            conn.commit()


def mark_page_failed(page_id: str):
    """Mark a page as failed."""
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE logbook_pages SET extraction_status = 'failed' WHERE id = %s",
                (page_id,)
            )
            conn.commit()
    except Exception:
        pass
