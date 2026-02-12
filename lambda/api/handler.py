"""API Lambda handler — all routes for the logbook service."""

import json
import math
import os
import uuid
import boto3

# Shared modules (bundled at build time)
from shared.db import execute_query, execute_insert, get_connection
from shared.models import api_response

s3 = boto3.client('s3')
sqs = boto3.client('sqs')
BUCKET = os.environ['BUCKET_NAME']
ANALYZE_QUEUE_URL = os.environ.get('ANALYZE_QUEUE_URL', '')

DEFAULT_PAGE_LIMIT = 25
MAX_PAGE_LIMIT = 100


def parse_query_params(event):
    """Extract pagination params from query string."""
    params = event.get('queryStringParameters') or {}
    limit = min(int(params.get('limit', DEFAULT_PAGE_LIMIT)), MAX_PAGE_LIMIT)
    page = max(int(params.get('page', 1)), 1)
    offset = (page - 1) * limit
    return params, page, limit, offset


def paginated_response(items, total, page, limit):
    """Build a pagination metadata dict."""
    return {
        'page': page,
        'limit': limit,
        'total': total,
        'totalPages': max(math.ceil(total / limit), 1) if limit else 1,
    }


def handler(event, context):
    """Main API Gateway proxy handler."""
    method = event['httpMethod']
    path = event['resource']
    path_params = event.get('pathParameters') or {}

    try:
        # POST /uploads
        if path == '/uploads' and method == 'POST':
            return handle_upload(event)

        # GET /uploads/{id}/status
        elif path == '/uploads/{id}/status' and method == 'GET':
            return handle_status(path_params['id'])

        # POST /uploads/{id}/process
        elif path == '/uploads/{id}/process' and method == 'POST':
            return handle_process(path_params['id'])

        # GET /uploads/{id}/pages/{pageNumber}/image
        elif path == '/uploads/{id}/pages/{pageNumber}/image' and method == 'GET':
            return handle_page_image(path_params['id'], int(path_params['pageNumber']))

        # GET /aircraft/{tailNumber}/uploads
        elif path == '/aircraft/{tailNumber}/uploads' and method == 'GET':
            return handle_list_uploads(path_params['tailNumber'])

        # GET /aircraft/{tailNumber}/summary
        elif path == '/aircraft/{tailNumber}/summary' and method == 'GET':
            return handle_summary(path_params['tailNumber'])

        # POST /aircraft/{tailNumber}/query
        elif path == '/aircraft/{tailNumber}/query' and method == 'POST':
            return handle_query(path_params['tailNumber'], event)

        # GET /aircraft/{tailNumber}/entries
        elif path == '/aircraft/{tailNumber}/entries' and method == 'GET':
            return handle_entries(path_params['tailNumber'], event)

        # GET /aircraft/{tailNumber}/entries/{entryId}
        elif path == '/aircraft/{tailNumber}/entries/{entryId}' and method == 'GET':
            return handle_entry_detail(path_params['tailNumber'], path_params['entryId'])

        # GET /aircraft/{tailNumber}/inspections
        elif path == '/aircraft/{tailNumber}/inspections' and method == 'GET':
            return handle_inspections(path_params['tailNumber'], event)

        # GET /aircraft/{tailNumber}/ads
        elif path == '/aircraft/{tailNumber}/ads' and method == 'GET':
            return handle_ads(path_params['tailNumber'], event)

        # GET /aircraft/{tailNumber}/parts
        elif path == '/aircraft/{tailNumber}/parts' and method == 'GET':
            return handle_parts(path_params['tailNumber'], event)

        return api_response(404, {'error': 'Not found'})

    except Exception as e:
        print(f'ERROR: {e}')
        return api_response(500, {'error': str(e)})


def handle_upload(event):
    """Create an upload batch and return presigned upload URL(s).

    PDF mode (default): single presigned URL for a PDF, triggers split Lambda on upload.
    Multi-image mode (pageCount present): presigned URLs per page image, requires POST /uploads/{id}/process.
    """
    body = json.loads(event.get('body') or '{}')
    tail_number = body.get('tailNumber', '').upper().strip()
    log_type = body.get('logType')
    filename = body.get('filename', 'logbook.pdf')
    page_count = body.get('pageCount')

    if not tail_number:
        return api_response(400, {'error': 'tailNumber is required'})

    # Upsert aircraft
    aircraft_id = execute_insert(
        """INSERT INTO aircraft (registration) VALUES (%s)
           ON CONFLICT (registration) DO UPDATE SET updated_at = NOW()
           RETURNING id""",
        (tail_number,)
    )

    batch_id = str(uuid.uuid4())

    if page_count:
        # Multi-image upload mode
        page_count = int(page_count)
        if page_count < 1 or page_count > 500:
            return api_response(400, {'error': 'pageCount must be between 1 and 500'})

        execute_insert(
            """INSERT INTO upload_batches (id, aircraft_id, logbook_type, upload_type, source_filename, page_count, processing_status)
               VALUES (%s, %s, %s, 'multi_image', %s, %s, 'pending') RETURNING id""",
            (batch_id, aircraft_id, log_type, filename, page_count)
        )

        uploads = []
        for i in range(1, page_count + 1):
            page_key = f'pages/{batch_id}/page_{i:04d}.jpg'
            url = s3.generate_presigned_url(
                'put_object',
                Params={'Bucket': BUCKET, 'Key': page_key, 'ContentType': 'image/jpeg'},
                ExpiresIn=3600,
            )
            uploads.append({'pageNumber': i, 'uploadUrl': url, 's3Key': page_key})

        return api_response(200, {
            'uploadId': batch_id,
            'uploadType': 'multi_image',
            'pageCount': page_count,
            'uploads': uploads,
        })
    else:
        # PDF upload mode (existing flow)
        s3_key = f'uploads/{batch_id}/{filename}'
        execute_insert(
            """INSERT INTO upload_batches (id, aircraft_id, logbook_type, upload_type, source_filename, s3_key, processing_status)
               VALUES (%s, %s, %s, 'pdf', %s, %s, 'pending') RETURNING id""",
            (batch_id, aircraft_id, log_type, filename, s3_key)
        )

        upload_url = s3.generate_presigned_url(
            'put_object',
            Params={'Bucket': BUCKET, 'Key': s3_key, 'ContentType': 'application/pdf'},
            ExpiresIn=3600,
        )

        return api_response(200, {
            'uploadId': batch_id,
            'uploadType': 'pdf',
            'uploadUrl': upload_url,
            's3Key': s3_key,
        })


def handle_status(batch_id):
    """Return processing status for an upload batch."""
    rows = execute_query(
        """SELECT ub.id, ub.processing_status, ub.page_count, ub.source_filename,
                  ub.logbook_type, ub.upload_type, ub.created_at,
                  COUNT(up.id) FILTER (WHERE up.extraction_status = 'completed') AS completed_pages,
                  COUNT(up.id) FILTER (WHERE up.extraction_status = 'failed') AS failed_pages,
                  COUNT(up.id) FILTER (WHERE up.needs_review = TRUE) AS needs_review_pages,
                  COUNT(up.id) AS total_pages
           FROM upload_batches ub
           LEFT JOIN upload_pages up ON up.document_id = ub.id
           WHERE ub.id = %s
           GROUP BY ub.id""",
        (batch_id,)
    )
    if not rows:
        return api_response(404, {'error': 'Upload not found'})

    row = rows[0]
    result = {
        'uploadId': str(row['id']),
        'status': row['processing_status'],
        'filename': row['source_filename'],
        'logType': row['logbook_type'],
        'uploadType': row['upload_type'],
        'pageCount': row['page_count'] or row['total_pages'],
        'completedPages': row['completed_pages'],
        'failedPages': row['failed_pages'],
        'needsReviewPages': row['needs_review_pages'],
        'createdAt': row['created_at'],
    }

    if row['failed_pages'] > 0:
        failed_rows = execute_query(
            """SELECT page_number FROM upload_pages
               WHERE document_id = %s AND extraction_status = 'failed'
               ORDER BY page_number""",
            (batch_id,)
        )
        result['failedPageNumbers'] = [r['page_number'] for r in failed_rows]

    return api_response(200, result)


def handle_list_uploads(tail_number):
    """List all uploads for an aircraft."""
    rows = execute_query(
        """SELECT ub.id, ub.logbook_type, ub.upload_type, ub.source_filename,
                  ub.processing_status, ub.page_count, ub.date_range_start,
                  ub.date_range_end, ub.created_at
           FROM upload_batches ub
           JOIN aircraft a ON ub.aircraft_id = a.id
           WHERE a.registration = %s
           ORDER BY ub.created_at DESC""",
        (tail_number.upper(),)
    )
    return api_response(200, {'tailNumber': tail_number.upper(), 'uploads': rows})


def handle_summary(tail_number):
    """Aggregate maintenance summary across all logbooks for an aircraft."""
    tail = tail_number.upper()

    # Get aircraft
    aircraft = execute_query(
        "SELECT * FROM aircraft WHERE registration = %s", (tail,)
    )
    if not aircraft:
        return api_response(404, {'error': f'Aircraft {tail} not found'})

    aid = aircraft[0]['id']

    # Last annual — join through inspection_records
    annual = execute_query(
        """SELECT me.entry_date, me.flight_time
           FROM inspection_records ir
           JOIN maintenance_entries me ON ir.entry_id = me.id
           WHERE ir.aircraft_id = %s AND ir.inspection_type = 'annual'
           ORDER BY ir.inspection_date DESC LIMIT 1""", (aid,)
    )

    # Last 100hr — join through inspection_records
    hundredhr = execute_query(
        """SELECT me.entry_date, me.flight_time
           FROM inspection_records ir
           JOIN maintenance_entries me ON ir.entry_id = me.id
           WHERE ir.aircraft_id = %s AND ir.inspection_type = '100hr'
           ORDER BY ir.inspection_date DESC LIMIT 1""", (aid,)
    )

    # Last oil change (heuristic: narrative contains 'oil change' or 'oil filter')
    oil = execute_query(
        """SELECT entry_date, flight_time FROM maintenance_entries
           WHERE aircraft_id = %s
             AND (lower(maintenance_narrative) LIKE '%%oil change%%'
                  OR lower(maintenance_narrative) LIKE '%%oil filter%%')
           ORDER BY entry_date DESC LIMIT 1""", (aid,)
    )

    # Latest total time
    tt = execute_query(
        """SELECT flight_time FROM maintenance_entries
           WHERE aircraft_id = %s AND flight_time IS NOT NULL
           ORDER BY entry_date DESC LIMIT 1""", (aid,)
    )

    # Upcoming expirations
    expirations = execute_query(
        """SELECT 'life_limited_part' AS type, part_name AS name, expiration_date
           FROM life_limited_parts WHERE aircraft_id = %s AND is_active = TRUE
             AND expiration_date IS NOT NULL AND expiration_date <= CURRENT_DATE + INTERVAL '90 days'
           UNION ALL
           SELECT inspection_type AS type, inspection_type || ' inspection' AS name, next_due_date AS expiration_date
           FROM inspection_records WHERE aircraft_id = %s
             AND next_due_date IS NOT NULL AND next_due_date <= CURRENT_DATE + INTERVAL '90 days'
           ORDER BY expiration_date""",
        (aid, aid)
    )

    return api_response(200, {
        'tailNumber': tail,
        'aircraft': aircraft[0],
        'lastAnnual': annual[0] if annual else None,
        'last100hr': hundredhr[0] if hundredhr else None,
        'lastOilChange': oil[0] if oil else None,
        'totalTime': tt[0]['flight_time'] if tt else None,
        'upcomingExpirations': expirations,
    })


def handle_process(batch_id):
    """Finalize a multi-image upload: verify images, create page records, queue for analysis."""
    conn = get_connection()

    # Atomic status transition: pending → processing (only for multi_image uploads)
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE upload_batches SET processing_status = 'processing', updated_at = NOW()
               WHERE id = %s AND upload_type = 'multi_image' AND processing_status = 'pending'
               RETURNING id, page_count""",
            (batch_id,)
        )
        row = cur.fetchone()
        conn.commit()

    if not row:
        return api_response(409, {
            'error': 'Upload not found, not a multi-image upload, or already processing'
        })

    page_count = row[1]

    # Verify all page images exist in S3
    prefix = f'pages/{batch_id}/'
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    found_keys = {obj['Key'] for obj in resp.get('Contents', [])}
    expected_keys = {f'pages/{batch_id}/page_{i:04d}.jpg' for i in range(1, page_count + 1)}
    missing = expected_keys - found_keys

    if missing:
        # Roll back to pending
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE upload_batches SET processing_status = 'pending', updated_at = NOW() WHERE id = %s",
                (batch_id,)
            )
            conn.commit()
        missing_pages = sorted(int(k.split('_')[-1].split('.')[0]) for k in missing)
        return api_response(400, {
            'error': f'Missing {len(missing)} page image(s)',
            'missingPages': missing_pages,
        })

    # Create upload_pages records and queue SQS messages
    for i in range(1, page_count + 1):
        page_key = f'pages/{batch_id}/page_{i:04d}.jpg'
        page_id = execute_insert(
            """INSERT INTO upload_pages (document_id, page_number, image_path, extraction_status)
               VALUES (%s, %s, %s, 'pending') RETURNING id""",
            (batch_id, i, page_key)
        )

        sqs.send_message(
            QueueUrl=ANALYZE_QUEUE_URL,
            MessageBody=json.dumps({
                'uploadId': batch_id,
                'pageId': page_id,
                'pageNumber': i,
                's3Key': page_key,
            }),
        )

    return api_response(200, {
        'uploadId': batch_id,
        'status': 'processing',
        'pagesQueued': page_count,
    })


def handle_page_image(batch_id, page_number):
    """Return a presigned GET URL for a page image."""
    rows = execute_query(
        """SELECT image_path FROM upload_pages
           WHERE document_id = %s AND page_number = %s""",
        (batch_id, page_number)
    )
    if not rows:
        return api_response(404, {'error': 'Page not found'})

    image_url = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': BUCKET, 'Key': rows[0]['image_path']},
        ExpiresIn=3600,
    )

    return api_response(200, {
        'uploadId': batch_id,
        'pageNumber': page_number,
        'imageUrl': image_url,
    })


def handle_query(tail_number, event):
    """RAG query: embed question, search pgvector, answer with Gemini."""
    body = json.loads(event.get('body') or '{}')
    question = body.get('question', '').strip()
    if not question:
        return api_response(400, {'error': 'question is required'})

    tail = tail_number.upper()

    # Get aircraft
    aircraft = execute_query(
        "SELECT id FROM aircraft WHERE registration = %s", (tail,)
    )
    if not aircraft:
        return api_response(404, {'error': f'Aircraft {tail} not found'})
    aid = aircraft[0]['id']

    # Get Gemini API key
    gemini_key = os.environ.get('GEMINI_API_KEY')
    if not gemini_key:
        sm = boto3.client('secretsmanager', region_name='us-west-2')
        gemini_key = json.loads(
            sm.get_secret_value(SecretId=os.environ['GEMINI_SECRET_ARN'])['SecretString']
        ).get('GEMINI_API_KEY', '')

    from google import genai
    from google.genai import types

    client = genai.Client(api_key=gemini_key)

    # Generate embedding for the question
    embed_resp = client.models.embed_content(
        model='gemini-embedding-001',
        contents=question,
    )
    query_embedding = embed_resp.embeddings[0].values

    # Search pgvector for relevant maintenance entries, including inspection subtype
    embedding_str = '[' + ','.join(str(v) for v in query_embedding) + ']'
    results = execute_query(
        """SELECT me.chunk_text, me.chunk_type,
                  m.entry_date, m.entry_type, m.maintenance_narrative,
                  ir.inspection_type,
                  1 - (me.embedding <=> %s::halfvec) AS similarity
           FROM maintenance_embeddings me
           JOIN maintenance_entries m ON me.entry_id = m.id
           LEFT JOIN inspection_records ir ON ir.entry_id = m.id
           WHERE m.aircraft_id = %s
           ORDER BY me.embedding <=> %s::halfvec
           LIMIT 10""",
        (embedding_str, aid, embedding_str)
    )

    if not results:
        return api_response(200, {
            'tailNumber': tail,
            'question': question,
            'answer': 'No maintenance records found for this aircraft.',
            'sources': [],
        })

    # Build context for Gemini — include inspection subtype in labels
    context_parts = []
    for r in results:
        label = r['entry_type']
        if r.get('inspection_type'):
            label = f"{label}/{r['inspection_type']}"
        context_parts.append(
            f"[{r['entry_date']}] ({label}) {r['maintenance_narrative']}"
        )
    context_text = '\n---\n'.join(context_parts)

    rag_prompt = f"""You are an aircraft maintenance expert assistant. Answer the question based ONLY on the maintenance records provided below.

Aircraft: {tail}

MAINTENANCE RECORDS:
{context_text}

QUESTION: {question}

Provide a clear, accurate answer. Cite specific dates and entries. If the records don't contain enough information, say so."""

    response = client.models.generate_content(
        model='gemini-2.5-flash',
        contents=rag_prompt,
        config=types.GenerateContentConfig(temperature=0.2),
    )

    answer = response.text if hasattr(response, 'text') else str(response)

    return api_response(200, {
        'tailNumber': tail,
        'question': question,
        'answer': answer,
        'sources': [{'date': str(r['entry_date']), 'type': r['entry_type'],
                      'inspectionType': r.get('inspection_type'),
                      'similarity': round(r['similarity'], 3)} for r in results[:5]],
    })


# ─── New Endpoints ────────────────────────────────────────


def _get_aircraft_id(tail_number):
    """Look up aircraft ID by registration, return (id, None) or (None, error_response)."""
    tail = tail_number.upper()
    rows = execute_query(
        "SELECT id FROM aircraft WHERE registration = %s", (tail,)
    )
    if not rows:
        return None, api_response(404, {'error': f'Aircraft {tail} not found'})
    return rows[0]['id'], None


def handle_entries(tail_number, event):
    """List/filter maintenance entries for an aircraft."""
    aid, err = _get_aircraft_id(tail_number)
    if err:
        return err

    params, page, limit, offset = parse_query_params(event)
    entry_type = params.get('type')
    date_from = params.get('dateFrom')
    date_to = params.get('dateTo')

    where_clauses = ["me.aircraft_id = %s"]
    query_params = [aid]

    if entry_type:
        where_clauses.append("me.entry_type = %s")
        query_params.append(entry_type)
    if date_from:
        where_clauses.append("me.entry_date >= %s")
        query_params.append(date_from)
    if date_to:
        where_clauses.append("me.entry_date <= %s")
        query_params.append(date_to)

    where_sql = " AND ".join(where_clauses)

    # Count
    count_rows = execute_query(
        f"SELECT COUNT(*) AS total FROM maintenance_entries me WHERE {where_sql}",
        query_params
    )
    total = count_rows[0]['total']

    # Fetch page
    entries = execute_query(
        f"""SELECT me.id, me.entry_type, me.entry_date, me.hobbs_time, me.tach_time,
                   me.flight_time, me.shop_name, me.mechanic_name,
                   me.maintenance_narrative, me.confidence_score, me.needs_review,
                   ir.inspection_type
            FROM maintenance_entries me
            LEFT JOIN inspection_records ir ON ir.entry_id = me.id
            WHERE {where_sql}
            ORDER BY me.entry_date DESC
            LIMIT %s OFFSET %s""",
        query_params + [limit, offset]
    )

    return api_response(200, {
        'tailNumber': tail_number.upper(),
        'entries': entries,
        'pagination': paginated_response(entries, total, page, limit),
    })


def handle_entry_detail(tail_number, entry_id):
    """Single entry detail with all related data."""
    aid, err = _get_aircraft_id(tail_number)
    if err:
        return err

    # Main entry
    entries = execute_query(
        """SELECT me.* FROM maintenance_entries me
           WHERE me.id = %s AND me.aircraft_id = %s""",
        (entry_id, aid)
    )
    if not entries:
        return api_response(404, {'error': 'Entry not found'})

    entry = entries[0]

    # Parts actions
    parts = execute_query(
        "SELECT * FROM parts_actions WHERE entry_id = %s ORDER BY created_at",
        (entry_id,)
    )

    # AD compliance
    ads = execute_query(
        "SELECT * FROM ad_compliance WHERE entry_id = %s ORDER BY compliance_date",
        (entry_id,)
    )

    # Inspection record
    inspections = execute_query(
        "SELECT * FROM inspection_records WHERE entry_id = %s",
        (entry_id,)
    )

    entry['partsActions'] = parts
    entry['adCompliance'] = ads
    entry['inspectionRecord'] = inspections[0] if inspections else None

    return api_response(200, {
        'tailNumber': tail_number.upper(),
        'entry': entry,
    })


def handle_inspections(tail_number, event):
    """Inspection history for an aircraft."""
    aid, err = _get_aircraft_id(tail_number)
    if err:
        return err

    params, page, limit, offset = parse_query_params(event)
    inspection_type = params.get('type')

    where_clauses = ["ir.aircraft_id = %s"]
    query_params = [aid]

    if inspection_type:
        where_clauses.append("ir.inspection_type = %s")
        query_params.append(inspection_type)

    where_sql = " AND ".join(where_clauses)

    # Count
    count_rows = execute_query(
        f"SELECT COUNT(*) AS total FROM inspection_records ir WHERE {where_sql}",
        query_params
    )
    total = count_rows[0]['total']

    # Fetch page with narrative from maintenance_entries
    inspections = execute_query(
        f"""SELECT ir.id, ir.inspection_type, ir.inspection_date, ir.aircraft_hours,
                   ir.next_due_date, ir.next_due_hours, ir.far_reference,
                   ir.inspector_name, ir.inspector_certificate, ir.notes,
                   me.maintenance_narrative, me.shop_name
            FROM inspection_records ir
            LEFT JOIN maintenance_entries me ON ir.entry_id = me.id
            WHERE {where_sql}
            ORDER BY ir.inspection_date DESC
            LIMIT %s OFFSET %s""",
        query_params + [limit, offset]
    )

    # Latest by type: most recent inspection per inspection_type
    latest_by_type = execute_query(
        """SELECT DISTINCT ON (ir.inspection_type)
                  ir.inspection_type, ir.inspection_date, ir.next_due_date, ir.next_due_hours
           FROM inspection_records ir
           WHERE ir.aircraft_id = %s
           ORDER BY ir.inspection_type, ir.inspection_date DESC""",
        (aid,)
    )

    return api_response(200, {
        'tailNumber': tail_number.upper(),
        'inspections': inspections,
        'latestByType': latest_by_type,
        'pagination': paginated_response(inspections, total, page, limit),
    })


def handle_ads(tail_number, event):
    """AD compliance records for an aircraft."""
    aid, err = _get_aircraft_id(tail_number)
    if err:
        return err

    params, page, limit, offset = parse_query_params(event)

    count_rows = execute_query(
        "SELECT COUNT(*) AS total FROM ad_compliance WHERE aircraft_id = %s",
        (aid,)
    )
    total = count_rows[0]['total']

    ads = execute_query(
        """SELECT ad.id, ad.ad_number, ad.compliance_date, ad.compliance_method,
                  ad.next_due_date, ad.next_due_hours, ad.notes,
                  me.entry_date, me.maintenance_narrative, me.shop_name
           FROM ad_compliance ad
           LEFT JOIN maintenance_entries me ON ad.entry_id = me.id
           WHERE ad.aircraft_id = %s
           ORDER BY ad.compliance_date DESC
           LIMIT %s OFFSET %s""",
        (aid, limit, offset)
    )

    return api_response(200, {
        'tailNumber': tail_number.upper(),
        'ads': ads,
        'pagination': paginated_response(ads, total, page, limit),
    })


def handle_parts(tail_number, event):
    """Life-limited parts inventory for an aircraft."""
    aid, err = _get_aircraft_id(tail_number)
    if err:
        return err

    params = (event.get('queryStringParameters') or {})
    status = params.get('status', 'active')

    where_clauses = ["aircraft_id = %s"]
    query_params = [aid]

    if status != 'all':
        where_clauses.append("is_active = TRUE")

    where_sql = " AND ".join(where_clauses)

    parts = execute_query(
        f"""SELECT id, part_name, part_number, serial_number,
                   install_date, install_hours, life_limit_hours, life_limit_months,
                   expiration_date, is_active, removal_date, notes
            FROM life_limited_parts
            WHERE {where_sql}
            ORDER BY expiration_date ASC NULLS LAST""",
        query_params
    )

    return api_response(200, {
        'tailNumber': tail_number.upper(),
        'parts': parts,
        'total': len(parts),
    })
