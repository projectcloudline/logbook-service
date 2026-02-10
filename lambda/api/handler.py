"""API Lambda handler — all routes for the logbook service."""

import json
import os
import uuid
import boto3

# Shared modules (bundled at build time)
from shared.db import execute_query, execute_insert, get_connection
from shared.models import api_response

s3 = boto3.client('s3')
BUCKET = os.environ['BUCKET_NAME']


def handler(event, context):
    """Main API Gateway proxy handler."""
    method = event['httpMethod']
    path = event['resource']
    path_params = event.get('pathParameters') or {}

    try:
        # POST /logbooks/upload
        if path == '/logbooks/upload' and method == 'POST':
            return handle_upload(event)

        # GET /logbooks/{id}/status
        elif path == '/logbooks/{id}/status' and method == 'GET':
            return handle_status(path_params['id'])

        # GET /aircraft/{tailNumber}/logbooks
        elif path == '/aircraft/{tailNumber}/logbooks' and method == 'GET':
            return handle_list_logbooks(path_params['tailNumber'])

        # GET /aircraft/{tailNumber}/summary
        elif path == '/aircraft/{tailNumber}/summary' and method == 'GET':
            return handle_summary(path_params['tailNumber'])

        # POST /aircraft/{tailNumber}/query
        elif path == '/aircraft/{tailNumber}/query' and method == 'POST':
            return handle_query(path_params['tailNumber'], event)

        return api_response(404, {'error': 'Not found'})

    except Exception as e:
        print(f'ERROR: {e}')
        return api_response(500, {'error': str(e)})


def handle_upload(event):
    """Create a logbook document record and return a presigned upload URL."""
    body = json.loads(event.get('body') or '{}')
    tail_number = body.get('tailNumber', '').upper().strip()
    logbook_type = body.get('logbookType', 'airframe')
    filename = body.get('filename', 'logbook.pdf')

    if not tail_number:
        return api_response(400, {'error': 'tailNumber is required'})

    # Upsert aircraft
    aircraft_id = execute_insert(
        """INSERT INTO aircraft (registration) VALUES (%s)
           ON CONFLICT (registration) DO UPDATE SET updated_at = NOW()
           RETURNING id""",
        (tail_number,)
    )

    # Create logbook_documents record
    logbook_id = str(uuid.uuid4())
    execute_insert(
        """INSERT INTO logbook_documents (id, aircraft_id, logbook_type, source_filename, s3_key, processing_status)
           VALUES (%s, %s, %s, %s, %s, 'pending') RETURNING id""",
        (logbook_id, aircraft_id, logbook_type, filename, f'uploads/{logbook_id}/{filename}')
    )

    # Generate presigned PUT URL
    s3_key = f'uploads/{logbook_id}/{filename}'
    upload_url = s3.generate_presigned_url(
        'put_object',
        Params={'Bucket': BUCKET, 'Key': s3_key, 'ContentType': 'application/pdf'},
        ExpiresIn=3600,
    )

    return api_response(200, {
        'logbookId': logbook_id,
        'uploadUrl': upload_url,
        's3Key': s3_key,
    })


def handle_status(logbook_id):
    """Return processing status for a logbook."""
    rows = execute_query(
        """SELECT ld.id, ld.processing_status, ld.page_count, ld.source_filename,
                  ld.logbook_type, ld.created_at,
                  COUNT(lp.id) FILTER (WHERE lp.extraction_status = 'completed') AS completed_pages,
                  COUNT(lp.id) AS total_pages
           FROM logbook_documents ld
           LEFT JOIN logbook_pages lp ON lp.document_id = ld.id
           WHERE ld.id = %s
           GROUP BY ld.id""",
        (logbook_id,)
    )
    if not rows:
        return api_response(404, {'error': 'Logbook not found'})

    row = rows[0]
    return api_response(200, {
        'logbookId': str(row['id']),
        'status': row['processing_status'],
        'filename': row['source_filename'],
        'logbookType': row['logbook_type'],
        'pageCount': row['page_count'] or row['total_pages'],
        'completedPages': row['completed_pages'],
        'createdAt': row['created_at'],
    })


def handle_list_logbooks(tail_number):
    """List all logbooks for an aircraft."""
    rows = execute_query(
        """SELECT ld.id, ld.logbook_type, ld.source_filename, ld.processing_status,
                  ld.page_count, ld.date_range_start, ld.date_range_end, ld.created_at
           FROM logbook_documents ld
           JOIN aircraft a ON ld.aircraft_id = a.id
           WHERE a.registration = %s
           ORDER BY ld.created_at DESC""",
        (tail_number.upper(),)
    )
    return api_response(200, {'tailNumber': tail_number.upper(), 'logbooks': rows})


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

    # Last annual
    annual = execute_query(
        """SELECT entry_date, flight_time FROM maintenance_entries
           WHERE aircraft_id = %s AND entry_type = 'annual'
           ORDER BY entry_date DESC LIMIT 1""", (aid,)
    )

    # Last 100hr
    hundredhr = execute_query(
        """SELECT entry_date, flight_time FROM maintenance_entries
           WHERE aircraft_id = %s AND entry_type = '100hr'
           ORDER BY entry_date DESC LIMIT 1""", (aid,)
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
    sm = boto3.client('secretsmanager', region_name='us-west-2')
    gemini_key = json.loads(
        sm.get_secret_value(SecretId=os.environ['GEMINI_SECRET_ARN'])['SecretString']
    ).get('api_key', '')

    from google import genai
    from google.genai import types

    client = genai.Client(api_key=gemini_key)

    # Generate embedding for the question
    embed_resp = client.models.embed_content(
        model='gemini-embedding-001',
        contents=question,
    )
    query_embedding = embed_resp.embeddings[0].values

    # Search pgvector for relevant maintenance entries
    embedding_str = '[' + ','.join(str(v) for v in query_embedding) + ']'
    results = execute_query(
        """SELECT me.chunk_text, me.chunk_type,
                  m.entry_date, m.entry_type, m.maintenance_narrative,
                  1 - (me.embedding <=> %s::vector) AS similarity
           FROM maintenance_embeddings me
           JOIN maintenance_entries m ON me.entry_id = m.id
           WHERE m.aircraft_id = %s
           ORDER BY me.embedding <=> %s::vector
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

    # Build context for Gemini
    context_parts = []
    for r in results:
        context_parts.append(
            f"[{r['entry_date']}] ({r['entry_type']}) {r['maintenance_narrative']}"
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
                      'similarity': round(r['similarity'], 3)} for r in results[:5]],
    })
