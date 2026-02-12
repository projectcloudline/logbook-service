"""Split Lambda — triggered by S3 upload, splits PDF into page images, queues for analysis."""

import json
import os
import uuid
import tempfile
from pathlib import Path
from urllib.parse import unquote_plus

import boto3
import fitz  # pymupdf

from shared.db import execute_query, execute_insert, get_connection

s3 = boto3.client('s3')
sqs = boto3.client('sqs')

BUCKET = os.environ['BUCKET_NAME']
QUEUE_URL = os.environ['ANALYZE_QUEUE_URL']

IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif'}


def handler(event, context):
    """Handle S3 PUT events for uploaded logbook files.

    Two triggers:
    - uploads/ prefix: PDFs and single images needing splitting
    - pages/ prefix: individual page images from multi-image uploads (page records pre-created)
    """
    for record in event.get('Records', []):
        s3_key = unquote_plus(record['s3']['object']['key'])
        bucket = record['s3']['bucket']['name']

        print(f'Processing upload: s3://{bucket}/{s3_key}')

        parts = s3_key.split('/')
        if len(parts) < 3:
            print(f'Ignoring key {s3_key} — unexpected format')
            continue

        if parts[0] == 'pages':
            # Multi-image: page record already exists, just queue for analysis
            handle_page_arrival(parts[1], s3_key)
        elif parts[0] == 'uploads':
            # PDF or single image: needs splitting
            handle_pdf_upload(parts[1], '/'.join(parts[2:]), s3_key, bucket)
        else:
            print(f'Ignoring key {s3_key} — not in uploads/ or pages/ prefix')


def handle_page_arrival(batch_id: str, s3_key: str):
    """Handle a single page image arriving in the pages/ prefix (multi-image upload)."""
    # Parse page number from key: pages/{batchId}/page_XXXX.jpg
    filename = s3_key.split('/')[-1]
    try:
        page_number = int(filename.split('_')[1].split('.')[0])
    except (IndexError, ValueError):
        print(f'Could not parse page number from {s3_key}')
        return

    # Look up existing page record
    rows = execute_query(
        "SELECT id FROM upload_pages WHERE document_id = %s AND page_number = %s",
        (batch_id, page_number)
    )
    if not rows:
        print(f'No page record found for batch {batch_id} page {page_number}, skipping')
        return

    page_id = str(rows[0]['id'])

    # Set batch to processing (idempotent — harmless if already processing)
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE upload_batches SET processing_status = 'processing', updated_at = NOW() WHERE id = %s AND processing_status = 'pending'",
            (batch_id,)
        )
        conn.commit()

    # Queue for analysis
    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps({
            'uploadId': batch_id,
            'pageId': page_id,
            'pageNumber': page_number,
            's3Key': s3_key,
        }),
    )
    print(f'Queued page {page_number} of batch {batch_id} for analysis')


def handle_pdf_upload(batch_id: str, filename: str, s3_key: str, bucket: str):
    """Handle a PDF or single image upload that needs splitting."""
    ext = Path(filename).suffix.lower()

    # Update status to processing
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE upload_batches SET processing_status = 'processing', updated_at = NOW() WHERE id = %s",
            (batch_id,)
        )
        conn.commit()

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            local_file = os.path.join(tmpdir, filename)
            s3.download_file(bucket, s3_key, local_file)

            if ext == '.pdf':
                page_keys = split_pdf(local_file, batch_id, tmpdir)
            elif ext in IMAGE_EXTENSIONS:
                page_keys = handle_single_image(local_file, batch_id, s3_key)
            else:
                print(f'Unsupported file type: {ext}')
                mark_failed(batch_id, f'Unsupported file type: {ext}')
                return

        # Update page count
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE upload_batches SET page_count = %s, updated_at = NOW() WHERE id = %s",
                (len(page_keys), batch_id)
            )
            conn.commit()

        # Create page records and queue messages
        for i, page_key in enumerate(page_keys, 1):
            page_id = execute_insert(
                """INSERT INTO upload_pages (document_id, page_number, image_path, extraction_status)
                   VALUES (%s, %s, %s, 'pending') RETURNING id""",
                (batch_id, i, page_key)
            )

            sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=json.dumps({
                    'uploadId': batch_id,
                    'pageId': page_id,
                    'pageNumber': i,
                    's3Key': page_key,
                }),
            )

        print(f'Queued {len(page_keys)} pages for analysis')

    except Exception as e:
        print(f'ERROR splitting {s3_key}: {e}')
        mark_failed(batch_id, str(e))
        raise


def split_pdf(pdf_path: str, batch_id: str, tmpdir: str) -> list[str]:
    """Split a PDF into page images and upload to S3."""
    doc = fitz.open(pdf_path)
    page_keys = []

    for page_num in range(len(doc)):
        page = doc[page_num]
        # Render at 200 DPI for good quality without huge files
        mat = fitz.Matrix(200 / 72, 200 / 72)
        pix = page.get_pixmap(matrix=mat)

        page_filename = f'page_{page_num + 1:04d}.jpg'
        local_path = os.path.join(tmpdir, page_filename)

        # Save as JPEG
        pix.save(local_path)

        # Upload to S3
        s3_key = f'pages/{batch_id}/{page_filename}'
        s3.upload_file(local_path, BUCKET, s3_key, ExtraArgs={'ContentType': 'image/jpeg'})
        page_keys.append(s3_key)

        print(f'  Uploaded page {page_num + 1}/{len(doc)}: {s3_key}')

    doc.close()
    return page_keys


def handle_single_image(local_file: str, batch_id: str, original_key: str) -> list[str]:
    """Handle a single image upload (just copy to pages prefix)."""
    s3_key = f'pages/{batch_id}/page_0001.jpg'
    s3.upload_file(local_file, BUCKET, s3_key, ExtraArgs={'ContentType': 'image/jpeg'})
    return [s3_key]


def mark_failed(batch_id: str, error: str):
    """Mark an upload batch as failed."""
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE upload_batches SET processing_status = 'failed', updated_at = NOW() WHERE id = %s",
            (batch_id,)
        )
        conn.commit()
