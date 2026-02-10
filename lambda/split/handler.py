"""Split Lambda — triggered by S3 upload, splits PDF into page images, queues for analysis."""

import json
import os
import uuid
import tempfile
from pathlib import Path

import boto3
import fitz  # pymupdf

from shared.db import execute_query, execute_insert, get_connection

s3 = boto3.client('s3')
sqs = boto3.client('sqs')

BUCKET = os.environ['BUCKET_NAME']
QUEUE_URL = os.environ['ANALYZE_QUEUE_URL']

IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif'}


def handler(event, context):
    """Handle S3 PUT event for uploaded logbook files."""
    for record in event.get('Records', []):
        s3_key = record['s3']['object']['key']
        bucket = record['s3']['bucket']['name']

        print(f'Processing upload: s3://{bucket}/{s3_key}')

        # Extract logbook ID from key: uploads/{logbookId}/filename
        parts = s3_key.split('/')
        if len(parts) < 3 or parts[0] != 'uploads':
            print(f'Ignoring key {s3_key} — not in uploads/{{id}}/filename format')
            continue

        logbook_id = parts[1]
        filename = '/'.join(parts[2:])
        ext = Path(filename).suffix.lower()

        # Update status to processing
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE logbook_documents SET processing_status = 'processing', updated_at = NOW() WHERE id = %s",
                (logbook_id,)
            )
            conn.commit()

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                local_file = os.path.join(tmpdir, filename)
                s3.download_file(bucket, s3_key, local_file)

                if ext == '.pdf':
                    page_keys = split_pdf(local_file, logbook_id, tmpdir)
                elif ext in IMAGE_EXTENSIONS:
                    page_keys = handle_single_image(local_file, logbook_id, s3_key)
                else:
                    print(f'Unsupported file type: {ext}')
                    mark_failed(logbook_id, f'Unsupported file type: {ext}')
                    continue

            # Update page count
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE logbook_documents SET page_count = %s, updated_at = NOW() WHERE id = %s",
                    (len(page_keys), logbook_id)
                )
                conn.commit()

            # Create page records and queue messages
            for i, page_key in enumerate(page_keys, 1):
                page_id = execute_insert(
                    """INSERT INTO logbook_pages (document_id, page_number, image_path, extraction_status)
                       VALUES (%s, %s, %s, 'pending') RETURNING id""",
                    (logbook_id, i, page_key)
                )

                sqs.send_message(
                    QueueUrl=QUEUE_URL,
                    MessageBody=json.dumps({
                        'logbookId': logbook_id,
                        'pageId': page_id,
                        'pageNumber': i,
                        's3Key': page_key,
                    }),
                )

            print(f'Queued {len(page_keys)} pages for analysis')

        except Exception as e:
            print(f'ERROR splitting {s3_key}: {e}')
            mark_failed(logbook_id, str(e))
            raise


def split_pdf(pdf_path: str, logbook_id: str, tmpdir: str) -> list[str]:
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
        s3_key = f'pages/{logbook_id}/{page_filename}'
        s3.upload_file(local_path, BUCKET, s3_key, ExtraArgs={'ContentType': 'image/jpeg'})
        page_keys.append(s3_key)

        print(f'  Uploaded page {page_num + 1}/{len(doc)}: {s3_key}')

    doc.close()
    return page_keys


def handle_single_image(local_file: str, logbook_id: str, original_key: str) -> list[str]:
    """Handle a single image upload (just copy to pages prefix)."""
    s3_key = f'pages/{logbook_id}/page_0001.jpg'
    s3.upload_file(local_file, BUCKET, s3_key, ExtraArgs={'ContentType': 'image/jpeg'})
    return [s3_key]


def mark_failed(logbook_id: str, error: str):
    """Mark a logbook as failed."""
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE logbook_documents SET processing_status = 'failed', updated_at = NOW() WHERE id = %s",
            (logbook_id,)
        )
        conn.commit()
