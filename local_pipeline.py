#!/usr/bin/env python3
"""Local end-to-end pipeline runner using LocalStack S3/SQS + local Postgres.

Usage:
    pip install -r lambda/api/requirements.txt -r lambda/split/requirements.txt flask
    python local_pipeline.py <logbook.pdf> [--tail-number N12345] [--logbook-type airframe]

This script:
  1. Creates S3 bucket + SQS queue in LocalStack
  2. Uploads the PDF to LocalStack S3
  3. Runs the split handler (PDF → page images → SQS messages)
  4. Runs the analyze handler for each queued page (Gemini extraction → DB)

Reads DB config from .env. Requires Gemini API key in Secrets Manager
(or set GEMINI_API_KEY env var to skip Secrets Manager).
"""

import json
import os
import sys
import time
import uuid
from pathlib import Path

# ─── Load .env ────────────────────────────────────────────
def load_dotenv():
    env_file = Path(__file__).parent / '.env'
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, _, value = line.partition('=')
        os.environ.setdefault(key.strip(), value.strip())

load_dotenv()

# ─── LocalStack + DB config ──────────────────────────────
LOCALSTACK_URL = os.environ.get('LOCALSTACK_URL', 'http://localhost:4566')
BUCKET_NAME = 'logbook-local-dev'
QUEUE_NAME = 'logbook-analyze-local'

os.environ.setdefault('DB_HOST', 'localhost')
os.environ.setdefault('DB_PORT', '5432')
os.environ.setdefault('DB_NAME', 'postgres')
os.environ.setdefault('DB_USER', 'postgres')
os.environ.setdefault('DB_PASSWORD', 'postgres')

# Point boto3 at LocalStack with dummy creds
os.environ['AWS_ACCESS_KEY_ID'] = 'test'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

os.environ['BUCKET_NAME'] = BUCKET_NAME

# Add shared lambda path (for shared.db, shared.models)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lambda'))

import boto3
import importlib.util


def load_handler(name, handler_dir):
    """Load a handler.py module from a specific directory without name collisions."""
    spec = importlib.util.spec_from_file_location(name, os.path.join(handler_dir, 'handler.py'))
    mod = importlib.util.module_from_spec(spec)
    # Add the handler's own directory to sys.path so its local imports resolve
    handler_dir_abs = os.path.abspath(handler_dir)
    if handler_dir_abs not in sys.path:
        sys.path.insert(0, handler_dir_abs)
    spec.loader.exec_module(mod)
    return mod


def setup_localstack():
    """Create S3 bucket and SQS queue in LocalStack."""
    s3 = boto3.client('s3', endpoint_url=LOCALSTACK_URL)
    sqs = boto3.client('sqs', endpoint_url=LOCALSTACK_URL)

    # Create bucket
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f'Created S3 bucket: {BUCKET_NAME}')
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f'S3 bucket already exists: {BUCKET_NAME}')
    except Exception as e:
        if 'BucketAlreadyOwnedByYou' in str(e) or 'BucketAlreadyExists' in str(e):
            print(f'S3 bucket already exists: {BUCKET_NAME}')
        else:
            raise

    # Create queue
    try:
        resp = sqs.create_queue(QueueName=QUEUE_NAME)
        queue_url = resp['QueueUrl']
        print(f'Created SQS queue: {queue_url}')
    except Exception:
        resp = sqs.get_queue_url(QueueName=QUEUE_NAME)
        queue_url = resp['QueueUrl']
        print(f'SQS queue already exists: {queue_url}')

    os.environ['ANALYZE_QUEUE_URL'] = queue_url
    return s3, sqs, queue_url


def patch_boto_clients(endpoint_url):
    """Monkey-patch boto3.client to route S3/SQS to LocalStack."""
    original_client = boto3.client

    def patched_client(service_name, **kwargs):
        if service_name in ('s3', 'sqs'):
            kwargs['endpoint_url'] = endpoint_url
        return original_client(service_name, **kwargs)

    boto3.client = patched_client


def upload_pdf(s3, pdf_path, logbook_id):
    """Upload a PDF to LocalStack S3."""
    filename = os.path.basename(pdf_path)
    s3_key = f'uploads/{logbook_id}/{filename}'
    s3.upload_file(pdf_path, BUCKET_NAME, s3_key, ExtraArgs={'ContentType': 'application/pdf'})
    print(f'Uploaded {pdf_path} → s3://{BUCKET_NAME}/{s3_key}')
    return s3_key


def create_logbook_record(tail_number, logbook_type, filename, logbook_id, s3_key):
    """Create aircraft + logbook_documents records in DB."""
    from shared.db import execute_insert

    aircraft_id = execute_insert(
        """INSERT INTO aircraft (registration) VALUES (%s)
           ON CONFLICT (registration) DO UPDATE SET updated_at = NOW()
           RETURNING id""",
        (tail_number,)
    )

    execute_insert(
        """INSERT INTO logbook_documents (id, aircraft_id, logbook_type, source_filename, s3_key, processing_status)
           VALUES (%s, %s, %s, %s, %s, 'pending') RETURNING id""",
        (logbook_id, aircraft_id, logbook_type, filename, s3_key)
    )

    print(f'Created logbook record: {logbook_id} for {tail_number}')
    return aircraft_id


def run_split(s3_key, logbook_id):
    """Run the split handler locally."""
    print(f'\n{"="*60}')
    print('SPLIT: Processing PDF into pages...')
    print(f'{"="*60}')

    base = os.path.dirname(__file__)
    split_handler = load_handler('split_handler', os.path.join(base, 'lambda', 'split'))

    event = {
        'Records': [{
            's3': {
                'bucket': {'name': BUCKET_NAME},
                'object': {'key': s3_key},
            }
        }]
    }

    split_handler.handler(event, {})
    print('Split complete.')


def run_analyze(sqs, queue_url):
    """Poll SQS and run the analyze handler for each message."""
    print(f'\n{"="*60}')
    print('ANALYZE: Processing pages with Gemini...')
    print(f'{"="*60}')

    base = os.path.dirname(__file__)
    analyze_handler = load_handler('analyze_handler', os.path.join(base, 'lambda', 'analyze'))

    processed = 0
    while True:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1,
        )

        messages = resp.get('Messages', [])
        if not messages:
            break

        for msg in messages:
            body = json.loads(msg['Body'])
            print(f'\nProcessing page {body["pageNumber"]} (page_id={body["pageId"]})')

            event = {
                'Records': [{
                    'body': msg['Body'],
                }]
            }

            try:
                analyze_handler.handler(event, {})
                processed += 1
            except Exception as e:
                print(f'ERROR analyzing page {body["pageNumber"]}: {e}')

            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=msg['ReceiptHandle'],
            )

    print(f'\nAnalyzed {processed} pages.')
    return processed


def show_results(tail_number):
    """Show what ended up in the DB."""
    from shared.db import execute_query

    print(f'\n{"="*60}')
    print(f'RESULTS for {tail_number}')
    print(f'{"="*60}')

    entries = execute_query(
        """SELECT entry_type, COUNT(*) as cnt
           FROM maintenance_entries me
           JOIN aircraft a ON me.aircraft_id = a.id
           WHERE a.registration = %s
           GROUP BY entry_type ORDER BY entry_type""",
        (tail_number,)
    )
    print('\nEntries by type:')
    for r in entries:
        print(f'  {r["entry_type"]}: {r["cnt"]}')

    inspections = execute_query(
        """SELECT inspection_type, COUNT(*) as cnt
           FROM inspection_records ir
           JOIN aircraft a ON ir.aircraft_id = a.id
           WHERE a.registration = %s
           GROUP BY inspection_type ORDER BY inspection_type""",
        (tail_number,)
    )
    if inspections:
        print('\nInspections by type:')
        for r in inspections:
            print(f'  {r["inspection_type"]}: {r["cnt"]}')

    total = execute_query(
        """SELECT COUNT(*) as cnt FROM maintenance_entries me
           JOIN aircraft a ON me.aircraft_id = a.id
           WHERE a.registration = %s""",
        (tail_number,)
    )
    print(f'\nTotal entries: {total[0]["cnt"]}')
    print(f'\nNow start the local server and query:')
    print(f'  python local_server.py')
    print(f'  curl http://localhost:8080/aircraft/{tail_number}/entries')
    print(f'  curl http://localhost:8080/aircraft/{tail_number}/summary')


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Run logbook processing pipeline locally')
    parser.add_argument('pdf', help='Path to logbook PDF')
    parser.add_argument('--tail-number', default='N12345', help='Aircraft tail number (default: N12345)')
    parser.add_argument('--logbook-type', default='airframe', help='Logbook type (default: airframe)')
    parser.add_argument('--skip-analyze', action='store_true', help='Only split, skip Gemini analysis')
    args = parser.parse_args()

    pdf_path = os.path.abspath(args.pdf)
    if not os.path.exists(pdf_path):
        print(f'File not found: {pdf_path}')
        sys.exit(1)

    tail_number = args.tail_number.upper()
    logbook_id = str(uuid.uuid4())

    # Setup
    s3, sqs, queue_url = setup_localstack()
    patch_boto_clients(LOCALSTACK_URL)

    # Reinitialize module-level clients that were created before patching
    import importlib

    # Upload
    s3_key = upload_pdf(s3, pdf_path, logbook_id)
    create_logbook_record(tail_number, args.logbook_type, os.path.basename(pdf_path), logbook_id, s3_key)

    # Split
    run_split(s3_key, logbook_id)

    # Analyze
    if args.skip_analyze:
        print('\nSkipping analysis (--skip-analyze). Messages are queued in SQS.')
    else:
        # Gemini needs real credentials for the API key
        if not os.environ.get('GEMINI_API_KEY') and not os.environ.get('GEMINI_SECRET_ARN'):
            os.environ['GEMINI_SECRET_ARN'] = 'dev/forge/gemini-api-key'
            # Restore real AWS creds for Secrets Manager
            for key in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']:
                if key in os.environ:
                    del os.environ[key]

        run_analyze(sqs, queue_url)

    show_results(tail_number)


if __name__ == '__main__':
    main()
