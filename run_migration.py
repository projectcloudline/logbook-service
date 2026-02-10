#!/usr/bin/env python3
"""Run a SQL migration against the database.

Usage:
    python run_migration.py sql/schema.sql [--dry-run]

Connection defaults to local Docker Postgres (localhost:5432, postgres/postgres).
Override with env vars: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD.

To run against AWS dev instead:
    DB_HOST="" DB_SECRET_ARN="arn:aws:..." python run_migration.py sql/schema.sql
"""

import json
import os
import sys
from pathlib import Path
import psycopg2


def load_dotenv():
    """Load .env file into os.environ (values don't override existing env vars)."""
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


def get_connection():
    host = os.environ.get('DB_HOST', 'localhost')
    port = int(os.environ.get('DB_PORT', '5432'))
    dbname = os.environ.get('DB_NAME', 'postgres')
    user = os.environ.get('DB_USER', 'postgres')
    password = os.environ.get('DB_PASSWORD', 'postgres')

    # If DB_HOST is explicitly empty and DB_SECRET_ARN is set, use Secrets Manager
    if not host and os.environ.get('DB_SECRET_ARN'):
        import boto3
        client = boto3.client('secretsmanager', region_name='us-west-2')
        resp = client.get_secret_value(SecretId=os.environ['DB_SECRET_ARN'])
        creds = json.loads(resp['SecretString'])
        host = creds['host']
        port = creds.get('port', 5432)
        dbname = creds.get('dbname', creds.get('database', 'postgres'))
        user = creds['username']
        password = creds['password']

    print(f'Connecting to {user}@{host}:{port}/{dbname}...')
    conn = psycopg2.connect(
        host=host, port=port, dbname=dbname,
        user=user, password=password,
        options='-c search_path=logbook,public',
    )
    conn.autocommit = True
    return conn


def main():
    if len(sys.argv) < 2:
        print(f'Usage: {sys.argv[0]} <sql_file> [--dry-run]')
        sys.exit(1)

    sql_file = sys.argv[1]
    dry_run = '--dry-run' in sys.argv

    with open(sql_file) as f:
        sql = f.read()

    if dry_run:
        print(f'\n--- DRY RUN: would execute {sql_file} ---')
        print(sql[:2000])
        if len(sql) > 2000:
            print(f'... ({len(sql)} chars total)')
        return

    conn = get_connection()

    print(f'Running {sql_file}...')
    with conn.cursor() as cur:
        cur.execute(sql)
    print('Migration complete.')

    # Verify
    print('\nVerification:')
    with conn.cursor() as cur:
        cur.execute("SELECT entry_type, COUNT(*) FROM maintenance_entries GROUP BY entry_type ORDER BY entry_type")
        rows = cur.fetchall()
        if rows:
            print('  entry_type counts:')
            for row in rows:
                print(f'    {row[0]}: {row[1]}')
        else:
            print('  (no entries yet)')

    conn.close()


if __name__ == '__main__':
    main()
