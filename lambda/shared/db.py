"""Shared database connection helper. Reads credentials from Secrets Manager."""

import os
import json
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor

_cached_secret = None
_cached_conn = None


def get_db_credentials() -> dict:
    """Fetch DB credentials from Secrets Manager (cached for Lambda warm starts)."""
    global _cached_secret
    if _cached_secret:
        return _cached_secret

    client = boto3.client('secretsmanager', region_name='us-west-2')
    resp = client.get_secret_value(SecretId=os.environ['DB_SECRET_ARN'])
    _cached_secret = json.loads(resp['SecretString'])
    return _cached_secret


def get_connection():
    """Get a psycopg2 connection (cached for Lambda warm starts)."""
    global _cached_conn
    if _cached_conn and not _cached_conn.closed:
        try:
            _cached_conn.execute('SELECT 1')  # type: ignore
            return _cached_conn
        except Exception:
            _cached_conn = None

    creds = get_db_credentials()
    _cached_conn = psycopg2.connect(
        host=creds['host'],
        port=creds.get('port', 5432),
        dbname=creds.get('dbname', creds.get('database', 'postgres')),
        user=creds['username'],
        password=creds['password'],
        options='-c search_path=logbook,public',
        connect_timeout=10,
    )
    _cached_conn.autocommit = False
    return _cached_conn


def execute_query(sql: str, params=None, fetch: bool = True):
    """Execute a query and return results as list of dicts."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            if fetch:
                rows = cur.fetchall()
                conn.commit()
                return [dict(r) for r in rows]
            conn.commit()
            return None
    except Exception:
        conn.rollback()
        raise


def execute_insert(sql: str, params=None) -> str | None:
    """Execute an INSERT ... RETURNING id and return the id."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            result = cur.fetchone()
            conn.commit()
            return str(result[0]) if result else None
    except Exception:
        conn.rollback()
        raise
