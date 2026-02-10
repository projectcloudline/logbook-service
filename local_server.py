#!/usr/bin/env python3
"""Local dev server — wraps the API Lambda handler behind Flask.

Usage:
    pip install -r lambda/api/requirements.txt flask
    python local_server.py

Defaults to local Docker Postgres (localhost:5432, postgres/postgres).
Override with DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD env vars.
"""

import json
import os
import sys
import re
from pathlib import Path


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

# Add lambda paths so imports resolve
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lambda', 'api'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lambda'))

# Defaults for anything not in .env
os.environ.setdefault('DB_HOST', 'localhost')
os.environ.setdefault('DB_PORT', '5432')
os.environ.setdefault('DB_NAME', 'postgres')
os.environ.setdefault('DB_USER', 'postgres')
os.environ.setdefault('DB_PASSWORD', 'postgres')
os.environ.setdefault('BUCKET_NAME', 'local-dev-unused')
os.environ.setdefault('GEMINI_SECRET_ARN', 'dev/forge/gemini-api-key')

from flask import Flask, request, Response

import handler as api_handler

app = Flask(__name__)

# Route patterns that match API Gateway resource templates
ROUTE_PATTERNS = [
    ('POST', '/logbooks/upload', '/logbooks/upload'),
    ('GET',  r'/logbooks/(?P<id>[^/]+)/status', '/logbooks/{id}/status'),
    ('GET',  r'/aircraft/(?P<tailNumber>[^/]+)/logbooks', '/aircraft/{tailNumber}/logbooks'),
    ('GET',  r'/aircraft/(?P<tailNumber>[^/]+)/summary', '/aircraft/{tailNumber}/summary'),
    ('POST', r'/aircraft/(?P<tailNumber>[^/]+)/query', '/aircraft/{tailNumber}/query'),
    ('GET',  r'/aircraft/(?P<tailNumber>[^/]+)/entries/(?P<entryId>[^/]+)', '/aircraft/{tailNumber}/entries/{entryId}'),
    ('GET',  r'/aircraft/(?P<tailNumber>[^/]+)/entries', '/aircraft/{tailNumber}/entries'),
    ('GET',  r'/aircraft/(?P<tailNumber>[^/]+)/inspections', '/aircraft/{tailNumber}/inspections'),
    ('GET',  r'/aircraft/(?P<tailNumber>[^/]+)/ads', '/aircraft/{tailNumber}/ads'),
    ('GET',  r'/aircraft/(?P<tailNumber>[^/]+)/parts', '/aircraft/{tailNumber}/parts'),
]


def match_route(method, path):
    """Match a request to an API Gateway resource template + path params."""
    for route_method, pattern, resource in ROUTE_PATTERNS:
        if method != route_method:
            continue
        m = re.fullmatch(pattern, path)
        if m:
            return resource, m.groupdict()
    return None, None


@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
@app.route('/', methods=['GET'])
def catch_all(path=''):
    path = '/' + path
    method = request.method

    resource, path_params = match_route(method, path)
    if not resource:
        return Response(json.dumps({'error': 'Not found', 'path': path}),
                        status=404, content_type='application/json')

    # Build Lambda proxy event
    event = {
        'httpMethod': method,
        'resource': resource,
        'path': path,
        'pathParameters': path_params or None,
        'queryStringParameters': dict(request.args) if request.args else None,
        'headers': dict(request.headers),
        'body': request.get_data(as_text=True) or None,
    }

    result = api_handler.handler(event, {})

    return Response(
        result.get('body', '{}'),
        status=result.get('statusCode', 200),
        headers=result.get('headers', {}),
    )


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    print(f'Local logbook API running on http://localhost:{port}')
    print(f'DB: {os.environ.get("DB_USER")}@{os.environ.get("DB_HOST")}:{os.environ.get("DB_PORT")}/{os.environ.get("DB_NAME")}')
    print()
    print('Try:')
    print(f'  curl http://localhost:{port}/aircraft/N12345/summary')
    print(f'  curl http://localhost:{port}/aircraft/N12345/entries')
    print(f'  curl http://localhost:{port}/aircraft/N12345/inspections')
    print(f'  curl http://localhost:{port}/aircraft/N12345/ads')
    print(f'  curl http://localhost:{port}/aircraft/N12345/parts')
    print()
    app.run(host='0.0.0.0', port=port, debug=True)
