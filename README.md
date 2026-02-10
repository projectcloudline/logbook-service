# Logbook Service

Serverless aircraft logbook digitization service built with AWS CDK.

## Architecture

- **API Gateway** (API key auth) → Lambda (Python 3.12)
- **S3** → Split Lambda (PDF → page images) → **SQS** → Analyze Lambda (Gemini extraction → RDS)
- **RDS Postgres** with pgvector for semantic search
- **Gemini AI** for OCR extraction and RAG queries

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/logbooks/upload` | Get presigned S3 upload URL |
| GET | `/logbooks/{id}/status` | Processing progress |
| GET | `/aircraft/{tailNumber}/logbooks` | List logbooks for aircraft |
| GET | `/aircraft/{tailNumber}/summary` | Maintenance summary |
| POST | `/aircraft/{tailNumber}/query` | RAG query over maintenance records |

All endpoints require `x-api-key` header.

## Prerequisites

- Node.js 20+, npm
- AWS CDK v2 (`npm install -g aws-cdk`)
- AWS CLI configured with `cl-admin` profile
- Docker (for Lambda bundling)

## Setup

```bash
npm install
```

## Database Migration

Run the schema SQL against your RDS Postgres instance:

```bash
psql -h <rds-host> -U <user> -d <database> -f sql/schema.sql
```

## Secrets

Create the Gemini API key secret:

```bash
aws secretsmanager create-secret \
  --name dev/forge/gemini-api-key \
  --secret-string '{"api_key": "YOUR_KEY"}' \
  --profile cl-admin --region us-west-2
```

DB credentials should already exist at `dev/forge/db-credentials`.

## Deploy

```bash
# Diff first
npx cdk diff --profile cl-admin

# Deploy
npx cdk deploy --profile cl-admin
```

## Usage

```bash
# Get API key
API_KEY=$(aws apigateway get-api-key --api-key <key-id> --include-value --query 'value' --output text --profile cl-admin)

# Upload a logbook
curl -X POST https://<api-url>/v1/logbooks/upload \
  -H "x-api-key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"tailNumber": "N69ZA", "logbookType": "airframe", "filename": "logbook.pdf"}'

# Upload the file to the presigned URL
curl -X PUT "<uploadUrl>" \
  -H "Content-Type: application/pdf" \
  --data-binary @logbook.pdf

# Check status
curl https://<api-url>/v1/logbooks/<id>/status -H "x-api-key: $API_KEY"

# Query
curl -X POST https://<api-url>/v1/aircraft/N69ZA/query \
  -H "x-api-key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"question": "When was the last oil change?"}'
```

## Project Structure

```
├── bin/logbook-service.ts       # CDK app entry
├── lib/logbook-service-stack.ts # CDK stack
├── lambda/
│   ├── api/handler.py           # API routes
│   ├── split/handler.py         # PDF splitting
│   ├── analyze/handler.py       # Gemini extraction
│   └── shared/db.py             # DB connection helper
├── sql/schema.sql               # Database migration
└── README.md
```
