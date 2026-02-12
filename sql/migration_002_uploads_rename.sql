-- Migration 002: Rename logbook_documents/logbook_pages to upload_batches/upload_pages
-- Adds: completed_with_errors status, upload_type column, nullable logbook_type
-- Idempotent — safe to run multiple times.

SET search_path TO logbook, public;
BEGIN;

-- Rename tables
ALTER TABLE IF EXISTS logbook_documents RENAME TO upload_batches;
ALTER TABLE IF EXISTS logbook_pages RENAME TO upload_pages;

-- Update processing_status CHECK to add completed_with_errors
ALTER TABLE upload_batches DROP CONSTRAINT IF EXISTS logbook_documents_processing_status_check;
ALTER TABLE upload_batches ADD CONSTRAINT upload_batches_processing_status_check
    CHECK (processing_status IN ('pending', 'processing', 'completed', 'completed_with_errors', 'failed'));

-- Add upload_type column
ALTER TABLE upload_batches ADD COLUMN IF NOT EXISTS upload_type VARCHAR(20) DEFAULT 'pdf'
    CHECK (upload_type IN ('pdf', 'multi_image'));

-- Make logbook_type nullable (was NOT NULL)
ALTER TABLE upload_batches ALTER COLUMN logbook_type DROP NOT NULL;

COMMIT;
