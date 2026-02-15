-- Migration 003: Add extraction_notes to maintenance_entries
-- Stores QA verification feedback so users can see why an entry was flagged for review.
-- Idempotent — safe to run multiple times.

SET search_path TO logbook, public;

ALTER TABLE maintenance_entries ADD COLUMN IF NOT EXISTS extraction_notes TEXT;
