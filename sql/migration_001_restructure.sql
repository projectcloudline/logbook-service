-- Migration 001: Restructure entry_type and logbook_type
-- Run BEFORE deploying new Lambda code.
-- Idempotent — safe to re-run.

SET search_path TO logbook, public;

BEGIN;

-- =====================================================
-- 1. Expand logbook_type: add avionics, appliance
-- =====================================================

ALTER TABLE logbook_documents DROP CONSTRAINT IF EXISTS logbook_documents_logbook_type_check;
ALTER TABLE logbook_documents ADD CONSTRAINT logbook_documents_logbook_type_check
    CHECK (logbook_type IN ('airframe', 'engine', 'propeller', 'avionics', 'appliance'));

-- =====================================================
-- 2. Migrate entry_type data BEFORE changing constraint
-- =====================================================

-- 2a. annual entries → create inspection_records if missing, set entry_type = 'inspection'
INSERT INTO inspection_records (aircraft_id, entry_id, inspection_type, inspection_date, aircraft_hours, inspector_name, inspector_certificate)
SELECT me.aircraft_id, me.id, 'annual', me.entry_date, me.flight_time, me.mechanic_name, me.mechanic_certificate
FROM maintenance_entries me
WHERE me.entry_type = 'annual'
  AND NOT EXISTS (
      SELECT 1 FROM inspection_records ir WHERE ir.entry_id = me.id
  );

UPDATE maintenance_entries SET entry_type = 'inspection' WHERE entry_type = 'annual';

-- 2b. 100hr entries → create inspection_records if missing, set entry_type = 'inspection'
INSERT INTO inspection_records (aircraft_id, entry_id, inspection_type, inspection_date, aircraft_hours, inspector_name, inspector_certificate)
SELECT me.aircraft_id, me.id, '100hr', me.entry_date, me.flight_time, me.mechanic_name, me.mechanic_certificate
FROM maintenance_entries me
WHERE me.entry_type = '100hr'
  AND NOT EXISTS (
      SELECT 1 FROM inspection_records ir WHERE ir.entry_id = me.id
  );

UPDATE maintenance_entries SET entry_type = 'inspection' WHERE entry_type = '100hr';

-- 2c. progressive entries → create inspection_records if missing, set entry_type = 'inspection'
INSERT INTO inspection_records (aircraft_id, entry_id, inspection_type, inspection_date, aircraft_hours, inspector_name, inspector_certificate)
SELECT me.aircraft_id, me.id, 'progressive', me.entry_date, me.flight_time, me.mechanic_name, me.mechanic_certificate
FROM maintenance_entries me
WHERE me.entry_type = 'progressive'
  AND NOT EXISTS (
      SELECT 1 FROM inspection_records ir WHERE ir.entry_id = me.id
  );

UPDATE maintenance_entries SET entry_type = 'inspection' WHERE entry_type = 'progressive';

-- 2d. altimeter_check → create inspection_records with inspection_type = 'altimeter_static'
INSERT INTO inspection_records (aircraft_id, entry_id, inspection_type, inspection_date, aircraft_hours, inspector_name, inspector_certificate)
SELECT me.aircraft_id, me.id, 'altimeter_static', me.entry_date, me.flight_time, me.mechanic_name, me.mechanic_certificate
FROM maintenance_entries me
WHERE me.entry_type = 'altimeter_check'
  AND NOT EXISTS (
      SELECT 1 FROM inspection_records ir WHERE ir.entry_id = me.id
  );

UPDATE maintenance_entries SET entry_type = 'inspection' WHERE entry_type = 'altimeter_check';

-- 2e. transponder_check → create inspection_records with inspection_type = 'transponder'
INSERT INTO inspection_records (aircraft_id, entry_id, inspection_type, inspection_date, aircraft_hours, inspector_name, inspector_certificate)
SELECT me.aircraft_id, me.id, 'transponder', me.entry_date, me.flight_time, me.mechanic_name, me.mechanic_certificate
FROM maintenance_entries me
WHERE me.entry_type = 'transponder_check'
  AND NOT EXISTS (
      SELECT 1 FROM inspection_records ir WHERE ir.entry_id = me.id
  );

UPDATE maintenance_entries SET entry_type = 'inspection' WHERE entry_type = 'transponder_check';

-- =====================================================
-- 3. Update entry_type CHECK constraint
-- =====================================================

ALTER TABLE maintenance_entries DROP CONSTRAINT IF EXISTS maintenance_entries_entry_type_check;
ALTER TABLE maintenance_entries ADD CONSTRAINT maintenance_entries_entry_type_check
    CHECK (entry_type IN ('maintenance', 'inspection', 'ad_compliance', 'other'));

COMMIT;
