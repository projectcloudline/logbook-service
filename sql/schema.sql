-- Logbook Service Schema
-- All tables in the 'logbook' schema

CREATE SCHEMA IF NOT EXISTS logbook;
SET search_path TO logbook, public;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "vector";

-- =====================================================
-- CORE TABLES
-- =====================================================

CREATE TABLE IF NOT EXISTS aircraft (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    registration VARCHAR(10) NOT NULL UNIQUE,
    serial_number VARCHAR(50),
    make VARCHAR(100),
    model VARCHAR(50),
    engine_model VARCHAR(50),
    engine_serial VARCHAR(50),
    propeller_model VARCHAR(50),
    propeller_serial VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS upload_batches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aircraft_id UUID REFERENCES aircraft(id),
    logbook_type VARCHAR(20) CHECK (logbook_type IN ('airframe', 'engine', 'propeller', 'avionics', 'appliance')),
    upload_type VARCHAR(20) DEFAULT 'pdf'
        CHECK (upload_type IN ('pdf', 'multi_image')),
    source_filename VARCHAR(500) NOT NULL,
    s3_key VARCHAR(500),
    file_hash VARCHAR(64),
    page_count INTEGER,
    date_range_start DATE,
    date_range_end DATE,
    processing_status VARCHAR(20) DEFAULT 'pending'
        CHECK (processing_status IN ('pending', 'processing', 'completed', 'completed_with_errors', 'failed')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS upload_pages (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    document_id UUID NOT NULL REFERENCES upload_batches(id) ON DELETE CASCADE,
    page_number INTEGER NOT NULL,
    image_path VARCHAR(500) NOT NULL,  -- S3 key
    page_type VARCHAR(50),
    extraction_status VARCHAR(20) DEFAULT 'pending'
        CHECK (extraction_status IN ('pending', 'processing', 'completed', 'failed', 'skipped')),
    extraction_model VARCHAR(50),
    extraction_timestamp TIMESTAMPTZ,
    raw_extraction JSONB,
    needs_review BOOLEAN DEFAULT FALSE,
    review_notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(document_id, page_number)
);

-- =====================================================
-- MAINTENANCE ENTRIES
-- =====================================================

CREATE TABLE IF NOT EXISTS maintenance_entries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aircraft_id UUID NOT NULL REFERENCES aircraft(id),
    page_id UUID REFERENCES upload_pages(id),
    entry_type VARCHAR(30) DEFAULT 'maintenance'
        CHECK (entry_type IN ('maintenance', 'inspection', 'ad_compliance', 'other')),
    entry_date DATE NOT NULL,
    hobbs_time DECIMAL(10,1),
    tach_time DECIMAL(10,1),
    flight_time DECIMAL(10,1),
    time_since_overhaul DECIMAL(10,1),
    shop_name VARCHAR(200),
    shop_address TEXT,
    shop_phone VARCHAR(100),
    repair_station_number VARCHAR(100),
    mechanic_name VARCHAR(200),
    mechanic_certificate VARCHAR(100),
    work_order_number VARCHAR(100),
    maintenance_narrative TEXT NOT NULL,
    confidence_score DECIMAL(3,2),
    needs_review BOOLEAN DEFAULT FALSE,
    missing_data TEXT[],
    extraction_notes TEXT,
    review_status VARCHAR(20) DEFAULT 'pending'
        CHECK (review_status IN ('pending', 'approved', 'corrected', 'rejected')),
    reviewed_by VARCHAR(100),
    reviewed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_maintenance_aircraft_date ON maintenance_entries(aircraft_id, entry_date);
CREATE INDEX IF NOT EXISTS idx_maintenance_needs_review ON maintenance_entries(needs_review) WHERE needs_review = TRUE;

-- =====================================================
-- PARTS TRACKING
-- =====================================================

CREATE TABLE IF NOT EXISTS parts_actions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entry_id UUID NOT NULL REFERENCES maintenance_entries(id) ON DELETE CASCADE,
    action_type VARCHAR(20) NOT NULL
        CHECK (action_type IN ('installed', 'removed', 'replaced', 'repaired', 'inspected', 'overhauled')),
    part_name VARCHAR(200),
    part_number VARCHAR(100),
    serial_number VARCHAR(100),
    quantity INTEGER DEFAULT 1,
    old_part_number VARCHAR(100),
    old_serial_number VARCHAR(100),
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_parts_entry ON parts_actions(entry_id);
CREATE INDEX IF NOT EXISTS idx_parts_pn ON parts_actions(part_number);

-- =====================================================
-- AD COMPLIANCE
-- =====================================================

CREATE TABLE IF NOT EXISTS ad_compliance (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entry_id UUID REFERENCES maintenance_entries(id) ON DELETE CASCADE,
    aircraft_id UUID NOT NULL REFERENCES aircraft(id),
    ad_number VARCHAR(50) NOT NULL,
    compliance_date DATE,
    compliance_method VARCHAR(20)
        CHECK (compliance_method IN ('inspection', 'replacement', 'modification', 'terminating_action', 'recurring', 'not_applicable', 'other')),
    next_due_date DATE,
    next_due_hours DECIMAL(10,1),
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ad_aircraft ON ad_compliance(aircraft_id);

-- =====================================================
-- LIFE-LIMITED PARTS
-- =====================================================

CREATE TABLE IF NOT EXISTS life_limited_parts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aircraft_id UUID NOT NULL REFERENCES aircraft(id),
    part_name VARCHAR(200) NOT NULL,
    part_number VARCHAR(100),
    serial_number VARCHAR(100),
    install_date DATE,
    install_hours DECIMAL(10,1),
    life_limit_hours DECIMAL(10,1),
    life_limit_months INTEGER,
    expiration_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    removal_date DATE,
    removal_entry_id UUID REFERENCES maintenance_entries(id),
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_llp_aircraft ON life_limited_parts(aircraft_id);
CREATE INDEX IF NOT EXISTS idx_llp_expiration ON life_limited_parts(expiration_date) WHERE is_active = TRUE;

-- =====================================================
-- INSPECTION RECORDS
-- =====================================================

CREATE TABLE IF NOT EXISTS inspection_records (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aircraft_id UUID NOT NULL REFERENCES aircraft(id),
    entry_id UUID REFERENCES maintenance_entries(id),
    inspection_type VARCHAR(30) NOT NULL
        CHECK (inspection_type IN ('annual', '100hr', '50hr', 'progressive', 'altimeter_static', 'transponder', 'elt', 'other')),
    inspection_date DATE NOT NULL,
    aircraft_hours DECIMAL(10,1),
    next_due_date DATE,
    next_due_hours DECIMAL(10,1),
    far_reference VARCHAR(100),
    inspector_name VARCHAR(200),
    inspector_certificate VARCHAR(100),
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_inspection_aircraft ON inspection_records(aircraft_id);

-- =====================================================
-- EMBEDDINGS (3072 half-precision dims for gemini-embedding-001)
-- =====================================================

CREATE TABLE IF NOT EXISTS maintenance_embeddings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entry_id UUID NOT NULL REFERENCES maintenance_entries(id) ON DELETE CASCADE,
    embedding halfvec(3072),
    chunk_text TEXT NOT NULL,
    chunk_type VARCHAR(30) DEFAULT 'narrative'
        CHECK (chunk_type IN ('narrative', 'parts', 'ad_compliance', 'full_entry')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(entry_id, chunk_type)
);

CREATE INDEX IF NOT EXISTS idx_embeddings_vector ON maintenance_embeddings
    USING hnsw (embedding halfvec_cosine_ops);

-- =====================================================
-- TRIGGERS
-- =====================================================

CREATE OR REPLACE FUNCTION logbook.update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS aircraft_updated_at ON aircraft;
CREATE TRIGGER aircraft_updated_at BEFORE UPDATE ON aircraft
    FOR EACH ROW EXECUTE FUNCTION logbook.update_updated_at();

DROP TRIGGER IF EXISTS maintenance_entries_updated_at ON maintenance_entries;
CREATE TRIGGER maintenance_entries_updated_at BEFORE UPDATE ON maintenance_entries
    FOR EACH ROW EXECUTE FUNCTION logbook.update_updated_at();

DROP TRIGGER IF EXISTS life_limited_parts_updated_at ON life_limited_parts;
CREATE TRIGGER life_limited_parts_updated_at BEFORE UPDATE ON life_limited_parts
    FOR EACH ROW EXECUTE FUNCTION logbook.update_updated_at();
