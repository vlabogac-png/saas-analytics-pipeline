-- ================================================
-- STAGING LAYER: Parsed and Validated Events
-- ================================================
--
-- Purpose:
--   This layer transforms raw JSONB events into typed, validated columns.
--   It serves as the input for the core layer transformations.
--
-- Characteristics:
--   - Typed columns: Each field has a specific data type
--   - Validated: Data is checked for type correctness
--   - Deduplicated: Ensures no duplicate event_id
--   - Temporary: Intermediate layer (not for long-term storage)
--
-- Process:
--   1. Parse JSONB raw_payload into typed columns
--   2. Extract and validate event properties
--   3. Handle default values for missing fields
--   4. Filter out duplicates using event_id
--
-- This layer is important for:
--   - Data quality and consistency
--   - Performance optimization (typed columns query faster)
--   - ETL transformation checkpoint
-- ================================================

-- Create staging schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS staging;

-- Create staging events table with typed columns for optimized querying
CREATE TABLE IF NOT EXISTS staging.events (
    event_id VARCHAR(255) PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    session_id VARCHAR(255),

    -- Event-specific properties extracted from JSON
    document_id VARCHAR(255),
    feature_id VARCHAR(100),
    duration_seconds INTEGER,
    characters_added INTEGER,

    -- Context information
    platform VARCHAR(50),
    user_agent TEXT,
    ip_address INET,

    -- Original properties JSON for additional data
    properties JSONB,

    -- Processing metadata
    batch_id VARCHAR(255) NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Performance indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_staging_events_type ON staging.events(event_type);
CREATE INDEX IF NOT EXISTS idx_staging_events_timestamp ON staging.events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_staging_events_user_id ON staging.events(user_id);
CREATE INDEX IF NOT EXISTS idx_staging_events_document_id ON staging.events(document_id) WHERE document_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_staging_events_feature_id ON staging.events(feature_id) WHERE feature_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_staging_events_batch_id ON staging.events(batch_id);

-- Table comments for documentation
COMMENT ON SCHEMA staging IS 'Staging layer: parsed and validated events with typed columns';
COMMENT ON TABLE staging.events IS 'Parsed events with typed columns, ready for core transformation';
