-- ================================================
-- RAW LAYER: Immutable Event Storage
-- ================================================
--
-- Purpose:
--   This layer serves as the single source of truth for all incoming events.
--   It preserves the original event data in JSONB format for audit trails
--   and future data exploration.
--
-- Characteristics:
--   - Immutable: No transformations applied
--   - Complete: All event data preserved
--   - Timestamped: Records when each event was loaded
--   - Batch-tracked: Associates events with loading batches
--
-- This layer is important for:
--   - Data lineage and audit trails
--   - Re-processing without losing original data
--   - Future schema evolution (new fields can be added)
-- ================================================

-- Create raw schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS raw;

-- Create raw events table for storing event data as JSONB
-- JSONB is chosen for efficient querying and JSON manipulation
CREATE TABLE IF NOT EXISTS raw.events (
    event_id VARCHAR(255) PRIMARY KEY,
    raw_payload JSONB NOT NULL,
    batch_id VARCHAR(255) NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Performance indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_raw_events_batch_id ON raw.events(batch_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_loaded_at ON raw.events(loaded_at);
CREATE INDEX IF NOT EXISTS idx_raw_events_payload_type ON raw.events((raw_payload->>'event_type'));

-- Table comments for documentation
COMMENT ON SCHEMA raw IS 'Raw layer: immutable JSONB storage of all events';
COMMENT ON TABLE raw.events IS 'Immutable raw events with full JSON payload for audit trails';
