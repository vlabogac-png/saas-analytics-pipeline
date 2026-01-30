-- ================================================
-- RAW LAYER: Immutable event storage
-- ================================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS raw;

-- Raw events table (JSONB storage)
CREATE TABLE IF NOT EXISTS raw.events (
    event_id VARCHAR(255) PRIMARY KEY,
    raw_payload JSONB NOT NULL,
    batch_id VARCHAR(255) NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_raw_events_batch_id ON raw.events(batch_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_loaded_at ON raw.events(loaded_at);
CREATE INDEX IF NOT EXISTS idx_raw_events_payload_type ON raw.events((raw_payload->>'event_type'));

COMMENT ON SCHEMA raw IS 'Raw layer: immutable JSONB storage of all events';
COMMENT ON TABLE raw.events IS 'Immutable raw events with full JSON payload';
