-- =====================================================
-- RAW LAYER: Immutable event storage
-- Purpose: Audit trail, reprocessing capability
-- =====================================================

CREATE SCHEMA IF NOT EXISTS raw;

-- Raw events table (exactly as received)
CREATE TABLE IF NOT EXISTS raw.events (
    event_id VARCHAR(100) PRIMARY KEY,
    raw_payload JSONB NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(100) NOT NULL,
    source_system VARCHAR(50) DEFAULT 'clouddocs_api'
);

-- Index for fast queries on event type
CREATE INDEX IF NOT EXISTS idx_raw_events_type 
ON raw.events USING GIN ((raw_payload -> 'event_type'));

-- Index for time-based queries
CREATE INDEX IF NOT EXISTS idx_raw_events_ingested 
ON raw.events(ingested_at);

-- Index for batch processing
CREATE INDEX IF NOT EXISTS idx_raw_events_batch 
ON raw.events(batch_id);

COMMENT ON TABLE raw.events IS 'Immutable raw events as received from source systems';
COMMENT ON COLUMN raw.events.raw_payload IS 'Complete JSON payload - never modify';
COMMENT ON COLUMN raw.events.batch_id IS 'Processing batch identifier for idempotency';