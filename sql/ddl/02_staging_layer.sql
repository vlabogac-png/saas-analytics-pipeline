-- ================================================
-- STAGING LAYER: Parsed and validated events
-- ================================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS staging;

-- Staging events table (typed columns)
CREATE TABLE IF NOT EXISTS staging.events (
    event_id VARCHAR(255) PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    session_id VARCHAR(255),

    -- Event-specific properties
    document_id VARCHAR(255),
    feature_id VARCHAR(100),
    duration_seconds INTEGER,
    characters_added INTEGER,

    -- Context
    platform VARCHAR(50),
    user_agent TEXT,
    ip_address INET,

    -- Full properties for additional data
    properties JSONB,

    -- Metadata
    batch_id VARCHAR(255) NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_staging_events_type ON staging.events(event_type);
CREATE INDEX IF NOT EXISTS idx_staging_events_timestamp ON staging.events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_staging_events_user_id ON staging.events(user_id);
CREATE INDEX IF NOT EXISTS idx_staging_events_document_id ON staging.events(document_id) WHERE document_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_staging_events_feature_id ON staging.events(feature_id) WHERE feature_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_staging_events_batch_id ON staging.events(batch_id);

COMMENT ON SCHEMA staging IS 'Staging layer: parsed and validated events with typed columns';
COMMENT ON TABLE staging.events IS 'Parsed events with typed columns, ready for core transformation';
