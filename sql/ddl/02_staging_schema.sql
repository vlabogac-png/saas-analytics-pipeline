-- =====================================================
-- STAGING LAYER: Typed and cleaned data
-- Purpose: Transform JSON to relational, deduplication
-- =====================================================

CREATE SCHEMA IF NOT EXISTS staging;

-- Staging events table (flattened from raw)
CREATE TABLE IF NOT EXISTS staging.events (
    event_id VARCHAR(100) PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    session_id VARCHAR(100),
    document_id VARCHAR(100),
    feature_id VARCHAR(100),
    
    -- Event-specific metrics
    duration_seconds INTEGER,
    characters_added INTEGER,
    
    -- Context
    platform VARCHAR(20),
    user_agent TEXT,
    ip_address INET,
    
    -- Metadata
    properties JSONB,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(100) NOT NULL,
    
    CONSTRAINT chk_event_type CHECK (event_type IN (
        'user_login',
        'user_logout',
        'document_created',
        'document_edited',
        'document_deleted',
        'document_shared',
        'subscription_started',
        'subscription_upgraded',
        'subscription_cancelled',
        'feature_used'
    )),
    CONSTRAINT chk_platform CHECK (platform IN ('web', 'mobile', 'desktop', 'api'))
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_staging_events_type 
ON staging.events(event_type);

CREATE INDEX IF NOT EXISTS idx_staging_events_user 
ON staging.events(user_id);

CREATE INDEX IF NOT EXISTS idx_staging_events_timestamp 
ON staging.events(event_timestamp);

CREATE INDEX IF NOT EXISTS idx_staging_events_document 
ON staging.events(document_id) WHERE document_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_staging_events_batch 
ON staging.events(batch_id);

COMMENT ON TABLE staging.events IS 'Typed and validated events from raw layer';
COMMENT ON COLUMN staging.events.properties IS 'Additional flexible fields not yet modeled';