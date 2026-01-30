-- ================================================
-- Transformation: Raw Layer → Staging Layer
-- ================================================
--
-- Purpose: Transform raw JSONB events into typed, validated staging tables
--
-- Logic:
--   1. Extract event_id from raw_payload
--   2. Parse JSONB fields into typed columns
--   3. Handle null values with COALESCE (use fallback values)
--   4. Extract nested properties (document_id, feature_id, etc.)
--   5. Filter out duplicates using event_id existence check
--   6. Idempotent: Can run multiple times without errors
--
-- Why Idempotent?
--   Allows re-running transformations when new raw data is loaded
--   without duplicate entries in staging table
--
-- Performance:
--   - Uses JSONB extraction operators for fast parsing
--   - COALESCE handles missing fields gracefully
--   - Subquery check ensures no duplicates
-- ================================================

-- Transform raw JSONB events into staging events with typed columns
-- This extracts event details and properties from the JSONB payload
INSERT INTO staging.events (
    event_id,
    event_type,
    event_timestamp,
    user_id,
    session_id,
    document_id,
    feature_id,
    duration_seconds,
    characters_added,
    platform,
    user_agent,
    ip_address,
    properties,
    batch_id
)
SELECT
    raw_payload->>'event_id',
    raw_payload->>'event_type',
    (raw_payload->>'event_timestamp')::TIMESTAMP,
    raw_payload->>'user_id',
    raw_payload->>'session_id',
    raw_payload->'properties'->>'document_id',
    raw_payload->'properties'->>'feature_id',
    COALESCE(
        (raw_payload->'properties'->>'edit_duration_sec')::INTEGER,
        (raw_payload->'properties'->>'duration_sec')::INTEGER
    ),
    (raw_payload->'properties'->>'characters_added')::INTEGER,
    raw_payload->'context'->>'platform',
    raw_payload->'context'->>'user_agent',
    (raw_payload->'context'->>'ip_address')::INET,
    raw_payload->'properties',
    batch_id
FROM raw.events r
WHERE NOT EXISTS (
    SELECT 1 FROM staging.events s
    WHERE s.event_id = r.raw_payload->>'event_id'
);
