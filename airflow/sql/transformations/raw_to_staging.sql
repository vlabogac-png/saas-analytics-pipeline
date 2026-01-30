-- Transformation: raw.events → staging.events (Idempotent)
INSERT INTO staging.events (
    event_id, event_type, event_timestamp, user_id, session_id,
    document_id, feature_id, duration_seconds, characters_added,
    platform, user_agent, ip_address, properties, batch_id
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
