-- =====================================================
-- Transformation: staging → core (Dimensions + Facts)
-- Run in order: dims first, then facts
-- =====================================================

-- 1. dim_date (static, run once)
INSERT INTO core.dim_date (date_key, full_date, day_of_week, day_name, week_of_year, month, month_name, quarter, year, is_weekend)
SELECT 
    TO_CHAR(d, 'YYYYMMDD')::INTEGER,
    d,
    EXTRACT(DOW FROM d)::INTEGER,
    TO_CHAR(d, 'Day'),
    EXTRACT(WEEK FROM d)::INTEGER,
    EXTRACT(MONTH FROM d)::INTEGER,
    TO_CHAR(d, 'Month'),
    EXTRACT(QUARTER FROM d)::INTEGER,
    EXTRACT(YEAR FROM d)::INTEGER,
    EXTRACT(DOW FROM d) IN (0, 6)
FROM generate_series('2020-01-01'::DATE, '2026-12-31'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT (date_key) DO NOTHING;

-- 2. dim_users (from events)
INSERT INTO core.dim_users (user_id, email, signup_date, current_plan, account_status)
SELECT DISTINCT
    user_id,
    user_id || '@example.com',
    MIN(event_timestamp)::DATE,
    'free',
    'active'
FROM staging.events
GROUP BY user_id
ON CONFLICT DO NOTHING;

-- 3. dim_documents
INSERT INTO core.dim_documents (document_id, title, owner_user_id, created_at)
SELECT DISTINCT
    document_id,
    'Document ' || document_id,
    MIN(user_id),
    MIN(event_timestamp)
FROM staging.events
WHERE document_id IS NOT NULL
GROUP BY document_id
ON CONFLICT (document_id) DO NOTHING;

-- 4. fact_events
INSERT INTO core.fact_events (
    event_id, user_sk, document_sk, feature_sk, date_key,
    event_type, session_id, platform, event_timestamp,
    duration_seconds, characters_added
)
SELECT 
    s.event_id, u.user_sk, d.document_sk, f.feature_sk,
    TO_CHAR(s.event_timestamp, 'YYYYMMDD')::INTEGER,
    s.event_type, s.session_id, s.platform, s.event_timestamp,
    s.duration_seconds, s.characters_added
FROM staging.events s
JOIN core.dim_users u ON s.user_id = u.user_id AND u.is_current = TRUE
LEFT JOIN core.dim_documents d ON s.document_id = d.document_id
LEFT JOIN core.dim_features f ON s.feature_id = f.feature_id
JOIN core.dim_date dt ON TO_CHAR(s.event_timestamp, 'YYYYMMDD')::INTEGER = dt.date_key
WHERE NOT EXISTS (SELECT 1 FROM core.fact_events fe WHERE fe.event_id = s.event_id);

-- 5. fact_daily_user_activity (aggregation)
INSERT INTO core.fact_daily_user_activity (
    activity_date, user_sk, total_events, login_count,
    documents_edited, documents_created, total_active_seconds, distinct_features_used
)
SELECT 
    event_timestamp::DATE, user_sk, COUNT(*),
    COUNT(*) FILTER (WHERE event_type = 'user_login'),
    COUNT(*) FILTER (WHERE event_type = 'document_edited'),
    COUNT(*) FILTER (WHERE event_type = 'document_created'),
    COALESCE(SUM(duration_seconds), 0),
    COUNT(DISTINCT feature_sk) FILTER (WHERE feature_sk IS NOT NULL)
FROM core.fact_events
GROUP BY event_timestamp::DATE, user_sk
ON CONFLICT (activity_date, user_sk) DO UPDATE SET
    total_events = EXCLUDED.total_events,
    login_count = EXCLUDED.login_count,
    documents_edited = EXCLUDED.documents_edited,
    documents_created = EXCLUDED.documents_created,
    total_active_seconds = EXCLUDED.total_active_seconds,
    distinct_features_used = EXCLUDED.distinct_features_used,
    updated_at = CURRENT_TIMESTAMP;
