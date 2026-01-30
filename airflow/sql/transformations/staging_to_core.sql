-- ================================================
-- Transformation: Staging Layer → Core Layer
-- ================================================
--
-- Purpose: Transform staging events into star schema dimensions and facts
--
-- Process Order (CRITICAL: Run in this exact order):
--   1. dim_date: Static date dimension (run once)
--   2. dim_users: Extract user profiles from events
--   3. dim_documents: Extract document metadata
--   4. fact_events: Create grain-level facts with foreign keys
--   5. fact_daily_user_activity: Pre-aggregate daily metrics
--
-- Key Concepts:
--   - Surrogate Keys (SK): Internal integer IDs for table joins
--   - Foreign Keys: Reference to dimension tables
--   - Idempotent: ON CONFLICT clauses prevent duplicate inserts
--   - SCD Type 2: Track historical changes for users
-- ================================================

-- =====================================================
-- 1. dim_date: Static Date Dimension
-- =====================================================
--
-- This is a static reference table containing all calendar dates.
-- It's pre-populated once and never changes.
--
-- Run this ONCE (after first initialization, not in daily ETL)
-- =====================================================

INSERT INTO core.dim_date (
    date_key,
    full_date,
    day_of_week,
    day_name,
    week_of_year,
    month,
    month_name,
    quarter,
    year,
    is_weekend
)
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
    EXTRACT(DOW FROM d) IN (0, 6)  -- 0=Sunday, 6=Saturday
FROM generate_series('2020-01-01'::DATE, '2026-12-31'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT (date_key) DO NOTHING;

-- =====================================================
-- 2. dim_users: User Dimension
-- =====================================================
--
-- Extract unique users from staging events
-- Assign each user a signup date based on first event
-- Initialize default values (free plan, active status)
--
-- SCD Type 2: Adds current flag, effective date, end date
-- These columns will be updated in subsequent runs
-- =====================================================

INSERT INTO core.dim_users (
    user_id,
    email,
    signup_date,
    current_plan,
    account_status
)
SELECT DISTINCT
    user_id,
    user_id || '@example.com',  -- Generate email from user_id
    MIN(event_timestamp)::DATE,  -- First event = signup date
    'free',  -- Default plan
    'active'  -- Default status
FROM staging.events
GROUP BY user_id
ON CONFLICT DO NOTHING;

-- =====================================================
-- 3. dim_documents: Document Dimension
-- =====================================================
--
-- Extract unique documents from staging events
-- Generate default title
--
-- Initialize with event-based metadata (owner, creation time)
-- =====================================================

INSERT INTO core.dim_documents (
    document_id,
    title,
    owner_user_id,
    created_at
)
SELECT DISTINCT
    document_id,
    'Document ' || document_id,  -- Generate title from ID
    MIN(user_id),
    MIN(event_timestamp)  -- First event with this doc = creation time
FROM staging.events
WHERE document_id IS NOT NULL
GROUP BY document_id
ON CONFLICT (document_id) DO NOTHING;

-- =====================================================
-- 4. fact_events: Granular Event Fact Table
-- =====================================================
--
-- Create fact events by joining staging events to dimension tables
--
-- Foreign Keys:
--   - user_sk: Links to dim_users
--   - document_sk: Links to dim_documents (nullable)
--   - feature_sk: Links to dim_features (nullable)
--   - date_key: Links to dim_date
--
-- Joins:
--   - dim_users: Filter for current users only
--   - dim_documents: Left join (documents may not exist)
--   - dim_features: Left join (features may not exist)
--   - dim_date: Required join for date parsing
--
-- Idempotent: Skips existing event_ids to avoid duplicates
-- =====================================================

INSERT INTO core.fact_events (
    event_id,
    user_sk,
    document_sk,
    feature_sk,
    date_key,
    event_type,
    session_id,
    platform,
    event_timestamp,
    duration_seconds,
    characters_added
)
SELECT
    s.event_id,
    u.user_sk,  -- Get user surrogate key
    d.document_sk,  -- Get document surrogate key
    f.feature_sk,  -- Get feature surrogate key
    TO_CHAR(s.event_timestamp, 'YYYYMMDD')::INTEGER,  -- Parse date to key
    s.event_type,
    s.session_id,
    s.platform,
    s.event_timestamp,
    s.duration_seconds,
    s.characters_added
FROM staging.events s
JOIN core.dim_users u ON s.user_id = u.user_id AND u.is_current = TRUE  -- Only current users
LEFT JOIN core.dim_documents d ON s.document_id = d.document_id
LEFT JOIN core.dim_features f ON s.feature_id = f.feature_id
JOIN core.dim_date dt ON TO_CHAR(s.event_timestamp, 'YYYYMMDD')::INTEGER = dt.date_key
WHERE NOT EXISTS (
    SELECT 1 FROM core.fact_events fe
    WHERE fe.event_id = s.event_id
);

-- =====================================================
-- 5. fact_daily_user_activity: Daily Aggregation
-- =====================================================
--
-- Pre-aggregate metrics for better query performance
--
-- Aggregates by date and user:
--   - total_events: Daily event count
--   - login_count: Filtered event types
--   - documents_edited: Document-related events
--   - documents_created: Creation events
--   - total_active_seconds: Sum of duration
--   - distinct_features_used: Unique features per day
--
-- Upsert Strategy:
--   Uses ON CONFLICT to update existing records
--   Updates all metrics and timestamps
-- =====================================================

INSERT INTO core.fact_daily_user_activity (
    activity_date,
    user_sk,
    total_events,
    login_count,
    documents_edited,
    documents_created,
    total_active_seconds,
    distinct_features_used
)
SELECT
    event_timestamp::DATE,
    user_sk,
    COUNT(*),
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
