-- ================================================
-- CORE LAYER: Star Schema (Dimensions + Facts)
-- ================================================
--
-- Purpose:
--   This layer implements a classic star schema data warehouse model.
--   It organizes data into dimensions (descriptive attributes) and facts
--   (quantitative measurements) for efficient analytical querying.
--
-- Characteristics:
--   - Star Schema: Single fact table connected to multiple dimension tables
--   - SCD Type 2: Slowly Changing Dimensions for tracking history
--   - Surrogate Keys: Performance-optimized integer keys (SK)
--   - Type-Safe: All columns have specific data types
--
-- Dimension Tables:
--   - dim_users: User profiles with signup date, plan, status
--   - dim_documents: Document metadata
--   - dim_features: Product feature catalog
--   - dim_date: Calendar date with temporal attributes
--
-- Fact Tables:
--   - fact_events: Granular event-level data
--   - fact_daily_user_activity: Daily aggregations for performance
--
-- Benefits:
--   - Optimized query performance for analytics
--   - Easy to maintain and extend
--   - Industry-standard data warehouse design
-- ================================================

-- Create core schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS core;

-- ================================================
-- DIMENSION TABLES
-- ================================================

-- dim_users: User dimension with SCD Type 2 support
-- SCD Type 2 tracks all historical changes to user attributes
CREATE TABLE IF NOT EXISTS core.dim_users (
    -- Surrogate key for table joining
    user_sk SERIAL PRIMARY KEY,

    -- Natural key (user identifier)
    user_id VARCHAR(255) NOT NULL,

    -- User profile data
    email VARCHAR(255),
    signup_date DATE,
    current_plan VARCHAR(50),
    account_status VARCHAR(50),

    -- SCD Type 2 columns: Track history of changes
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    effective_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date TIMESTAMP,

    -- Audit fields
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_users_user_id ON core.dim_users(user_id);
CREATE INDEX IF NOT EXISTS idx_dim_users_is_current ON core.dim_users(is_current) WHERE is_current = TRUE;
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_users_current ON core.dim_users(user_id) WHERE is_current = TRUE;

-- dim_documents: Document dimension (static, reference data)
CREATE TABLE IF NOT EXISTS core.dim_documents (
    document_sk SERIAL PRIMARY KEY,
    document_id VARCHAR(255) UNIQUE NOT NULL,
    title VARCHAR(500),
    owner_user_id VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_documents_owner ON core.dim_documents(owner_user_id);

-- dim_features: Product features dimension (static reference data)
CREATE TABLE IF NOT EXISTS core.dim_features (
    feature_sk SERIAL PRIMARY KEY,
    feature_id VARCHAR(100) UNIQUE NOT NULL,
    feature_name VARCHAR(255),
    feature_category VARCHAR(100),
    is_premium BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Pre-populate with 8 premium and free product features
INSERT INTO core.dim_features (feature_id, feature_name, feature_category, is_premium)
VALUES
    ('real_time_collab', 'Real-time Collaboration', 'collaboration', TRUE),
    ('comments', 'Comments', 'collaboration', FALSE),
    ('version_history', 'Version History', 'editing', TRUE),
    ('export_pdf', 'Export to PDF', 'editing', FALSE),
    ('templates', 'Templates', 'editing', FALSE),
    ('cloud_storage', 'Cloud Storage', 'storage', FALSE),
    ('advanced_search', 'Advanced Search', 'analytics', TRUE),
    ('team_analytics', 'Team Analytics', 'analytics', TRUE)
ON CONFLICT (feature_id) DO NOTHING;

-- dim_date: Date dimension (static, run once)
-- Provides calendar attributes for time-based analysis
CREATE TABLE IF NOT EXISTS core.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    week_of_year INTEGER,
    month INTEGER,
    month_name VARCHAR(10),
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_dim_date_full_date ON core.dim_date(full_date);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON core.dim_date(year, month);

-- ================================================
-- FACT TABLES
-- ================================================

-- fact_events: Granular event-level fact table
-- Contains individual user events with foreign key references to dimensions
CREATE TABLE IF NOT EXISTS core.fact_events (
    event_sk SERIAL PRIMARY KEY,
    event_id VARCHAR(255) UNIQUE NOT NULL,

    -- Foreign keys to dimension tables
    user_sk INTEGER NOT NULL REFERENCES core.dim_users(user_sk),
    document_sk INTEGER REFERENCES core.dim_documents(document_sk),
    feature_sk INTEGER REFERENCES core.dim_features(feature_sk),
    date_key INTEGER NOT NULL REFERENCES core.dim_date(date_key),

    -- Event details
    event_type VARCHAR(100) NOT NULL,
    session_id VARCHAR(255),
    platform VARCHAR(50),
    event_timestamp TIMESTAMP NOT NULL,

    -- Event metrics
    duration_seconds INTEGER,
    characters_added INTEGER,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_events_user_sk ON core.fact_events(user_sk);
CREATE INDEX IF NOT EXISTS idx_fact_events_date_key ON core.fact_events(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_events_event_type ON core.fact_events(event_type);
CREATE INDEX IF NOT EXISTS idx_fact_events_timestamp ON core.fact_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_fact_events_document_sk ON core.fact_events(document_sk) WHERE document_sk IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fact_events_feature_sk ON core.fact_events(feature_sk) WHERE feature_sk IS NOT NULL;

-- fact_daily_user_activity: Daily aggregated fact table
-- Pre-aggregates metrics for better performance on historical analysis
CREATE TABLE IF NOT EXISTS core.fact_daily_user_activity (
    activity_date DATE NOT NULL,
    user_sk INTEGER NOT NULL REFERENCES core.dim_users(user_sk),

    -- Daily metrics
    total_events INTEGER NOT NULL DEFAULT 0,
    login_count INTEGER NOT NULL DEFAULT 0,
    documents_edited INTEGER NOT NULL DEFAULT 0,
    documents_created INTEGER NOT NULL DEFAULT 0,
    total_active_seconds INTEGER NOT NULL DEFAULT 0,
    distinct_features_used INTEGER NOT NULL DEFAULT 0,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Composite primary key ensures uniqueness
    PRIMARY KEY (activity_date, user_sk)
);

CREATE INDEX IF NOT EXISTS idx_fact_daily_activity_user_sk ON core.fact_daily_user_activity(user_sk);
CREATE INDEX IF NOT EXISTS idx_fact_daily_activity_date ON core.fact_daily_user_activity(activity_date);

-- ================================================
-- TABLE COMMENTS
-- ================================================

COMMENT ON SCHEMA core IS 'Core layer: star schema with dimensions and facts';
COMMENT ON TABLE core.dim_users IS 'User dimension with SCD Type 2 support for historical tracking';
COMMENT ON TABLE core.dim_documents IS 'Document dimension (reference data)';
COMMENT ON TABLE core.dim_features IS 'Product features dimension (reference data)';
COMMENT ON TABLE core.dim_date IS 'Date dimension (2020-2026)';
COMMENT ON TABLE core.fact_events IS 'Granular event-level fact table';
COMMENT ON TABLE core.fact_daily_user_activity IS 'Daily aggregated user activity metrics for performance';
