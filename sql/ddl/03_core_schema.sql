-- =====================================================
-- CORE LAYER: Star Schema (3NF)
-- Purpose: Single source of truth for analytics
-- =====================================================

CREATE SCHEMA IF NOT EXISTS core;

-- =====================================================
-- DIMENSIONS
-- =====================================================

-- Date Dimension (pre-populated for 5 years)
CREATE TABLE IF NOT EXISTS core.dim_date (
    date_key INTEGER PRIMARY KEY,  -- Format: YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    week_of_year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE
);

-- User Dimension (SCD Type 2)
CREATE TABLE IF NOT EXISTS core.dim_users (
    user_sk SERIAL PRIMARY KEY,  -- Surrogate key
    user_id VARCHAR(100) NOT NULL,  -- Natural key
    email VARCHAR(255),
    signup_date DATE NOT NULL,
    current_plan VARCHAR(20) NOT NULL,  -- free, pro, enterprise
    account_status VARCHAR(20) NOT NULL,  -- active, churned, suspended
    
    -- SCD Type 2 fields
    valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    
    CONSTRAINT chk_user_plan CHECK (current_plan IN ('free', 'pro', 'enterprise')),
    CONSTRAINT chk_user_status CHECK (account_status IN ('active', 'churned', 'suspended'))
);

CREATE INDEX IF NOT EXISTS idx_dim_users_natural 
ON core.dim_users(user_id, is_current);

CREATE INDEX IF NOT EXISTS idx_dim_users_valid 
ON core.dim_users(valid_from, valid_to);

-- Document Dimension
CREATE TABLE IF NOT EXISTS core.dim_documents (
    document_sk SERIAL PRIMARY KEY,
    document_id VARCHAR(100) NOT NULL UNIQUE,
    title VARCHAR(500),
    owner_user_id VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    deleted_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_documents_owner 
ON core.dim_documents(owner_user_id);

-- Feature Dimension
CREATE TABLE IF NOT EXISTS core.dim_features (
    feature_sk SERIAL PRIMARY KEY,
    feature_id VARCHAR(100) NOT NULL UNIQUE,
    feature_name VARCHAR(100) NOT NULL,
    feature_category VARCHAR(50) NOT NULL,  -- collaboration, editing, storage, admin
    is_premium_feature BOOLEAN NOT NULL DEFAULT FALSE,
    release_date DATE,
    
    CONSTRAINT chk_feature_category CHECK (feature_category IN (
        'collaboration', 'editing', 'storage', 'admin', 'analytics'
    ))
);

-- =====================================================
-- FACTS
-- =====================================================

-- Fact: Individual Events (transactional grain)
CREATE TABLE IF NOT EXISTS core.fact_events (
    event_sk BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(100) NOT NULL UNIQUE,
    
    -- Foreign keys
    user_sk INTEGER NOT NULL REFERENCES core.dim_users(user_sk),
    document_sk INTEGER REFERENCES core.dim_documents(document_sk),
    feature_sk INTEGER REFERENCES core.dim_features(feature_sk),
    date_key INTEGER NOT NULL REFERENCES core.dim_date(date_key),
    
    -- Degenerate dimensions
    event_type VARCHAR(50) NOT NULL,
    session_id VARCHAR(100),
    platform VARCHAR(20),
    
    -- Metrics
    event_timestamp TIMESTAMP NOT NULL,
    duration_seconds INTEGER,
    characters_added INTEGER,
    
    -- Metadata
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_events_user 
ON core.fact_events(user_sk);

CREATE INDEX IF NOT EXISTS idx_fact_events_date 
ON core.fact_events(date_key);

CREATE INDEX IF NOT EXISTS idx_fact_events_document 
ON core.fact_events(document_sk) WHERE document_sk IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_fact_events_timestamp 
ON core.fact_events(event_timestamp);

-- Fact: Daily User Activity (aggregated grain)
CREATE TABLE IF NOT EXISTS core.fact_daily_user_activity (
    activity_date DATE NOT NULL,
    user_sk INTEGER NOT NULL REFERENCES core.dim_users(user_sk),
    
    -- Metrics
    total_events INTEGER NOT NULL DEFAULT 0,
    login_count INTEGER NOT NULL DEFAULT 0,
    documents_edited INTEGER NOT NULL DEFAULT 0,
    documents_created INTEGER NOT NULL DEFAULT 0,
    total_active_seconds INTEGER NOT NULL DEFAULT 0,
    distinct_features_used INTEGER NOT NULL DEFAULT 0,
    
    -- Metadata
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (activity_date, user_sk)
);

CREATE INDEX IF NOT EXISTS idx_fact_daily_activity_user 
ON core.fact_daily_user_activity(user_sk);

CREATE INDEX IF NOT EXISTS idx_fact_daily_activity_date 
ON core.fact_daily_user_activity(activity_date);

COMMENT ON TABLE core.fact_events IS 'Transactional fact: one row per event';
COMMENT ON TABLE core.fact_daily_user_activity IS 'Aggregated fact: one row per user per day';