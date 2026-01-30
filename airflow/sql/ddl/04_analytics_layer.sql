-- ================================================
-- ANALYTICS LAYER: Materialized Views for BI
-- ================================================
--
-- Purpose:
--   This layer provides pre-aggregated, business-ready metrics for
--   BI tools like Metabase. Materialized views cache complex queries
--   for fast repeated access.
--
-- Characteristics:
--   - Pre-aggregated: Reduces query complexity
--   - Cached: Improves query performance
--   - Refreshable: Can be updated via REFRESH MATERIALIZED VIEW
--
-- Views:
--   - user_retention_cohorts: Monthly user retention analysis
--   - feature_adoption_funnel: Feature usage metrics
--   - churn_risk_scores: User churn risk classification
--
-- Refresh Strategy:
--   These views are refreshed daily by the Airflow DAG to ensure
--   metrics stay current with new event data.
--
-- Benefits:
--   - Fast BI queries
--   - Consistent metrics
--   - Pre-computed aggregations
-- ================================================

-- Create analytics schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS analytics;

-- ================================================
-- MATERIALIZED VIEW: User Retention Cohorts
-- ================================================
--
-- Purpose: Analyze user retention over time by signup cohort
--
-- Logic:
--   1. Group users by signup month (cohort)
--   2. Track which users are active in each subsequent month
--   3. Calculate retention rate: retained users / total cohort size
--   4. Track months since signup
--
-- Example Result:
--   Cohort Jan 2024 | Active in Feb | Active in Mar | ... | Retention %
--   --------------------------------------------|----------------
--   Cohort of 1000  | 800            | 650           | ... | 65%
-- ================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.user_retention_cohorts AS
WITH user_cohorts AS (
    SELECT
        user_sk,
        DATE_TRUNC('month', signup_date)::DATE AS cohort_month
    FROM core.dim_users
    WHERE is_current = TRUE
),
user_activity AS (
    SELECT
        user_sk,
        DATE_TRUNC('month', activity_date)::DATE AS activity_month
    FROM core.fact_daily_user_activity
)
SELECT
    uc.cohort_month,
    ua.activity_month,
    EXTRACT(YEAR FROM AGE(ua.activity_month, uc.cohort_month)) * 12 +
    EXTRACT(MONTH FROM AGE(ua.activity_month, uc.cohort_month)) AS months_since_signup,
    COUNT(DISTINCT uc.user_sk) AS cohort_size,
    COUNT(DISTINCT ua.user_sk) AS retained_users,
    ROUND(100.0 * COUNT(DISTINCT ua.user_sk) / COUNT(DISTINCT uc.user_sk), 2) AS retention_rate
FROM user_cohorts uc
LEFT JOIN user_activity ua ON uc.user_sk = ua.user_sk
GROUP BY uc.cohort_month, ua.activity_month
ORDER BY uc.cohort_month, ua.activity_month;

CREATE UNIQUE INDEX IF NOT EXISTS idx_retention_cohorts_month
ON analytics.user_retention_cohorts(cohort_month, activity_month);

-- ================================================
-- MATERIALIZED VIEW: Feature Adoption Funnel
-- ================================================
--
-- Purpose: Track feature usage metrics and engagement
--
-- Metrics:
--   - unique_users: How many different users used each feature
--   - total_uses: Total number of feature access events
--   - avg_duration_seconds: Average time spent using the feature
--   - first_used_at: When the feature was first used (by any user)
--   - last_used_at: When the feature was last used
--
-- Use Case: Identify most-used features and engagement patterns
-- ================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.feature_adoption_funnel AS
SELECT
    f.feature_name,
    f.feature_category,
    f.is_premium AS is_premium_feature,
    COUNT(DISTINCT fe.user_sk) AS unique_users,
    COUNT(fe.event_sk) AS total_uses,
    ROUND(AVG(fe.duration_seconds), 2) AS avg_duration_seconds,
    MIN(fe.event_timestamp) AS first_used_at,
    MAX(fe.event_timestamp) AS last_used_at
FROM core.fact_events fe
JOIN core.dim_features f ON fe.feature_sk = f.feature_sk
WHERE fe.event_type = 'feature_used'
GROUP BY f.feature_name, f.feature_category, f.is_premium
ORDER BY unique_users DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_feature_adoption_name
ON analytics.feature_adoption_funnel(feature_name);

-- ================================================
-- MATERIALIZED VIEW: Churn Risk Scores
-- ================================================
--
-- Purpose: Classify users by their churn risk level
--
-- Risk Calculation:
--   - active: User logged in within 7 days
--   - low: User logged in within 14 days
--   - medium: User logged in within 30 days
--   - high: User logged in more than 30 days ago
--
-- Fields:
--   - user_sk, user_id, current_plan
--   - last_active_date: Most recent activity
--   - days_since_last_activity: Time since last login
--   - lifetime_events: Total events user has generated
--   - churn_risk_category: Risk classification
-- ================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.churn_risk_scores AS
WITH user_last_activity AS (
    SELECT
        user_sk,
        MAX(activity_date) AS last_active_date,
        SUM(total_events) AS lifetime_events
    FROM core.fact_daily_user_activity
    GROUP BY user_sk
)
SELECT
    u.user_sk,
    u.user_id,
    u.current_plan,
    ula.last_active_date,
    CURRENT_DATE - ula.last_active_date AS days_since_last_activity,
    ula.lifetime_events,
    CASE
        WHEN CURRENT_DATE - ula.last_active_date > 30 THEN 'high'
        WHEN CURRENT_DATE - ula.last_active_date > 14 THEN 'medium'
        WHEN CURRENT_DATE - ula.last_active_date > 7 THEN 'low'
        ELSE 'active'
    END AS churn_risk_category
FROM core.dim_users u
JOIN user_last_activity ula ON u.user_sk = ula.user_sk
WHERE u.is_current = TRUE
  AND u.account_status = 'active'
ORDER BY days_since_last_activity DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_churn_risk_user
ON analytics.churn_risk_scores(user_sk);

-- ================================================
-- VIEW COMMENTS
-- ================================================

COMMENT ON SCHEMA analytics IS 'Analytics layer: pre-aggregated materialized views for BI tools';
COMMENT ON MATERIALIZED VIEW analytics.user_retention_cohorts IS 'Monthly cohort retention analysis showing how users retain over time';
COMMENT ON MATERIALIZED VIEW analytics.feature_adoption_funnel IS 'Feature usage metrics, unique users, and engagement patterns';
COMMENT ON MATERIALIZED VIEW analytics.churn_risk_scores IS 'User churn risk classification based on activity frequency';
