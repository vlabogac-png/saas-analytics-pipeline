-- =====================================================
-- ANALYTICS LAYER: Business metrics & KPIs
-- Purpose: Optimized for BI tools and reporting
-- =====================================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- =====================================================
-- Materialized View: User Retention Cohorts
-- =====================================================
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

CREATE UNIQUE INDEX ON analytics.user_retention_cohorts(cohort_month, activity_month);

COMMENT ON MATERIALIZED VIEW analytics.user_retention_cohorts IS 
'Monthly cohort retention analysis - refresh daily';

-- =====================================================
-- Materialized View: Feature Adoption Funnel
-- =====================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.feature_adoption_funnel AS
SELECT 
    f.feature_name,
    f.feature_category,
    f.is_premium_feature,
    COUNT(DISTINCT fe.user_sk) AS unique_users,
    COUNT(fe.event_sk) AS total_uses,
    ROUND(AVG(fe.duration_seconds), 2) AS avg_duration_seconds,
    MIN(fe.event_timestamp) AS first_used_at,
    MAX(fe.event_timestamp) AS last_used_at
FROM core.fact_events fe
JOIN core.dim_features f ON fe.feature_sk = f.feature_sk
WHERE fe.event_type = 'feature_used'
GROUP BY f.feature_name, f.feature_category, f.is_premium_feature
ORDER BY unique_users DESC;

CREATE UNIQUE INDEX ON analytics.feature_adoption_funnel(feature_name);

COMMENT ON MATERIALIZED VIEW analytics.feature_adoption_funnel IS 
'Feature usage metrics for product analytics';

-- =====================================================
-- Materialized View: Churn Risk Scores
-- =====================================================
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

CREATE UNIQUE INDEX ON analytics.churn_risk_scores(user_sk);

COMMENT ON MATERIALIZED VIEW analytics.churn_risk_scores IS 
'User engagement scoring for retention campaigns';