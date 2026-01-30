-- ================================================
-- Refresh Analytics Materialized Views
-- ================================================
--
-- Purpose: Update cached analytics metrics with latest data
--
-- This script refreshes all materialized views to reflect:
--   - New user retention cohorts from recent signups
--   - Updated feature adoption metrics
--   - Current churn risk scores based on latest activity
--
-- Run Frequency: Daily (via Airflow DAG)
-- ================================================

-- Refresh all materialized views in analytics schema
REFRESH MATERIALIZED VIEW analytics.user_retention_cohorts;
REFRESH MATERIALIZED VIEW analytics.feature_adoption_funnel;
REFRESH MATERIALIZED VIEW analytics.churn_risk_scores;
