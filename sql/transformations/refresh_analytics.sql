-- Refresh all materialized views in analytics schema
REFRESH MATERIALIZED VIEW analytics.user_retention_cohorts;
REFRESH MATERIALIZED VIEW analytics.feature_adoption_funnel;
REFRESH MATERIALIZED VIEW analytics.churn_risk_scores;
