"""
SaaS Analytics Pipeline DAG
Orchestrates the full ETL: raw → staging → core → analytics
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'saas_analytics_pipeline',
    default_args=default_args,
    description='Daily SaaS analytics ETL pipeline',
    schedule_interval='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['saas', 'analytics', 'etl'],
)

# Task 1: Raw to Staging
raw_to_staging = PostgresOperator(
    task_id='raw_to_staging',
    postgres_conn_id='postgres_saas',
    sql="""
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
    """,
    dag=dag,
)

# Task 2: Load Dimensions
load_dim_users = PostgresOperator(
    task_id='load_dim_users',
    postgres_conn_id='postgres_saas',
    sql="""
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
    """,
    dag=dag,
)

load_dim_documents = PostgresOperator(
    task_id='load_dim_documents',
    postgres_conn_id='postgres_saas',
    sql="""
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
    """,
    dag=dag,
)

# Task 3: Load Fact Events
load_fact_events = PostgresOperator(
    task_id='load_fact_events',
    postgres_conn_id='postgres_saas',
    sql="""
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
    """,
    dag=dag,
)

# Task 4: Load Daily Aggregation
load_daily_activity = PostgresOperator(
    task_id='load_daily_activity',
    postgres_conn_id='postgres_saas',
    sql="""
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
    """,
    dag=dag,
)

# Task 5: Refresh Analytics Views
refresh_retention = PostgresOperator(
    task_id='refresh_retention_cohorts',
    postgres_conn_id='postgres_saas',
    sql="REFRESH MATERIALIZED VIEW analytics.user_retention_cohorts;",
    dag=dag,
)

refresh_funnel = PostgresOperator(
    task_id='refresh_feature_funnel',
    postgres_conn_id='postgres_saas',
    sql="REFRESH MATERIALIZED VIEW analytics.feature_adoption_funnel;",
    dag=dag,
)

refresh_churn = PostgresOperator(
    task_id='refresh_churn_scores',
    postgres_conn_id='postgres_saas',
    sql="REFRESH MATERIALIZED VIEW analytics.churn_risk_scores;",
    dag=dag,
)

# Dependencies
raw_to_staging >> [load_dim_users, load_dim_documents]
[load_dim_users, load_dim_documents] >> load_fact_events >> load_daily_activity
load_daily_activity >> [refresh_retention, refresh_funnel, refresh_churn]
