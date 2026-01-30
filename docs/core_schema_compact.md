# Core Schema - Compact Overview

## Dimension Tables

### dim_users (500 rows)
| Column | Type | Key | Description |
|--------|------|-----|-------------|
| user_sk | SERIAL | PK | Surrogate Key |
| user_id | VARCHAR(255) | NK | Business ID |
| email | VARCHAR(255) | | Email address |
| signup_date | DATE | | Registration date |
| current_plan | VARCHAR(50) | | free/pro/enterprise |
| account_status | VARCHAR(50) | | Account status |
| is_current | BOOLEAN | | Current record flag (SCD Type 2) |
| effective_date | TIMESTAMP | | Valid from |
| end_date | TIMESTAMP | | Valid until |
| created_at | TIMESTAMP | | Record created |
| updated_at | TIMESTAMP | | Record updated |

### dim_documents (2,000 rows)
| Column | Type | Key | Description |
|--------|------|-----|-------------|
| document_sk | SERIAL | PK | Surrogate Key |
| document_id | VARCHAR(255) | UK | Business ID |
| title | VARCHAR(500) | | Document title |
| owner_user_id | VARCHAR(255) | | Owner reference |
| created_at | TIMESTAMP | | Creation timestamp |
| updated_at | TIMESTAMP | | Last update |

### dim_features (8 rows)
| Column | Type | Key | Description |
|--------|------|-----|-------------|
| feature_sk | SERIAL | PK | Surrogate Key |
| feature_id | VARCHAR(100) | UK | Business ID |
| feature_name | VARCHAR(255) | | Display name |
| feature_category | VARCHAR(100) | | Category |
| is_premium | BOOLEAN | | Premium feature flag |
| created_at | TIMESTAMP | | Record created |

**Pre-loaded features:**
- real_time_collab, comments, version_history, export_pdf
- templates, cloud_storage, advanced_search, team_analytics

### dim_date (2,557 rows - 2020-2026)
| Column | Type | Key | Description |
|--------|------|-----|-------------|
| date_key | INTEGER | PK | YYYYMMDD format |
| full_date | DATE | | Actual date |
| day_of_week | INTEGER | | 0-6 (Sun-Sat) |
| day_name | VARCHAR(10) | | Monday, Tuesday, ... |
| week_of_year | INTEGER | | Week number |
| month | INTEGER | | 1-12 |
| month_name | VARCHAR(10) | | January, ... |
| quarter | INTEGER | | 1-4 |
| year | INTEGER | | Year |
| is_weekend | BOOLEAN | | Weekend flag |

---

## Fact Tables

### fact_events (4.8M rows)
| Column | Type | Key | Description |
|--------|------|-----|-------------|
| event_sk | SERIAL | PK | Surrogate Key |
| event_id | VARCHAR(255) | UK | Business ID |
| **user_sk** | INTEGER | FK | → dim_users.user_sk |
| **document_sk** | INTEGER | FK | → dim_documents.document_sk |
| **feature_sk** | INTEGER | FK | → dim_features.feature_sk |
| **date_key** | INTEGER | FK | → dim_date.date_key |
| event_type | VARCHAR(100) | | Event type |
| session_id | VARCHAR(255) | | Session identifier |
| platform | VARCHAR(50) | | web/mobile/desktop/api |
| event_timestamp | TIMESTAMP | | Event occurrence time |
| duration_seconds | INTEGER | | Duration (nullable) |
| characters_added | INTEGER | | Characters added (nullable) |
| created_at | TIMESTAMP | | Record created |

**Event Types:**
- document_edited, document_created, document_deleted, document_shared
- user_login, user_logout, feature_used
- subscription_started, subscription_upgraded, subscription_cancelled

### fact_daily_user_activity (278K rows)
| Column | Type | Key | Description |
|--------|------|-----|-------------|
| activity_date | DATE | PK | Activity date |
| **user_sk** | INTEGER | PK,FK | → dim_users.user_sk |
| total_events | INTEGER | | Total events count |
| login_count | INTEGER | | Login events |
| documents_edited | INTEGER | | Edited docs count |
| documents_created | INTEGER | | Created docs count |
| total_active_seconds | INTEGER | | Total active time |
| distinct_features_used | INTEGER | | Unique features count |
| created_at | TIMESTAMP | | Record created |
| updated_at | TIMESTAMP | | Last update |

---

## Relationships

```
dim_users (1) ───────────────┬──► fact_events (M)
                             │
                             └──► fact_daily_user_activity (M)

dim_documents (1) ───────────────► fact_events (M)

dim_features (1) ────────────────► fact_events (M)

dim_date (1) ────────────────────► fact_events (M)
```

---

## Key Indexes

### fact_events
- `idx_fact_events_user_sk` on user_sk
- `idx_fact_events_date_key` on date_key
- `idx_fact_events_event_type` on event_type
- `idx_fact_events_timestamp` on event_timestamp
- `idx_fact_events_document_sk` on document_sk (where not null)
- `idx_fact_events_feature_sk` on feature_sk (where not null)

### fact_daily_user_activity
- `idx_fact_daily_activity_user_sk` on user_sk
- `idx_fact_daily_activity_date` on activity_date

---

## Storage

| Table | Rows | Size |
|-------|------|------|
| fact_events | 4,804,048 | 1.4 GB |
| fact_daily_user_activity | 278,474 | 61 MB |
| dim_users | 500 | 232 KB |
| dim_documents | 2,000 | 456 KB |
| dim_features | 8 | < 10 KB |
| dim_date | 2,557 | < 100 KB |
