# Core Layer - Star Schema Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    CORE LAYER SCHEMA                                         │
│                                  (Star Schema Design)                                        │
└─────────────────────────────────────────────────────────────────────────────────────────────┘


┌────────────────────────────────┐
│       dim_users (500)          │
├────────────────────────────────┤
│ PK  user_sk           SERIAL   │
│     user_id           VARCHAR  │◄────┐
│     email             VARCHAR  │     │
│     signup_date       DATE     │     │
│     current_plan      VARCHAR  │     │
│     account_status    VARCHAR  │     │
│     is_current        BOOLEAN  │     │
│     effective_date    TIMESTAMP│     │
│     end_date          TIMESTAMP│     │
│     created_at        TIMESTAMP│     │
│     updated_at        TIMESTAMP│     │
└────────────────────────────────┘     │
                                       │
                                       │
┌────────────────────────────────┐     │
│    dim_documents (2,000)       │     │
├────────────────────────────────┤     │
│ PK  document_sk       SERIAL   │◄──┐ │
│ UK  document_id       VARCHAR  │   │ │
│     title             VARCHAR  │   │ │
│     owner_user_id     VARCHAR  │   │ │
│     created_at        TIMESTAMP│   │ │
│     updated_at        TIMESTAMP│   │ │
└────────────────────────────────┘   │ │
                                     │ │
                                     │ │
┌────────────────────────────────┐   │ │
│     dim_features (8)           │   │ │
├────────────────────────────────┤   │ │
│ PK  feature_sk        SERIAL   │◄┐ │ │
│ UK  feature_id        VARCHAR  │ │ │ │
│     feature_name      VARCHAR  │ │ │ │
│     feature_category  VARCHAR  │ │ │ │
│     is_premium        BOOLEAN  │ │ │ │
│     created_at        TIMESTAMP│ │ │ │
└────────────────────────────────┘ │ │ │
                                   │ │ │
                                   │ │ │
┌────────────────────────────────┐ │ │ │
│      dim_date (2,557)          │ │ │ │
├────────────────────────────────┤ │ │ │
│ PK  date_key          INTEGER  │◄┼─┼─┼─────┐
│     full_date         DATE     │ │ │ │     │
│     day_of_week       INTEGER  │ │ │ │     │
│     day_name          VARCHAR  │ │ │ │     │
│     week_of_year      INTEGER  │ │ │ │     │
│     month             INTEGER  │ │ │ │     │
│     month_name        VARCHAR  │ │ │ │     │
│     quarter           INTEGER  │ │ │ │     │
│     year              INTEGER  │ │ │ │     │
│     is_weekend        BOOLEAN  │ │ │ │     │
└────────────────────────────────┘ │ │ │     │
                                   │ │ │     │
                ┌──────────────────┼─┼─┼─────┤
                │                  │ │ │     │
                ▼                  │ │ │     │
┌─────────────────────────────────────────────────────────────────┐
│                  fact_events (4.8M)                             │
│                    [FACT TABLE]                                 │
├─────────────────────────────────────────────────────────────────┤
│ PK  event_sk              SERIAL                                │
│ UK  event_id              VARCHAR(255)                          │
│                                                                  │
│ ╔═══════════════════ FOREIGN KEYS ═══════════════════╗         │
│ ║ FK  user_sk              INTEGER  ─────────────────╫─────────┘
│ ║ FK  document_sk          INTEGER  ─────────────────╫──────┘
│ ║ FK  feature_sk           INTEGER  ─────────────────╫───┘
│ ║ FK  date_key             INTEGER  ─────────────────╫───────────┘
│ ╚════════════════════════════════════════════════════╝
│
│ ╔═══════════════ DEGENERATE DIMENSIONS ═══════════════╗
│ ║     event_type           VARCHAR(100)               ║
│ ║     session_id           VARCHAR(255)               ║
│ ║     platform             VARCHAR(50)                ║
│ ║     event_timestamp      TIMESTAMP                  ║
│ ╚═════════════════════════════════════════════════════╝
│
│ ╔═══════════════════════ MEASURES ════════════════════╗
│ ║     duration_seconds     INTEGER (nullable)         ║
│ ║     characters_added     INTEGER (nullable)         ║
│ ╚═════════════════════════════════════════════════════╝
│
│     created_at            TIMESTAMP                             │
└─────────────────────────────────────────────────────────────────┘
                │
                │ (aggregates to)
                │
                ▼
┌─────────────────────────────────────────────────────────────────┐
│            fact_daily_user_activity (278K)                      │
│                    [AGGREGATE FACT TABLE]                       │
├─────────────────────────────────────────────────────────────────┤
│ PK  activity_date         DATE                                  │
│ PK  user_sk               INTEGER  ──────────────────┐          │
│                                                       │          │
│ ╔═══════════════════════ MEASURES ════════════════════════╗    │
│ ║     total_events              INTEGER                   ║    │
│ ║     login_count               INTEGER                   ║    │
│ ║     documents_edited          INTEGER                   ║    │
│ ║     documents_created         INTEGER                   ║    │
│ ║     total_active_seconds      INTEGER                   ║    │
│ ║     distinct_features_used    INTEGER                   ║    │
│ ╚═════════════════════════════════════════════════════════╝    │
│                                                                  │
│     created_at                TIMESTAMP                         │
│     updated_at                TIMESTAMP                         │
└─────────────────────────────────────────────────────────────────┘
                                                       │
                                                       │
                            ┌──────────────────────────┘
                            │
                            ▼
                  (references dim_users)


═══════════════════════════════════════════════════════════════════════════════

LEGEND:
  PK  = Primary Key
  FK  = Foreign Key
  UK  = Unique Key
  ─►  = Relationship (1:Many)

CARDINALITIES:
  dim_users (1)      ──────► fact_events (Many)
  dim_documents (1)  ──────► fact_events (Many)
  dim_features (1)   ──────► fact_events (Many)
  dim_date (1)       ──────► fact_events (Many)
  dim_users (1)      ──────► fact_daily_user_activity (Many)

═══════════════════════════════════════════════════════════════════════════════
