# SaaS Analytics Pipeline

A production-grade data engineering pipeline for SaaS product analytics, built with Apache Airflow, PostgreSQL, and Docker. This project demonstrates end-to-end ETL practices with a 4-layer data architecture and real-time analytics dashboards.

![Architecture](https://img.shields.io/badge/Architecture-4--Layer%20ETL-blue)
![Python](https://img.shields.io/badge/Python-3.11-green)
![Airflow](https://img.shields.io/badge/Airflow-2.8-red)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)

## 🎯 Project Overview

This pipeline processes synthetic SaaS product events (user logins, document edits, feature usage) through a multi-layer architecture:

```
Raw Events (JSON) → Staging (Typed) → Core (Star Schema) → Analytics (Materialized Views)
```

**Key Features:**
- 📊 **1.8M+ synthetic events** covering 6 months of activity
- 🔄 **Automated ETL** orchestrated by Airflow DAGs
- 🗄️ **Star schema** with dimension and fact tables
- 📈 **Analytics layer** with retention cohorts, feature adoption, and churn risk
- 🎨 **Metabase dashboards** for business intelligence
- 🐳 **Fully Dockerized** infrastructure

## 🏗️ Architecture

### Data Layers

1. **Raw Layer** (`raw` schema)
   - Immutable JSON events stored as JSONB
   - Batch tracking and audit trails

2. **Staging Layer** (`staging` schema)
   - Parsed and typed event data
   - Deduplication and basic validation

3. **Core Layer** (`core` schema)
   - **Star Schema Design:**
     - Dimensions: `dim_users`, `dim_documents`, `dim_features`, `dim_date`
     - Facts: `fact_events`, `fact_daily_user_activity`

4. **Analytics Layer** (`analytics` schema)
   - Materialized views for performance:
     - `user_retention_cohorts`
     - `feature_adoption_funnel`
     - `churn_risk_scores`

### Tech Stack

| Component | Technology |
|-----------|-----------|
| **Orchestration** | Apache Airflow 2.8 |
| **Data Warehouse** | PostgreSQL 15 |
| **BI Tool** | Metabase |
| **Language** | Python 3.11, SQL |
| **Infrastructure** | Docker Compose |

## 📁 Project Structure

```
saas-analytics-pipeline/
├── airflow/
│   ├── dags/                      # Airflow DAG definitions (auto-synced)
│   ├── logs/                      # Execution logs (gitignored)
│   └── plugins/                   # Custom operators/hooks
├── dags/
│   └── saas_analytics_dag.py      # Main ETL pipeline DAG
├── sql/
│   ├── ddl/                       # Schema definitions (empty - schemas created via init scripts)
│   └── transformations/
│       ├── raw_to_staging.sql     # Parse JSON to typed columns
│       ├── staging_to_core.sql    # Load star schema
│       └── refresh_analytics.sql  # Refresh materialized views
├── src/
│   ├── ingestion/
│   │   └── event_generator.py     # Synthetic event generator
│   └── utils/                     # Shared utilities
├── generate_realistic_events.py   # Script to generate historical events (Jan-Jun 2024)
├── generate_current_events.py     # Script to generate current events (Jul 2024-Jan 2026)
├── docker-compose.yml             # Infrastructure definition
├── requirements.txt               # Python dependencies
└── .env.example                   # Environment variables template
```

## 🚀 Quick Start

### Prerequisites

- Docker Desktop (or Docker Engine + Docker Compose)
- 4GB+ free RAM
- Ports available: `5432` (PostgreSQL), `8080` (Airflow), `3000` (Metabase), `5050` (pgAdmin)

### Setup

**1. Clone the repository:**
```bash
git clone https://github.com/vlabogac-png/saas-analytics-pipeline.git
cd saas-analytics-pipeline
```

**2. Configure environment:**
```bash
cp .env.example .env
# Edit .env with your credentials (or use defaults for local development)
```

**3. Generate Airflow UID (macOS/Linux):**
```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

**4. Start infrastructure:**
```bash
docker-compose up -d
```

**5. Wait for services to be healthy (~2 minutes):**
```bash
docker ps
```

**6. Access the UIs:**
- **Airflow:** http://localhost:8080 (username: `admin`, password: `admin`)
- **Metabase:** http://localhost:3000
- **pgAdmin:** http://localhost:5050 (credentials in `.env`)

### Generate Sample Data

**Option 1: Historical data (Jan-Jun 2024, ~1.8M events):**
```bash
# Activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Generate events
python generate_realistic_events.py
```

**Option 2: Current data (Jul 2024-Jan 2026, ~5.5M events):**
```bash
python generate_current_events.py
```

### Run the ETL Pipeline

**Trigger the Airflow DAG:**
```bash
docker exec saas_airflow_webserver airflow dags trigger saas_analytics_pipeline
```

**Monitor execution:**
- Open Airflow UI: http://localhost:8080
- Navigate to DAGs → `saas_analytics_pipeline`
- View task logs and execution graph

## 🔄 ETL Pipeline Flow

The Airflow DAG (`saas_analytics_dag.py`) orchestrates the following tasks:

```
raw_to_staging
    ├─> load_dim_users
    ├─> load_dim_documents
    ├─> load_dim_features
    └─> load_dim_date
            └─> load_fact_events
                    └─> load_fact_daily_user_activity
                            ├─> refresh_user_retention
                            ├─> refresh_feature_adoption
                            └─> refresh_churn_risk
```

**Schedule:** Daily at 2:00 UTC

## 📊 Analytics & Dashboards

### Metabase Dashboard: "Executive Overview"

Access at http://localhost:3000 after connecting to PostgreSQL:
- **Host:** `postgres` (Docker network)
- **Port:** `5432`
- **Database:** `saas_analytics`
- **User:** `dataeng`
- **Password:** (from `.env`)

**Available Visualizations:**
1. **Daily Active Users** - Line chart showing user engagement trends
2. **User Retention Cohorts** - Cohort analysis table
3. **Feature Adoption** - Bar chart of feature usage
4. **Churn Risk Distribution** - Pie chart of user churn risk categories

### Sample Queries

**Feature adoption:**
```sql
SELECT 
    feature_name,
    total_uses,
    unique_users,
    ROUND(total_uses::numeric / unique_users, 1) as avg_uses_per_user
FROM analytics.feature_adoption_funnel
ORDER BY total_uses DESC;
```

**Churn risk:**
```sql
SELECT 
    churn_risk_category,
    COUNT(*) AS user_count
FROM analytics.churn_risk_scores
WHERE last_active_date >= '2024-01-01'
GROUP BY churn_risk_category;
```

## 🛠️ Development

### Running SQL Transformations Manually

```bash
# Connect to PostgreSQL
docker exec -it saas_postgres psql -U dataeng -d saas_analytics

# Run transformation
\i /opt/sql/transformations/raw_to_staging.sql
```

### Refresh Materialized Views

```bash
docker exec saas_postgres psql -U dataeng -d saas_analytics -c "
REFRESH MATERIALIZED VIEW analytics.user_retention_cohorts;
REFRESH MATERIALIZED VIEW analytics.feature_adoption_funnel;
REFRESH MATERIALIZED VIEW analytics.churn_risk_scores;
"
```

### View Logs

```bash
# Airflow scheduler
docker-compose logs -f airflow-scheduler

# PostgreSQL
docker-compose logs -f postgres
```

## 📈 Data Model

### Dimensions
- `core.dim_users` - 500 users with registration dates and subscription tiers
- `core.dim_documents` - 2,000 documents with ownership and creation dates
- `core.dim_features` - 8 product features (Templates, Comments, Export, etc.)
- `core.dim_date` - Date dimension for time-based analysis

### Facts
- `core.fact_events` - 1.8M+ granular event records
- `core.fact_daily_user_activity` - Aggregated daily user metrics

### Analytics Views
- `analytics.user_retention_cohorts` - Monthly cohort retention analysis
- `analytics.feature_adoption_funnel` - Feature usage metrics
- `analytics.churn_risk_scores` - User churn risk classification

## 🧪 Testing

```bash
# Validate data quality
docker exec saas_postgres psql -U dataeng -d saas_analytics -c "
SELECT 'Raw Events' as layer, COUNT(*) as record_count FROM raw.events
UNION ALL
SELECT 'Staging Events', COUNT(*) FROM staging.events
UNION ALL
SELECT 'Core Events', COUNT(*) FROM core.fact_events;
"
```

## 🚧 Production Considerations

This is a **portfolio/learning project**. For production use, consider:

- ✅ Use managed Airflow (AWS MWAA, Google Cloud Composer, Astronomer)
- ✅ Implement `CeleryExecutor` with Redis/RabbitMQ for scalability
- ✅ Store secrets in AWS Secrets Manager / HashiCorp Vault
- ✅ Add monitoring (Datadog, Prometheus + Grafana)
- ✅ Implement CI/CD (GitHub Actions → Docker Registry → ECS/K8s)
- ✅ Use dbt for SQL transformations with version control
- ✅ Add data quality tests (Great Expectations, dbt tests)
- ✅ Implement incremental processing for large datasets
- ✅ Set up alerting for pipeline failures

## 📝 License

MIT License - feel free to use this project for learning and portfolio purposes.

## 🤝 Contributing

This is a personal portfolio project, but suggestions and feedback are welcome! Open an issue or reach out.

---

**Built with ❤️ as a data engineering portfolio project**
