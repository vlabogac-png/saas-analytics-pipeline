# SaaS Analytics Pipeline

End-to-end data engineering pipeline for product usage analytics, demonstrating production-grade ETL practices.

## Architecture
```
Ingestion (Python) → Raw Layer → Staging Layer → Core Layer (Star Schema) → Analytics Layer
```

**Tech Stack:**
- **Orchestration:** Apache Airflow 2.8
- **Data Warehouse:** PostgreSQL 15
- **Language:** Python 3.11, SQL
- **Infrastructure:** Docker Compose

## Project Structure
```
.
├── airflow/
│   ├── dags/          # Airflow DAG definitions
│   ├── logs/          # Execution logs (gitignored)
│   └── plugins/       # Custom operators/hooks
├── sql/
│   ├── ddl/           # Schema definitions
│   └── transformations/ # Incremental SQL logic
├── src/
│   ├── ingestion/     # Event generation & API simulation
│   └── utils/         # Shared helpers
└── docker-compose.yml
```

## Quick Start

### Prerequisites
- Docker Desktop (or Docker Engine + Compose)
- 4GB free RAM
- Port 8080, 5432, 5050 available

### Setup

1. **Clone and configure:**
```bash
   git clone <repo-url>
   cd saas-analytics-pipeline
   cp .env.example .env
   # Edit .env with your credentials
```

2. **Start infrastructure:**
```bash
   echo "AIRFLOW_UID=$(id -u)" >> .env
   docker-compose up -d
```

3. **Access UIs:**
   - Airflow: http://localhost:8080 (admin / admin)
   - PgAdmin: http://localhost:5050 (see .env for credentials)

4. **Initialize database schemas:**
```bash
   docker exec -it saas_postgres psql -U dataeng -d saas_analytics -f /docker-entrypoint-initdb.d/01_raw_schema.sql
   # Repeat for 02, 03, 04
```

## Development Workflow
```bash
# Make changes to DAGs (auto-reloaded by Airflow)
vim airflow/dags/ingestion_pipeline.py

# Test SQL transformations
docker exec -it saas_postgres psql -U dataeng -d saas_analytics
\i /opt/sql/transformations/staging_events.sql

# View logs
docker-compose logs -f airflow-scheduler
```

## Data Model

**Raw Layer:** Immutable JSON events  
**Staging Layer:** Typed, deduplicated records  
**Core Layer:** Star schema (3NF dimensions + fact tables)  
**Analytics Layer:** Denormalized business metrics

See `docs/data_model.md` for detailed ERD.

## Testing
```bash
# Run data quality checks (TBD)
pytest tests/

# Manual validation queries
psql -h localhost -U dataeng -d saas_analytics -f tests/validation_queries.sql
```

## Production Considerations

This is a portfolio project. In production, you would:
- Use managed Airflow (MWAA, Cloud Composer, Astronomer)
- Implement CeleryExecutor with Redis/RabbitMQ
- Store secrets in AWS Secrets Manager / Vault
- Add monitoring (Datadog, Prometheus)
- Implement CI/CD (GitHub Actions → Docker Registry)
- Use dbt for SQL transformations

## License

MIT