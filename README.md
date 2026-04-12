# E-Commerce Order Analytics Pipeline

> Near real-time data pipeline built with **Airflow · dbt · PostgreSQL · Streamlit · Docker**

[![Pipeline CI](https://github.com/mikailtipi/near_real_time_ecommerce_project/actions/workflows/ci.yml/badge.svg)](https://github.com/mikailtipi/near_real_time_ecommerce_project/actions)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Docker Compose                          │
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │   Faker /    │───▶│  PostgreSQL  │───▶│   dbt Core       │  │
│  │   Python     │    │  raw schema  │    │   staging+marts  │  │
│  │  (generator) │    └──────────────┘    └──────────────────┘  │
│  └──────────────┘           │                     │            │
│                             ▼                     ▼            │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │  DQ Checker  │    │   Airflow    │    │   Streamlit      │  │
│  │  (terminal)  │    │   DAG        │    │   Dashboard      │  │
│  └──────────────┘    └──────────────┘    └──────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**Data flow:**
1. Python generator creates simulated e-commerce orders (near real-time or batch)
2. Airflow DAG orchestrates the daily pipeline at 06:00 UTC
3. DQ checker validates raw data before transformation
4. dbt transforms `raw` → `staging` (views) → `marts` (tables)
5. Streamlit dashboard visualizes KPIs from mart tables

---

## Tech Stack

| Layer | Tool | Version |
|---|---|---|
| Orchestration | Apache Airflow | 2.9 |
| Transformation | dbt Core | 1.8 |
| Storage | PostgreSQL | 15 |
| Ingestion | Python + Faker | 3.11 |
| Dashboard | Streamlit + Plotly | 1.35 |
| Containerization | Docker Compose | v3.8 |
| CI/CD | GitHub Actions | — |

---

## Quick Start

### Prerequisites
- Docker + Docker Compose
- Python 3.11+

### Run locally

```bash
# 1. Clone the repo
git clone https://github.com/mikailtipi/near_real_time_ecommerce_project.git
cd ecommerce-pipeline

# 2. Start all services
docker compose up -d

# 3. Seed initial data
DB_HOST=localhost python ingestion/generate_orders.py --mode seed

# 4. Run data quality checks
DB_HOST=localhost python data_quality/dq_check.py

# 5. Open Airflow UI → trigger the DAG
open http://localhost:8080   # admin / admin

# 6. Open dashboard
open http://localhost:8501
```

---

## Project Structure

```
ecommerce-pipeline/
├── airflow/
│   └── dags/
│       └── ecommerce_pipeline_dag.py   # Main orchestration DAG
├── ingestion/
│   └── generate_orders.py              # Faker-based data generator (seed/stream/batch)
├── dbt_project/
│   ├── models/
│   │   ├── staging/stg_orders.sql      # Cleaned raw orders
│   │   └── marts/mart_order_metrics.sql # Daily KPIs
│   └── dbt_project.yml
├── data_quality/
│   └── dq_check.py                     # Null/duplicate/range checks with colored output
├── dashboard/
│   ├── app.py                          # Streamlit dashboard
│   └── Dockerfile
├── scripts/
│   └── init_db.sh                      # DB schema init (raw/staging/marts)
├── .github/workflows/ci.yml            # GitHub Actions CI
├── docker-compose.yml
└── requirements.txt
```

---

## Data Quality Checks

The `dq_check.py` tool runs automatically in the Airflow DAG before dbt transforms.

```bash
python data_quality/dq_check.py --table orders
```

Checks include: null values, duplicate primary keys, referential integrity, invalid enum values, date logic violations, and negative/zero numeric values. Results are saved to `raw.dq_results` for trend monitoring.

---

## dbt Models

```
raw.orders + raw.order_items
        │
        ▼
staging.stg_orders          ← cleaned, typed, derived fields (delivery_days, is_late)
        │
        ▼
marts.mart_order_metrics    ← daily KPIs: revenue, AOV, return rate, delivery performance
```

---

## Dashboard KPIs

- Total orders & revenue (daily)
- Average order value
- Delivery performance (avg days, late %)
- Return rate trend
- Latest DQ check results

---

## What I Learned

Building this pipeline end-to-end surfaced a few non-obvious things:

- **Airflow task isolation**: dbt run and dbt test should be separate tasks — failing tests shouldn't block you from seeing *which* models ran successfully.
- **DQ before transform**: Running data quality checks on raw data *before* dbt prevents silent bad data flowing into mart tables that executives then act on.
- **Docker networking**: Airflow and Streamlit need to reference `postgres` (the service name), not `localhost` — caught me off guard the first time.

---

## About

Built as a portfolio project to demonstrate end-to-end data engineering skills for EU-based roles.
Connect with me on [LinkedIn](https://linkedin.com/in/mikailtipi).
