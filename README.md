# UK Economic Indicators Pipeline

An Airflow-orchestrated medallion data pipeline (Bronze → Silver → Gold) built on ONS (Office for National Statistics) open data, containerised with Docker Compose.

**Tech stack:** Apache Airflow · Docker · Python · PostgreSQL · dbt · GitHub Actions CI

---

## Architecture

```
ONS Beta API (UK Gov open data)
         │
         ▼
┌─────────────────────────────┐
│  BRONZE — raw ingestion     │  Airflow task: ingest_bronze
│  bronze.ons_raw             │  Append-only. Exact API response.
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│  SILVER — cleaned & typed   │  Airflow task: process_silver
│  silver.ons_indicators      │  Period parsing, type coercion,
│                             │  dedup via ON CONFLICT upsert
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│  GOLD — business-ready      │  Airflow task: run_dbt_models
│  gold.uk_economic_kpis      │  QoQ change, YoY change,
│                             │  rolling avg, trend flags
└──────────────┬──────────────┘
               │
               ▼
         Power BI / reporting
```

**Orchestration:** Airflow DAG runs daily at 06:00 UTC with automatic retries and data quality gates between each layer.

---

## Indicators tracked

| Series | Indicator | Source |
|---|---|---|
| L55O | CPIH Inflation Rate | ONS cpih01 |
| LF24 | UK Unemployment Rate | ONS labour market |
| YBHA | UK GDP (chained volume) | ONS ukea |
| K37M | Producer Price Index | ONS ppi |
| ABMI | UK Nominal GDP | ONS qna |

Data published under the [Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence).

---

## Quickstart

### Prerequisites
- Docker Desktop installed and running
- Git

### 1 — Clone and configure

```bash
git clone https://github.com/Arun-rd/uk-economic-pipeline.git
cd uk-economic-pipeline
cp .env.example .env
```

### 2 — Start the full stack (one command)

```bash
docker compose up airflow-init
docker compose up -d
```

Wait ~60 seconds for Airflow to initialise. Then open:

```
Airflow UI:  http://localhost:8080
             username: admin  password: admin
```

### 3 — Trigger the pipeline

In the Airflow UI:
1. Find the `ons_uk_economic_pipeline` DAG
2. Click the play button (▶) to trigger a run
3. Watch tasks turn green: health_check → ingest_bronze → validate_bronze → process_silver → validate_silver → run_dbt_models → run_dbt_tests → pipeline_summary

### 4 — Inspect the data

```bash
docker compose exec postgres psql -U airflow -d ons_pipeline
```

```sql
-- Bronze: raw rows
SELECT dataset_id, series_id, period, value FROM bronze.ons_raw LIMIT 10;

-- Silver: cleaned
SELECT indicator_name, period_date, value_numeric, unit
FROM silver.ons_indicators
WHERE is_valid = TRUE ORDER BY period_date DESC LIMIT 10;

-- Gold: KPI mart with calculated metrics
SELECT indicator_name, period_quarter, value,
       qoq_pct_change, yoy_pct_change, trend_direction
FROM gold.uk_economic_kpis
ORDER BY indicator_name, period_quarter DESC LIMIT 20;
```

### 5 — Run dbt manually (optional)

```bash
docker compose exec airflow-scheduler bash -c "
  cd /opt/airflow/dbt_ons &&
  dbt run --profiles-dir . &&
  dbt test --profiles-dir . &&
  dbt docs generate --profiles-dir .
"
```

### 6 — Stop the stack

```bash
docker compose down          # stop containers
docker compose down -v       # stop + delete data volumes
```

---

## Project structure

```
uk-economic-pipeline/
├── dags/
│   └── ons_pipeline_dag.py         # Main Airflow DAG (8 tasks)
├── ingestion/
│   ├── ons_client.py               # ONS API client + synthetic fallback
│   ├── bronze_loader.py            # Raw → bronze.ons_raw
│   └── silver_processor.py         # Bronze → silver.ons_indicators
├── dbt_ons/
│   ├── models/
│   │   ├── bronze/brz_ons_raw.sql
│   │   ├── silver/slv_ons_indicators.sql
│   │   └── gold/gold_uk_economic_kpis.sql   # QoQ, YoY, rolling avg
│   ├── tests/
│   │   ├── assert_no_future_quarters.sql
│   │   └── assert_pct_change_reasonable.sql
│   ├── dbt_project.yml
│   └── profiles.yml
├── sql/
│   └── init_db.sql                 # Schema DDL (auto-runs in Docker)
├── tests/
│   ├── test_silver_processor.py    # Unit tests: period parsing, numeric coercion
│   └── test_ons_client.py          # Unit tests: synthetic data generation
├── .github/
│   └── workflows/ci.yml            # GitHub Actions: lint + test on push
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

---

## DAG task graph

```
health_check
    │
ingest_bronze
    │
validate_bronze ── (asserts row count, series count, null %)
    │
process_silver
    │
validate_silver ── (asserts valid rows, no future dates, null numeric %)
    │
run_dbt_models
    │
run_dbt_tests
    │
pipeline_summary ── (logs Bronze / Silver / Gold counts)
```

---

## Gold layer metrics

| Column | Description |
|---|---|
| `value` | Quarterly average of indicator |
| `qoq_change` | Quarter-over-quarter absolute change |
| `qoq_pct_change` | Quarter-over-quarter % change |
| `yoy_change` | Year-over-year absolute change |
| `yoy_pct_change` | Year-over-year % change |
| `rolling_4q_avg` | 4-quarter rolling average (smoothed trend) |
| `trend_direction` | Rising / Falling / Flat |
| `inflation_status` | Above target / At/below target (CPIH only) |

---

## CI/CD

GitHub Actions runs on every push to `main` or `develop`:
- Python lint with `flake8`
- dbt project validation (`dbt parse`)
- Unit tests with `pytest` against a live PostgreSQL service

---

## Skills demonstrated

`Apache Airflow` `Docker Compose` `Medallion architecture` `PostgreSQL` `dbt` `Python` `GitHub Actions CI/CD` `Data quality testing` `ONS API` `ETL pipeline design` `Unit testing (pytest)`

---

## Author

**Arun Kumar Ravi** — BI Developer → Data Engineer · Birmingham, UK
[LinkedIn](https://www.linkedin.com/in/arun-ravi-07/) · [GitHub](https://github.com/Arun-rd)
