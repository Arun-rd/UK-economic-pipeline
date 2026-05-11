"""
ons_pipeline_dag.py
-------------------
Main Airflow DAG for the UK Economic Indicators pipeline.

Schedule: Daily at 06:00 UTC
Layers:   Bronze (raw ingest) -> Silver (clean) -> Gold (dbt)

Tasks:
  1. health_check         -- verify DB connectivity
  2. ingest_bronze        -- fetch ONS API -> bronze.ons_raw
  3. validate_bronze      -- row count & null checks
  4. process_silver       -- bronze -> silver.ons_indicators
  5. validate_silver      -- data quality assertions
  6. run_dbt_models       -- dbt run (gold layer)
  7. run_dbt_tests        -- dbt test
  8. pipeline_summary     -- log final counts
"""
import logging
from datetime import datetime, timedelta
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner":            "arun-ravi",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
}

DB_CONN = {
    "host": "postgres", "port": 5432,
    "dbname": "ons_pipeline", "user": "airflow", "password": "airflow",
}


def _get_conn():
    return psycopg2.connect(**DB_CONN)


def health_check(**ctx):
    """Verify PostgreSQL is reachable and schemas exist."""
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT schema_name FROM information_schema.schemata "
                    "WHERE schema_name IN ('bronze','silver','gold')")
        schemas = [r[0] for r in cur.fetchall()]
    conn.close()
    assert len(schemas) == 3, "Expected 3 schemas, found: {}".format(schemas)
    log.info("Health check passed. Schemas present: %s", schemas)


def ingest_bronze(**ctx):
    """Fetch ONS series and load into bronze.ons_raw."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from ingestion.ons_client import fetch_all_series
    from ingestion.bronze_loader import load_bronze

    rows = fetch_all_series()
    loaded = load_bronze(rows)
    ctx["ti"].xcom_push(key="bronze_rows", value=loaded)
    log.info("Bronze ingestion complete: %d rows", loaded)


def validate_bronze(**ctx):
    """Assert bronze has data and no fully-null values."""
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM bronze.ons_raw")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM bronze.ons_raw WHERE value IS NULL")
        nulls = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT series_id) FROM bronze.ons_raw")
        series = cur.fetchone()[0]
    conn.close()

    assert total > 0, "Bronze table is empty!"
    assert series >= 3, "Expected at least 3 series, got {}".format(series)
    null_pct = nulls / total * 100
    assert null_pct < 20, "Too many null values: {:.1f}%".format(null_pct)
    log.info("Bronze validation passed: %d rows, %d series, %.1f%% nulls",
             total, series, null_pct)


def process_silver(**ctx):
    """Transform bronze rows into silver.ons_indicators."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from ingestion.bronze_loader import get_latest_bronze
    from ingestion.silver_processor import process_bronze_to_silver

    bronze_rows = get_latest_bronze()
    upserted = process_bronze_to_silver(bronze_rows)
    ctx["ti"].xcom_push(key="silver_rows", value=upserted)
    log.info("Silver processing complete: %d rows upserted", upserted)


def validate_silver(**ctx):
    """Data quality checks on silver layer."""
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM silver.ons_indicators WHERE is_valid = TRUE")
        valid = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM silver.ons_indicators "
            "WHERE value_numeric IS NULL AND is_valid = TRUE"
        )
        null_numeric = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM silver.ons_indicators WHERE period_date > CURRENT_DATE"
        )
        future = cur.fetchone()[0]
    conn.close()

    assert valid > 0, "No valid rows in silver!"
    assert future == 0, "Found {} rows with future dates".format(future)
    null_pct = null_numeric / valid * 100 if valid > 0 else 100
    assert null_pct < 10, "Too many null numeric values: {:.1f}%".format(null_pct)
    log.info("Silver validation passed: %d valid rows, %.1f%% null numeric",
             valid, null_pct)


def pipeline_summary(**ctx):
    """Log final row counts across all three layers."""
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM bronze.ons_raw")
        bronze = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM silver.ons_indicators")
        silver = cur.fetchone()[0]
        try:
            cur.execute("SELECT COUNT(*) FROM gold.uk_economic_kpis")
            gold = cur.fetchone()[0]
        except Exception:
            gold = "not yet built"
    conn.close()
    log.info("=" * 50)
    log.info("PIPELINE SUMMARY")
    log.info("  Bronze rows : %s", bronze)
    log.info("  Silver rows : %s", silver)
    log.info("  Gold rows   : %s", gold)
    log.info("=" * 50)


with DAG(
    dag_id="ons_uk_economic_pipeline",
    description="UK Economic Indicators: Bronze -> Silver -> Gold medallion pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["data-engineering", "ons", "medallion", "uk-economics"],
) as dag:

    t_health = PythonOperator(
        task_id="health_check",
        python_callable=health_check,
    )

    t_bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=ingest_bronze,
    )

    t_val_bronze = PythonOperator(
        task_id="validate_bronze",
        python_callable=validate_bronze,
    )

    t_silver = PythonOperator(
        task_id="process_silver",
        python_callable=process_silver,
    )

    t_val_silver = PythonOperator(
        task_id="validate_silver",
        python_callable=validate_silver,
    )

    t_dbt_run = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /opt/airflow/dbt_ons && dbt run --profiles-dir /opt/airflow/dbt_ons",
        retries=1,
    )

    t_dbt_test = BashOperator(
        task_id="run_dbt_tests",
        bash_command="cd /opt/airflow/dbt_ons && dbt test --profiles-dir /opt/airflow/dbt_ons",
    )

    t_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=pipeline_summary,
        trigger_rule="all_done",
    )

    t_health >> t_bronze >> t_val_bronze >> t_silver >> t_val_silver >> t_dbt_run >> t_dbt_test >> t_summary
