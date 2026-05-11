"""
bronze_loader.py
----------------
Loads raw ONS data into the Bronze schema (append-only).
Bronze = exact API response, no transformations.
"""
import json
import logging
import os
from typing import List, Dict
import psycopg2
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "ons_pipeline"),
        user=os.getenv("DB_USER", "airflow"),
        password=os.getenv("DB_PASSWORD", "airflow"),
    )

def load_bronze(rows: List[Dict]) -> int:
    """Insert raw rows into bronze.ons_raw. Returns rows inserted."""
    if not rows:
        log.warning("No rows to load into bronze")
        return 0

    sql = """
        INSERT INTO bronze.ons_raw
            (dataset_id, series_id, period, value, raw_json)
        VALUES (%s, %s, %s, %s, %s)
    """
    conn = get_conn()
    loaded = 0
    try:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(sql, (
                    row["dataset_id"],
                    row["series_id"],
                    row["period"],
                    row["value"],
                    json.dumps(row),
                ))
                loaded += 1
        conn.commit()
        log.info(f"Bronze: inserted {loaded} rows into bronze.ons_raw")
    finally:
        conn.close()
    return loaded

def get_latest_bronze() -> List[Dict]:
    """Read the latest bronze snapshot for silver processing."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (dataset_id, series_id, period)
                    dataset_id, series_id, period, value, raw_json, ingested_at
                FROM bronze.ons_raw
                ORDER BY dataset_id, series_id, period, ingested_at DESC
            """)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()
