"""
silver_processor.py
-------------------
Transforms bronze rows into the Silver schema.
Silver = cleaned, typed, deduplicated, validated.
"""
import logging
import os
import re
from datetime import date
from typing import List, Dict, Optional
import psycopg2
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

INDICATOR_NAMES = {
    "L55O": "CPIH Inflation Rate",
    "LF24": "UK Unemployment Rate",
    "YBHA": "UK GDP Chained Volume",
    "K37M": "Producer Price Index Output",
    "ABMI": "UK Nominal GDP",
}

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "ons_pipeline"),
        user=os.getenv("DB_USER", "airflow"),
        password=os.getenv("DB_PASSWORD", "airflow"),
    )

def _parse_period_to_date(period: str) -> Optional[date]:
    """Convert ONS period strings to dates. Handles Q1 2023, 2023 Q1, Jan 2023, 2023 formats."""
    period = period.strip()
    # Quarterly: "2023 Q1" or "2023Q1"
    m = re.match(r"(\d{4})\s*Q(\d)", period)
    if m:
        year, q = int(m.group(1)), int(m.group(2))
        month = (q - 1) * 3 + 1
        return date(year, month, 1)
    # Monthly: "2023 JAN" or "Jan 2023"
    months = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,
              "JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}
    m = re.match(r"(\d{4})\s+([A-Za-z]{3})", period)
    if m:
        try:
            return date(int(m.group(1)), months[m.group(2).upper()], 1)
        except (KeyError, ValueError):
            pass
    # Annual: "2023"
    m = re.match(r"^(\d{4})$", period)
    if m:
        return date(int(m.group(1)), 1, 1)
    return None

def _parse_numeric(value: str) -> Optional[float]:
    """Parse value string to float, handling ONS special codes."""
    if not value or value.strip() in ("", "..", "N/A", "-"):
        return None
    try:
        return float(value.replace(",", ""))
    except ValueError:
        return None

def _determine_unit(dataset_id: str, series_id: str, raw_json: dict) -> str:
    unit_map = {
        "cpih01": "% change",
        "employmentandlabourmarket": "%",
        "ppi": "index",
    }
    return raw_json.get("unit", unit_map.get(dataset_id, "£ million"))

def process_bronze_to_silver(bronze_rows: List[Dict]) -> int:
    """Upsert bronze rows into silver.ons_indicators. Returns rows upserted."""
    if not bronze_rows:
        log.warning("No bronze rows to process")
        return 0

    sql = """
        INSERT INTO silver.ons_indicators
            (dataset_id, series_id, indicator_name, period, period_date,
             value_raw, value_numeric, unit, is_valid)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (dataset_id, series_id, period)
        DO UPDATE SET
            value_raw     = EXCLUDED.value_raw,
            value_numeric = EXCLUDED.value_numeric,
            period_date   = EXCLUDED.period_date,
            is_valid      = EXCLUDED.is_valid,
            loaded_at     = NOW()
    """
    conn = get_conn()
    upserted = 0
    try:
        with conn.cursor() as cur:
            for row in bronze_rows:
                raw_json  = row.get("raw_json") or {}
                period    = str(row["period"])
                value_str = str(row["value"]) if row["value"] else ""

                period_date   = _parse_period_to_date(period)
                value_numeric = _parse_numeric(value_str)
                is_valid      = period_date is not None
                indicator     = INDICATOR_NAMES.get(row["series_id"],
                                raw_json.get("label", row["series_id"]))
                unit = _determine_unit(row["dataset_id"], row["series_id"], raw_json)

                cur.execute(sql, (
                    row["dataset_id"], row["series_id"], indicator,
                    period, period_date, value_str, value_numeric,
                    unit, is_valid,
                ))
                upserted += 1

        conn.commit()
        log.info(f"Silver: upserted {upserted} rows into silver.ons_indicators")
    finally:
        conn.close()
    return upserted
