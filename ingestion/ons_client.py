"""
ons_client.py
-------------
Client for the ONS (Office for National Statistics) Beta API.
Fetches UK economic indicator time series with a synthetic fallback
so the pipeline works even when the API is unavailable.

Endpoints used:
  https://api.beta.ons.gov.uk/v1/datasets/{dataset}/timeseries/{series}/data
"""
import json
import logging
import time
from typing import Optional
import numpy as np
import pandas as pd
import requests

log = logging.getLogger(__name__)

ONS_BASE = "https://api.beta.ons.gov.uk/v1"
REQUEST_DELAY = 0.5  # seconds between requests — respect rate limits

# Datasets to ingest: (dataset_id, series_id, human name, unit)
ONS_SERIES = [
    ("cpih01",  "L55O", "CPIH inflation rate",           "% change"),
    ("employmentandlabourmarket", "LF24", "UK unemployment rate", "%"),
    ("ukea",    "YBHA", "UK GDP (chained volume)",       "£ million"),
    ("ppi",     "K37M", "Producer price index (output)", "index"),
    ("qna",     "ABMI", "UK nominal GDP",                "£ million"),
]


def _fetch_series(dataset_id: str, series_id: str, timeout: int = 20) -> Optional[dict]:
    url = f"{ONS_BASE}/datasets/{dataset_id}/timeseries/{series_id}/data"
    try:
        r = requests.get(url, timeout=timeout, headers={"Accept": "application/json"})
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        log.warning(f"ONS API unavailable for {dataset_id}/{series_id}: {exc}")
        return None


def _synthetic_series(series_id: str, indicator_name: str, unit: str) -> list[dict]:
    """Generate realistic synthetic time series for CI / offline use."""
    rng = np.random.default_rng(seed=abs(hash(series_id)) % 9999)
    quarters = pd.period_range("2018Q1", periods=24, freq="Q")
    base = rng.uniform(50, 200)
    trend = rng.uniform(-0.5, 1.5)
    rows = []
    for i, q in enumerate(quarters):
        noise = rng.normal(0, base * 0.02)
        value = round(base + trend * i + noise, 2)
        rows.append({
            "period":     str(q),
            "value":      str(value),
            "label":      indicator_name,
            "unit":       unit,
            "is_synthetic": True,
        })
    return rows


def _parse_response(data: dict, series_id: str, indicator_name: str, unit: str) -> list[dict]:
    """Parse ONS API response into a flat list of period/value dicts."""
    rows = []
    for key in ("quarters", "months", "years"):
        for item in data.get(key, []):
            rows.append({
                "period":     item.get("date", ""),
                "value":      item.get("value", ""),
                "label":      data.get("description", {}).get("title", indicator_name),
                "unit":       unit,
                "is_synthetic": False,
            })
    return rows


def fetch_all_series() -> list[dict]:
    """
    Fetch all configured ONS series.
    Falls back to synthetic data per-series if API unavailable.
    Returns list of dicts ready for bronze loading.
    """
    results = []
    for dataset_id, series_id, indicator_name, unit in ONS_SERIES:
        log.info(f"Fetching {indicator_name} ({dataset_id}/{series_id})")
        data = _fetch_series(dataset_id, series_id)
        if data:
            rows = _parse_response(data, series_id, indicator_name, unit)
            log.info(f"  API returned {len(rows)} rows")
        else:
            rows = _synthetic_series(series_id, indicator_name, unit)
            log.info(f"  Using synthetic data: {len(rows)} rows")

        for row in rows:
            results.append({
                "dataset_id":  dataset_id,
                "series_id":   series_id,
                "indicator_name": indicator_name,
                "unit":        unit,
                **row,
            })
        time.sleep(REQUEST_DELAY)

    log.info(f"Total rows fetched: {len(results)}")
    return results
