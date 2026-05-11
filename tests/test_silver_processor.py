"""
Unit tests for silver_processor.py
Run with: pytest tests/ -v
"""
import sys
sys.path.insert(0, ".")
from ingestion.silver_processor import _parse_period_to_date, _parse_numeric
from datetime import date

def test_parse_quarterly_period():
    assert _parse_period_to_date("2023 Q1") == date(2023, 1, 1)
    assert _parse_period_to_date("2023Q3")  == date(2023, 7, 1)
    assert _parse_period_to_date("2020 Q4") == date(2020, 10, 1)

def test_parse_annual_period():
    assert _parse_period_to_date("2022") == date(2022, 1, 1)
    assert _parse_period_to_date("2019") == date(2019, 1, 1)

def test_parse_invalid_period():
    assert _parse_period_to_date("N/A") is None
    assert _parse_period_to_date("")    is None

def test_parse_numeric_normal():
    assert _parse_numeric("3.5")     == 3.5
    assert _parse_numeric("100,000") == 100000.0
    assert _parse_numeric("-2.1")    == -2.1

def test_parse_numeric_special_codes():
    assert _parse_numeric("..") is None
    assert _parse_numeric("N/A") is None
    assert _parse_numeric("")    is None
    assert _parse_numeric(None)  is None

def test_parse_numeric_zero():
    assert _parse_numeric("0")   == 0.0
    assert _parse_numeric("0.0") == 0.0
