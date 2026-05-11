"""Unit tests for ONS client synthetic data generation."""
import sys
sys.path.insert(0, ".")
from ingestion.ons_client import _synthetic_series

def test_synthetic_series_returns_rows():
    rows = _synthetic_series("L55O", "CPIH Inflation", "% change")
    assert len(rows) > 0

def test_synthetic_series_has_required_fields():
    rows = _synthetic_series("L55O", "CPIH Inflation", "% change")
    for row in rows:
        assert "period" in row
        assert "value"  in row
        assert row["is_synthetic"] is True

def test_synthetic_series_deterministic():
    rows1 = _synthetic_series("YBHA", "GDP", "£ million")
    rows2 = _synthetic_series("YBHA", "GDP", "£ million")
    assert [r["value"] for r in rows1] == [r["value"] for r in rows2]
