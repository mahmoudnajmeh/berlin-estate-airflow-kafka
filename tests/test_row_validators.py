"""
Tests for Berlin estate ETL pipeline.
"""

import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from dags.berlin_estate_etl import _read_csv, DATA_DIR


def test_berlin_properties_file_exists():
    """Test that the Berlin properties CSV file exists in DATA_DIR."""
    
    test_file = DATA_DIR / "berlin_properties_v1.0.csv"
    assert test_file.exists(), f"File not found: {test_file}"


def test_berlin_properties_has_21_rows():
    """Test that the Berlin properties CSV has 21 rows."""
    
    test_file = DATA_DIR / "berlin_properties_v1.0.csv"
    if not test_file.exists():
        pytest.skip("Test data not found")
    
    rows = _read_csv(test_file)
    assert len(rows) == 21


def test_read_csv_returns_valid_data():
    """Test that _read_csv returns valid data."""
    
    test_file = DATA_DIR / "berlin_properties_v1.0.csv"
    if not test_file.exists():
        pytest.skip("Test data not found")
    
    rows = _read_csv(test_file)
    assert isinstance(rows, list)
    assert len(rows) == 21
    assert "property_id" in rows[0]
    assert "BER-001" in [r["property_id"] for r in rows]


def test_berlin_007_missing_address():
    """Test BER-007 has missing address."""
    
    test_file = DATA_DIR / "berlin_properties_v1.0.csv"
    if not test_file.exists():
        pytest.skip("Test data not found")
    
    rows = _read_csv(test_file)
    ber_007 = next((r for r in rows if r["property_id"] == "BER-007"), None)
    assert ber_007 is not None
    assert ber_007["address"] == ""


def test_berlin_021_invalid_price():
    """Test BER-021 has invalid price."""
    
    test_file = DATA_DIR / "berlin_properties_v1.0.csv"
    if not test_file.exists():
        pytest.skip("Test data not found")
    
    rows = _read_csv(test_file)
    ber_021 = next((r for r in rows if r["property_id"] == "BER-021"), None)
    assert ber_021 is not None
    assert ber_021["price_eur"] == "invalid_price"

def test_clean_csv_exists():
    """Test that clean CSV file is generated."""
    
    clean_dir = DATA_DIR / "cleaned"
    clean_files = list(clean_dir.glob("berlin_properties_clean_*.csv"))
    assert len(clean_files) > 0, "No clean CSV file found"


def test_clean_csv_has_no_nan():
    """Test that clean CSV has no NaN values."""
    
    clean_dir = DATA_DIR / "cleaned"
    clean_files = list(clean_dir.glob("berlin_properties_clean_*.csv"))
    if not clean_files:
        pytest.skip("No clean CSV file found")
    
    latest_clean = max(clean_files, key=lambda f: f.stat().st_mtime)
    rows = _read_csv(latest_clean)
    
    for row in rows:
        assert "nan" not in str(row.values()).lower()
        assert "null" not in str(row.values()).lower()


def test_clean_csv_has_integers_for_years():
    """Test that construction_year_clean and property_age_years are integers or floats without decimals."""
    
    clean_dir = DATA_DIR / "cleaned"
    clean_files = list(clean_dir.glob("berlin_properties_clean_*.csv"))
    if not clean_files:
        pytest.skip("No clean CSV file found")
    
    latest_clean = max(clean_files, key=lambda f: f.stat().st_mtime)
    rows = _read_csv(latest_clean)
    
    for row in rows:
        if row.get("construction_year_clean"):
            val = float(row["construction_year_clean"])
            assert val.is_integer(), f"Not a whole number: {row['construction_year_clean']}"
        if row.get("property_age_years"):
            val = float(row["property_age_years"])
            assert val.is_integer(), f"Not a whole number: {row['property_age_years']}"


def test_clean_csv_has_required_fields():
    """Test that clean CSV has all required fields populated."""
    
    clean_dir = DATA_DIR / "cleaned"
    clean_files = list(clean_dir.glob("berlin_properties_clean_*.csv"))
    if not clean_files:
        pytest.skip("No clean CSV file found")
    
    latest_clean = max(clean_files, key=lambda f: f.stat().st_mtime)
    rows = _read_csv(latest_clean)
    
    required_fields = ["property_id", "address", "district", "price_eur_clean", "size_sqm"]
    
    for row in rows:
        for field in required_fields:
            assert row.get(field), f"Missing {field} in row {row.get('property_id')}"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
