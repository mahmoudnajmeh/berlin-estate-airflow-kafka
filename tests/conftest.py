"""
Pytest configuration and fixtures for Berlin estate tests.
"""

from datetime import datetime, timedelta
from pathlib import Path
import shutil
import pytest
import csv


TEST_DATA_DIR = Path(__file__).parent / "test_data"
TEST_DATA_DIR.mkdir(exist_ok=True)


@pytest.fixture(autouse=True)
def setup_test_environment():
    """
    Setup and teardown test environment.
    
    Creates necessary directories before tests and cleans up after.
    """
    
    raw_dir = TEST_DATA_DIR / "raw"
    processed_dir = TEST_DATA_DIR / "processed"
    quality_dir = TEST_DATA_DIR / "quality"
    governance_dir = TEST_DATA_DIR / "governance"
    
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)
    quality_dir.mkdir(parents=True, exist_ok=True)
    governance_dir.mkdir(parents=True, exist_ok=True)
    
    yield
    
    if TEST_DATA_DIR.exists():
        shutil.rmtree(TEST_DATA_DIR)


@pytest.fixture
def sample_property_row():
    """
    Sample valid property row for testing.
    
    Returns a complete, valid property record that should pass
    all validation rules.
    """
    
    return {
        "property_id": "BER-TEST-001",
        "address": "Unter den Linden 1",
        "district": "Mitte",
        "property_type": "Apartment",
        "size_sqm": "85",
        "rooms": "3",
        "price_eur": "650000",
        "construction_year": "2019",
        "energy_class": "A+",
        "listing_date": "2025-01-15",
        "seller_tax_id": "DE123456789",
        "seller_name": "Central Berlin Realty"
    }


@pytest.fixture
def sample_test_csv(tmp_path):
    """
    Create a temporary CSV file for testing.
    
    Returns path to a CSV file containing sample test data.
    """
    
    test_data = [
        {
            "property_id": "TEST-001",
            "address": "Test Street 1",
            "district": "Mitte",
            "property_type": "Apartment",
            "size_sqm": "85",
            "rooms": "3",
            "price_eur": "650000",
            "construction_year": "2019",
            "energy_class": "A+",
            "listing_date": "2025-01-15",
            "seller_tax_id": "DE123456789",
            "seller_name": "Test Seller"
        },
        {
            "property_id": "TEST-002",
            "address": "Test Street 2",
            "district": "Charlottenburg",
            "property_type": "House",
            "size_sqm": "120",
            "rooms": "5",
            "price_eur": "890000",
            "construction_year": "1950",
            "energy_class": "E",
            "listing_date": "2025-01-20",
            "seller_tax_id": "DE987654321",
            "seller_name": "Another Seller"
        }
    ]
    
    csv_file = tmp_path / "test_properties.csv"
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=test_data[0].keys())
        writer.writeheader()
        writer.writerows(test_data)
    
    return csv_file