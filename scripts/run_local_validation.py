"""
Run validation locally without Airflow for quick testing.

This script reads the Berlin properties CSV, runs row-level validations,
and displays results without needing Airflow or a database.
"""

import sys
import re
from pathlib import Path
from collections import Counter

sys.path.insert(0, str(Path(__file__).parent.parent))

from dags.berlin_estate_etl import _read_csv, _write_csv


DATA_DIR = Path(__file__).parent.parent / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
OUTPUT_DIR = DATA_DIR / "quality"


REQUIRED_FIELDS = ["property_id", "address", "district", "price_eur", "size_sqm"]


BERLIN_DISTRICTS = {
    "Mitte", "Charlottenburg", "Kreuzberg", "Neukölln",
    "Friedrichshain", "Prenzlauer Berg", "Tempelhof", "Moabit"
}


def validate_required(row):
    """
    Return one reason string per missing required field.
    """
    
    reasons = []
    for field in REQUIRED_FIELDS:
        if row.get(field, '') in (None, ""):
            reasons.append(f"missing:{field}")
    return reasons


def validate_district(row):
    """
    Return ['bad_district'] if district is not a valid Berlin district.
    """
    
    district = row.get('district', '').strip()
    if district and district not in BERLIN_DISTRICTS:
        return ["bad_district"]
    return []


def validate_price(row):
    """
    Return ['bad_price'] if price is not a valid number or out of range.
    """
    
    price = row.get('price_eur', '').strip()
    if price:
        try:
            price_num = int(price.replace(',', ''))
            if price_num < 50000 or price_num > 10000000:
                return ["bad_price_out_of_range"]
        except (ValueError, AttributeError):
            return ["bad_price_not_numeric"]
    return []


def validate_size_sqm(row):
    """
    Return ['bad_size'] if size is unrealistic for Berlin.
    """
    
    size = row.get('size_sqm', '').strip()
    if size:
        try:
            size_num = float(size)
            if size_num < 10 or size_num > 1000:
                return ["bad_size_out_of_range"]
        except (ValueError, AttributeError):
            return ["bad_size_not_numeric"]
    return []


def validate_row(row):
    """
    Aggregate all row-level validations.
    """
    
    reasons = []
    reasons.extend(validate_required(row))
    reasons.extend(validate_district(row))
    reasons.extend(validate_price(row))
    reasons.extend(validate_size_sqm(row))
    return reasons


def validate_rows(rows):
    """
    Process all rows, separating accepted from rejected with reasons.
    """
    
    accepted = []
    rejected = []

    for row in rows:
        reasons = validate_row(row)
        if reasons:
            rejected.append({"row": row, "reasons": reasons})
        else:
            accepted.append(row)

    return accepted, rejected


def rejection_reasons_summary(rejected_entries):
    """
    Create summary count of rejection reasons.
    """
    
    summary = Counter()
    for entry in rejected_entries:
        for reason in entry["reasons"]:
            summary[reason] += 1
    return dict(summary)


def main():
    """
    Run local validation and display results.
    """
    
    print("\n" + "="*60)
    print("Berlin Estate Data Validation")
    print("="*60 + "\n")
    
    input_file = RAW_DATA_DIR / "berlin_properties.csv"
    
    if not input_file.exists():
        print(f"Error: {input_file} not found.")
        print("Run: python scripts/generate_test_data.py")
        return
    
    rows = _read_csv(input_file)
    print(f"Total rows: {len(rows)}")
    
    accepted, rejected = validate_rows(rows)
    summary = rejection_reasons_summary(rejected)
    
    print(f"\n✅ Accepted: {len(accepted)}")
    print(f"❌ Rejected: {len(rejected)}")
    
    print("\n📊 Rejection Summary:")
    for reason, count in sorted(summary.items(), key=lambda x: x[1], reverse=True):
        print(f"   {reason}: {count}")
    
    print("\n🔴 Rejected Rows (first 10):")
    for entry in rejected[:10]:
        row = entry["row"]
        reasons = entry["reasons"]
        print(f"   ID: {row.get('property_id', 'N/A')} - Issues: {', '.join(reasons)}")
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_DIR / "validation_results.csv"
    
    if rejected:
        rejected_rows = []
        for entry in rejected:
            row_copy = entry["row"].copy()
            row_copy["rejection_reasons"] = ", ".join(entry["reasons"])
            rejected_rows.append(row_copy)
        _write_csv(output_file, rejected_rows)
        print(f"\n💾 Rejected rows saved to: {output_file}")
    
    print("\n" + "="*60)
    print("Validation Complete")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()