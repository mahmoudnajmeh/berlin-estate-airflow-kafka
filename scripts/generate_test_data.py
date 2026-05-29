"""
Generate synthetic Berlin estate test data with planted issues.

This script creates a CSV file with 21 rows containing various
data quality issues for testing the validation pipeline.
"""

from datetime import datetime, timedelta
from pathlib import Path
import csv
import random


DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)


BERLIN_DISTRICTS = [
    "Mitte", "Charlottenburg", "Kreuzberg", "Neukölln",
    "Friedrichshain", "Prenzlauer Berg", "Tempelhof", "Moabit"
]


STREETS = [
    "Unter den Linden", "Kurfürstendamm", "Friedrichstraße", "Boxhagener Str",
    "Schönhauser Allee", "Maybachufer", "Karl-Marx-Straße", "Skalitzer Str",
    "Revaler Str", "Stresemannstr", "Danckelmannstr", "Boddinstr",
    "Simon-Dach-Str", "Warschauer Str", "Richardstr", "Oranienstr",
    "Kottbusser Damm", "Schlesische Str", "Turmstr", "Beusselstr"
]


PROPERTY_TYPES = ["Apartment", "House", "Commercial"]
ENERGY_CLASSES = ["A+", "A", "A-", "B", "C", "D", "E", "F", "G"]


def generate_test_data():
    """
    Generate test data with planted quality issues.
    
    Creates 21 rows with specific issues at known positions:
    - BER-006: Missing size_sqm and seller_name
    - BER-007: Missing address
    - BER-009: Invalid construction_year
    - BER-010: Missing seller_name
    - BER-015: Zero size_sqm
    - BER-017: Missing seller_tax_id
    - BER-021: Invalid price (non-numeric)
    """
    
    rows = []
    
    for i in range(1, 22):
        property_id = f"BER-{i:03d}"
        
        if i == 7:
            address = ""
        else:
            address = f"{random.choice(STREETS)} {random.randint(1, 150)}"
        
        district = random.choice(BERLIN_DISTRICTS)
        property_type = random.choice(PROPERTY_TYPES)
        
        if i == 6:
            size_sqm = ""
            rooms = "2"
        elif i == 15:
            size_sqm = "0"
            rooms = "2"
        else:
            size_sqm = str(random.randint(30, 200))
            rooms = str(round(int(size_sqm) / 30, 1))
        
        if i == 21:
            price_eur = "invalid_price"
        else:
            price_eur = str(random.randint(200000, 1500000))
        
        if i == 9:
            construction_year = "invalid_year"
        else:
            construction_year = str(random.randint(1880, 2024))
        
        energy_class = random.choice(ENERGY_CLASSES)
        listing_date = (datetime.now() - timedelta(days=random.randint(0, 60))).strftime("%Y-%m-%d")
        
        if i == 17:
            seller_tax_id = ""
        else:
            seller_tax_id = f"DE{random.randint(100000000, 999999999)}"
        
        if i in [6, 10]:
            seller_name = ""
        else:
            seller_name = f"{district} Realty {random.randint(1, 99)}"
        
        row = {
            "property_id": property_id,
            "address": address,
            "district": district,
            "property_type": property_type,
            "size_sqm": size_sqm,
            "rooms": rooms,
            "price_eur": price_eur,
            "construction_year": construction_year,
            "energy_class": energy_class,
            "listing_date": listing_date,
            "seller_tax_id": seller_tax_id,
            "seller_name": seller_name,
        }
        
        rows.append(row)
    
    output_file = DATA_DIR / "berlin_properties.csv"
    
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"Generated {len(rows)} rows at {output_file}")
    print("\nPlanted issues:")
    print("  - BER-006: Missing size_sqm and seller_name")
    print("  - BER-007: Missing address")
    print("  - BER-009: Invalid construction_year")
    print("  - BER-010: Missing seller_name")
    print("  - BER-015: Zero size_sqm")
    print("  - BER-017: Missing seller_tax_id")
    print("  - BER-021: Invalid price (non-numeric)")


if __name__ == "__main__":
    generate_test_data()