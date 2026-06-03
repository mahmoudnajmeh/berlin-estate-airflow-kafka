# Berlin Estate ETL Pipeline

## Overview

A production-grade event-driven ETL pipeline for Berlin estate data using Apache Airflow, Apache Kafka, Soda Core for data governance, and PostgreSQL for storage. The pipeline consumes streaming property data from Kafka topics, performs comprehensive data quality checks, transforms and cleans the data in near real-time, then loads it into PostgreSQL while generating clean CSV outputs and quality reports.

## Author
**Mahmoud Najmeh**  
<img src="https://avatars.githubusercontent.com/u/78208459?u=c3f9c7d6b49fc9726c5ea8bce260656bcb9654b3&v=4" width="200px" style="border-radius: 50%;">

------------------------------------------------------------------------

## 📋 Table of Contents

- 🏗️ [Architecture](#architecture)
- 🧩 [DAG Task Dependencies](#dag-task-dependencies)
- 🔄 [Data Flow](#data-flow)
- 📊 [Sequence Diagram](#sequence-diagram-pipeline-run)
- 🧪 [Data Quality Rules (Soda)](#data-quality-rules-soda)
- 📁 [Project Structure](#project-structure)
- 🧰 [Testing Framework](#testing-framework)
- ⚙️ [Environment & Dependencies](#environment--dependencies)
- 🌐 [Airflow UI](#airflow-ui)
- 📦 [Results](#results)
- 🚀 [How to Run](#how-to-run)
- 📌 [Prerequisites](#prerequisites)
- 🛠️ [Installation](#installation)
- 🗄️ [Database Schema](#database-schema)
- 🐞 [Troubleshooting](#troubleshooting)
- 🤝 [Contributing](#contributing)

## Architecture

<img width="4979" height="3517" alt="Image" src="https://github.com/user-attachments/assets/c566dfec-2cfe-4209-a910-4556748b0312" />

The pipeline follows a modular ETL architecture with:
- **Source Layer**: Kafka streaming (berlin-properties topic) + CSV files
- **Streaming Layer**: Kafka 4.3.0 broker consuming JSON messages
- **Governance Layer**: Soda Core quality checks
- **Processing Layer**: Airflow DAG with sequential tasks
- **Output Layer**: PostgreSQL database and clean CSV files

## DAG Task Dependencies

<img width="4086" height="230" alt="Image" src="https://github.com/user-attachments/assets/de997d27-f8fe-451d-b531-7d2b406a64da" />

The Airflow DAG executes tasks in the following order:
1. `cleanup_old_files` - Removes previous run artifacts
2. `extract_data` - Consumes JSON messages from Kafka topic
3. `transform_data` - Cleans data and derives new fields
4. `load_to_database` - Loads to PostgreSQL
5. `save_clean_csv` - Exports final clean CSV
6. `generate_quality_report` - Creates quality metrics JSON

## Kafka Streaming Data Flow

<p align="center">
  <img src="https://github.com/user-attachments/assets/96518128-d886-4482-89c1-e5c6216a47fa" width="600" alt="Image">
</p>

**Data Sources**:
- **Batch Mode**: CSV file (21 raw records with planted issues)
- **Streaming Mode**: Kafka topic 'berlin-properties' (JSON messages)

**Extract**: 
- Batch: Reads CSV, adds metadata (`extracted_at`, `source_file`, `data_year`)
- Streaming: Kafka consumer polls for 30 seconds, extracts JSON messages

**Transform**: 
- Fills missing addresses
- Converts data types
- Filters invalid values
- Derives new fields: `price_per_sqm`, `property_age_years`, `age_category`, `district_zone`, `price_percentile_district`

**Load**: PostgreSQL `berlin_properties_cleaned` table

**Output**: Clean CSV file (`data/cleaned/`) and quality report JSON

## Sequence Diagram (Pipeline Run)

<img width="6140" height="4880" alt="Image" src="https://github.com/user-attachments/assets/a3f38b73-650d-4409-a6f8-1bba68222d88" />

The sequence diagram illustrates the complete execution flow:
- **Scheduler** triggers the DAG and coordinates task execution
- **cleanup_old_files** removes previous run artifacts from the file system
- **extract_data** reads the raw CSV and writes staged data
- **transform_data** performs cleaning and field derivation
- **load_to_database** batch inserts records into PostgreSQL
- **save_clean_csv** exports the final clean data to CSV
- **generate_quality_report** creates the quality metrics JSON report

Each task completes sequentially, with data passing from one stage to the next.

## Combined Batch + Streaming Architecture

<p align="center">
  <img src="https://github.com/user-attachments/assets/2475732b-d908-4894-9442-0da56f061d94" width="400" alt="Image">
</p>

## Kafka Message Flow Sequence

<img width="5451" height="3300" alt="Image" src="https://github.com/user-attachments/assets/491b08b7-2d4b-4afe-9e4b-fe6ae433b817" />

## Data Quality Rules (Soda)

<img width="7283" height="1326" alt="Image" src="https://github.com/user-attachments/assets/0cba5dac-c017-4eeb-8278-3d054c5761a3" />

Soda Core enforces these quality checks:
- Row count validation
- Required field checks (property_id, address)
- Duplicate detection
- District whitelist validation
- Price range validation (50,000€ - 10,000,000€)
- Size range validation (10 - 1000 sqm)
- Data freshness (listing_date < 30 days)

## Project Structure

```text
berlin-estate-etl/
├── dags/
│   └── berlin_estate_etl.py
├── data/
│   ├── raw/
│   │   ├── berlin_properties.csv
│   │   ├── berlin_properties_cleaned.csv
│   │   └── berlin_properties_v1.0.csv
│   ├── processed/
│   │   ├── ingestion_date=2026-05-26/
│   │   ├── ingestion_date=2026-05-28/
│   │   └── transformed_properties_20260528_082402.csv
│   ├── cleaned/
│   └── quality/
│       ├── pipeline_report_20260528_082405.json
│       ├── transformation_stats_20260528_082402.json
│       └── validation_results.csv
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   └── test_row_validators.py
├── scripts/
│   ├── generate_test_data.py
│   └── run_local_validation.py
├── soda/
│   ├── checks.yml
│   └── configuration.yml
├── logs/
├── htmlcov/
├── .venv/
├── .pytest_cache/
├── .env
├── .gitignore
├── airflow.cfg
├── airflow.db
├── pyproject.toml
├── uv.lock
└── README.md
```

## Testing Framework

<p align="center">
  <img src="https://github.com/user-attachments/assets/96518128-d886-4482-89c1-e5c6216a47fa" width="400" alt="Image">
</p>

The test suite validates:
- Raw file existence and row count
- CSV parsing functionality
- Specific planted issues (missing address, invalid price)
- Clean CSV generation
- No NaN/null values in output
- Integer year fields
- Required field completeness

## Environment & Dependencies

<img width="3151" height="2633" alt="Image" src="https://github.com/user-attachments/assets/fdbffa5b-efd5-4423-803f-7652ec33d221" />

- **Python**: 3.11
- **Orchestration**: Apache Airflow 3.x
- **Database**: PostgreSQL 17
- **Package Manager**: UV
- **Key Libraries**: pandas, psycopg2-binary, soda-core, pytest

## Airflow UI

<img width="1917" height="1080" alt="Image" src="https://github.com/user-attachments/assets/56567d6a-25bb-467b-9c31-c0a92421f68d" />

The Airflow web interface shows successful execution of all six tasks with their durations and states.

## Results

- **Input**: 21 raw records with 7 planted quality issues
- **Output**: 357 cleaned records in PostgreSQL (from multiple test runs)
- **Clean CSV**: `data/cleaned/berlin_properties_clean_*.csv`
- **Quality Report**: `data/quality/pipeline_report_*.json`

## Kafka Setup

```bash
# Download Kafka 4.3.0
wget https://downloads.apache.org/kafka/4.3.0/kafka_2.13-4.3.0.tgz
tar -xzf kafka_2.13-4.3.0.tgz
cd kafka_2.13-4.3.0

# Start Kafka broker
bin/kafka-server-start.sh config/broker.properties

# Create topic
bin/kafka-topics.sh --create --topic berlin-properties --bootstrap-server localhost:9092

# Send test message
echo '{"property_id":"BER-001",...}' | bin/kafka-console-producer.sh --topic berlin-properties --bootstrap-server localhost:9092
```

## How to Run

### Batch Mode (CSV)

```bash
# Set up environment
export PROJECT_ROOT=/home/d-i-student/berlin-estate-etl

# Generate test data
python scripts/generate_test_data.py

# Run local validation (without Airflow)
python scripts/run_local_validation.py

# Start Airflow
airflow standalone

# Trigger DAG
airflow dags trigger berlin_estate_etl

# Stop Airflow
pkill -f "airflow"
```

### Streaming Mode (Kafka)

```bash
# Terminal 1: Start Kafka
cd ~/berlin-estate-etl/kafka_2.13-4.3.0
bin/kafka-server-start.sh config/broker.properties
```

```bash
# Terminal 2: Start Airflow
export PROJECT_ROOT=/home/d-i-student/berlin-estate-etl
airflow standalone
```

```bash
# Terminal 3: Send a message to Kafka
cd ~/berlin-estate-etl/kafka_2.13-4.3.0

echo '{"property_id":"BER-001","address":"Unter den Linden 1","district":"Mitte","property_type":"Apartment","size_sqm":"85","rooms":"3","price_eur":"650000","construction_year":"2019","energy_class":"A+","listing_date":"2025-01-15","seller_tax_id":"DE123456789","seller_name":"Central Berlin Realty"}' | bin/kafka-console-producer.sh --topic berlin-properties --bootstrap-server localhost:9092
```

```bash
# Trigger DAG
airflow dags trigger berlin_estate_etl
```

### Stop All Services

```bash
# Stop Airflow
pkill -f "airflow"

# Stop Kafka
# Press Ctrl+C in the Kafka terminal

# Stop PostgreSQL (if needed)
sudo systemctl stop postgresql
```

## Prerequisites

- Python 3.11
- PostgreSQL 17
- UV package manager
- Airflow 3.x

## Installation

```bash
# Clone the repository
git clone https://github.com/mahmoudnajmeh/berlin-estate-etl-airflow.git
cd berlin-estate-etl

# UV creates virtual environment automatically during sync
uv sync

# Activate the environment
source .venv/bin/activate

# Set up PostgreSQL
sudo systemctl start postgresql
sudo -u postgres psql -c "CREATE DATABASE berlin_estate;"

# Configure Airflow
export AIRFLOW_HOME=$(pwd)
airflow db migrate
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

## Database Schema

The cleaned data is stored in `berlin_properties_cleaned` table:

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| property_id | VARCHAR(50) | Property identifier |
| address | TEXT | Street address |
| district | VARCHAR(100) | Berlin district |
| property_type | VARCHAR(50) | Apartment/House/Commercial |
| size_sqm | FLOAT | Size in square meters |
| price_eur_clean | FLOAT | Cleaned price |
| construction_year_clean | VARCHAR(10) | Construction year |
| price_per_sqm | FLOAT | Price per square meter |
| property_age_years | VARCHAR(10) | Age in years |
| age_category | VARCHAR(50) | New/Recent/Moderate/Historic |
| district_zone | VARCHAR(50) | Central/West/East/South |

## Troubleshooting

### PostgreSQL connection issues

```bash
sudo systemctl status postgresql
psql -h localhost -U postgres -d berlin_estate -c "SELECT 1"
```

### Soda quality checks failing

```bash
soda scan -c soda/configuration.yml -d berlin_estate soda/checks.yml
```

### Clean CSV not generated

Check `data/cleaned/` directory and Airflow task logs:

```bash
airflow tasks logs berlin_estate_etl save_clean_csv <run_id>
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Run tests:

```bash
pytest tests/ -v
```

4. Submit a pull request
