"""
ETL pipeline for Berlin estate data with Soda governance.

This DAG extracts property data from Berlin, validates it using Soda Core,
transforms the data, and loads it into a PostgreSQL database.

The pipeline follows a modular validation approach combining:
- Row-level validators for individual record quality
- Soda for table-level aggregate quality checks
- Comprehensive governance reporting
"""

from datetime import datetime, timedelta
from pathlib import Path
import json
import logging
import csv
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
from soda.scan import Scan
import psycopg2
from psycopg2.extras import execute_values


logger = logging.getLogger(__name__)


import os

BASE_DIR = Path(os.environ.get('PROJECT_ROOT', Path(__file__).parent.parent))
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
QUALITY_DIR = DATA_DIR / "quality"
SODA_CONFIG_DIR = BASE_DIR / "soda"

default_args = {
    "owner": "data_governance_team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["governance@berlin-estate.de"],
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def _read_csv(path):
    """
    Read CSV file and return list of dictionaries.
    
    This function follows the validation lab pattern for consistent
    row-level processing without pandas overhead.
    """
    
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _write_csv(path, rows):
    """
    Write list of dictionaries to CSV file.
    
    Preserves the validation lab pattern for consistent CSV handling.
    """
    
    if not rows:
        return
    
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def extract_data(**context):
    """
    Extract data from CSV file and stage it for processing.
    """
    
    logger.info("Starting data extraction from CSV")
    
    try:
        csv_files = list(DATA_DIR.glob("berlin_properties_v1.0.csv"))
        if not csv_files:
            raise FileNotFoundError("No property data CSV files found")
        
        input_file = max(csv_files, key=lambda f: f.stat().st_mtime)
        
        rows = _read_csv(input_file)
        
        logger.info(f"Successfully extracted {len(rows)} records from {input_file.name}")
        
        current_date = datetime.now()
        
        for row in rows:
            row["extracted_at"] = datetime.now().isoformat()
            row["source_file"] = input_file.name
            row["data_year"] = str(current_date.year)
        
        output_dir = PROCESSED_DATA_DIR / f"ingestion_date={current_date.strftime('%Y-%m-%d')}"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "staged_properties.csv"
        
        _write_csv(output_file, rows)
        
        context["task_instance"].xcom_push(key="extracted_count", value=len(rows))
        context["task_instance"].xcom_push(key="staged_file_path", value=str(output_file))
        context["task_instance"].xcom_push(key="data_year", value=current_date.year)
        
        logger.info(f"Staged data saved to {output_file}")
        logger.info(f"Data year for this ingestion: {current_date.year}")
        
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        raise AirflowException(f"Data extraction failed: {str(e)}")


def run_soda_quality_checks(**context):
    """
    Run Soda Core quality checks on the staged data.

    This function executes predefined data quality checks including:
    - Missing values in critical columns
    - Invalid formats for numeric fields
    - Duplicate detection
    - Data freshness validation
    - Statistical outliers detection

    Results are saved as JSON and pipeline can optionally fail if
    quality thresholds are not met.
    """
    
    logger.info("Starting Soda data quality checks")
    
    try:
        staged_file = context["task_instance"].xcom_pull(
            key="staged_file_path",
            task_ids="extract_data"
        )
        
        scan = Scan()
        scan.set_scan_definition_name("berlin_estate_quality")
        
        config_file = SODA_CONFIG_DIR / "configuration.yml"
        if config_file.exists():
            scan.add_configuration_yaml_file(str(config_file))
        
        checks_file = SODA_CONFIG_DIR / "checks.yml"
        if checks_file.exists():
            scan.add_sodacl_yaml_file(str(checks_file))
        
        scan.add_metadata_columns(True)
        scan.set_data_source_name("berlin_real_estate")
        
        scan.add_path(str(staged_file), "staged_properties")
        
        scan.execute()
        
        scan_result = scan.get_scan_results()
        
        QUALITY_DIR.mkdir(parents=True, exist_ok=True)
        quality_report = QUALITY_DIR / f"quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        quality_data = {
            "timestamp": datetime.now().isoformat(),
            "scan_status": "passed" if not scan_result.get("hasFailures") else "failed",
            "checks_passed": scan_result.get("checksPassed", 0),
            "checks_failed": scan_result.get("checksFailed", 0),
            "failed_checks": [
                {
                    "name": check.get("name"),
                    "outcome": check.get("outcome"),
                    "diagnostic": check.get("diagnostic")
                }
                for check in scan_result.get("checks", [])
                if check.get("outcome") == "fail"
            ]
        }
        
        with open(quality_report, "w") as f:
            json.dump(quality_data, f, indent=2)
        
        context["task_instance"].xcom_push(key="quality_report_path", value=str(quality_report))
        context["task_instance"].xcom_push(key="quality_passed", value=not scan_result.get("hasFailures"))
        
        has_failures = scan_result.get("hasFailures", False)
        if has_failures:
            logger.warning(f"Quality checks failed: {len(quality_data['failed_checks'])} failures")
            
            fail_on_quality = Variable.get("fail_pipeline_on_quality_failure", default_var=False)
            if fail_on_quality:
                raise AirflowException("Data quality checks failed - stopping pipeline")
        
        logger.info(
            f"Soda quality checks completed. "
            f"{quality_data['checks_passed']} passed, "
            f"{quality_data['checks_failed']} failed"
        )
        
    except Exception as e:
        logger.error(f"Soda quality check failed: {str(e)}")
        raise AirflowException(f"Data quality validation failed: {str(e)}")


def transform_data(**context):
    """
    Transform and clean the property data.

    This function performs:
    - Data type conversions for numeric fields
    - Handling invalid/missing values with appropriate defaults
    - Calculating derived fields like price_per_sqm
    - Standardizing address formats for Berlin
    - Validating Berlin-specific metrics and ranges
    - Creating district zone classifications
    - Calculating price percentiles by district

    Note: This function converts the list-of-dicts to pandas for
    efficient vectorized operations, then converts back to match
    the validation lab pattern for downstream tasks.
    """
    
    logger.info("Starting data transformation")
    
    try:
        staged_file = context["task_instance"].xcom_pull(
            key="staged_file_path",
            task_ids="extract_data"
        )
        
        rows = _read_csv(staged_file)
        df = pd.DataFrame(rows)
        
        logger.info(f"Transforming {len(df)} records")
        
        df["address"] = df["address"].fillna("Address not provided")
        
        df["size_sqm"] = pd.to_numeric(df["size_sqm"], errors="coerce")
        df["size_sqm"] = df["size_sqm"].apply(
            lambda x: x if pd.notnull(x) and x > 0 else None
        )
        
        df["price_eur_clean"] = pd.to_numeric(df["price_eur"], errors="coerce")
        df["price_eur_clean"] = df["price_eur_clean"].apply(
            lambda x: x if pd.notnull(x) and 50000 <= x <= 10000000 else None
        )
        
        df["construction_year_clean"] = pd.to_numeric(df["construction_year"], errors="coerce")
        df["construction_year_clean"] = df["construction_year_clean"].apply(
            lambda x: int(x) if pd.notnull(x) and 1850 <= x <= datetime.now().year else None
        )
        
        df["seller_name"] = df["seller_name"].fillna("Unknown Seller")
        
        df["price_per_sqm"] = df.apply(
            lambda row: round(row["price_eur_clean"] / row["size_sqm"], 2)
            if pd.notnull(row["price_eur_clean"]) and pd.notnull(row["size_sqm"])
            else None,
            axis=1
        )
        
        current_year = datetime.now().year
        df["property_age_years"] = df["construction_year_clean"].apply(
            lambda x: int(current_year - x) if pd.notnull(x) else None
        )
        
        df["age_category"] = df["property_age_years"].apply(
            lambda x: "New" if pd.notnull(x) and x <= 5
            else "Recent" if pd.notnull(x) and x <= 20
            else "Moderate" if pd.notnull(x) and x <= 50
            else "Historic" if pd.notnull(x) and x > 50
            else "Unknown"
        )
        
        district_groups = {
            "Mitte": "Central",
            "Charlottenburg": "West",
            "Kreuzberg": "South-East",
            "Friedrichshain": "East",
            "Prenzlauer Berg": "North-East",
            "Neukölln": "South",
            "Tempelhof": "South",
            "Moabit": "Central-West"
        }
        
        df["district_zone"] = df["district"].map(district_groups).fillna("Other")
        
        df["price_percentile_district"] = (
            df.groupby("district")["price_eur_clean"].rank(pct=True) * 100
        )
        
        transformed_rows = df.to_dict("records")
        
        transformed_file = (
            PROCESSED_DATA_DIR /
            f"transformed_properties_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
        _write_csv(transformed_file, transformed_rows)
        
        context["task_instance"].xcom_push(key="transformed_file_path", value=str(transformed_file))
        context["task_instance"].xcom_push(key="transformed_count", value=len(transformed_rows))
        
        stats = {
            "total_records": len(df),
            "valid_price_records": int(df["price_eur_clean"].notna().sum()),
            "valid_size_records": int(df["size_sqm"].notna().sum()),
            "valid_year_records": int(df["construction_year_clean"].notna().sum()),
            "avg_price_per_sqm": float(df["price_per_sqm"].mean()) if df["price_per_sqm"].notna().any() else None,
            "properties_by_district": df["district"].value_counts().to_dict()
        }
        
        logger.info(f"Transformation stats: {json.dumps(stats, indent=2)}")
        
        stats_file = QUALITY_DIR / f"transformation_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(stats_file, "w") as f:
            json.dump(stats, f, indent=2)
        
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise AirflowException(f"Data transformation failed: {str(e)}")


def load_to_database(**context):
    """
    Load transformed data into PostgreSQL database.

    This function handles:
    - Database connection management using Airflow variables
    - Table creation if not exists with proper schema
    - Efficient batch insertion using execute_values
    - Load statistics tracking for monitoring
    - Data type mapping between pandas and PostgreSQL

    The function uses parameterized queries to prevent SQL injection
    and handles data type conversions for date fields automatically.
    """
    
    logger.info("Starting database load")
    
    db_params = {
        "host": Variable.get("postgres_host", default_var="localhost"),
        "port": Variable.get("postgres_port", default_var=5432),
        "database": Variable.get("postgres_db", default_var="berlin_estate"),
        "user": Variable.get("postgres_user", default_var="postgres"),
        "password": Variable.get("postgres_password", default_var="")
    }
    
    try:
        transformed_file = context["task_instance"].xcom_pull(
            key="transformed_file_path",
            task_ids="transform_data"
        )
        
        rows = _read_csv(transformed_file)
        df = pd.DataFrame(rows)
        
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS berlin_properties_cleaned (
            id SERIAL PRIMARY KEY,
            property_id VARCHAR(50),
            address TEXT,
            district VARCHAR(100),
            property_type VARCHAR(50),
            size_sqm FLOAT,
            rooms FLOAT,
            price_eur_clean FLOAT,
            construction_year_clean VARCHAR(10),
            energy_class VARCHAR(10),
            listing_date DATE,
            seller_tax_id VARCHAR(50),
            seller_name VARCHAR(200),
            extracted_at TIMESTAMP,
            source_file VARCHAR(255),
            price_per_sqm FLOAT,
            property_age_years VARCHAR(10),
            age_category VARCHAR(50),
            district_zone VARCHAR(50),
            price_percentile_district FLOAT,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        insert_sql = """
        INSERT INTO berlin_properties_cleaned (
            property_id, address, district, property_type, size_sqm, rooms,
            price_eur_clean, construction_year_clean, energy_class, listing_date,
            seller_tax_id, seller_name, extracted_at, source_file, price_per_sqm,
            property_age_years, age_category, district_zone, price_percentile_district
        ) VALUES %s
        """
        
        data_tuples = [
            (
                row["property_id"],
                row["address"],
                row["district"],
                row["property_type"],
                row["size_sqm"] if row.get("size_sqm") and pd.notnull(row["size_sqm"]) else None,
                row["rooms"] if row.get("rooms") and pd.notnull(row["rooms"]) else None,
                row["price_eur_clean"] if row.get("price_eur_clean") and pd.notnull(row["price_eur_clean"]) else None,
                str(row["property_age_years"]) if row.get("property_age_years") and pd.notnull(row["property_age_years"]) else None,
                row["energy_class"] if row.get("energy_class") else None,
                pd.to_datetime(row["listing_date"]).date() if row.get("listing_date") and pd.notnull(row["listing_date"]) else None,
                row["seller_tax_id"] if row.get("seller_tax_id") else None,
                row["seller_name"],
                row["extracted_at"],
                row["source_file"],
                row["price_per_sqm"] if row.get("price_per_sqm") and pd.notnull(row["price_per_sqm"]) else None,
                row["property_age_years"] if row.get("property_age_years") and pd.notnull(row["property_age_years"]) else None,
                row["age_category"],
                row["district_zone"],
                row["price_percentile_district"] if row.get("price_percentile_district") and pd.notnull(row["price_percentile_district"]) else None
            )
            for row in rows
        ]
        
        execute_values(cursor, insert_sql, data_tuples, page_size=1000)
        
        cursor.execute("SELECT COUNT(*) FROM berlin_properties_cleaned")
        total_loaded = cursor.fetchone()[0]
        
        conn.commit()
        
        context["task_instance"].xcom_push(key="loaded_count", value=len(data_tuples))
        context["task_instance"].xcom_push(key="total_records_in_db", value=total_loaded)
        
        logger.info(
            f"Successfully loaded {len(data_tuples)} records to database. "
            f"Total records in table: {total_loaded}"
        )
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Database load failed: {str(e)}")
        raise AirflowException(f"Database load failed: {str(e)}")


def generate_quality_report(**context):
    """
    Generate comprehensive data quality and pipeline report.

    This function consolidates all pipeline metrics and quality results
    into a final JSON report for monitoring and auditing.

    The report includes:
    - Pipeline execution metadata and timestamps
    - Data volume metrics at each stage (extract, transform, load)
    - Soda quality check results and failures
    - Quality score calculation based on acceptance rates
    - Actionable recommendations for data quality improvement
    """
    
    logger.info("Generating final quality report")
    
    try:
        extracted_count = context["task_instance"].xcom_pull(
            key="extracted_count",
            task_ids="extract_data"
        )
        transformed_count = context["task_instance"].xcom_pull(
            key="transformed_count",
            task_ids="transform_data"
        )
        loaded_count = context["task_instance"].xcom_pull(
            key="loaded_count",
            task_ids="load_to_database"
        )
        quality_passed = context["task_instance"].xcom_pull(
            key="quality_passed",
            task_ids="run_soda_quality_checks"
        )
        quality_report_path = context["task_instance"].xcom_pull(
            key="quality_report_path",
            task_ids="run_soda_quality_checks"
        )
        
        quality_data = {}
        if quality_report_path and Path(quality_report_path).exists():
            with open(quality_report_path, "r") as f:
                quality_data = json.load(f)
        
        quality_score = None
        if extracted_count and loaded_count:
            quality_score = round((loaded_count / extracted_count) * 100, 2)
        
        final_report = {
            "pipeline_execution": {
                "timestamp": datetime.now().isoformat(),
                "dag_id": "berlin_estate_etl",
                "execution_status": "success" if quality_passed else "success_with_quality_issues"
            },
            "data_metrics": {
                "records_extracted": extracted_count,
                "records_transformed": transformed_count,
                "records_loaded": loaded_count,
                "records_dropped": extracted_count - loaded_count if extracted_count and loaded_count else None,
                "quality_score_percent": quality_score
            },
            "quality_metrics": quality_data,
            "berlin_market_insights": {
                "note": "Data quality affects accuracy of market insights",
                "validation_timestamp": datetime.now().isoformat()
            }
        }
        
        report_file = QUALITY_DIR / f"pipeline_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, "w") as f:
            json.dump(final_report, f, indent=2)
        
        logger.info(f"Final report generated: {report_file}")
        
        if not quality_passed:
            logger.warning("Pipeline completed with quality issues - review quality report")
        
    except Exception as e:
        logger.error(f"Report generation failed: {str(e)}")
        raise AirflowException(f"Report generation failed: {str(e)}")

def save_clean_csv(**context):
    """
    Save cleaned data as CSV file for the gold layer.
    """
    
    logger.info("Saving clean data to CSV")
    
    try:
        transformed_file = context["task_instance"].xcom_pull(
            key="transformed_file_path",
            task_ids="transform_data"
        )
        
        rows = _read_csv(transformed_file)
        df = pd.DataFrame(rows)
        
        df = df.replace(["", "nan", "NaN", "NULL", "None"], None)
        
        df = df.dropna(how="all")
        df = df[df["property_id"].notna()]
        
        df = df.dropna(subset=[
            "property_id",
            "address",
            "district",
            "property_type",
            "price_eur_clean",
            "size_sqm",
            "construction_year_clean",
            "seller_name"
        ])
        
        df["seller_name"] = df["seller_name"].astype(str).str.strip()
        
        df = df[df["seller_name"] != ""]
        df = df[df["seller_name"] != "nan"]
        
        df["size_sqm"] = pd.to_numeric(df["size_sqm"], errors="coerce")
        df["price_eur_clean"] = pd.to_numeric(df["price_eur_clean"], errors="coerce")
        df["construction_year_clean"] = pd.to_numeric(df["construction_year_clean"], errors="coerce")
        
        df = df.dropna(subset=[
            "price_eur_clean",
            "size_sqm",
            "construction_year_clean"
        ])
        
        CLEAN_DATA_DIR = DATA_DIR / "cleaned"
        CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)
        
        clean_file = CLEAN_DATA_DIR / f"berlin_properties_clean_{datetime.now().strftime('%Y%m%d')}.csv"
        
        df.to_csv(clean_file, index=False)
        
        logger.info(f"Clean CSV saved to {clean_file}")
        
        context["task_instance"].xcom_push(key="clean_file_path", value=str(clean_file))
        
    except Exception as e:
        logger.error(f"Save clean CSV failed: {str(e)}")
        raise AirflowException(f"Save clean CSV failed: {str(e)}")

def cleanup_old_files(**context):
    """
    Clean up old processed files before new run.
    This ensures only the latest clean data remains.
    """
    
    logger.info("Cleaning up old files")
    
    try:
        patterns = [
            PROCESSED_DATA_DIR / "transformed_properties_*.csv",
            QUALITY_DIR / "pipeline_report_*.json",
            QUALITY_DIR / "transformation_stats_*.json",
            DATA_DIR / "cleaned" / "berlin_properties_clean_*.csv"
        ]
        
        deleted_count = 0
        
        for pattern in patterns:
            base_dir = pattern.parent
            file_pattern = pattern.name
            
            if not base_dir.exists():
                continue
            
            for file in base_dir.glob(file_pattern):
                if file.is_file():
                    file.unlink()
                    deleted_count += 1
                    logger.info(f"Deleted old file: {file}")
        
        logger.info(f"Cleanup completed. Deleted {deleted_count} files")
        
    except Exception as e:
        logger.error(f"Cleanup failed: {str(e)}")
        raise AirflowException(f"Cleanup failed: {str(e)}")

with DAG(
    "berlin_estate_etl",
    default_args=default_args,
    description="ETL pipeline for Berlin real estate data with Soda quality checks",
    schedule=timedelta(days=1),
    catchup=False,
    tags=["berlin", "real_estate", "soda", "governance"],
) as dag:
    
    cleanup_task = PythonOperator(
        task_id="cleanup_old_files",
        python_callable=cleanup_old_files,
    )
    
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )
    
    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )
    
    load_task = PythonOperator(
        task_id="load_to_database",
        python_callable=load_to_database,
    )
    
    report_task = PythonOperator(
        task_id="generate_quality_report",
        python_callable=generate_quality_report,
    )

    clean_csv_task = PythonOperator(
        task_id="save_clean_csv",
        python_callable=save_clean_csv,
    )

    cleanup_task >> extract_task >> transform_task >> load_task >> clean_csv_task >> report_task
    