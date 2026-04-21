"""
ETL Pipeline DAG
================
Pattern: Extract → Transform → Load  (ETL)

Step 1  extract          Read CSV into a pandas DataFrame; push to XCom.
Step 2  transform        Apply Python-based transformations; push result to XCom.
Step 3  load             Write the transformed DataFrame into PostgreSQL production.
Step 4  run_soda_checks  Run Soda data quality checks on the loaded table.

How to use
----------
1. Drop your CSV file into  data/raw/
2. Update the four CONFIG variables below.
3. Add your own transformation logic inside the  transform()  function.
4. Add/edit Soda checks in  soda/checks/production_checks.yml
5. Trigger the DAG manually from the Airflow UI.

XCom note
---------
DataFrames are serialised to JSON for XCom transport.  For very large files
(> a few hundred MB) consider writing intermediate results to a file or
Postgres temp table instead of XCom.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Airflow 3.0: PythonOperator moved to apache-airflow-providers-standard
from airflow.providers.standard.operators.python import PythonOperator

# Resolve scripts/python/ relative to this file so the import works whether
# the DAG is loaded by Airflow inside Docker (/opt/airflow/dags/) or by
# Pylance/pytest on your local machine.
_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts" / "python"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from etl_transforms import apply_transforms  # noqa: E402

# ─── Configuration — edit these ───────────────────────────────────────────────
CSV_FILE_PATH = "/opt/airflow/data/raw/your_file.csv"  # path to your CSV
TABLE_NAME = "your_table_transformed"  # production table name
PRODUCTION_SCHEMA = "production"
POSTGRES_CONN_ID = "playground_postgres"  # registered via env var
SODA_CONFIG_FILE = "/opt/airflow/soda/configuration.yml"
SODA_CHECKS_FILE = "/opt/airflow/soda/checks/production_checks.yml"
# ──────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ── Task functions ─────────────────────────────────────────────────────────────


def extract(**context):
    """
    E (Extract):
    Read the CSV file with pandas and push the raw data to XCom as JSON.
    """
    if not os.path.exists(CSV_FILE_PATH):
        raise FileNotFoundError(
            f"CSV not found at: {CSV_FILE_PATH}\nPlace your file in the data/raw/ directory."
        )

    df = pd.read_csv(CSV_FILE_PATH)
    print(f"Extracted {len(df):,} rows  |  columns: {list(df.columns)}")
    print(df.dtypes.to_string())

    context["ti"].xcom_push(key="raw_data", value=df.to_json(orient="records"))


def transform(**context):
    """
    T (Transform):
    Pull raw JSON from XCom, call apply_transforms() from etl_transforms.py,
    push the cleaned DataFrame back to XCom.

    Add your own transformation logic inside  scripts/python/etl_transforms.py
    — that file has no Airflow dependency and can be unit-tested locally.
    """
    raw_json = context["ti"].xcom_pull(key="raw_data", task_ids="extract")
    df = pd.read_json(raw_json, orient="records")
    print(f"Starting transform on {len(df):,} rows…")

    df_clean = apply_transforms(df, source_file=CSV_FILE_PATH)

    print(f"After transform: {len(df_clean):,} rows")
    context["ti"].xcom_push(key="transformed_data", value=df_clean.to_json(orient="records"))


def load(**context):
    """
    L (Load):
    Pull the transformed JSON from XCom and write it to PostgreSQL production.
    """
    transformed_json = context["ti"].xcom_pull(key="transformed_data", task_ids="transform")
    df = pd.read_json(transformed_json, orient="records")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    df.to_sql(
        name=TABLE_NAME,
        schema=PRODUCTION_SCHEMA,
        con=engine,
        if_exists="replace",  # change to 'append' to accumulate runs
        index=False,
        chunksize=1_000,
        method="multi",
    )
    print(f"Loaded {len(df):,} rows → {PRODUCTION_SCHEMA}.{TABLE_NAME}")


def run_soda_checks(**context):
    """
    Run a Soda scan against the configured checks file.
    Exit codes:  0 = all passed,  2 = warnings,  anything else = failure.
    """
    from soda.scan import Scan

    scan = Scan()
    scan.set_data_source_name("playground")
    scan.add_configuration_yaml_file(SODA_CONFIG_FILE)
    scan.add_sodacl_yaml_files(SODA_CHECKS_FILE)
    scan.set_scan_definition_name(f"ETL Quality — {TABLE_NAME}")
    scan.set_verbose()

    exit_code = scan.execute()
    print(scan.get_logs_text())

    if exit_code not in (0, 2):
        raise Exception(f"Soda scan failed (exit code {exit_code})")


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="etl_pipeline",
    description="ETL: CSV → Python transform → production → Soda QC",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,  # manual trigger — Airflow 3.0 removed schedule_interval
    catchup=False,
    tags=["etl", "postgres", "python", "soda"],
) as dag:
    t1_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    t2_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    t3_load = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    t4_soda = PythonOperator(
        task_id="run_soda_checks",
        python_callable=run_soda_checks,
    )

    t1_extract >> t2_transform >> t3_load >> t4_soda
