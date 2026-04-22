"""
ELT Pipeline DAG
================
Pattern: Extract → Load → Transform  (ELT)

Step 1  load_csv_to_staging   Read CSV with pandas and load raw data into
                               PostgreSQL  staging  schema unchanged.
Step 2  run_dbt_transform      Run dbt models to transform staging → production.
Step 3  run_dbt_tests          Run dbt schema/data tests on production models.
Step 4  run_soda_checks        Run Soda data quality checks on the result.

How to use
----------
1. Drop your CSV file into  data/raw/
2. Update the four CONFIG variables below.
3. Add/edit dbt models in  dbt/models/
4. Add/edit Soda checks in  soda/checks/production_checks.yml
5. Trigger the DAG manually from the Airflow UI.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow import DAG

# Operator import: providers-standard (Airflow 3.x) with fallback to core (Airflow 2.x)
try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.bash import BashOperator  # type: ignore[no-redef]
    from airflow.operators.python import PythonOperator  # type: ignore[no-redef]

# ─── Configuration — edit these ───────────────────────────────────────────────
CSV_FILE_PATH = "/opt/airflow/data/raw/BankChurners.csv"  # path to your CSV
TABLE_NAME = "bank"  # production table name
STAGING_SCHEMA = "staging"
PRODUCTION_SCHEMA = "production"
POSTGRES_CONN_ID = "playground_postgres"  # registered via env var
DBT_PROJECT_DIR = "/opt/airflow/dbt"
SODA_CONFIG_FILE = "/opt/airflow/soda/configuration.yml"
SODA_CHECKS_FILE = "/opt/airflow/soda/checks/production_checks.yml"
# ──────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ── Task functions ─────────────────────────────────────────────────────────────


def load_csv_to_staging(**context):
    """
    E + L  (the ELT 'Load' step):
    Read the CSV with pandas and write it as-is into the staging schema.
    The table is recreated on every run (if_exists='replace').
    """
    if not os.path.exists(CSV_FILE_PATH):
        raise FileNotFoundError(
            f"CSV not found at: {CSV_FILE_PATH}\nPlace your file in the data/raw/ directory."
        )

    df = pd.read_csv(CSV_FILE_PATH)
    print(f"Read {len(df):,} rows  |  columns: {list(df.columns)}")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    df.to_sql(
        name=TABLE_NAME,
        schema=STAGING_SCHEMA,
        con=engine,
        if_exists="replace",  # drop & recreate — change to 'append' to keep history
        index=False,
        chunksize=1_000,
        method="multi",  # faster batch inserts
    )
    print(f"Loaded {len(df):,} rows → {STAGING_SCHEMA}.{TABLE_NAME}")


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
    scan.set_scan_definition_name(f"ELT Quality — {TABLE_NAME}")
    scan.set_verbose()

    exit_code = scan.execute()
    print(scan.get_logs_text())

    if exit_code not in (0, 2):  # treat warnings as non-fatal
        raise Exception(f"Soda scan failed (exit code {exit_code})")


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="elt_pipeline",
    description="ELT: CSV → staging → dbt transform → production → Soda QC",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,  # manual trigger — Airflow 3.0 removed schedule_interval
    catchup=False,
    tags=["elt", "postgres", "dbt", "soda"],
) as dag:
    t1_load = PythonOperator(
        task_id="load_csv_to_staging",
        python_callable=load_csv_to_staging,
    )

    t2_dbt_run = BashOperator(
        task_id="run_dbt_transform",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
    )

    t3_dbt_test = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
    )

    t4_soda = PythonOperator(
        task_id="run_soda_checks",
        python_callable=run_soda_checks,
    )

    t1_load >> t2_dbt_run >> t3_dbt_test >> t4_soda
