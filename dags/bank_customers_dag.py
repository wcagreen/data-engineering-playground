import logging
from datetime import datetime, timedelta

import pandas as pd

try:
    from airflow.sdk import DAG, Param
except ImportError:
    from airflow.models.param import Param  # type: ignore[no-redef]

    from airflow import DAG


try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator  # type: ignore[no-redef]

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow import DAG

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

# Path inside the Airflow containers (./data is mounted at /opt/airflow/data)
CSV_FILE_PATH = "/opt/airflow/data/raw/BankChurners.csv"
TARGET_SCHEMA = "staging"
TARGET_TABLE = "bank_customers"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

TRANSFORM_SQL = """
DROP TABLE IF EXISTS preprod.bank_customers;

CREATE TABLE preprod.bank_customers AS
select
	clientnum,
	attrition_flag,
	case when attrition_flag = 'Attrited Customer' then true else false  end as is_attrited,
	customer_age,
	gender,
	dependent_count,
	education_level,
	marital_status,
	income_category,
	card_category,
	months_on_book,
	total_relationship_count,
	months_inactive_12_mon,
	contacts_count_12_mon,
	credit_limit,
	total_revolving_bal,
	avg_open_to_buy,
	total_amt_chng_q4_q1,
	total_trans_amt,
	total_trans_ct,
	total_ct_chng_q4_q1,
	avg_utilization_ratio,
    DATE '{{ params.publish_date or ds }}' as published_date
from
	staging.bank_customers;"""


PUBLISH_SQL = """
create table if not exists production.bank_customers (like preprod.bank_customers including all);

begin;

delete from production.bank_customers
where published_date = DATE '{{ params.publish_date or ds }}';

insert into production.bank_customers
select * from preprod.bank_customers
where published_date = DATE '{{ params.publish_date or ds }}';

commit;
"""


ROW_COUNT_CHECK = "SELECT COUNT(*) FROM {schema}.{table} HAVING COUNT(*) = 0;"
NULL_QUERY_TEMPLATE = (
    "SELECT COUNT(*) FROM {schema}.{table} WHERE {column} IS NULL HAVING COUNT(*) > 0;"
)
DUPLICATE_KEYS_TEMPLATE = """SELECT COUNT(*) - COUNT(DISTINCT {column}) FROM {schema}.{table} HAVING COUNT(*) != COUNT(DISTINCT {column});"""

MIN_TEMPLATE = "SELECT MIN({column}) FROM {schema}.{table} HAVING MIN({column}) < {value};"


def notify_on_failure(context):
    """Callback to notify on task failure."""
    log.error(
        "DAG FAILURE | dag_id=%s | task_id=%s | run_id=%s | error=%s",
        context.get("dag").dag_id,
        context.get("task_instance").task_id,
        context.get("run_id"),
        context.get("exception"),
    )


def infer_and_load_csv(csv_path: str, conn_id: str, schema: str, table: str) -> None:
    """Read a CSV, let pandas infer the schema, and load into Postgres."""
    engine = PostgresHook(postgres_conn_id=conn_id).get_sqlalchemy_engine()

    df = pd.read_csv(csv_path)
    log.info("Normalising column names to lowercase with underscores...")
    # Normalise column names: lowercase + underscores (mirrors BigQuery behaviour)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    df.to_sql(
        name=table,
        schema=schema,
        con=engine,
        if_exists="replace",  # drop & recreate — change to "append" to keep existing rows
        index=False,
        method="multi",  # faster bulk insert
        chunksize=1000,
    )
    log.info("Data loaded into staging table successfully.")


def validate_staging_table(conn_id: str, schema: str, table: str) -> None:
    """Run some basic checks on the loaded data."""

    failures = []
    engine = PostgresHook(postgres_conn_id=conn_id)

    checks = {
        "row_count": ROW_COUNT_CHECK.format(schema=schema, table=table),
        "nulls_in_clientnum": NULL_QUERY_TEMPLATE.format(
            schema=schema, table=table, column="clientnum"
        ),
        "nulls_in_attrition_flag": NULL_QUERY_TEMPLATE.format(
            schema=schema, table=table, column="attrition_flag"
        ),
        "duplicate_clientnum": DUPLICATE_KEYS_TEMPLATE.format(
            schema=schema, table=table, column="clientnum"
        ),
        "negative_credit_limit": MIN_TEMPLATE.format(
            schema=schema, table=table, column="credit_limit", value=0
        ),
        "negative_total_trans_amt": MIN_TEMPLATE.format(
            schema=schema, table=table, column="total_trans_amt", value=0
        ),
    }

    for check_name, query in checks.items():
        log.info(f"Running validation check: {check_name}")
        result = engine.get_records(sql=query)
        if result:
            log.error(f"Validation check failed: {check_name} - Result: {result}")
            failures.append((check_name, result))
        else:
            log.info(f"Validation check passed: {check_name}")

    if failures:
        raise ValueError(f"Data validation failed for checks: {failures}")


def validate_transformed_data(conn_id: str, schema: str, table: str) -> None:
    """Run Basic checks on the transformed data in preprod."""
    failures = []
    engine = PostgresHook(postgres_conn_id=conn_id)
    checks = {
        "row_count": ROW_COUNT_CHECK.format(schema=schema, table=table),
        "nulls_in_clientnum": NULL_QUERY_TEMPLATE.format(
            schema=schema, table=table, column="clientnum"
        ),
        "nulls_in_attrition_flag": NULL_QUERY_TEMPLATE.format(
            schema=schema, table=table, column="attrition_flag"
        ),
        "duplicate_clientnum": DUPLICATE_KEYS_TEMPLATE.format(
            schema=schema, table=table, column="clientnum"
        ),
        "negative_credit_limit": MIN_TEMPLATE.format(
            schema=schema, table=table, column="credit_limit", value=0
        ),
        "negative_total_trans_amt": MIN_TEMPLATE.format(
            schema=schema, table=table, column="total_trans_amt", value=0
        ),
    }
    for check_name, query in checks.items():
        log.info(f"Running validation check: {check_name}")
        result = engine.get_records(sql=query)
        if result:
            log.error(f"Validation check failed: {check_name} - Result: {result}")
            failures.append((check_name, result))
        else:
            log.info(f"Validation check passed: {check_name}")

    if failures:
        raise ValueError(f"Data validation failed for checks: {failures}")


with DAG(
    dag_id="bank_customers_dag",
    schedule=None,
    start_date=datetime(2026, 4, 23),
    default_args=default_args,
    description="ETL: infer schema from CSV -> load to staging -> validate -> transform -> production",
    catchup=False,
    tags=["bank", "customers", "postgres", "sql"],
    params={"publish_date": Param(default=None, type=["null", "string"], format="date")},  # type: ignore[no-redef]
    on_failure_callback=notify_on_failure,
) as dag:
    load_data = PythonOperator(
        task_id="load_customers_into_staging",
        python_callable=infer_and_load_csv,
        op_kwargs={
            "csv_path": CSV_FILE_PATH,
            "conn_id": "playground_postgres",
            "schema": TARGET_SCHEMA,
            "table": TARGET_TABLE,
        },
    )

    validate_load = PythonOperator(
        task_id="validate_loaded_data",
        python_callable=validate_staging_table,
        op_kwargs={
            "conn_id": "playground_postgres",
            "schema": TARGET_SCHEMA,
            "table": TARGET_TABLE,
        },
    )

    transform_data = SQLExecuteQueryOperator(
        task_id="transform_data",
        conn_id="playground_postgres",
        sql=TRANSFORM_SQL,
    )

    validate_transformed = PythonOperator(
        task_id="validate_transformed_data",
        python_callable=validate_transformed_data,
        op_kwargs={
            "conn_id": "playground_postgres",
            "schema": "preprod",
            "table": TARGET_TABLE,
        },
    )

    publish_transformed = SQLExecuteQueryOperator(
        task_id="publish_transformed_data",
        conn_id="playground_postgres",
        sql=PUBLISH_SQL,
    )

    load_data >> validate_load >> transform_data >> validate_transformed >> publish_transformed  # type: ignore[no-redef]
