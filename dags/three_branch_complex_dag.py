import logging
import os
from datetime import datetime, timedelta

try:
    from airflow.providers.standard.sensors.python import PythonSensor
except ImportError:
    from airflow.sensors.python import PythonSensor  # type: ignore[no-redef]

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator  # type: ignore[no-redef]

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator  # type: ignore[no-redef]


from airflow import DAG

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}


def is_file_ready(file_path: str) -> bool:
    exists = os.path.exists(file_path)
    if exists:
        log.info("File is available: %s", file_path)
    else:
        log.info("Waiting for file: %s", file_path)
    return exists


with DAG(
    "mortage_loans_dag",
    default_args=default_args,
    description="A DAG to process mortgage loans data",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
):
    # ── Sensors ────────────────────────────────────────────────────────────────
    wait_for_loan_file = PythonSensor(
        task_id="wait_for_loan_file",
        python_callable=is_file_ready,
        op_kwargs={"file_path": "/opt/airflow/data/loans_application.csv"},
        poke_interval=60,
        timeout=120,
        mode="reschedule",
        soft_fail=False,
    )

    wait_for_borrow_file = PythonSensor(
        task_id="wait_for_borrow_file",
        python_callable=is_file_ready,
        op_kwargs={"file_path": "/opt/airflow/data/borrowers.csv"},
        poke_interval=60,
        timeout=120,
        mode="reschedule",
        soft_fail=False,
    )

    wait_for_decision_file = PythonSensor(
        task_id="wait_for_decision_file",
        python_callable=is_file_ready,
        op_kwargs={"file_path": "/opt/airflow/data/decisions.csv"},
        poke_interval=60,
        timeout=120,
        mode="reschedule",
        soft_fail=True,
    )

    # ── Loaders ───────────────────────────────────────────────────────────────

    load_loans_application = PythonOperator(
        task_id="load_loans_application",
        python_callable=lambda: log.info("Loading loans application data..."),
    )

    load_borrowsers = PythonOperator(
        task_id="load_borrowers",
        python_callable=lambda: log.info("Loading borrowers data..."),
    )

    load_decisions = PythonOperator(
        task_id="load_decisions",
        python_callable=lambda: log.info("Loading decisions data..."),
    )

    # ── Validators ─────────────────────────────────────────────────────────────
    validate_loans_application = PythonOperator(
        task_id="validate_loans_application",
        python_callable=lambda: log.info("Validating loans application data..."),
    )
    validate_borrowsers = PythonOperator(
        task_id="validate_borrowers",
        python_callable=lambda: log.info("Validating borrowers data..."),
    )
    validate_decisions = PythonOperator(
        task_id="validate_decisions",
        python_callable=lambda: log.info("Validating decisions data..."),
    )

    # ── Transformation  ─────────────────────────────────────────────────────────────

    daily_snapshot_transformation = PythonOperator(
        task_id="daily_snapshot_transformation",
        python_callable=lambda: log.info("Transforming data to create daily snapshot..."),
    )

    decisions_summary_transformation = PythonOperator(
        task_id="decisions_summary_transformation",
        python_callable=lambda: log.info("Transforming data to create decisions summary..."),
    )

    # ── Validate Transformations ─────────────────────────────────────────────────────────────

    validate_daily_snapshot = PythonOperator(
        task_id="validate_daily_snapshot",
        python_callable=lambda: log.info("Validating daily snapshot..."),
    )

    validate_decisions_summary = PythonOperator(
        task_id="validate_decisions_summary",
        python_callable=lambda: log.info("Validating decisions summary..."),
    )

    # ── Publishers  ─────────────────────────────────────────────────────────────

    publish_daily_snapshot = PythonOperator(
        task_id="publish_daily_snapshot",
        python_callable=lambda: log.info("Publishing daily snapshot..."),
    )
    publish_decisions_summary = PythonOperator(
        task_id="publish_decisions_summary",
        python_callable=lambda: log.info("Publishing decisions summary..."),
    )

    # ── Notifications  ─────────────────────────────────────────────────────────────

    error_notification = EmptyOperator(task_id="error_notification", trigger_rule="one_failed")

    final_summary_notification = PythonOperator(
        task_id="final_summary_notification",
        python_callable=lambda: log.info("Sending final summary notification..."),
        trigger_rule="all_done",
    )

    # ── Task Dependencies ─────────────────────────────────────────────────────────────
    (
        wait_for_decision_file
        >> load_decisions
        >> validate_decisions
        >> decisions_summary_transformation
        >> validate_decisions_summary
        >> publish_decisions_summary
    )  # type: ignore[no-redef]
    wait_for_loan_file >> load_loans_application >> validate_loans_application  # type: ignore[no-redef]
    wait_for_borrow_file >> load_borrowsers >> validate_borrowsers  # type: ignore[no-redef]
    (
        [validate_loans_application, validate_borrowsers]
        >> daily_snapshot_transformation
        >> validate_daily_snapshot
        >> publish_daily_snapshot
    )  # type: ignore[no-redef]
    [  # type: ignore[no-redef]
        validate_loans_application,
        validate_borrowsers,
        daily_snapshot_transformation,
        validate_daily_snapshot,
        publish_daily_snapshot,
        validate_decisions,
        decisions_summary_transformation,
        validate_decisions_summary,
        publish_decisions_summary,
    ] >> error_notification
    [publish_daily_snapshot, publish_decisions_summary] >> final_summary_notification  # type: ignore[no-redef]
