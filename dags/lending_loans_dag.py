import logging
import os
from datetime import datetime, timedelta
from typing import Any

from airflow.hooks.base import BaseHook  # type: ignore[no-redef]
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

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


ACCEPTED_LOANS_FILE_PATH = "/opt/airflow/data/raw/accepted_2007_to_2018Q4.csv"
REJECTED_LOANS_FILE_PATH = "/opt/airflow/data/raw/rejected_2007_to_2018Q4.csv"
POSTGRES_CONN_ID = "playground_postgres"
RUNNER_IMAGE = "airflow-playground-python-runner:latest"
# Adjust this if your compose project name differs.
DOCKER_NETWORK = "airflow-playground_default"
CONTAINER_DATA_DIR = "/opt/airflow/data"
CONTAINER_SCRIPTS_DIR = "/scripts"
HOST_DATA_DIR = os.getenv("HOST_DATA_DIR", "").strip()
HOST_SCRIPTS_DIR = os.getenv("HOST_SCRIPTS_DIR", "").strip()

_FAILED = {"failed"}
_BLOCKED = {"upstream_failed", "skipped", "none"}


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def build_db_env(conn_id: str) -> dict[str, str]:
    conn = BaseHook.get_connection(conn_id)
    return {
        "PGHOST": conn.host or "",
        "PGPORT": str(conn.port or 5432),
        "PGDATABASE": conn.schema or "",
        "PGUSER": conn.login or "",
        "PGPASSWORD": conn.password or "",
    }


def build_loader_task(
    task_id: str, script_name: str, file_path: str, table_name: str
) -> DockerOperator:
    env = build_db_env(POSTGRES_CONN_ID)
    mounts = []
    if HOST_DATA_DIR:
        mounts.append(
            Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type="bind", read_only=True)
        )
    else:
        log.warning("HOST_DATA_DIR is not set; loader tasks will use image-bundled data only")
    if HOST_SCRIPTS_DIR:
        mounts.append(
            Mount(
                source=HOST_SCRIPTS_DIR, target=CONTAINER_SCRIPTS_DIR, type="bind", read_only=True
            )
        )
    else:
        log.warning("HOST_SCRIPTS_DIR is not set; loader tasks will use image-bundled scripts only")
    command = (
        f"python /scripts/runner.py --script {script_name} "
        f"--file-path {file_path} --table-name {table_name} --schema staging"
    )
    return DockerOperator(
        task_id=task_id,
        image=RUNNER_IMAGE,
        command=command,
        environment=env,
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mounts=mounts,
        mount_tmp_dir=False,
        auto_remove="success",
        force_pull=False,
    )


def is_file_ready(file_path: str) -> bool:
    exists = os.path.exists(file_path)
    if exists:
        log.info("File is available: %s", file_path)
    else:
        log.info("Waiting for file: %s", file_path)
    return exists


def _state_of(context: dict[str, Any], task_id: str) -> str:
    dag_run = context["dag_run"]
    ti = dag_run.get_task_instance(task_id)
    return ti.state if ti is not None and ti.state is not None else "none"


def _task_outcome(task_state: str) -> str:
    """Normalize a raw Airflow state to a simple outcome."""
    if task_state == "success":
        return "success"
    if task_state in _FAILED:
        return "failed"
    if task_state in _BLOCKED:
        return "skipped"
    return "incomplete"


def _pipeline_status(stages: list[tuple[str, str]]) -> str:
    """
    stages: ordered list of (task_state, failure_label) from first to last.
    Returns the failure_label of the first failed task, 'MISSING' if skipped
    before completion, or 'PUBLISHED' if all succeeded.
    """
    for task_state, failure_label in stages:
        outcome = _task_outcome(task_state)
        if outcome == "failed":
            return failure_label
        if outcome == "skipped":
            return "MISSING"
        if outcome == "incomplete":
            return "INCOMPLETE"
    return "PUBLISHED"


def send_branch_summary(**context: dict[str, Any]) -> None:
    accepted_wait = _state_of(context, "wait_for_accepted_file")
    accepted_run = _state_of(context, "run_accepted_loader")
    rejected_wait = _state_of(context, "wait_for_rejected_file")
    rejected_run = _state_of(context, "run_rejected_loader")
    publish_accepted = _state_of(context, "publish_accepted_loans")
    publish_rejected = _state_of(context, "publish_rejected_loans")
    validate_accepted = _state_of(context, "validate_accepted_loans_staging")
    validate_rejected = _state_of(context, "validate_rejected_loans_staging")

    accepted_status = _pipeline_status(
        [
            (accepted_run, "PROCESSING_FAILED"),
            (validate_accepted, "VALIDATION_FAILED"),
            (publish_accepted, "PUBLISH_FAILED"),
        ]
    )
    rejected_status = _pipeline_status(
        [
            (rejected_run, "PROCESSING_FAILED"),
            (validate_rejected, "VALIDATION_FAILED"),
            (publish_rejected, "PUBLISH_FAILED"),
        ]
    )

    payload = {
        "dag_id": context["dag"].dag_id,  # type: ignore[no-redef]
        "run_id": context["dag_run"].run_id,  # type: ignore[no-redef]
        "execution_date": context["logical_date"].isoformat(),  # type: ignore[no-redef]
        "summary": {
            "accepted": {
                "status": accepted_status,
                "wait_task_state": accepted_wait,
                "loader_task_state": accepted_run,
                "validate_task_state": validate_accepted,
                "publish_task_state": publish_accepted,
            },
            "rejected": {
                "status": rejected_status,
                "wait_task_state": rejected_wait,
                "loader_task_state": rejected_run,
                "validate_task_state": validate_rejected,
                "publish_task_state": publish_rejected,
            },
        },
    }

    message = (
        "lending_loans_dag branch summary | "
        f"accepted={accepted_status} (wait={accepted_wait}, run={accepted_run}) | "
        f"rejected={rejected_status} (wait={rejected_wait}, run={rejected_run})"
    )
    log.info(message)
    log.info("Branch summary payload: %s", payload)


def validate_rejected_loans_staging(conn_id, schema, table) -> None:
    failures = []
    log.info("Validating rejected loans data...")
    # Placeholder for validation logic
    if failures:
        log.error("Rejected loans data validation failed with issues: %s", failures)
        raise ValueError(f"Rejected loans data validation failed: {failures}")
    log.info("Rejected loans data validation completed successfully.")


def validate_accepted_loans_staging(conn_id, schema, table) -> None:
    failures = []
    log.info("Validating accepted loans data...")
    # Placeholder for validation logic
    if failures:
        log.error("Accepted loans data validation failed with issues: %s", failures)
        raise ValueError(f"Accepted loans data validation failed: {failures}")
    log.info("Accepted loans data validation completed successfully.")


with DAG(
    dag_id="lending_loans_dag",
    default_args=default_args,
    description="Process accepted and rejected loans independently as files arrive",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    wait_for_accepted_file = PythonSensor(
        task_id="wait_for_accepted_file",
        python_callable=is_file_ready,
        op_kwargs={"file_path": ACCEPTED_LOANS_FILE_PATH},
        poke_interval=60,
        timeout=120,
        mode="reschedule",
        soft_fail=True,
    )

    run_accepted_loader = build_loader_task(
        task_id="run_accepted_loader",
        script_name="load_accepted_loans.py",
        file_path=ACCEPTED_LOANS_FILE_PATH,
        table_name="accepted_loans",
    )

    validate_accepted_load = PythonOperator(
        task_id="validate_accepted_loans_staging",
        python_callable=validate_accepted_loans_staging,
        op_kwargs={
            "conn_id": "playground_postgres",
            "schema": "staging",
            "table": "accepted_loans",
        },
    )

    publish_accepted_loans = EmptyOperator(task_id="publish_accepted_loans")

    # publish_accepted_loans = SQLExecuteQueryOperator(
    #     task_id="publish_accepted_loans",
    #     sql="INSERT INTO accepted_loans_archive SELECT * FROM accepted_loans",
    #     conn_id="playground_postgres",
    # )

    wait_for_rejected_file = PythonSensor(
        task_id="wait_for_rejected_file",
        python_callable=is_file_ready,
        op_kwargs={"file_path": REJECTED_LOANS_FILE_PATH},
        poke_interval=60,
        timeout=120,
        mode="reschedule",
        soft_fail=True,
    )

    run_rejected_loader = build_loader_task(
        task_id="run_rejected_loader",
        script_name="load_rejected_loans.py",
        file_path=REJECTED_LOANS_FILE_PATH,
        table_name="rejected_loans",
    )

    validate_rejected_load = PythonOperator(
        task_id="validate_rejected_loans_staging",
        python_callable=validate_rejected_loans_staging,
        op_kwargs={
            "conn_id": "playground_postgres",
            "schema": "staging",
            "table": "rejected_loans",
        },
    )

    publish_rejected_loans = EmptyOperator(task_id="publish_rejected_loans")

    # publish_rejected_loans = SQLExecuteQueryOperator(
    #     task_id="publish_rejected_loans",
    #     sql="INSERT INTO rejected_loans_archive SELECT * FROM rejected_loans",
    #     conn_id="playground_postgres",
    # )

    notify_branch_summary = PythonOperator(
        task_id="notify_branch_summary",
        python_callable=send_branch_summary,
        trigger_rule="all_done",
    )

    error_notice = EmptyOperator(task_id="error_notice", trigger_rule="one_failed")

    (
        wait_for_accepted_file
        >> run_accepted_loader
        >> validate_accepted_load
        >> publish_accepted_loans
    )  # type: ignore[no-redef]
    (
        wait_for_rejected_file
        >> run_rejected_loader
        >> validate_rejected_load
        >> publish_rejected_loans
    )  # type: ignore[no-redef]
    [  # type: ignore[no-redef]
        validate_accepted_load,
        validate_rejected_load,
        publish_accepted_loans,
        publish_rejected_loans,
    ] >> error_notice
    [publish_accepted_loans, publish_rejected_loans] >> notify_branch_summary  # type: ignore[no-redef]

    [  # type: ignore[no-redef]
        validate_accepted_load,
        validate_rejected_load,
    ] >> error_notice

    [  # type: ignore[no-redef]
        publish_accepted_loans,
        publish_rejected_loans,
    ] >> error_notice  # type: ignore[no-redef]
