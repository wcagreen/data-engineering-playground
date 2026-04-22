"""
Script Runner DAG
=================
Pattern: Invoke an external Python script via the python-runner container
         → capture output → check exit code.

This mirrors the ECS / Lambda pattern:
  1.  invoke_script   — POSTs {"script": ..., "args": ...} to the
                        python-runner HTTP service, streams the output into
                        the task log, and pushes the exit code to XCom.
  2.  check_exit_code — pulls the exit code from XCom and explicitly
                        fails the DAG if it is non-zero.

Why two tasks instead of one?
------------------------------
Separating "run" from "check" means:
  • You can see the script's full output in the 'invoke_script' log even
    when the downstream check fails — exactly like inspecting an ECS task
    that stopped with a bad exit code.
  • You can insert additional tasks between the two steps later (e.g.
    upload the output file, notify Slack) without restructuring the DAG.

How to use
----------
1.  Drop your Python script into  scripts/python/
2.  Update SCRIPT_NAME and SCRIPT_ARGS below.
3.  Trigger the DAG manually from the Airflow UI.
    You can also override at runtime via the trigger conf:
      {"script": "other_script.py", "args": ["--flag", "value"]}

python-runner API
-----------------
POST http://python-runner:8000/run
  Body  : {"script": "script_name.py", "args": [...], "timeout": 3600}
  Return: {"script": "...", "exit_code": 0, "output": "..."}

GET  http://python-runner:8000/health
  Return: {"status": "ok"}
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import requests

from airflow import DAG

# Operator import: providers-standard (Airflow 3.x) with fallback to core (Airflow 2.x)
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator  # type: ignore[no-redef]

# ─── Configuration — edit these ───────────────────────────────────────────────
# Script file name (relative to scripts/python/ — mounted at /scripts/ inside
# the python-runner container).
SCRIPT_NAME = "example_script.py"

# Static arguments passed to the script every run.
# Override at trigger time with: {"args": ["--input", "/scripts/data.csv"]}
SCRIPT_ARGS = [
    "--input",
    "/opt/airflow/data/raw/your_file.csv",
    "--min-rows",
    "1",
]

# URL of the python-runner service (reachable by service name inside Docker).
RUNNER_URL = "http://python-runner:8000/run"

# How long (seconds) to let the script run before the runner kills it.
SCRIPT_TIMEOUT_SEC = 3600  # 1 hour
# ──────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "retries": 0,  # don't retry script failures automatically
    "retry_delay": timedelta(minutes=2),
}


# ── Task functions ─────────────────────────────────────────────────────────────


def invoke_script(**context):
    """
    POST the script name + args to the python-runner container.

    • Script and args can be overridden at trigger time via dag_run.conf:
        {"script": "other_script.py", "args": ["--input", "/scripts/data.csv"]}

    • The runner's stdout/stderr is printed into the Airflow task log
      (visible in the UI under Logs > invoke_script).

    • The script's exit code is pushed to XCom so check_exit_code can read it.
      The task itself does NOT raise — that is intentionally left to the next
      task so the full output is always visible regardless of outcome.
    """
    dag_run = context.get("dag_run")
    dag_run_conf = dag_run.conf if dag_run is not None else {}

    script_name = dag_run_conf.get("script", SCRIPT_NAME)
    script_args = dag_run_conf.get("args", SCRIPT_ARGS)

    payload = {"script": script_name, "args": script_args, "timeout": SCRIPT_TIMEOUT_SEC}

    print(f"Runner URL : {RUNNER_URL}")
    print(f"Payload    : {json.dumps(payload)}")
    print("-" * 60)

    resp = requests.post(
        RUNNER_URL,
        json=payload,
        # Allow slightly longer than the script timeout so the HTTP call
        # doesn't cut off before the runner has a chance to reply.
        timeout=SCRIPT_TIMEOUT_SEC + 30,
    )
    resp.raise_for_status()

    result = resp.json()
    output: str = result.get("output", "")
    exit_code: int = result.get("exit_code", -1)

    for line in output.splitlines():
        print(line)

    print("-" * 60)
    print(f"Script finished with exit code: {exit_code}")

    # Push to XCom — downstream task reads these
    context["ti"].xcom_push(key="exit_code", value=exit_code)
    # Store last 200 lines of output so check task can surface them on failure
    context["ti"].xcom_push(key="output_tail", value="\n".join(output.splitlines()[-200:]))


def check_exit_code(**context):
    """
    Read the exit code pushed by invoke_script and explicitly fail the DAG
    if it is non-zero.  This is the "check task status" step in the
    ECS / Lambda pattern.

    Exit code conventions mirrored here:
      0  — success
      1  — general script error
      2  — validation / data quality failure
      Any other non-zero value is also treated as a failure.
    """
    exit_code = context["ti"].xcom_pull(key="exit_code", task_ids="invoke_script")
    output_tail = context["ti"].xcom_pull(key="output_tail", task_ids="invoke_script")

    print("Exit code :", exit_code)

    if exit_code == 0:
        print("Script completed successfully.")
        return

    # Surface the last lines of output in this task's log to make debugging easy
    if output_tail:
        print("\n── Last script output ────────────────────────────────────────")
        print(output_tail)
        print("──────────────────────────────────────────────────────────────")

    exit_messages = {
        1: "Script reported a general error (exit 1).",
        2: "Script reported a validation / data quality failure (exit 2).",
    }
    msg = exit_messages.get(exit_code, f"Script failed with unexpected exit code {exit_code}.")
    raise Exception(msg)


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="script_runner",
    description="Invoke an external Python script and check its exit code (ECS/Lambda-style)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,  # manual trigger
    catchup=False,
    tags=["script-runner", "docker-runner", "python"],
    # Override script name or args at trigger time via the UI / API:
    #   {"script": "my_script.py", "args": ["--input", "/scripts/data.csv"]}
) as dag:
    t1_invoke = PythonOperator(
        task_id="invoke_script",
        python_callable=invoke_script,
        execution_timeout=timedelta(
            seconds=SCRIPT_TIMEOUT_SEC + 60
        ),  # slightly above subprocess timeout
    )

    t2_check = PythonOperator(
        task_id="check_exit_code",
        python_callable=check_exit_code,
    )

    t1_invoke >> t2_check
