"""
Script Runner DAG
=================
Pattern: Invoke external Python script → capture output → check exit code

This mirrors the ECS / Lambda pattern:
  1.  invoke_script   — launches the script as a subprocess, streams its
                        stdout/stderr into the task logs, pushes the exit
                        code to XCom.
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
2.  Update SCRIPT_PATH and SCRIPT_ARGS below.
3.  Trigger the DAG manually from the Airflow UI.
    You can also pass args at runtime using Airflow Params (see the
    'dag_run.conf' example in the comments below).
"""

from __future__ import annotations

import subprocess
from datetime import datetime, timedelta

from airflow import DAG

# Airflow 3.0: PythonOperator moved to apache-airflow-providers-standard
from airflow.providers.standard.operators.python import PythonOperator

# ─── Configuration — edit these ───────────────────────────────────────────────
SCRIPT_PATH = "/opt/airflow/scripts/python/example_script.py"

# Static arguments passed to the script every run.
# For dynamic args from the UI trigger, see the dag_run.conf example below.
SCRIPT_ARGS = [
    "--input",
    "/opt/airflow/data/raw/your_file.csv",
    "--min-rows",
    "1",
]

# How long (seconds) to let the script run before Airflow kills it.
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
    Run  SCRIPT_PATH  as a subprocess.

    • Arguments can be overridden at trigger time via dag_run.conf:
        {"script_args": ["--input", "/opt/airflow/data/raw/other.csv"]}

    • stdout and stderr are streamed line-by-line into the Airflow task log
      (visible in the UI under Logs > invoke_script).

    • The script's exit code is pushed to XCom so check_exit_code can read it.
      The task itself does NOT raise — that is intentionally left to the next
      task so the full output is always visible regardless of outcome.
    """
    # Allow runtime override of args via UI / API trigger conf
    dag_run = context.get("dag_run")
    dag_run_conf = dag_run.conf if dag_run is not None else {}
    runtime_args = dag_run_conf.get("script_args", SCRIPT_ARGS)

    cmd = ["python", SCRIPT_PATH, *runtime_args]
    print(f"Invoking: {' '.join(cmd)}")
    print(f"Timeout : {SCRIPT_TIMEOUT_SEC}s")
    print("-" * 60)

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # merge stderr into stdout stream
        text=True,
    )

    output_lines: list[str] = []
    assert process.stdout is not None
    for line in process.stdout:
        stripped = line.rstrip()
        print(stripped)  # visible in Airflow task log
        output_lines.append(stripped)

    process.wait(timeout=SCRIPT_TIMEOUT_SEC)
    exit_code = process.returncode

    print("-" * 60)
    print(f"Script finished with exit code: {exit_code}")

    # Push to XCom — downstream task reads these
    context["ti"].xcom_push(key="exit_code", value=exit_code)
    # Store last 200 lines of output so check task can surface them on failure
    context["ti"].xcom_push(key="output_tail", value="\n".join(output_lines[-200:]))


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
    tags=["script-runner", "subprocess", "python"],
    # Params let you change script_args from the Airflow UI trigger form.
    # Click 'Trigger DAG w/ config' and enter:
    #   {"script_args": ["--input", "/opt/airflow/data/raw/other.csv"]}
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
