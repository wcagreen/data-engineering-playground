#!/usr/bin/env python3
"""
example_script.py
-----------------
Example external Python script invoked by script_runner_dag.py.

This script is intentionally self-contained — it only uses the standard
library so it runs inside the Airflow container without extra installs.

Usage
-----
    python example_script.py --input /opt/airflow/data/raw/your_file.csv
    python example_script.py --input /opt/airflow/data/raw/your_file.csv --min-rows 100

Exit codes
----------
    0  — all checks passed
    1  — input file not found or unreadable
    2  — data validation failed (e.g. row count below threshold)

Airflow reads exit code 0 as success, anything else as failure.
"""

import argparse
import csv
import os
import sys
from datetime import datetime, timezone


def log(level: str, message: str) -> None:
    """Print a timestamped log line (shows up in the Airflow task log)."""
    ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{ts}] [{level}] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Example CSV validation script called by Airflow script_runner_dag."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Absolute path to the CSV file to validate.",
    )
    parser.add_argument(
        "--min-rows",
        type=int,
        default=1,
        help="Minimum number of data rows required for the file to pass (default: 1).",
    )
    parser.add_argument(
        "--required-columns",
        nargs="*",
        default=[],
        help="Column names that must be present in the CSV header.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    log("INFO", f"Script started — PID {os.getpid()}")
    log("INFO", f"Input file   : {args.input}")
    log("INFO", f"Min rows     : {args.min_rows}")
    if args.required_columns:
        log("INFO", f"Req. columns : {args.required_columns}")

    # ── Step 1: file exists ────────────────────────────────────────────────────
    if not os.path.isfile(args.input):
        log("ERROR", f"File not found: {args.input}")
        log("ERROR", "Place your CSV in the data/raw/ directory and update SCRIPT_ARGS in the DAG.")
        sys.exit(1)

    file_size_kb = os.path.getsize(args.input) / 1024
    log("INFO", f"File size    : {file_size_kb:.1f} KB")

    # ── Step 2: read & inspect ─────────────────────────────────────────────────
    try:
        with open(args.input, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            columns = reader.fieldnames or []
            rows = list(reader)
    except Exception as exc:
        log("ERROR", f"Could not read CSV: {exc}")
        sys.exit(1)

    log("INFO", f"Columns ({len(columns)}): {columns}")
    log("INFO", f"Rows read    : {len(rows):,}")

    # ── Step 3: required columns ───────────────────────────────────────────────
    if args.required_columns:
        missing = [c for c in args.required_columns if c not in columns]
        if missing:
            log("ERROR", f"Missing required columns: {missing}")
            sys.exit(2)
        log("INFO", "Required columns check: PASSED")

    # ── Step 4: row count ──────────────────────────────────────────────────────
    if len(rows) < args.min_rows:
        log("ERROR", f"Row count {len(rows):,} is below the minimum {args.min_rows:,}")
        sys.exit(2)
    log("INFO", f"Row count check: PASSED ({len(rows):,} >= {args.min_rows:,})")

    # ── Step 5: null check on all columns ─────────────────────────────────────
    null_counts: dict[str, int] = {col: 0 for col in columns}
    for row in rows:
        for col in columns:
            if row.get(col) in (None, "", "NULL", "null", "NA", "N/A"):
                null_counts[col] += 1

    has_nulls = False
    for col, count in null_counts.items():
        if count > 0:
            pct = count / len(rows) * 100
            log("WARN", f"Column '{col}' has {count:,} null/empty values ({pct:.1f}%)")
            has_nulls = True

    if not has_nulls:
        log("INFO", "Null check: no nulls found across all columns")

    # ── Done ───────────────────────────────────────────────────────────────────
    log("INFO", "All checks passed. Exiting 0.")
    sys.exit(0)


if __name__ == "__main__":
    main()
