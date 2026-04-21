"""
scripts/python/etl_transforms.py
---------------------------------
Pure pandas transformation functions — zero Airflow dependency.

Imported by both:
  • dags/etl_dag.py   (inside the Airflow container)
  • tests/             (locally via pytest, no Airflow init needed)

Add your own transformation logic to  apply_transforms().
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

import pandas as pd


def apply_transforms(df: pd.DataFrame, source_file: str = "") -> pd.DataFrame:
    """
    Apply all transformations to a raw DataFrame and return the cleaned result.

    Parameters
    ----------
    df          : Raw DataFrame read from CSV.
    source_file : Name of the source file (used to populate the audit column).

    Returns
    -------
    Transformed DataFrame ready to be loaded into production.
    """
    result = df.copy()

    # ── Strip whitespace from all string columns ──────────────────────────────
    # pandas 3.x: use include="str" instead of include="object" to select string columns
    str_cols = result.select_dtypes(include="str").columns
    result[str_cols] = result[str_cols].apply(lambda col: col.str.strip())

    # ── Drop fully duplicate rows ─────────────────────────────────────────────
    before = len(result)
    result = result.drop_duplicates()
    dropped = before - len(result)
    if dropped:
        print(f"[transform] Dropped {dropped:,} duplicate rows")

    # ── YOUR CUSTOM TRANSFORMATIONS ───────────────────────────────────────────
    # Add any business logic here, for example:
    #
    # result.fillna({"amount": 0, "quantity": 0}, inplace=True)
    # result["order_date"] = pd.to_datetime(result["order_date"], errors="coerce")
    # result = result.rename(columns={"OrderID": "order_id"})
    # result["full_name"] = result["first_name"] + " " + result["last_name"]
    # result = result[result["status"] != "cancelled"]
    # ─────────────────────────────────────────────────────────────────────────

    # ── Audit columns ─────────────────────────────────────────────────────────
    result["_loaded_at"] = datetime.now(tz=UTC).isoformat()
    result["_source_file"] = os.path.basename(source_file) if source_file else ""

    return result
