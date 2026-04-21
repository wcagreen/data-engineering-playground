"""
tests/test_etl_transforms.py
-----------------------------
Unit tests for  scripts/python/etl_transforms.py.

No Airflow runtime needed — tests import pure pandas functions only.

Run with:
    uv run pytest
  or:
    .venv/Scripts/pytest   (Windows)
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts", "python"))

import pandas as pd
import pytest
from etl_transforms import apply_transforms


class TestApplyTransforms:
    """Tests for apply_transforms()."""

    def test_strips_whitespace_from_string_columns(self):
        df = pd.DataFrame({"name": ["  Alice  ", "  Bob"], "score": [10, 20]})
        result = apply_transforms(df)
        assert result["name"].tolist() == ["Alice", "Bob"]

    def test_drops_duplicate_rows(self):
        df = pd.DataFrame({"id": [1, 1, 2], "val": ["a", "a", "b"]})
        result = apply_transforms(df)
        assert len(result) == 2

    def test_adds_audit_columns(self):
        df = pd.DataFrame({"id": [1], "val": ["x"]})
        result = apply_transforms(df)
        assert "_loaded_at" in result.columns
        assert "_source_file" in result.columns

    def test_source_file_basename_only(self):
        df = pd.DataFrame({"id": [1]})
        result = apply_transforms(df, source_file="/opt/airflow/data/raw/sales.csv")
        assert result["_source_file"].iloc[0] == "sales.csv"

    def test_empty_dataframe_passes(self):
        df = pd.DataFrame({"id": [], "val": []})
        result = apply_transforms(df)
        assert len(result) == 0
        assert "_loaded_at" in result.columns

    def test_numeric_columns_untouched(self):
        df = pd.DataFrame({"id": [1, 2], "amount": [9.99, 19.99]})
        result = apply_transforms(df)
        assert result["amount"].tolist() == pytest.approx([9.99, 19.99])

    def test_does_not_mutate_input(self):
        df = pd.DataFrame({"name": ["  Alice  "]})
        original_val = df["name"].iloc[0]
        apply_transforms(df)
        assert df["name"].iloc[0] == original_val  # input df unchanged
