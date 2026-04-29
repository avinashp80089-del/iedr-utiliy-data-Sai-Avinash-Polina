"""Unit tests for audit-column stamping."""

from __future__ import annotations

from datetime import date, datetime

import pytest

from iedr.common.context import PipelineContext, add_audit_columns

pytestmark = pytest.mark.unit


def test_add_audit_columns_adds_all_three(spark):
    ctx = PipelineContext(
        env="test", catalog="x",
        pipeline_run_id="11111111-1111-1111-1111-111111111111",
        batch_date=date(2026, 4, 1),
        ingestion_ts=datetime(2026, 4, 1, 12, 0, 0),
    )
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    stamped = add_audit_columns(df, ctx)

    assert "pipeline_run_id" in stamped.columns
    assert "batch_date" in stamped.columns
    assert "ingestion_ts" in stamped.columns

    rows = stamped.collect()
    assert all(r["pipeline_run_id"] == "11111111-1111-1111-1111-111111111111" for r in rows)
    assert all(r["batch_date"] == date(2026, 4, 1) for r in rows)


def test_audit_columns_consistent_within_run(spark):
    """Every row in a run carries the SAME pipeline_run_id."""
    ctx = PipelineContext(env="test", catalog="x")
    df = spark.createDataFrame([(i,) for i in range(100)], ["id"])
    stamped = add_audit_columns(df, ctx)
    distinct_run_ids = stamped.select("pipeline_run_id").distinct().count()
    assert distinct_run_ids == 1
