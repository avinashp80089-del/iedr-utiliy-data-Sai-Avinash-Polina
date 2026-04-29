"""Unit tests for the lightweight expectations module."""

from __future__ import annotations

import pytest

from iedr.common.expectations import Expectation, apply_expectations

pytestmark = pytest.mark.unit


def test_expectation_warn_does_not_drop(spark):
    df = spark.createDataFrame([(1, 10.0), (2, -5.0)], ["id", "value"])
    expectations = [Expectation("non_negative", "value >= 0", severity="warn")]
    result = apply_expectations(df, expectations, "test")
    assert result.count() == 2   # warn keeps everything


def test_expectation_drop_removes_failing_rows(spark):
    df = spark.createDataFrame([(1, 10.0), (2, -5.0), (3, 0.0)], ["id", "value"])
    expectations = [Expectation("non_negative", "value >= 0", severity="drop")]
    result = apply_expectations(df, expectations, "test")
    assert result.count() == 2
    ids = {r["id"] for r in result.collect()}
    assert ids == {1, 3}


def test_expectation_fail_raises(spark):
    df = spark.createDataFrame([(1, 10.0), (2, -5.0)], ["id", "value"])
    expectations = [Expectation("non_negative", "value >= 0", severity="fail")]
    with pytest.raises(AssertionError, match="non_negative"):
        apply_expectations(df, expectations, "test")


def test_expectations_compose(spark):
    """Multiple expectations apply in order; later ones see the earlier results."""
    df = spark.createDataFrame([(1, 10.0), (2, -5.0), (3, 0.0)], ["id", "value"])
    expectations = [
        Expectation("non_negative", "value >= 0", severity="drop"),
        Expectation("nonzero",      "value > 0",  severity="drop"),
    ]
    result = apply_expectations(df, expectations, "test")
    assert result.count() == 1
    assert result.collect()[0]["id"] == 1
