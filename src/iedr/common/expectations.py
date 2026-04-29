"""
Lightweight data-quality expectations.

Mirrors DLT's @dlt.expect_or_drop decorator so a future migration to DLT
is a copy-paste of the predicates.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

Severity = Literal["warn", "drop", "fail"]


@dataclass(frozen=True)
class Expectation:
    """A single named predicate applied to a DataFrame."""
    name: str
    predicate: str       # SQL boolean expression e.g. "feeder_id IS NOT NULL"
    severity: Severity   # warn → log; drop → remove bad rows; fail → raise

    def evaluate(self, df: DataFrame) -> tuple[DataFrame, DataFrame, int]:
        passing = df.filter(F.expr(self.predicate))
        failing = df.filter(~F.expr(self.predicate))
        return passing, failing, failing.count()


def apply_expectations(
    df: DataFrame,
    expectations: list[Expectation],
    table_name: str,
) -> DataFrame:
    """Apply expectations in order. Returns cleaned DataFrame."""
    cleaned = df
    for exp in expectations:
        passing, failing, fail_count = exp.evaluate(cleaned)
        total = cleaned.count()

        if fail_count == 0:
            print(f"  [{table_name}] {exp.name}: PASS ({total:,} rows)")
            continue

        msg = f"[{table_name}] {exp.name}: {fail_count:,}/{total:,} rows failed"

        if exp.severity == "fail":
            sample = failing.limit(5).collect()
            raise AssertionError(f"{msg}\nSample: {sample}")

        if exp.severity == "drop":
            print(f"  [{table_name}] DROP {msg}")
            cleaned = passing
            continue

        print(f"  [{table_name}] WARN {msg}")

    return cleaned
