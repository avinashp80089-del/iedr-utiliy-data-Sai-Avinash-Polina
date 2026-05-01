"""
Pipeline context: the single object that carries run-scoped state through every layer.

Why this matters
----------------
A senior pipeline can answer questions like:
  - "Which rows were written by run 4f3e...?"
  - "Show me everything ingested on 2026-03-01"
  - "Re-run only the data that came in last Tuesday."

Achieving that requires three things, propagated through every Delta table:
  * pipeline_run_id — UUID generated once per pipeline invocation
  * batch_date — logical date of the data drop (typically the file delivery date)
  * ingestion_ts — wall-clock timestamp of when this row was written

Without these columns surgical reprocessing is impossible — you have to rerun
the whole pipeline from scratch.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class PipelineContext:
    """
    Run-scoped immutable context. Created once at job start, threaded through
    every layer. `frozen=True` prevents accidental mutation halfway through a run.
    """

    env: str                                    # 'dev' / 'qa' / 'prod'
    catalog: str                                # Unity Catalog name
    pipeline_run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    batch_date: date = field(default_factory=date.today)
    ingestion_ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def bronze_schema(self) -> str:
        return f"iedr_{self.env}_bronze"

    @property
    def silver_schema(self) -> str:
        return f"iedr_{self.env}_silver"

    @property
    def platinum_schema(self) -> str:
        return f"iedr_{self.env}_platinum"

    @property
    def volume_base(self) -> str:
        return f"/Volumes/{self.catalog}/default/iedr_data"

    @classmethod
    def from_widgets(cls, dbutils) -> PipelineContext:
        """
        Notebook entry point. Pulls env / catalog / batch_date from widgets.
        batch_date can be overridden for back-fills: pass YYYY-MM-DD string.
        """
        env = dbutils.widgets.get("env")
        catalog = dbutils.widgets.get("catalog")
        batch_date_str = dbutils.widgets.get("batch_date") or ""
        batch_date_val = (
            date.fromisoformat(batch_date_str) if batch_date_str else date.today()
        )
        return cls(env=env, catalog=catalog, batch_date=batch_date_val)

    @classmethod
    def from_env(cls) -> PipelineContext:
        """For local testing / non-Databricks runners."""
        return cls(
            env=os.environ.get("IEDR_ENV", "dev"),
            catalog=os.environ.get("IEDR_CATALOG", "workspace"),
        )


def add_audit_columns(df: DataFrame, ctx: PipelineContext) -> DataFrame:
    """
    Stamp every row with the run-scoped audit columns.
    Apply this exactly once per layer at write-time.
    """
    return (
        df.withColumn("pipeline_run_id", F.lit(ctx.pipeline_run_id))
        .withColumn("batch_date",        F.lit(ctx.batch_date))
        .withColumn("ingestion_ts",      F.lit(ctx.ingestion_ts).cast("timestamp"))
    )
