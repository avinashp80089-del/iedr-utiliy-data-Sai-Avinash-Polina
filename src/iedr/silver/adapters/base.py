"""
Abstract base class for utility-specific silver-layer adapters.

The contract
------------
Every utility must produce DataFrames matching the canonical schemas in
iedr.common.schemas, regardless of how its raw CSV columns are named.

Subclasses encapsulate ALL utility-specific quirks:
  * column-name mapping
  * type coercions (e.g. U1's float feeder IDs need long→string cast)
  * derivation logic (e.g. U1's wide boolean DER-type columns → single label)
  * feeder ID resolution (e.g. U1 has segment IDs that need bridging)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from iedr.common.context import PipelineContext


class UtilityAdapter(ABC):

    def __init__(
        self,
        spark: SparkSession,
        config: dict[str, Any],
        ctx: PipelineContext,
    ) -> None:
        self.spark = spark
        self.config = config
        self.ctx = ctx
        self.utility_id: str = config["utility_id"]

    def _bronze_table(self, dataset: str) -> str:
        return f"{self.ctx.bronze_schema}.{self.utility_id}_{dataset}"

    def _read_bronze(self, dataset: str) -> DataFrame:
        """Read bronze table filtered to this batch_date for idempotency."""
        df = self.spark.table(self._bronze_table(dataset))
        if "batch_date" in df.columns:
            df = df.filter(df["batch_date"] == self.ctx.batch_date)
        return df

    @abstractmethod
    def build_dim_feeder(self) -> DataFrame:
        """Produce rows matching DIM_FEEDER_SCHEMA (audit columns added later)."""

    @abstractmethod
    def build_fact_der(self, status: str) -> DataFrame:
        """
        Produce rows matching FACT_DER_SCHEMA.
        status is one of 'installed' / 'planned'.
        """
