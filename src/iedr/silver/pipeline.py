"""
Silver-layer orchestration.

For each active utility (per pipeline.yaml):
  1. Build dim_feeder rows via utility adapter
  2. Build the U1 bridge table (skipped for utilities that don't need one)
  3. Build fact_der rows for 'installed' and 'planned' statuses
  4. Stamp audit columns
  5. MERGE into destination Delta tables (idempotent monthly upserts)

Adding utility 3
----------------
1. Add config/utilities/utility3.yaml
2. Add Utility3Adapter subclassing UtilityAdapter
3. Register it in adapters/__init__.py::ADAPTERS
4. Add 'utility3' to pipeline.yaml

This module does NOT change.
"""

from __future__ import annotations

from functools import reduce
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, row_number

from iedr.common.config import load_pipeline_config, load_utility_config
from iedr.common.context import PipelineContext, add_audit_columns
from iedr.common.expectations import Expectation, apply_expectations
from iedr.silver.adapters import ADAPTERS
from iedr.silver.adapters.base import UtilityAdapter
from iedr.silver.adapters.utility1 import Utility1Adapter

DIM_FEEDER_EXPECTATIONS = [
    # severity="drop" — removes the 1 null-feeder-id row from U1 source data
    # and logs it instead of stopping the pipeline. The null row is a known
    # data quality issue in the source; dropping it is safer than failing
    # the entire monthly run.
    Expectation("feeder_id_not_null",       "feeder_id IS NOT NULL",                                     severity="drop"),
    Expectation("utility_id_not_null",      "utility_id IS NOT NULL",                                    severity="fail"),
    Expectation("hosting_capacity_non_neg", "max_hosting_capacity IS NULL OR max_hosting_capacity >= 0", severity="warn"),
]

FACT_DER_EXPECTATIONS = [
    Expectation("der_id_not_null",  "der_id IS NOT NULL",                      severity="fail"),
    Expectation("status_in_enum",   "status IN ('installed', 'planned')",      severity="fail"),
    Expectation("capacity_non_neg", "capacity_kw IS NULL OR capacity_kw >= 0", severity="warn"),
]


def run_silver(spark: SparkSession, ctx: PipelineContext) -> None:
    """Top-level silver pipeline. Idempotent for a given batch_date."""
    try:
        spark.sql(f"USE CATALOG {ctx.catalog}")
    except Exception:
        # Local Spark doesn't support USE CATALOG; Databricks will succeed
        pass
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.silver_schema}")

    adapters = _instantiate_adapters(spark, ctx)
    print(f"Silver: active utilities = {[a.utility_id for a in adapters]}")

    _build_bridges(spark, ctx, adapters)
    _build_dim_feeder(spark, ctx, adapters)

    for status in ("installed", "planned"):
        _build_fact_der(spark, ctx, adapters, status)

    print("Silver layer complete.")


def _instantiate_adapters(spark, ctx) -> list[UtilityAdapter]:
    pipeline_cfg: dict[str, Any] = load_pipeline_config(ctx.env)
    adapters = []
    for utility_id in pipeline_cfg["utilities"]:
        if utility_id not in ADAPTERS:
            raise KeyError(
                f"Utility '{utility_id}' listed in pipeline.yaml but no adapter registered. "
                f"Add it to adapters/__init__.py::ADAPTERS."
            )
        adapters.append(ADAPTERS[utility_id](spark, load_utility_config(utility_id), ctx))
    return adapters


def _build_bridges(spark, ctx, adapters) -> None:
    for adapter in adapters:
        if isinstance(adapter, Utility1Adapter):
            bridge = adapter.build_circuit_to_feeder_bridge()
            (bridge.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(f"{ctx.silver_schema}.bridge_u1_circuit_to_feeder"))
            print(f"  bridge_u1_circuit_to_feeder: {bridge.count():,} rows")


def _build_dim_feeder(spark, ctx, adapters) -> None:
    union = reduce(DataFrame.unionByName, (a.build_dim_feeder() for a in adapters))
    union = apply_expectations(union, DIM_FEEDER_EXPECTATIONS, "dim_feeder")
    union = add_audit_columns(union, ctx)

    # Deduplicate: keep only one row per (utility_id, feeder_id)
    # Prefer the most recently loaded record
    order_cols = []
    if "batch_date" in union.columns:
        order_cols.append(col("batch_date").desc())
    if "load_ts" in union.columns:
        order_cols.append(col("load_ts").desc())
    # Window function requires at least one ORDER BY; use row_id if no timestamps
    if not order_cols:
        # Use Spark's natural row order as fallback
        from pyspark.sql.functions import monotonically_increasing_id
        union = union.withColumn("_row_id", monotonically_increasing_id())
        order_cols.append(col("_row_id").desc())

    window = Window.partitionBy("utility_id", "feeder_id").orderBy(*order_cols)
    union = union.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")
    if "_row_id" in union.columns:
        union = union.drop("_row_id")

    target = f"{ctx.silver_schema}.dim_feeder"
    if spark.catalog.tableExists(target):
        DeltaTable.forName(spark, target).alias("t").merge(
            union.alias("s"),
            "t.utility_id = s.utility_id AND t.feeder_id = s.feeder_id",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print(f"  dim_feeder MERGED: {union.count():,} rows")
    else:
        (union.write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("utility_id")
            .saveAsTable(target))
        print(f"  dim_feeder CREATED: {union.count():,} rows")


def _build_fact_der(spark, ctx, adapters, status) -> None:
    union = reduce(DataFrame.unionByName, (a.build_fact_der(status) for a in adapters))

    # Deduplicate: keep only one row per (utility_id, der_id, status)
    # Prefer the most recently loaded record
    order_cols = []
    if "batch_date" in union.columns:
        order_cols.append(col("batch_date").desc())
    if "load_ts" in union.columns:
        order_cols.append(col("load_ts").desc())
    # Window function requires at least one ORDER BY; use row_id if no timestamps
    if not order_cols:
        # Use Spark's natural row order as fallback
        from pyspark.sql.functions import monotonically_increasing_id
        union = union.withColumn("_row_id", monotonically_increasing_id())
        order_cols.append(col("_row_id").desc())

    window = Window.partitionBy("utility_id", "der_id", "status").orderBy(*order_cols)
    union = union.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")
    if "_row_id" in union.columns:
        union = union.drop("_row_id")

    union = apply_expectations(union, FACT_DER_EXPECTATIONS, f"fact_der ({status})")
    union = add_audit_columns(union, ctx)

    target = f"{ctx.silver_schema}.fact_der"
    if spark.catalog.tableExists(target):
        DeltaTable.forName(spark, target).alias("t").merge(
            union.alias("s"),
            "t.utility_id = s.utility_id AND t.der_id = s.der_id AND t.status = s.status",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print(f"  fact_der ({status}) MERGED: {union.count():,} rows")
    else:
        (union.write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("utility_id")
            .saveAsTable(target))
        print(f"  fact_der ({status}) CREATED: {union.count():,} rows")
