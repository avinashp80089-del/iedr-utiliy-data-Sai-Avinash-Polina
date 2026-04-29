"""
Platinum-layer builder.

Two API-facing tables (case prompt requirements 2c and 2d):

  feeder_capacity     → "All feeders with max_hosting_capacity > X"
                        Z-ORDER on max_hosting_capacity
                        Partitioned by utility_id

  feeder_der_details  → "All installed and planned DER for feeder X"
                        Z-ORDER on feeder_id
                        LEFT join so DERs with unresolved feeder_id are still
                        surfaced with feeder_data_missing=True

Plus data_quality table — one row per (utility, status) capturing
null counts, unresolved counts, last refresh date.
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from iedr.common.context import PipelineContext, add_audit_columns


def run_platinum(spark: SparkSession, ctx: PipelineContext) -> None:
    spark.sql(f"USE CATALOG {ctx.catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.platinum_schema}")
    print(f"Platinum: silver={ctx.silver_schema} → platinum={ctx.platinum_schema}")

    df_feeders = spark.table(f"{ctx.silver_schema}.dim_feeder")
    df_der     = spark.table(f"{ctx.silver_schema}.fact_der")

    _build_feeder_capacity(spark, ctx, df_feeders)
    _build_feeder_der_details(spark, ctx, df_feeders, df_der)
    _build_data_quality(spark, ctx, df_feeders, df_der)

    print("Platinum tables built and optimized.")


def _build_feeder_capacity(spark, ctx, df_feeders) -> None:
    """Req 2c — feeder hosting-capacity lookup."""
    table = f"{ctx.platinum_schema}.feeder_capacity"
    (df_feeders.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("utility_id")
        .saveAsTable(table))
    spark.sql(f"OPTIMIZE {table} ZORDER BY (max_hosting_capacity)")
    print(f"  feeder_capacity: {df_feeders.count():,} rows — Z-ORDERed")


def _build_feeder_der_details(spark, ctx, df_feeders, df_der) -> None:
    """Req 2d — DERs by feeder."""
    # LEFT join preserves DERs with unresolved feeder IDs
    # so feeder_data_missing=True surfaces in the UI instead of silently dropping
    enriched = (
        df_der.join(
            df_feeders.select("utility_id", "feeder_id", "voltage_kv", "last_refresh_date"),
            on=["utility_id", "feeder_id"],
            how="left",
        )
        .withColumn("feeder_data_missing", F.col("voltage_kv").isNull())
    )
    table = f"{ctx.platinum_schema}.feeder_der_details"
    (enriched.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("utility_id")
        .saveAsTable(table))
    spark.sql(f"OPTIMIZE {table} ZORDER BY (feeder_id)")
    print(f"  feeder_der_details: {enriched.count():,} rows — Z-ORDERed")


def _build_data_quality(spark, ctx, df_feeders, df_der) -> None:
    """Data quality summary per (utility, status)."""
    feeder_summary = (
        df_feeders.groupBy("utility_id")
        .agg(
            F.count("*").alias("total_rows"),
            F.max("last_refresh_date").alias("last_refresh_date"),
        )
        .withColumn("dataset", F.lit("circuits"))
        .withColumn("status",  F.lit(None).cast("string"))
        .withColumn("null_feeder_id_count",       F.lit(0).cast("long"))
        .withColumn("unresolved_feeder_id_count", F.lit(0).cast("long"))
    )

    der_summary = (
        df_der.groupBy("utility_id", "status")
        .agg(
            F.count("*").alias("total_rows"),
            F.sum(F.when(F.col("feeder_id").isNull(), 1).otherwise(0))
             .alias("null_feeder_id_count"),
            F.sum(F.when(F.col("feeder_id_unresolved"), 1).otherwise(0))
             .alias("unresolved_feeder_id_count"),
        )
        .withColumn("dataset", F.lit("der"))
        .withColumn("last_refresh_date", F.lit(None).cast("timestamp"))
    )

    combined = reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        [feeder_summary, der_summary],
    )
    combined = add_audit_columns(combined, ctx)

    table = f"{ctx.platinum_schema}.data_quality"
    (combined.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table))
    print(f"  data_quality: {combined.count()} rows")
