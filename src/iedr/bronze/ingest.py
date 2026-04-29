"""
Bronze ingest.

Key design decisions
--------------------
1. APPEND, not overwrite. Each monthly drop is a new partition (batch_date).
   If utility 1 sends a corrected file in April, the original March file is
   preserved in the March partition. The original mode("overwrite") destroyed
   that history every run — unsafe for a regulated utility dataset.

2. Streaming-ready. The spark.read.csv path can be swapped 1:1 to
   spark.readStream.format("cloudFiles") for Auto Loader. See the commented
   block at the bottom for the swap recipe.

3. BOM stripping preserved — utility 2 CSVs ship as UTF-8-BOM and PySpark
   does not strip the marker from column names automatically.

4. Config-driven. File patterns and dataset names come from utility YAML
   configs, not hardcoded strings. Adding utility 3 = one YAML file.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from iedr.common.config import load_pipeline_config, load_utility_config
from iedr.common.context import PipelineContext, add_audit_columns


def run_bronze(spark: SparkSession, ctx: PipelineContext) -> None:
    """Top-level bronze ingest. Idempotent for a given batch_date."""
    spark.sql(f"USE CATALOG {ctx.catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.bronze_schema}")

    pipeline_cfg: dict[str, Any] = load_pipeline_config(ctx.env)
    print(f"Bronze: env={ctx.env}, batch_date={ctx.batch_date}, run_id={ctx.pipeline_run_id}")

    total_rows = 0
    for utility_id in pipeline_cfg["utilities"]:
        utility_cfg = load_utility_config(utility_id)
        for dataset in utility_cfg["datasets"]:
            total_rows += _ingest_one(spark, ctx, utility_id, dataset, utility_cfg)

    print(f"Bronze layer complete. Total rows ingested: {total_rows:,}")


def _ingest_one(
    spark: SparkSession,
    ctx: PipelineContext,
    utility_id: str,
    dataset: str,
    utility_cfg: dict[str, Any],
) -> int:
    """Read one utility CSV and append it to its bronze Delta table."""
    filename  = utility_cfg["file_pattern"].format(utility_id=utility_id, dataset=dataset)
    file_path = f"{ctx.volume_base}/{filename}"

    df_raw = (
        spark.read.format("csv")
        .option("header",    "true")
        .option("inferSchema", "true")
        .option("encoding",  "utf-8")
        .option("multiLine", "true")
        .load(file_path)
    )

    # Strip BOM and whitespace from header names (UTF-8-BOM CSVs from U2)
    clean_cols = [c.replace("\ufeff", "").strip() for c in df_raw.columns]
    df_raw = df_raw.toDF(*clean_cols)

    df_bronze = (
        df_raw
        .withColumn("utility_id",  F.lit(utility_id))
        .withColumn("source_file", F.lit(file_path))
    )
    df_bronze = add_audit_columns(df_bronze, ctx)

    table_name = f"{ctx.bronze_schema}.{utility_id}_{dataset}"

    # First run: create with partitioning.
    # Subsequent runs: replace only this batch_date partition (idempotent).
    if not spark.catalog.tableExists(table_name):
        (df_bronze.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("utility_id", "batch_date")
            .saveAsTable(table_name))
    else:
        (df_bronze.write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"batch_date = '{ctx.batch_date}'")
            .partitionBy("utility_id", "batch_date")
            .saveAsTable(table_name, mode="append"))

    row_count = df_bronze.count()
    print(f"  {table_name}: {row_count:,} rows")
    return row_count


# ---------------------------------------------------------------------------- #
# Auto Loader swap recipe (production streaming path)                           #
# ---------------------------------------------------------------------------- #
# Replace the spark.read block in _ingest_one with:
#
#     df_raw = (
#         spark.readStream.format("cloudFiles")
#             .option("cloudFiles.format", "csv")
#             .option("cloudFiles.schemaLocation", f"{ctx.volume_base}/_schemas/{utility_id}_{dataset}")
#             .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
#             .option("header", "true")
#             .option("multiLine", "true")
#             .load(f"{ctx.volume_base}/incoming/{utility_id}/{dataset}/")
#     )
#
# And the write becomes:
#
#     query = (df_bronze.writeStream
#         .format("delta")
#         .option("checkpointLocation", f"{ctx.volume_base}/_checkpoints/{utility_id}_{dataset}")
#         .partitionBy("utility_id", "batch_date")
#         .trigger(availableNow=True)
#         .toTable(table_name))
#     query.awaitTermination()
