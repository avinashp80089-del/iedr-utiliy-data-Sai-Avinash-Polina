# Databricks notebook source
# MAGIC %md
# MAGIC # Platinum Tables
# MAGIC
# MAGIC Builds the two API-facing tables (Req 2c, 2d) and the data_quality table.
# MAGIC All logic lives in `src/iedr/platinum/`.

# COMMAND ----------

import sys
import os

BUNDLE_ROOT = "/Workspace/Users/avinashp80089@gmail.com/.bundle/iedr-utility-data-lakehouse/dev/files"
SRC_PATH    = os.path.join(BUNDLE_ROOT, "src")
CONFIG_PATH = os.path.join(BUNDLE_ROOT, "config")

if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

os.environ["IEDR_CONFIG_ROOT"] = CONFIG_PATH

# COMMAND ----------

dbutils.widgets.dropdown("env",        "dev", ["dev", "qa", "prod"])
dbutils.widgets.text("catalog",        "workspace")
dbutils.widgets.text("batch_date",     "")

# COMMAND ----------

from iedr.platinum.build import run_platinum
from iedr.common.context import PipelineContext

ctx = PipelineContext.from_widgets(dbutils)
print(f"env={ctx.env} | catalog={ctx.catalog} | batch_date={ctx.batch_date} | run_id={ctx.pipeline_run_id}")

run_platinum(spark, ctx)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Smoke checks — Req 2c and 2d

# COMMAND ----------

display(spark.sql(f"""
    SELECT utility_id, feeder_id, max_hosting_capacity, voltage_kv, last_refresh_date
    FROM {ctx.platinum_schema}.feeder_capacity
    WHERE max_hosting_capacity > 5
    ORDER BY max_hosting_capacity DESC
    LIMIT 20
"""))

# COMMAND ----------

SAMPLE_FEEDER = spark.sql(f"""
    SELECT feeder_id FROM {ctx.platinum_schema}.feeder_der_details
    WHERE feeder_id IS NOT NULL LIMIT 1
""").collect()[0]["feeder_id"]

display(spark.sql(f"""
    SELECT utility_id, feeder_id, der_id, der_type, capacity_kw, status,
           voltage_kv, feeder_data_missing
    FROM {ctx.platinum_schema}.feeder_der_details
    WHERE feeder_id = '{SAMPLE_FEEDER}'
    ORDER BY status, der_type
"""))
