# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Transformation
# MAGIC
# MAGIC Reads bronze Delta tables, normalizes per-utility data into the canonical
# MAGIC dim_feeder / fact_der schemas, and MERGEs into Delta tables.
# MAGIC All logic lives in `src/iedr/silver/`.

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

dbutils.widgets.dropdown("env", "dev", ["dev", "qa", "prod"])
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("batch_date", "")

# COMMAND ----------

from iedr.silver.pipeline import run_silver
from iedr.common.context import PipelineContext

ctx = PipelineContext.from_widgets(dbutils)
print(f"env={ctx.env} | catalog={ctx.catalog} | batch_date={ctx.batch_date} | run_id={ctx.pipeline_run_id}")

run_silver(spark, ctx)
