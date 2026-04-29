# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingest
# MAGIC
# MAGIC Thin wrapper around `iedr.bronze.run_bronze`.
# MAGIC All real logic lives in `src/iedr/` — this notebook just wires up
# MAGIC widgets and delegates. No pip install needed — we add src/ to sys.path.

# COMMAND ----------

import sys
import os

# Add src/ to Python path so we can import iedr without a wheel install.
# Works on any Databricks tier including free edition.
BUNDLE_ROOT = "/Workspace/Users/avinashp80089@gmail.com/.bundle/iedr-utility-data-lakehouse/dev/files"
SRC_PATH    = os.path.join(BUNDLE_ROOT, "src")
CONFIG_PATH = os.path.join(BUNDLE_ROOT, "config")

if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

os.environ["IEDR_CONFIG_ROOT"] = CONFIG_PATH

# COMMAND ----------

dbutils.widgets.dropdown("env",        "dev", ["dev", "qa", "prod"])
dbutils.widgets.text("catalog",        "workspace")
dbutils.widgets.text("batch_date",     "")   # blank → today; YYYY-MM-DD for back-fills

# COMMAND ----------

from iedr.bronze.ingest import run_bronze
from iedr.common.context import PipelineContext

ctx = PipelineContext.from_widgets(dbutils)
print(f"env={ctx.env} | catalog={ctx.catalog} | batch_date={ctx.batch_date} | run_id={ctx.pipeline_run_id}")

run_bronze(spark, ctx)
