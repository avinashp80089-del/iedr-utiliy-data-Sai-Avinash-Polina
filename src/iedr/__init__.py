"""
IEDR — Utility Data Lakehouse package.

Public entry points for the three medallion layers. Notebooks should be thin
wrappers that call these — all real logic lives in importable modules so it
can be unit-tested in CI without a Databricks cluster.
"""

from iedr.bronze.ingest import run_bronze
from iedr.platinum.build import run_platinum
from iedr.silver.pipeline import run_silver

__all__ = ["run_bronze", "run_silver", "run_platinum"]
__version__ = "0.2.0"
