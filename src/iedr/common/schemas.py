"""
Canonical schemas for silver and platinum layers.

Declaring schemas explicitly means:
  * Type drift breaks the test suite, not production
  * New utilities can't silently introduce unexpected columns
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

AUDIT_COLUMNS = [
    StructField("pipeline_run_id", StringType(),    nullable=False),
    StructField("batch_date",      DateType(),      nullable=False),
    StructField("ingestion_ts",    TimestampType(), nullable=False),
]

DIM_FEEDER_SCHEMA = StructType([
    StructField("utility_id",           StringType(),    nullable=False),
    StructField("feeder_id",            StringType(),    nullable=False),
    StructField("max_hosting_capacity", DoubleType(),    nullable=True),
    StructField("min_hosting_capacity", DoubleType(),    nullable=True),
    StructField("last_refresh_date",    TimestampType(), nullable=True),
    StructField("voltage_kv",           DoubleType(),    nullable=True),
    *AUDIT_COLUMNS,
])

# status carries lifecycle state — "installed" or "planned"
# NOT the source dataset name (install_der / planned_der)
FACT_DER_SCHEMA = StructType([
    StructField("utility_id",           StringType(),  nullable=False),
    StructField("der_id",               StringType(),  nullable=False),
    StructField("der_type",             StringType(),  nullable=True),
    StructField("capacity_kw",          DoubleType(),  nullable=True),
    StructField("status",               StringType(),  nullable=False),
    StructField("feeder_id",            StringType(),  nullable=True),
    StructField("feeder_id_unresolved", BooleanType(), nullable=False),
    *AUDIT_COLUMNS,
])

DER_STATUS_INSTALLED = "installed"
DER_STATUS_PLANNED   = "planned"
DER_STATUS_VALUES    = (DER_STATUS_INSTALLED, DER_STATUS_PLANNED)
