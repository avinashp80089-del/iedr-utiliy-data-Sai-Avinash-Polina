"""
Utility 2 adapter.

U2 records are full circuits (one row per feeder — no segment rollup needed).
DER type comes from a single string column. DER feeder ID is given directly
in DER_INTERCONNECTION_LOCATION.

The planned-DER dataset uses INTERCONNECTION_QUEUE_REQUEST_ID as the primary
key while installed uses DER_ID. The YAML config handles that difference —
no code branch needed here.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from iedr.silver.adapters.base import UtilityAdapter


class Utility2Adapter(UtilityAdapter):

    def build_dim_feeder(self) -> DataFrame:
        cols   = self.config["columns"]["circuits"]
        u2_raw = self._read_bronze("circuits")
        return (
            u2_raw
            .select(
                F.lit(self.utility_id).alias("utility_id"),
                F.col(cols["feeder_id"]).cast("string").alias("feeder_id"),
                F.col(cols["max_hc"]).cast("double").alias("max_hosting_capacity"),
                F.col(cols["min_hc"]).cast("double").alias("min_hosting_capacity"),
                F.to_timestamp(cols["refresh_date"], "yyyy/MM/dd HH:mm:ssx").alias("last_refresh_date"),
                F.col(cols["voltage"]).cast("double").alias("voltage_kv"),
            )
        )

    def build_fact_der(self, status: str) -> DataFrame:
        ds_key = self.config["status_to_dataset"][status]
        cols   = self.config["columns"][ds_key]
        u2_raw = self._read_bronze(ds_key)

        return (
            u2_raw
            .select(
                F.lit(self.utility_id).alias("utility_id"),
                F.col(cols["der_id"]).cast("string").alias("der_id"),
                F.col(cols["der_type"]).alias("der_type"),
                F.expr(f"try_cast(`{cols['nameplate']}` as double)").alias("capacity_kw"),
                F.lit(status).alias("status"),
                F.col(cols["feeder_id"]).cast("string").alias("feeder_id"),
                F.col(cols["feeder_id"]).isNull().alias("feeder_id_unresolved"),
            )
        )
