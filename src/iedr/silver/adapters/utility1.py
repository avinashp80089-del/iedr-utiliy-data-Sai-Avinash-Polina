"""
Utility 1 adapter.

What's special about U1
-----------------------
1. Circuit records are segments. Multiple bronze rows share a common feeder
   (NYHCPV_csv_NFEEDER) but each describes one segment. We roll up to feeder
   level using FMAXHC / FMINHC — NOT the segment-level NMAXHC / NMINHC.

2. Float feeder IDs. PySpark casts NYHCPV_csv_NFEEDER directly to '1105354.0'
   on string conversion. We bounce through long first to strip the .0 —
   otherwise downstream joins to DER ProjectCircuitID miss every row.

3. Wide-boolean DER type encoding. U1 stores DER type as ~14 nameplate-rating
   columns (SolarPV, Wind, EnergyStorageSystem ...) where any nonzero value
   flags the type. We collapse to a single label. Two edge cases:
     a) Hybrid='Y' string flag (NOT a numeric column) marks intentional hybrids
     b) Multiple nonzero numeric columns also indicate a hybrid

4. Feeder ID resolution. DER records carry ProjectCircuitID, which is sometimes
   the feeder ID directly and sometimes a circuit-segment ID.
   Two-step resolution: bridge lookup first, direct match fallback.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from iedr.silver.adapters.base import UtilityAdapter


class Utility1Adapter(UtilityAdapter):

    # ------------------------------------------------------------------ #
    # 1. dim_feeder — segments rolled up to feeders                       #
    # ------------------------------------------------------------------ #
    def build_dim_feeder(self) -> DataFrame:
        cols = self.config["columns"]["circuits"]
        u1_raw = self._read_bronze("circuits")

        # FMAXHC / FMINHC are feeder-level constants across all segments.
        # Using NMAXHC here would wrongly inflate the result.
        return (
            u1_raw
            .groupBy(cols["feeder_id"])
            .agg(
                F.max(cols["max_hc"]).alias("max_hosting_capacity"),
                F.min(cols["min_hc"]).alias("min_hosting_capacity"),
                F.max(cols["refresh_date"]).alias("last_refresh_date"),
                F.first(cols["voltage"], ignorenulls=True).alias("voltage_kv"),
            )
            # long → string strips the .0 suffix from float-typed source IDs
            .withColumn("feeder_id", F.col(cols["feeder_id"]).cast("long").cast("string"))
            .withColumn("utility_id", F.lit(self.utility_id))
            .withColumn("last_refresh_date", F.to_timestamp("last_refresh_date"))
            .select(
                "utility_id", "feeder_id",
                "max_hosting_capacity", "min_hosting_capacity",
                "last_refresh_date", "voltage_kv",
            )
        )

    # ------------------------------------------------------------------ #
    # 2. Bridge — circuit segment ID → feeder ID                          #
    # ------------------------------------------------------------------ #
    def build_circuit_to_feeder_bridge(self) -> DataFrame:
        cols = self.config["columns"]["circuits"]
        u1_raw = self._read_bronze("circuits")
        return (
            u1_raw
            .select(
                F.col(cols["circuit_id"]).cast("string").alias("circuit_id"),
                F.col(cols["feeder_id"]).cast("long").cast("string").alias("feeder_id"),
            )
            .distinct()
            .filter(F.col("circuit_id").isNotNull() & F.col("feeder_id").isNotNull())
        )

    # ------------------------------------------------------------------ #
    # 3. DER type derivation                                              #
    # ------------------------------------------------------------------ #
    def _derive_der_type(self, df: DataFrame) -> DataFrame:
        """
        Collapse wide-boolean DER columns into a single 'der_type' label.

        Edge cases covered by tests:
          * Hybrid='Y' string flag → 'Hybrid' (cannot be cast to numeric)
          * 2+ nonzero numeric columns → 'Hybrid'
          * Exactly 1 nonzero column → that label
          * All zero → 'Unknown'

        Chained .when() avoids PySpark null-propagation:
          (null & null) → null, NOT True
        """
        type_map: dict[str, str] = self.config["der_type_map"]
        type_cols = list(type_map.keys())

        for c in type_cols:
            if c in df.columns:
                df = df.withColumn(c, F.coalesce(F.expr(f"try_cast(`{c}` as double)"), F.lit(0.0)))
            else:
                df = df.withColumn(c, F.lit(0.0))

        if "Hybrid" not in df.columns:
            df = df.withColumn("Hybrid", F.lit("N"))

        nonzero_count = sum(F.when(F.col(c) > 0, 1).otherwise(0) for c in type_cols)
        df = df.withColumn("_nonzero_count", nonzero_count)

        case_expr = F.when(
            (F.upper(F.col("Hybrid")) == F.lit("Y")) | (F.col("_nonzero_count") > 1),
            F.lit("Hybrid"),
        )
        for col_name, label in type_map.items():
            case_expr = case_expr.when(F.col(col_name) > 0, F.lit(label))
        case_expr = case_expr.otherwise(F.lit("Unknown"))

        return df.withColumn("der_type", case_expr).drop("_nonzero_count")

    # ------------------------------------------------------------------ #
    # 4. fact_der — DER records with two-step feeder resolution           #
    # ------------------------------------------------------------------ #
    def build_fact_der(self, status: str) -> DataFrame:
        ds_key = self.config["status_to_dataset"][status]
        cols   = self.config["columns"][ds_key]

        u1_raw = self._read_bronze(ds_key)
        u1_raw = self._derive_der_type(u1_raw)

        u1_bridge = (
            self.spark.table(f"{self.ctx.silver_schema}.bridge_u1_circuit_to_feeder")
            .dropDuplicates(["circuit_id"])
        )
        u1_known_feeders = (
            self.spark.table(f"{self.ctx.silver_schema}.dim_feeder")
            .filter(F.col("utility_id") == self.utility_id)
            .select(F.col("feeder_id").alias("direct_feeder_id"))
        )

        return (
            u1_raw
            .withColumn("_circuit_id", F.col(cols["circuit_id"]).cast("string"))
            .join(u1_bridge,       F.col("_circuit_id") == u1_bridge["circuit_id"],              "left")
            .join(u1_known_feeders, F.col("_circuit_id") == u1_known_feeders["direct_feeder_id"], "left")
            .select(
                F.lit(self.utility_id).alias("utility_id"),
                F.col(cols["der_id"]).cast("string").alias("der_id"),
                F.col("der_type"),
                F.expr(f"try_cast(`{cols['nameplate']}` as double)").alias("capacity_kw"),
                F.lit(status).alias("status"),
                F.coalesce(
                    u1_bridge["feeder_id"],
                    u1_known_feeders["direct_feeder_id"],
                ).alias("feeder_id"),
                # Chained .when() — avoids (null & null) = null, not True
                F.when(F.col("_circuit_id").isNull(), F.lit(True))
                 .when(
                     u1_bridge["feeder_id"].isNull() & u1_known_feeders["direct_feeder_id"].isNull(),
                     F.lit(True),
                 )
                 .otherwise(F.lit(False))
                 .alias("feeder_id_unresolved"),
            )
        )
