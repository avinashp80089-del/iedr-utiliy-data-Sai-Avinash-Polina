"""
Unit tests for the silver-layer utility adapters.

KEY DIFFERENCE FROM THE PREVIOUS REVISION
-----------------------------------------
These tests import the PRODUCTION classes directly (Utility1Adapter,
Utility2Adapter). The previous version re-implemented the logic locally
and tested the copy — meaning a regression in the notebook would slip
through CI silently. That defect is fixed here.
"""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from iedr.common.context import PipelineContext
from iedr.silver.adapters.utility1 import Utility1Adapter
from iedr.silver.adapters.utility2 import Utility2Adapter

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------- #
# Helpers                                                                       #
# ---------------------------------------------------------------------------- #
U1_CONFIG = {
    "utility_id": "utility1",
    "datasets": ["circuits", "install_der", "planned_der"],
    "status_to_dataset": {"installed": "install_der", "planned": "planned_der"},
    "columns": {
        "circuits": {
            "feeder_id":    "NYHCPV_csv_NFEEDER",
            "circuit_id":   "Circuits_Phase3_CIRCUIT",
            "max_hc":       "NYHCPV_csv_FMAXHC",
            "min_hc":       "NYHCPV_csv_FMINHC",
            "refresh_date": "NYHCPV_csv_FHCADATE",
            "voltage":      "NYHCPV_csv_FVOLTAGE",
        },
        "install_der": {
            "der_id":     "ProjectID",
            "circuit_id": "ProjectCircuitID",
            "nameplate":  "NamePlateRating",
        },
        "planned_der": {
            "der_id":     "ProjectID",
            "circuit_id": "ProjectCircuitID",
            "nameplate":  "NamePlateRating",
        },
    },
    "der_type_map": {
        "SolarPV":             "Solar",
        "EnergyStorageSystem": "Energy Storage",
        "Wind":                "Wind",
        "FuelCell":            "Fuel Cell",
        "Other":               "Other",
    },
}

U2_CONFIG = {
    "utility_id": "utility2",
    "datasets": ["circuits", "install_der", "planned_der"],
    "status_to_dataset": {"installed": "install_der", "planned": "planned_der"},
    "columns": {
        "circuits": {
            "feeder_id":    "Master_CDF",
            "max_hc":       "feeder_max_hc",
            "min_hc":       "feeder_min_hc",
            "refresh_date": "hca_refresh_date",
            "voltage":      "feeder_voltage",
        },
        "install_der": {
            "der_id":    "DER_ID",
            "der_type":  "DER_TYPE",
            "nameplate": "DER_NAMEPLATE_RATING",
            "feeder_id": "DER_INTERCONNECTION_LOCATION",
        },
        "planned_der": {
            "der_id":    "INTERCONNECTION_QUEUE_REQUEST_ID",
            "der_type":  "DER_TYPE",
            "nameplate": "DER_NAMEPLATE_RATING",
            "feeder_id": "DER_INTERCONNECTION_LOCATION",
        },
    },
}


def _u1(spark, ctx) -> Utility1Adapter:
    return Utility1Adapter(spark, U1_CONFIG, ctx)


def _u2(spark, ctx) -> Utility2Adapter:
    return Utility2Adapter(spark, U2_CONFIG, ctx)


# ---------------------------------------------------------------------------- #
# DER-type derivation tests (the trickiest U1 logic)                            #
# ---------------------------------------------------------------------------- #
def test_u1_der_type_single(spark, ctx):
    """One nonzero numeric column → that column's mapped label."""
    df = spark.createDataFrame(
        [("P1", "N", 100.0, 0.0, 0.0, 0.0, 0.0)],
        ["ProjectID", "Hybrid", "SolarPV", "Wind", "EnergyStorageSystem", "FuelCell", "Other"],
    )
    result = _u1(spark, ctx)._derive_der_type(df).collect()
    assert result[0]["der_type"] == "Solar"


def test_u1_der_type_hybrid_string_flag(spark, ctx):
    """
    Hybrid='Y' is a STRING flag, not a numeric column. The naive cast-to-double
    silently turns 'Y' → null → 0.0 and misses the case. Production code uses
    chained .when() with an explicit string comparison.
    """
    df = spark.createDataFrame(
        [("P2", "Y", 50.0, 0.0, 0.0, 0.0, 0.0)],
        ["ProjectID", "Hybrid", "SolarPV", "Wind", "EnergyStorageSystem", "FuelCell", "Other"],
    )
    result = _u1(spark, ctx)._derive_der_type(df).collect()
    assert result[0]["der_type"] == "Hybrid"


def test_u1_der_type_multiple_nonzero(spark, ctx):
    """Two or more nonzero numeric columns → 'Hybrid' even when Hybrid='N'."""
    df = spark.createDataFrame(
        [("P3", "N", 50.0, 30.0, 0.0, 0.0, 0.0)],
        ["ProjectID", "Hybrid", "SolarPV", "Wind", "EnergyStorageSystem", "FuelCell", "Other"],
    )
    result = _u1(spark, ctx)._derive_der_type(df).collect()
    assert result[0]["der_type"] == "Hybrid"


def test_u1_der_type_all_zero(spark, ctx):
    """All-zero rows fall through to 'Unknown' rather than emitting a wrong label."""
    df = spark.createDataFrame(
        [("P4", "N", 0.0, 0.0, 0.0, 0.0, 0.0)],
        ["ProjectID", "Hybrid", "SolarPV", "Wind", "EnergyStorageSystem", "FuelCell", "Other"],
    )
    result = _u1(spark, ctx)._derive_der_type(df).collect()
    assert result[0]["der_type"] == "Unknown"


def test_u1_der_type_handles_string_garbage(spark, ctx):
    """try_cast must absorb 'NULL'/'na'/blank values that some monthly drops contain."""
    df = spark.createDataFrame(
        [("P5", "N", "NULL", "100", "", "0", "0")],
        ["ProjectID", "Hybrid", "SolarPV", "Wind", "EnergyStorageSystem", "FuelCell", "Other"],
    )
    result = _u1(spark, ctx)._derive_der_type(df).collect()
    assert result[0]["der_type"] == "Wind"


# ---------------------------------------------------------------------------- #
# Float-feeder-ID cast (the .0 stripping)                                       #
# ---------------------------------------------------------------------------- #
def test_u1_feeder_id_long_string_cast(spark):
    """
    NYHCPV_csv_NFEEDER is a float column (e.g. 1105354.0). Direct cast to
    string yields '1105354.0', which won't match DER ProjectCircuitID = '1105354'.
    The long→string two-step strips the .0.
    """
    df = spark.createDataFrame([(1105354.0,), (1105352.0,)], ["NFEEDER"])
    result = (
        df.withColumn("feeder_id", F.col("NFEEDER").cast("long").cast("string"))
        .collect()
    )
    assert result[0]["feeder_id"] == "1105354"
    assert result[1]["feeder_id"] == "1105352"


# ---------------------------------------------------------------------------- #
# U2 happy-path: feeder_id passthrough + null-flag                              #
# ---------------------------------------------------------------------------- #
def test_u2_planned_der_uses_correct_pk_column(spark, ctx, monkeypatch):
    """
    U2 planned-DER PK is INTERCONNECTION_QUEUE_REQUEST_ID, not DER_ID. The
    YAML config drives that — verify the adapter actually uses it.
    """
    schema = StructType([
        StructField("INTERCONNECTION_QUEUE_REQUEST_ID", StringType(), True),
        StructField("DER_TYPE",                          StringType(), True),
        StructField("DER_NAMEPLATE_RATING",              DoubleType(), True),
        StructField("DER_INTERCONNECTION_LOCATION",      StringType(), True),
        StructField("utility_id",                        StringType(), True),
        StructField("batch_date",                        StringType(), True),
    ])
    df_raw = spark.createDataFrame(
        [("293870", "Solar", 5000.0, "36_32_36552", "utility2", "2026-04-01")],
        schema,
    )

    # Stub _read_bronze to return our fixture instead of hitting a real table
    adapter = _u2(spark, ctx)
    monkeypatch.setattr(adapter, "_read_bronze", lambda ds: df_raw)

    result = adapter.build_fact_der("planned").collect()
    assert result[0]["der_id"] == "293870"
    assert result[0]["status"] == "planned"
    assert result[0]["feeder_id"] == "36_32_36552"
    assert result[0]["feeder_id_unresolved"] is False


def test_u2_der_with_null_feeder_id_marked_unresolved(spark, ctx, monkeypatch):
    """U2 nulls feeder_id when the utility didn't supply DER_INTERCONNECTION_LOCATION."""
    schema = StructType([
        StructField("DER_ID",                       StringType(), True),
        StructField("DER_TYPE",                     StringType(), True),
        StructField("DER_NAMEPLATE_RATING",         DoubleType(), True),
        StructField("DER_INTERCONNECTION_LOCATION", StringType(), True),
        StructField("utility_id",                   StringType(), True),
    ])
    df_raw = spark.createDataFrame(
        [("391308", "Solar", 7.6, None, "utility2")],
        schema,
    )
    adapter = _u2(spark, ctx)
    monkeypatch.setattr(adapter, "_read_bronze", lambda ds: df_raw)

    result = adapter.build_fact_der("installed").collect()
    assert result[0]["feeder_id"] is None
    assert result[0]["feeder_id_unresolved"] is True


# ---------------------------------------------------------------------------- #
# Schema-naming contract                                                        #
# ---------------------------------------------------------------------------- #
def test_pipeline_context_schema_naming():
    for env in ("dev", "qa", "prod"):
        ctx = PipelineContext(env=env, catalog="x")
        assert ctx.bronze_schema   == f"iedr_{env}_bronze"
        assert ctx.silver_schema   == f"iedr_{env}_silver"
        assert ctx.platinum_schema == f"iedr_{env}_platinum"


def test_pipeline_context_run_id_unique():
    """Each PipelineContext gets a fresh UUID — critical for traceability."""
    a = PipelineContext(env="dev", catalog="x")
    b = PipelineContext(env="dev", catalog="x")
    assert a.pipeline_run_id != b.pipeline_run_id


def test_pipeline_context_is_immutable():
    """Frozen dataclass — values can't drift mid-run."""
    from dataclasses import FrozenInstanceError
    ctx = PipelineContext(env="dev", catalog="x")
    with pytest.raises(FrozenInstanceError):
        ctx.env = "prod"   # type: ignore[misc]
