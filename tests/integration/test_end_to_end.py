"""
End-to-end integration test.

Runs the full bronze → silver → platinum chain against the fixture CSVs and
verifies the platinum tables satisfy the case-prompt API requirements
(2c and 2d). This is the test that gives a future change author confidence
that they didn't break the contract.

Why the monkeypatching
----------------------
The production code reads CSVs from a Databricks Volume path
(`/Volumes/...`). For a local pytest run we need to redirect to the fixture
directory. We patch `PipelineContext.volume_base` to point at tests/fixtures.
Everything else runs unchanged.
"""

from __future__ import annotations

from datetime import date

import pytest

from iedr import run_bronze, run_platinum, run_silver
from iedr.common.context import PipelineContext

pytestmark = pytest.mark.integration


@pytest.fixture
def integration_ctx(spark, tmp_path, fixtures_dir, monkeypatch) -> PipelineContext:
    """
    Build a context that reads from tests/fixtures/ and writes to a tmpdir
    Spark warehouse so we don't pollute anything else.
    """
    # Create a dedicated catalog/schema for this test run to keep state isolated
    spark.sql("CREATE DATABASE IF NOT EXISTS iedr_integration_bronze")
    spark.sql("CREATE DATABASE IF NOT EXISTS iedr_integration_silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS iedr_integration_platinum")

    ctx = PipelineContext(
        env="integration",
        catalog="spark_catalog",
        batch_date=date(2026, 4, 1),
    )
    # Redirect the volume path to our fixture dir
    monkeypatch.setattr(
        type(ctx),
        "volume_base",
        property(lambda self: str(fixtures_dir)),
    )
    return ctx


def test_full_pipeline_end_to_end(spark, integration_ctx):
    """
    Bronze → silver → platinum should populate all tables and the platinum
    queries should return the rows we expect from the fixtures.
    """
    ctx = integration_ctx

    # Run all three layers
    run_bronze(spark, ctx)
    run_silver(spark, ctx)
    run_platinum(spark, ctx)

    # ---- bronze ---- #
    assert spark.table(f"{ctx.bronze_schema}.utility1_circuits").count() == 3
    assert spark.table(f"{ctx.bronze_schema}.utility2_install_der").count() == 2

    # BOM-stripping check: U2's CSVs ship with UTF-8-BOM. The first column header
    # comes out as '\ufeffDER_ID' from spark.read.csv. If bronze's BOM-strip
    # logic regresses, this assertion fails loudly instead of corrupting silver.
    u2_install_cols = spark.table(f"{ctx.bronze_schema}.utility2_install_der").columns
    assert "DER_ID" in u2_install_cols, f"BOM not stripped from header: {u2_install_cols[:3]}"
    assert not any(c.startswith("\ufeff") for c in u2_install_cols), "BOM still present"

    # ---- silver dim_feeder ---- #
    feeders = spark.table(f"{ctx.silver_schema}.dim_feeder")
    assert feeders.count() == 4   # 2 U1 feeders + 2 U2 feeders
    # Float-ID strip: feeder 1105354 should NOT be '1105354.0'
    u1_feeder_ids = {r["feeder_id"] for r in feeders.filter("utility_id = 'utility1'").collect()}
    assert "1105354" in u1_feeder_ids
    assert "1105354.0" not in u1_feeder_ids

    # Segment rollup: feeder 1105354 has two segments — its max_hosting_capacity
    # must be the FMAXHC value (3.7), not a sum or anything else
    feeder_1105354 = feeders.filter("feeder_id = '1105354'").collect()[0]
    assert feeder_1105354["max_hosting_capacity"] == 3.7

    # ---- silver fact_der ---- #
    der = spark.table(f"{ctx.silver_schema}.fact_der")
    assert der.filter("status = 'installed'").count() >= 5   # U1: 4, U2: 2
    assert der.filter("status = 'planned'").count()   >= 4   # U1: 2, U2: 2

    # Hybrid edge case from the fixture
    hybrid = der.filter("der_id = '9999'").collect()[0]
    assert hybrid["der_type"] == "Hybrid"

    # Unresolved feeder ID → flag set, feeder_id null
    unresolved = der.filter("der_id = '1234'").collect()[0]
    assert unresolved["feeder_id"] is None
    assert unresolved["feeder_id_unresolved"] is True

    # ---- platinum Req 2c: feeders with hosting capacity > X ---- #
    high_cap = (
        spark.sql(f"""
            SELECT * FROM {ctx.platinum_schema}.feeder_capacity
            WHERE max_hosting_capacity > 1
        """)
        .collect()
    )
    # From fixtures: 1105354 (3.7), 1105352 (5.0), 36_13_81757 (1.1) → 3 feeders
    assert len(high_cap) == 3

    # ---- platinum Req 2d: all DERs for a given feeder ---- #
    feeder_ders = (
        spark.sql(f"""
            SELECT * FROM {ctx.platinum_schema}.feeder_der_details
            WHERE feeder_id = '1105354'
        """)
        .collect()
    )
    # From fixtures: 2 installed + 1 planned on feeder 1105354
    assert len(feeder_ders) == 3
    statuses = sorted({r["status"] for r in feeder_ders})
    assert statuses == ["installed", "planned"]

    # ---- platinum data_quality table populated ---- #
    dq = spark.table(f"{ctx.platinum_schema}.data_quality")
    assert dq.count() >= 4   # at least one row per (utility, status) + per utility for circuits

    # ---- audit columns present everywhere ---- #
    for table in [
        f"{ctx.bronze_schema}.utility1_circuits",
        f"{ctx.silver_schema}.dim_feeder",
        f"{ctx.silver_schema}.fact_der",
        f"{ctx.platinum_schema}.feeder_capacity",
    ]:
        cols = spark.table(table).columns
        assert "pipeline_run_id" in cols, f"{table} missing pipeline_run_id"
        assert "batch_date" in cols, f"{table} missing batch_date"
        assert "ingestion_ts" in cols, f"{table} missing ingestion_ts"


def test_silver_idempotency(spark, integration_ctx):
    """
    Running silver twice with the same batch_date must produce the same row
    counts. This catches MERGE bugs that would silently double-count.
    """
    ctx = integration_ctx
    run_bronze(spark, ctx)
    run_silver(spark, ctx)

    feeders_after_first  = spark.table(f"{ctx.silver_schema}.dim_feeder").count()
    der_after_first      = spark.table(f"{ctx.silver_schema}.fact_der").count()

    run_silver(spark, ctx)   # second run, same batch_date

    feeders_after_second = spark.table(f"{ctx.silver_schema}.dim_feeder").count()
    der_after_second     = spark.table(f"{ctx.silver_schema}.fact_der").count()

    assert feeders_after_first == feeders_after_second
    assert der_after_first     == der_after_second
