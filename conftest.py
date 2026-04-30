"""Root conftest — re-exports all shared fixtures so subdirectories can find them."""

from __future__ import annotations

from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from iedr.common.context import PipelineContext


@pytest.fixture(scope="session")
def spark():
    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName("iedr-pytest")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


@pytest.fixture(scope="session")
def ctx():
    return PipelineContext(env="test", catalog="spark_catalog")


@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    return Path(__file__).parent / "tests" / "fixtures"