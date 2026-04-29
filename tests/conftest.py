"""Shared pytest fixtures."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from iedr.common.context import PipelineContext


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("iedr-pytest")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def ctx():
    return PipelineContext(env="test", catalog="spark_catalog")


@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    return Path(__file__).parent / "fixtures"
