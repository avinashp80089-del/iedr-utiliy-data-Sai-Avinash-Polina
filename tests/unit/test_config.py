"""Tests for the YAML config loader."""

from __future__ import annotations

import pytest

from iedr.common.config import load_pipeline_config, load_utility_config

pytestmark = pytest.mark.unit


def test_load_pipeline_config_dev():
    cfg = load_pipeline_config("dev")
    assert "utilities" in cfg
    assert "utility1" in cfg["utilities"]
    assert "utility2" in cfg["utilities"]


def test_load_pipeline_config_unknown_env_raises():
    with pytest.raises(ValueError, match="not declared"):
        load_pipeline_config("does-not-exist")


def test_load_utility1_config_has_required_keys():
    cfg = load_utility_config("utility1")
    assert cfg["utility_id"] == "utility1"
    assert set(cfg["datasets"]) == {"circuits", "install_der", "planned_der"}
    assert cfg["status_to_dataset"]["installed"] == "install_der"
    assert cfg["status_to_dataset"]["planned"]   == "planned_der"
    # The DER type map must include at least the core types — guards against
    # accidental YAML truncation
    assert "SolarPV" in cfg["der_type_map"]
    assert "Wind" in cfg["der_type_map"]
    assert "FuelCell" in cfg["der_type_map"]


def test_load_utility2_config_has_required_keys():
    cfg = load_utility_config("utility2")
    assert cfg["utility_id"] == "utility2"
    # U2 install vs planned use different PK columns — explicit check
    assert cfg["columns"]["install_der"]["der_id"] == "DER_ID"
    assert cfg["columns"]["planned_der"]["der_id"] == "INTERCONNECTION_QUEUE_REQUEST_ID"


def test_load_unknown_utility_raises():
    with pytest.raises(FileNotFoundError, match="No config found"):
        load_utility_config("utility_does_not_exist")
