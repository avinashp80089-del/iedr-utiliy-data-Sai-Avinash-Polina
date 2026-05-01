"""
Configuration loader.

Why a config layer at all
-------------------------
The case prompt says "5 large utilities." Hardcoding utility-specific column
names and file patterns in code means every new utility = a code change + a
deploy. Pulling that to YAML means:

    Adding utility 3 = drop in config/utilities/utility3.yaml + register an
    adapter class. The orchestration code is untouched.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


def _config_root() -> Path:
    """
    Resolve the config/ directory.

    Priority:
    1. IEDR_CONFIG_ROOT env var (set by notebooks pointing at bundle files)
    2. repo-root ./config directory (local dev)
    """
    env_root = os.environ.get("IEDR_CONFIG_ROOT")
    if env_root:
        return Path(env_root)

    repo_root_config = Path.cwd() / "config"
    if repo_root_config.exists():
        return repo_root_config

    raise FileNotFoundError(
        "Cannot find config/ directory. "
        "Set IEDR_CONFIG_ROOT environment variable or run from the repo root."
    )


def load_pipeline_config(env: str = "dev") -> dict[str, Any]:
    """Load top-level pipeline config — which utilities are active per env."""
    path = _config_root() / "pipeline.yaml"
    with open(path) as f:
        full = yaml.safe_load(f)
    if env not in full["environments"]:
        raise ValueError(f"Env '{env}' not declared in pipeline.yaml")
    return full["environments"][env]


def load_utility_config(utility_id: str) -> dict[str, Any]:
    """Load a single utility's config (column mappings, file patterns)."""
    path = _config_root() / "utilities" / f"{utility_id}.yaml"
    if not path.exists():
        raise FileNotFoundError(
            f"No config found for {utility_id} at {path}. "
            f"Did you add the YAML when onboarding this utility?"
        )
    with open(path) as f:
        return yaml.safe_load(f)
