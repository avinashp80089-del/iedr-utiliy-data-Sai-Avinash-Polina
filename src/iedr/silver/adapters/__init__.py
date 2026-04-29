"""
Utility adapter registry.

Adding a new utility (NY has 8):
  1. Add config/utilities/utility3.yaml
  2. Add Utility3Adapter(UtilityAdapter) in utility3.py
  3. Register it in ADAPTERS below
  4. Add 'utility3' to config/pipeline.yaml for the target env

Nothing else changes — bronze, silver orchestration, platinum, tests,
and CI/CD all stay untouched.
"""

from iedr.silver.adapters.base import UtilityAdapter
from iedr.silver.adapters.utility1 import Utility1Adapter
from iedr.silver.adapters.utility2 import Utility2Adapter

ADAPTERS: dict[str, type[UtilityAdapter]] = {
    "utility1": Utility1Adapter,
    "utility2": Utility2Adapter,
    # "utility3": Utility3Adapter,  # ← onboard a new utility by adding here + a YAML
}

__all__ = ["UtilityAdapter", "Utility1Adapter", "Utility2Adapter", "ADAPTERS"]
