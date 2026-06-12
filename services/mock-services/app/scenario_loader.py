"""
YAML scenario loader with runtime switching.

Each mock type (k8s/redis/network) has its own YAML file under
configs/mock_scenarios/.  A scenario is a named dict inside the file:

    scenarios:
      default:         { ... }
      oom:             { ... }
      slow_query:      { ... }

Runtime switching is exposed via:
    POST /scenario/{name}     -- switch the active scenario
    GET  /scenario            -- list available scenarios + show current

Thread-safe: uses a threading.Lock to protect the active scenario pointer.
"""

from __future__ import annotations

import os
import threading
import logging
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict

import yaml

logger = logging.getLogger(__name__)

SCENARIO_DIR = Path(os.getenv("SCENARIO_PATH", "/app/scenarios"))
_lock = threading.Lock()


class ScenarioRegistry:
    """Holds all scenarios for a single mock type and tracks the active one."""

    def __init__(self, mock_type: str):
        self.mock_type = mock_type
        self._scenarios: Dict[str, dict] = {}
        self._active_name: str = "default"
        self._loaded = False

    # ------------------------------------------------------------------
    # load
    # ------------------------------------------------------------------

    def load(self) -> None:
        """Load scenarios from YAML file.  Called once at startup."""
        yaml_path = SCENARIO_DIR / f"{self.mock_type}_scenarios.yaml"
        if not yaml_path.exists():
            logger.warning("Scenario file not found: %s — using empty registry", yaml_path)
            self._scenarios = {"default": {}}
            self._loaded = True
            return

        with open(yaml_path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)

        self._scenarios = raw.get("scenarios", {}) if raw else {}
        if not self._scenarios:
            self._scenarios = {"default": {}}

        # If there is a global_config section, merge it into every scenario
        global_cfg = raw.get("global_config", {}) if raw else {}
        if global_cfg:
            for name in self._scenarios:
                self._scenarios[name] = _deep_merge(
                    deepcopy(global_cfg), self._scenarios[name]
                )

        self._loaded = True
        logger.info(
            "Loaded %d scenarios for mock_type=%s (active=%s)",
            len(self._scenarios),
            self.mock_type,
            self._active_name,
        )

    def reload(self) -> None:
        """Re-read the YAML file on disk (useful during development)."""
        with _lock:
            self._loaded = False
            self.load()

    # ------------------------------------------------------------------
    # access
    # ------------------------------------------------------------------

    @property
    def active_name(self) -> str:
        return self._active_name

    def get_active(self) -> dict:
        """Return a **deep copy** of the active scenario data."""
        with _lock:
            if not self._loaded:
                self.load()
            data = self._scenarios.get(self._active_name, self._scenarios.get("default", {}))
            return deepcopy(data)

    def get(self, name: str) -> dict | None:
        """Return a specific scenario by name (deep copy)."""
        with _lock:
            if not self._loaded:
                self.load()
            data = self._scenarios.get(name)
            return deepcopy(data) if data else None

    def set_active(self, name: str) -> bool:
        """Switch the active scenario. Returns True on success."""
        with _lock:
            if not self._loaded:
                self.load()
            if name not in self._scenarios:
                logger.warning(
                    "Scenario '%s' not found in %s registry. Available: %s",
                    name,
                    self.mock_type,
                    list(self._scenarios.keys()),
                )
                return False
            self._active_name = name
            logger.info("Switched %s scenario → %s", self.mock_type, name)
            return True

    def list_scenarios(self) -> list[str]:
        with _lock:
            if not self._loaded:
                self.load()
            return sorted(self._scenarios.keys())


# ------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------

def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge *override* into *base*.  Lists are replaced, not merged."""
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = deepcopy(value)
    return base
