"""
Metric value generator with day-of-week and time-of-day patterns.

Each metric has a baseline value plus:
  - Diurnal pattern:  ±10% depending on hour of day (8-18h = peak production)
  - Weekly pattern:   weekdays full baseline, weekends 60% baseline
  - Random noise:     gaussian around the computed mean, clipped to [min, max]
  - Anomaly injection: overrides the normal value (see anomaly_scenarios.yaml)
"""

from __future__ import annotations

import math
import random
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .node_registry import Node, metric_templates_for

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Time-of-day / day-of-week multipliers
# ---------------------------------------------------------------------------

def _diurnal_multiplier(ts: datetime) -> float:
    """
    Simulate a production day:
      - 08:00–18:00  → peak     (1.0)
      - 18:00–22:00  → ramp down (0.7)
      - 22:00–06:00  → night    (0.5)
      - 06:00–08:00  → ramp up  (0.8)
    """
    h = ts.hour + ts.minute / 60.0
    if 8 <= h < 18:
        return 1.0
    if 18 <= h < 22:
        return 0.85
    if 22 <= h or h < 6:
        return 0.55
    return 0.75  # 6–8 am


def _weekly_multiplier(ts: datetime) -> float:
    """Weekdays = 1.0, Saturday = 0.7, Sunday = 0.5."""
    wd = ts.weekday()  # 0=Mon .. 6=Sun
    if wd < 5:
        return 1.0
    if wd == 5:
        return 0.7
    return 0.5


# ---------------------------------------------------------------------------
# Anomaly injection
# ---------------------------------------------------------------------------

@dataclass
class AnomalyRule:
    """A single anomaly-injection rule loaded from YAML."""
    name: str
    target_nodes: List[str]          # node_id patterns (supports prefix match)
    target_metrics: List[str]        # metric names to affect
    multiplier: float = 1.0          # multiply baseline by this
    adder: float = 0.0               # add this after multiplication
    fixed_value: Optional[float] = None  # if set, ignore multiplier/adder
    duration_seconds: int = 300      # how long the anomaly lasts
    start_offset_seconds: int = 0    # seconds after scenario activation


class AnomalyInjector:
    """
    Manages active anomaly rules and applies them to metric values.

    Rules are activated via `activate_scenario(name)` which looks up
    a named scenario in the YAML config.
    """

    def __init__(self):
        self._scenarios: Dict[str, List[AnomalyRule]] = {}
        self._active_rules: List[AnomalyRule] = []
        self._activated_at: Optional[float] = None  # monotonic time

    def load_scenarios(self, scenarios_dict: Dict[str, Any]) -> None:
        """Parse YAML config into AnomalyRule objects."""
        self._scenarios.clear()
        for scenario_name, rules_list in scenarios_dict.items():
            parsed: List[AnomalyRule] = []
            for rule in rules_list:
                parsed.append(AnomalyRule(
                    name=rule.get("name", scenario_name),
                    target_nodes=rule.get("target_nodes", []),
                    target_metrics=rule.get("target_metrics", []),
                    multiplier=rule.get("multiplier", 1.0),
                    adder=rule.get("adder", 0.0),
                    fixed_value=rule.get("fixed_value"),
                    duration_seconds=rule.get("duration_seconds", 300),
                    start_offset_seconds=rule.get("start_offset_seconds", 0),
                ))
            self._scenarios[scenario_name] = parsed
        logger.info("Loaded %d anomaly scenarios", len(self._scenarios))

    def activate_scenario(self, name: str) -> bool:
        """Activate a named scenario (replaces any active rules)."""
        rules = self._scenarios.get(name)
        if rules is None:
            logger.warning("Anomaly scenario '%s' not found. Available: %s",
                           name, list(self._scenarios.keys()))
            return False
        self._active_rules = list(rules)
        self._activated_at = _monotonic_seconds()
        logger.info("Anomaly scenario '%s' activated (%d rules)", name, len(rules))
        return True

    def deactivate_all(self) -> None:
        self._active_rules.clear()
        self._activated_at = None
        logger.info("All anomaly rules deactivated")

    def apply(self, node_id: str, metric_name: str, normal_value: float) -> float:
        """
        Apply active anomaly rules to *normal_value*.
        Returns the (possibly modified) value.
        """
        elapsed = _monotonic_seconds() - self._activated_at if self._activated_at else float("inf")
        for rule in self._active_rules:
            if elapsed < rule.start_offset_seconds:
                continue
            if elapsed > rule.start_offset_seconds + rule.duration_seconds:
                continue
            if not _node_matches(rule.target_nodes, node_id):
                continue
            if rule.target_metrics and metric_name not in rule.target_metrics:
                continue
            if rule.fixed_value is not None:
                return rule.fixed_value
            return normal_value * rule.multiplier + rule.adder
        return normal_value

    @property
    def active_scenario_name(self) -> Optional[str]:
        if not self._active_rules:
            return None
        return self._active_rules[0].name if self._active_rules else None


# ---------------------------------------------------------------------------
# Metric stream generator
# ---------------------------------------------------------------------------

class MetricGenerator:
    """
    Generates metric data points for all registered nodes.

    Usage:
        gen = MetricGenerator(nodes, injector)
        for point in gen.tick():
            producer.send(point)
    """

    def __init__(self, nodes: List[Node], injector: AnomalyInjector,
                 time_multiplier: float = 1.0):
        self.nodes = nodes
        self.injector = injector
        self.time_multiplier = time_multiplier  # >1 for accelerated simulation
        self._tick_count = 0

    def tick(self, ts: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Generate one round of metrics for all nodes.

        Returns a list of JSON-serialisable dicts, one per (node, metric).
        """
        if ts is None:
            ts = datetime.now(timezone.utc)

        points: List[Dict[str, Any]] = []
        diurnal = _diurnal_multiplier(ts)
        weekly = _weekly_multiplier(ts)

        for node in self.nodes:
            templates = metric_templates_for(node.node_type)
            for tmpl in templates:
                # Compute the normal (non-anomalous) value
                base = tmpl["baseline"]
                std = tmpl["std"]

                # Diurnal + weekly adjustment
                adjusted_base = base * diurnal * weekly

                # Add gaussian noise
                normal_value = random.gauss(adjusted_base, std)

                # Clamp to [min, max]
                normal_value = max(tmpl.get("min", 0), min(tmpl.get("max", float("inf")), normal_value))

                # Apply anomaly injection
                final_value = self.injector.apply(node.node_id, tmpl["name"], normal_value)

                point = {
                    "timestamp": int(ts.timestamp() * 1000),
                    "node_id": node.node_id,
                    "node_type": node.node_type,
                    "line_profile": node.line_profile,
                    "metric_type": tmpl["name"],
                    "metric_value": round(final_value, 4),
                    "unit": tmpl["unit"],
                    "severity": _classify_severity(node.node_type, tmpl["name"], final_value, tmpl),
                    "tags": {**node.tags},
                    "batch_id": f"BATCH-{ts.strftime('%Y%m%d')}-{ts.hour:02d}",
                    "source": "FakeGenerator",
                    "firmware_version": "2.1.0",
                    "cycle_time_ms": 850,
                    "anomaly_active": self.injector.active_scenario_name,
                }
                points.append(point)

        self._tick_count += 1
        return points


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _monotonic_seconds() -> float:
    """Return monotonic seconds (uses time.monotonic in real impl)."""
    import time
    return time.monotonic()


def _node_matches(patterns: List[str], node_id: str) -> bool:
    """Check if node_id matches any prefix pattern."""
    if not patterns:
        return True  # empty list = match all
    for pat in patterns:
        if node_id.startswith(pat):
            return True
    return False


def _classify_severity(node_type: str, metric_name: str, value: float, tmpl: dict) -> str:
    """
    Classify a metric value into severity levels.

    Uses the metric template's baseline and std to compute deviation.
    """
    base = tmpl.get("baseline", 0)
    std = tmpl.get("std", 1)
    if std == 0:
        return "warning"

    sigma = abs(value - base) / std

    if sigma >= 4.0:
        return "critical"
    if sigma >= 3.0:
        return "major"
    if sigma >= 2.0:
        return "minor"
    return "warning"


