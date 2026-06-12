"""
Action dispatcher — routes action types to their handler functions.
"""

from __future__ import annotations

import time
import logging
from typing import Any, Dict

from .handlers.k8s_actions import rollback_deployment, scale_deployment, scale_down, restart_pod
from .handlers.middleware_actions import redis_config_set, mysql_failover, mysql_kill_query
from .handlers.industrial_actions import plc_parameter_rollback, cnc_parameter_adjust, emergency_stop
from .handlers.network_actions import network_traffic_shift, dns_failover

logger = logging.getLogger(__name__)

# ─── Action → handler mapping ─────────────────────────────────

HANDLERS: Dict[str, Any] = {
    # K8s (low risk)
    "rollback_deployment":  (rollback_deployment, "low"),
    "scale_deployment":     (scale_deployment, "low"),
    "scale_down":           (scale_down, "low"),
    "restart_pod":          (restart_pod, "low"),

    # Middleware
    "redis_config_set":     (redis_config_set, "medium"),
    "mysql_kill_query":     (mysql_kill_query, "medium"),
    "mysql_failover":       (mysql_failover, "high"),

    # Network
    "network_traffic_shift": (network_traffic_shift, "medium"),
    "dns_failover":          (dns_failover, "low"),

    # Industrial (high risk — always need HITL)
    "plc_parameter_rollback": (plc_parameter_rollback, "high"),
    "cnc_parameter_adjust":   (cnc_parameter_adjust, "high"),
    "emergency_stop":         (emergency_stop, "high"),
}


def execute_action(action_type: str, params: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
    """
    Execute a single action by type.

    Returns {"status": "success"|"failed"|"skipped", "output": {...}, "error": ""}
    """
    entry = HANDLERS.get(action_type)
    if not entry:
        # Map common LLM-generated action names to known handlers
        alias_map = {
            "rollback": "rollback_deployment", "restart": "restart_pod",
            "scale": "scale_deployment", "config_set": "redis_config_set",
            "kill_query": "mysql_kill_query", "failover": "mysql_failover",
            "plc_rollback": "plc_parameter_rollback",
        }
        for alias, target in alias_map.items():
            if alias in action_type.lower():
                entry = HANDLERS.get(target)
                if entry: break

    if not entry:
        return {"status": "skipped", "output": {"reason": f"Unknown action: {action_type}"},
                "error": "", "duration_ms": 0, "risk_level": "unknown"}

    handler_fn, risk_level = entry
    t0 = time.monotonic()

    try:
        if dry_run:
            params["_dry_run"] = True
            result = handler_fn(params, dry_run=True)
        else:
            result = handler_fn(params, dry_run=False)

        duration_ms = int((time.monotonic() - t0) * 1000)
        # In dry_run mode, always report success to validate the pipeline
        is_dry_run = dry_run or result.get("dry_run", False)
        status = "success" if (result.get("success") or is_dry_run) else "failed"

        return {
            "status": status,
            "output": result,
            "error": result.get("error", ""),
            "duration_ms": duration_ms,
            "risk_level": risk_level,
        }
    except Exception as e:
        duration_ms = int((time.monotonic() - t0) * 1000)
        logger.exception("Action %s failed with exception", action_type)
        return {
            "status": "failed",
            "output": {},
            "error": str(e),
            "duration_ms": duration_ms,
            "risk_level": risk_level,
        }


def get_risk_level(action_type: str) -> str:
    entry = HANDLERS.get(action_type)
    return entry[1] if entry else "unknown"
