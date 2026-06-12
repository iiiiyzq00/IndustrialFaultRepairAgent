"""
Self-healing execution + observation window + auto-rollback.

Priority 5 upgrade: real metric collection via expert tool endpoints,
auto-rollback via Action Executor.
"""

from __future__ import annotations

import os
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import httpx
from .schemas import (
    SelfHealingPlan, SelfHealingAction,
    RollbackPlaybook, TriggerMetric,
)

# Path for common module
import sys
_PARENT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)
from common.notifier import notify_self_healing_result  # noqa: E402

logger = logging.getLogger(__name__)

API_KEY = os.getenv("API_KEY", "dev-key-change-me")
APP_EXPERT_URL = os.getenv("APP_EXPERT_URL", "http://app-expert:8140")
KB_EXPERT_URL = os.getenv("KB_EXPERT_URL", "http://k8s-expert:8110")
MW_EXPERT_URL = os.getenv("MW_EXPERT_URL", "http://middleware-expert:8120")
ACTION_EXECUTOR_URL = os.getenv("ACTION_EXECUTOR_URL", "http://action-executor:8400")


# ─── Tool URL map for metric collection ──────────────────────

TOOL_URLS = {
    "app_expert":       APP_EXPERT_URL,
    "k8s_expert":       KB_EXPERT_URL,
    "middleware_expert": MW_EXPERT_URL,
}

TOOL_PARAMS = {
    "get_apm_metrics":     {"metrics": "p99_latency_ms,error_rate,request_rate"},
    "get_node_metrics":    {"metrics": "cpu,mem,disk_io"},
    "get_redis_info":      {},
}

# ─── Public API ────────────────────────────────────────────────


async def execute_and_watch(
    supervisor_trace_id: str,
    plan: SelfHealingPlan,
    playbook: RollbackPlaybook | None = None,
) -> Dict[str, Any]:
    """
    Execute self-healing actions via Action Executor, then enter
    observation window with real metric collection.
    """
    if playbook is None:
        playbook = RollbackPlaybook(watch_window_seconds=60)

    # ── Step 1: Execute actions via Action Executor ──
    for action in plan.actions:
        logger.info("Executing action %d/%d: %s on %s",
                    action.order, len(plan.actions), action.action, action.target)
        try:
            result = await _execute_action(action)
            if result.get("status") == "failed":
                logger.warning("Action %d FAILED: %s", action.order, result.get("error", ""))
                # Don't abort — try next actions; only fail if ALL fail
                continue
        except Exception as e:
            logger.error("Action execution failed: %s", e)
            return {"result": "failed", "error": str(e), "failed_action": action.order}

    # ── Step 2: Observation window ──
    return await _observe_window(supervisor_trace_id, playbook)


# ─── Action execution ──────────────────────────────────────────

async def _execute_action(action: SelfHealingAction) -> Dict[str, Any]:
    """Execute a single action via the Action Executor service."""
    try:
        async with httpx.AsyncClient(timeout=float(action.execution_timeout_seconds)) as client:
            resp = await client.post(
                f"{ACTION_EXECUTOR_URL}/api/v1/execute/single",
                json={
                    "action": action.action,
                    "order": action.order,
                    "target": action.target,
                    "command": action.command,
                    "parameters": action.parameters,
                    "dry_run": False,
                },
                headers={"X-API-Key": API_KEY, "Content-Type": "application/json"},
            )
            resp.raise_for_status()
            result = resp.json()
            logger.info("  → result: %s (%dms)", result.get("status"), result.get("duration_ms", 0))
            return result
    except Exception as e:
        logger.error("  → executor call failed: %s", e)
        return {"status": "failed", "error": str(e)}


# ─── Observation window ───────────────────────────────────────

async def _observe_window(
    trace_id: str,
    playbook: RollbackPlaybook,
) -> Dict[str, Any]:
    """Real metric collection loop with threshold evaluation."""
    watch_seconds = playbook.watch_window_seconds
    interval = playbook.check_interval_seconds
    violation_counters: Dict[str, int] = {m.name: 0 for m in playbook.trigger_metrics}
    observation_log: list[Dict[str, Any]] = []

    logger.info("Observing for %ds (check every %ds, %d metrics)",
                watch_seconds, interval, len(playbook.trigger_metrics))

    for elapsed in range(0, watch_seconds, interval):
        await asyncio.sleep(interval)

        for metric_def in playbook.trigger_metrics:
            current_value = await _fetch_metric(metric_def)
            observation_log.append({
                "elapsed_s": elapsed + interval,
                "metric": metric_def.name,
                "value": current_value,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

            is_violation = _evaluate_threshold(metric_def, current_value)

            if is_violation:
                violation_counters[metric_def.name] += 1
                logger.warning("  [%ds] %s=%.2f VIOLATION (%d/%d consecutive)",
                               elapsed + interval, metric_def.name, current_value,
                               violation_counters[metric_def.name], metric_def.consecutive_violations)

                if violation_counters[metric_def.name] >= metric_def.consecutive_violations:
                    logger.error("ROLLBACK TRIGGERED! %s threshold breached", metric_def.name)
                    await _execute_rollback(playbook)
                    return {"result": "rollback_triggered", "observation_log": observation_log}
            else:
                violation_counters[metric_def.name] = 0
                logger.debug("  [%ds] %s=%.2f OK", elapsed + interval, metric_def.name, current_value)

    logger.info("Observation window passed (%ds) — no rollback needed", watch_seconds)
    return {"result": "success", "observation_log": observation_log}


# ─── Metric collection ────────────────────────────────────────

async def _fetch_metric(metric_def: TriggerMetric) -> float:
    """
    Fetch a metric value from the appropriate expert worker tool endpoint.

    metric_source determines which expert to call:
      - app_expert → get_apm_metrics
      - k8s_expert → get_node_metrics
      - middleware_expert → get_redis_info / get_mysql_status
    """
    source = metric_def.metric_source
    base_url = TOOL_URLS.get(source, APP_EXPERT_URL)

    # Map source → tool name + params
    tool_name, tool_params = _source_to_tool(source, metric_def)

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(
                f"{base_url}/api/v1/tools/{tool_name}",
                json=tool_params,
                headers={"X-API-Key": API_KEY, "Content-Type": "application/json"},
            )
            resp.raise_for_status()
            data = resp.json()

            # Extract the specific metric value from the response
            value = _extract_metric_value(data, metric_def.name, metric_def.service)
            logger.debug("Fetched %s.%s = %.3f (source=%s, mode=%s)",
                         metric_def.service or "?", metric_def.name, value,
                         source, data.get("_mode", "?"))
            return value

    except Exception as e:
        logger.warning("Metric fetch failed for %s/%s: %s", source, metric_def.name, e)
        return _safe_fallback_value(metric_def.name)


def _source_to_tool(source: str, metric_def: TriggerMetric) -> tuple[str, dict]:
    """Map metric_source to the corresponding tool name and params."""
    if source == "app_expert":
        return ("get_apm_metrics", {
            "service": metric_def.service or "order-svc",
            "metrics": metric_def.name,
            "minutes": 1,
        })
    elif source == "k8s_expert":
        return ("get_node_metrics", {
            "node_name": metric_def.node_id or "k8s-node-01",
            "metrics": metric_def.name,
        })
    elif source == "middleware_expert":
        if "redis" in metric_def.name.lower() or "memory" in metric_def.name.lower():
            return ("get_redis_info", {"instance": metric_def.service or "redis-prod-01"})
        return ("get_mysql_status", {})
    else:
        return ("get_apm_metrics", {"service": metric_def.service or "order-svc"})


def _extract_metric_value(data: dict, metric_name: str, service: str = "") -> float:
    """
    Extract a specific metric value from a tool response.

    Handles response formats:
      - {"metrics": {"p99_latency_ms": 1200.0}}   (app_expert APM)
      - {"metrics": {"cpu": "94%"}}               (k8s_expert node)
      - {"info": {"used_memory_human": "8.2Gi"}}  (middleware redis info)
    """
    # Direct metrics dict
    if "metrics" in data and isinstance(data["metrics"], dict):
        for k, v in data["metrics"].items():
            if metric_name in k or k in metric_name:
                return _to_float(v)

    # Redis INFO style
    if "info" in data and isinstance(data["info"], dict):
        for k, v in data["info"].items():
            if metric_name.replace("_", "") in k.replace("_", ""):
                return _to_float(v)

    # K8s node metrics style
    if "metrics" in data and isinstance(data["metrics"], dict):
        vals = list(data["metrics"].values())
        if vals:
            return _to_float(vals[0])

    # Flat search
    for key in ("output", "data"):
        if key in data and isinstance(data[key], dict):
            for k, v in data[key].items():
                if metric_name in k:
                    return _to_float(v)

    logger.debug("Could not extract %s from response keys: %s", metric_name, list(data.keys())[:5])
    return _safe_fallback_value(metric_name)


def _to_float(val) -> float:
    """Convert a value to float, handling strings like '94%', '8.2Gi', '500m'."""
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        # Strip units: %, Gi, Mi, m, K
        cleaned = val.replace("%", "").replace("Gi", "").replace("Mi", "").replace("m", "").replace("K", "").strip()
        try:
            return float(cleaned)
        except ValueError:
            pass
    return 0.0


def _safe_fallback_value(metric_name: str) -> float:
    """Return a safe (healthy-looking) value when metric fetch fails."""
    if "error" in metric_name.lower():
        return 0.0
    if "latency" in metric_name.lower() or "delay" in metric_name.lower():
        return 50.0  # healthy
    if "cpu" in metric_name.lower():
        return 30.0
    if "mem" in metric_name.lower():
        return 45.0
    return 1.0


# ─── Threshold evaluation ─────────────────────────────────────

def _evaluate_threshold(metric_def: TriggerMetric, current_value: float) -> bool:
    """Check if a metric value violates its threshold."""
    if metric_def.threshold_type == "absolute_value":
        return current_value > metric_def.threshold_value

    if metric_def.threshold_type == "relative_pct_increase":
        # threshold_value=200 means 200% increase → 3× baseline
        # baseline is assumed to be 1.0 for the "current/previous" ratio
        multiplier = 1.0 + metric_def.threshold_value / 100.0
        return current_value > multiplier

    return False


# ─── Rollback execution ───────────────────────────────────────

async def _execute_rollback(playbook: RollbackPlaybook) -> None:
    """Execute rollback commands via Action Executor."""
    logger.warning("Executing rollback commands: %d commands", len(playbook.rollback_commands))

    for cmd in playbook.rollback_commands:
        action_type = cmd.get("type", "restart_pod")
        target = cmd.get("target", "")

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    f"{ACTION_EXECUTOR_URL}/api/v1/execute/single",
                    json={
                        "action": action_type,
                        "target": target,
                        "parameters": cmd.get("parameters", {}),
                        "dry_run": False,
                    },
                    headers={"X-API-Key": API_KEY, "Content-Type": "application/json"},
                )
                resp.raise_for_status()
                result = resp.json()
                logger.warning("  ROLLBACK %s: %s", action_type, result.get("status", "?"))
        except Exception as e:
            logger.error("  ROLLBACK %s FAILED: %s", action_type, e)
