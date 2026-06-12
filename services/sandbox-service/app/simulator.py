"""
Action Effect Simulator — predicts how self-healing actions change system metrics.

Uses predefined action→effect mappings based on known fault-repair patterns.
These are used as inputs to the LLM safety evaluation, not as authoritative
predictions — the LLM has the final say on risk assessment.

Design principle:
  - Each action type has a list of expected_effects (metric, direction, magnitude)
  - Direction: "improving" (metric returns to normal), "degrading" (could worsen),
    "spike_then_recover" (transient degradation then improvement)
  - Magnitude: rough multiplier on the current anomalous value
"""

from __future__ import annotations

import copy
import logging
from typing import Any, Dict, List

from .schemas import SimulatedMetricPoint, RiskItem

logger = logging.getLogger(__name__)

# ─── Action → Expected Metric Effects ───────────────────────────
#
# Each entry describes what typically happens to metrics when this
# action is applied to fix a fault.
#
# Fields:
#   metric_name:     the metric affected
#   direction:       "improving" | "degrading" | "spike_then_recover" | "neutral"
#   magnitude_pct:   typical % change from the anomalous value
#   confidence:      how consistently this effect is observed (0..1)

ACTION_EFFECTS: Dict[str, List[Dict[str, Any]]] = {
    # ── K8s / container actions ──
    "rollback_deployment": [
        {"metric_name": "p99_latency_ms",    "direction": "improving",         "magnitude_pct": -85, "confidence": 0.90},
        {"metric_name": "error_rate",        "direction": "improving",         "magnitude_pct": -90, "confidence": 0.85},
        {"metric_name": "cpu_usage",         "direction": "improving",         "magnitude_pct": -60, "confidence": 0.80},
        {"metric_name": "memory_usage",      "direction": "improving",         "magnitude_pct": -50, "confidence": 0.75},
    ],
    "rollback": [  # alias
        {"metric_name": "p99_latency_ms",    "direction": "improving",         "magnitude_pct": -85, "confidence": 0.90},
        {"metric_name": "error_rate",        "direction": "improving",         "magnitude_pct": -90, "confidence": 0.85},
    ],
    "restart_pod": [
        {"metric_name": "p99_latency_ms",    "direction": "spike_then_recover","magnitude_pct": +40, "confidence": 0.70},
        {"metric_name": "error_rate",        "direction": "spike_then_recover","magnitude_pct": +20, "confidence": 0.65},
        {"metric_name": "cpu_usage",         "direction": "improving",         "magnitude_pct": -40, "confidence": 0.75},
        {"metric_name": "memory_usage",      "direction": "improving",         "magnitude_pct": -70, "confidence": 0.80},
    ],
    "restart": [  # alias
        {"metric_name": "cpu_usage",         "direction": "improving",         "magnitude_pct": -40, "confidence": 0.75},
        {"metric_name": "memory_usage",      "direction": "improving",         "magnitude_pct": -70, "confidence": 0.80},
    ],
    "scale": [
        {"metric_name": "queue_depth",       "direction": "improving",         "magnitude_pct": -70, "confidence": 0.85},
        {"metric_name": "p99_latency_ms",    "direction": "improving",         "magnitude_pct": -50, "confidence": 0.80},
        {"metric_name": "cpu_usage",         "direction": "degrading",         "magnitude_pct": +15, "confidence": 0.60},
    ],
    "scale_deployment": [
        {"metric_name": "queue_depth",       "direction": "improving",         "magnitude_pct": -70, "confidence": 0.85},
        {"metric_name": "p99_latency_ms",    "direction": "improving",         "magnitude_pct": -50, "confidence": 0.80},
        {"metric_name": "cpu_usage",         "direction": "degrading",         "magnitude_pct": +15, "confidence": 0.60},
    ],

    # ── Middleware actions ──
    "redis_config_set": [
        {"metric_name": "p99_latency_ms",    "direction": "improving",         "magnitude_pct": -70, "confidence": 0.80},
        {"metric_name": "blocked_clients",   "direction": "improving",         "magnitude_pct": -95, "confidence": 0.90},
        {"metric_name": "cpu_usage",         "direction": "improving",         "magnitude_pct": -30, "confidence": 0.70},
    ],
    "mysql_failover": [
        {"metric_name": "error_rate",        "direction": "spike_then_recover","magnitude_pct": +300,"confidence": 0.60},
        {"metric_name": "p99_latency_ms",    "direction": "spike_then_recover","magnitude_pct": +100,"confidence": 0.60},
        {"metric_name": "connection_count",  "direction": "spike_then_recover","magnitude_pct": +50, "confidence": 0.55},
    ],
    "kill_query": [
        {"metric_name": "p99_latency_ms",    "direction": "improving",         "magnitude_pct": -60, "confidence": 0.75},
        {"metric_name": "cpu_usage",         "direction": "improving",         "magnitude_pct": -30, "confidence": 0.70},
    ],

    # ── Industrial / PLC actions (HIGH RISK) ──
    "plc_parameter_rollback": [
        {"metric_name": "vibration_um",      "direction": "improving",         "magnitude_pct": -80, "confidence": 0.70},
        {"metric_name": "temperature_c",     "direction": "improving",         "magnitude_pct": -40, "confidence": 0.65},
        {"metric_name": "comms_latency_ms",  "direction": "improving",         "magnitude_pct": -50, "confidence": 0.60},
        {"metric_name": "joint_deviation_deg","direction": "improving",        "magnitude_pct": -60, "confidence": 0.55},
    ],
    "cnc_parameter_adjust": [
        {"metric_name": "vibration_um",      "direction": "improving",         "magnitude_pct": -60, "confidence": 0.60},
        {"metric_name": "joint_deviation_deg","direction": "improving",        "magnitude_pct": -50, "confidence": 0.55},
    ],
    "emergency_stop": [
        {"metric_name": "*",                 "direction": "degrading",         "magnitude_pct": -100,"confidence": 1.00},
        # All metrics drop to 0 — production line halts. This is a LAST RESORT.
    ],

    # ── Network actions ──
    "network_traffic_shift": [
        {"metric_name": "p99_latency_ms",    "direction": "improving",         "magnitude_pct": -80, "confidence": 0.80},
        {"metric_name": "error_rate",        "direction": "improving",         "magnitude_pct": -85, "confidence": 0.75},
        {"metric_name": "packet_loss_pct",   "direction": "improving",         "magnitude_pct": -95, "confidence": 0.85},
    ],
    "dns_failover": [
        {"metric_name": "p99_latency_ms",    "direction": "improving",         "magnitude_pct": -60, "confidence": 0.75},
        {"metric_name": "error_rate",        "direction": "improving",         "magnitude_pct": -70, "confidence": 0.80},
    ],

    # ── K8s actions (additional) ──
    "scale_down": [
        {"metric_name": "cpu_usage",         "direction": "improving",         "magnitude_pct": -30, "confidence": 0.70},
        {"metric_name": "queue_depth",       "direction": "neutral",           "magnitude_pct": +5,  "confidence": 0.60},
    ],
}


# ─── Known cross-action risks ───────────────────────────────────
#
# Patterns where a "fix" for one problem commonly causes another.

CROSS_ACTION_RISKS: List[Dict[str, Any]] = [
    {
        "action": "restart_pod",
        "risk": "Pod 重启期间短暂不可用，可能导致调用方超时和错误率上升",
        "affected": ["error_rate", "p99_latency_ms"],
        "severity": "low",
        "duration_estimate_seconds": 15,
    },
    {
        "action": "scale_deployment",
        "risk": "扩容可能耗尽集群资源配额，触发其他 Pod 被驱逐",
        "affected": ["memory_usage", "cpu_usage"],
        "severity": "medium",
        "duration_estimate_seconds": 0,
    },
    {
        "action": "mysql_failover",
        "risk": "主备切换期间可能有短暂数据不一致窗口，且切换后原主库需要重建",
        "affected": ["error_rate", "p99_latency_ms"],
        "severity": "high",
        "duration_estimate_seconds": 30,
    },
    {
        "action": "plc_parameter_rollback",
        "risk": "PLC 参数回滚可能影响产线其他工位的时序同步，需确认上下游联动",
        "affected": ["joint_deviation_deg", "comms_latency_ms"],
        "severity": "high",
        "duration_estimate_seconds": 0,
    },
    {
        "action": "emergency_stop",
        "risk": "紧急停机会导致产线全线停产，恢复时间未知，仅应作为最后手段",
        "affected": ["*"],
        "severity": "critical",
        "duration_estimate_seconds": 0,
    },
    {
        "action": "cnc_parameter_adjust",
        "risk": "CNC 参数调整可能影响加工精度，需确认 G-code 兼容性",
        "affected": ["vibration_um", "joint_deviation_deg"],
        "severity": "high",
        "duration_estimate_seconds": 0,
    },
]


# ─── Public API ──────────────────────────────────────────────────

def simulate_actions(
    plan: Dict[str, Any],
    incident: Dict[str, Any],
) -> List[SimulatedMetricPoint]:
    """
    Simulate the expected metric changes if the proposed actions are applied.

    Args:
        plan: SelfHealingPlan dict with 'actions' list
        incident: IncidentEvent dict with severity, node info, alert data

    Returns:
        List of SimulatedMetricPoint showing pre/post action values
    """
    actions = plan.get("actions", [])
    alerts = incident.get("aggregated_alerts", [])
    node_id = incident.get("node_id", "unknown")

    # Build a map of current metric values from alerts
    current_metrics: Dict[str, float] = {}
    for alert in alerts:
        mt = alert.get("metric_type", "")
        cv = alert.get("current_value", 0)
        if mt and cv:
            current_metrics[mt] = float(cv)

    simulated: List[SimulatedMetricPoint] = []
    step = 0

    for action in actions:
        step += 1
        action_type = action.get("action", "")
        target = action.get("target", node_id)

        # Get expected effects for this action type
        effects = ACTION_EFFECTS.get(action_type, [])
        if not effects:
            # Unknown action — assume neutral
            for mt, cv in current_metrics.items():
                simulated.append(SimulatedMetricPoint(
                    metric_name=mt, node_id=target,
                    pre_action_value=cv, post_action_value=cv,
                    delta_pct=0.0, trend="stable", evaluated_at_step=step,
                ))
            continue

        for effect in effects:
            metric_name = effect["metric_name"]
            if metric_name == "*":
                continue  # wildcard handled separately

            direction = effect["direction"]
            magnitude_pct = effect["magnitude_pct"]

            # Get current value (use alert data, or default)
            pre_value = current_metrics.get(metric_name, _default_for_metric(metric_name))

            # Compute post-action value
            if direction == "improving":
                # Value moves toward baseline
                post_value = pre_value * (1.0 + magnitude_pct / 100.0)
            elif direction == "degrading":
                # Value worsens
                post_value = pre_value * (1.0 + abs(magnitude_pct) / 100.0)
            elif direction == "spike_then_recover":
                # Simulate the spike peak (we show the worst case for safety)
                post_value = pre_value * (1.0 + abs(magnitude_pct) / 100.0)
            else:
                post_value = pre_value

            delta_pct = round((post_value - pre_value) / max(pre_value, 0.001) * 100, 1)
            trend = _trend_label(direction)

            simulated.append(SimulatedMetricPoint(
                metric_name=metric_name,
                node_id=target,
                pre_action_value=round(pre_value, 2),
                post_action_value=round(post_value, 2),
                delta_pct=delta_pct,
                trend=trend,
                evaluated_at_step=step,
            ))

    return simulated


def evaluate_safety(
    simulated_metrics: List[SimulatedMetricPoint],
    plan: Dict[str, Any],
) -> List[RiskItem]:
    """
    Evaluate whether simulated metric changes would trigger any safety thresholds.

    Returns list of RiskItem for any detected risks.
    """
    risks: List[RiskItem] = []
    actions = plan.get("actions", [])

    # Check each simulated metric against known danger thresholds
    for sm in simulated_metrics:
        # Error rate above 10% is dangerous
        if sm.metric_name == "error_rate" and sm.post_action_value > 10.0:
            risks.append(RiskItem(
                risk_id=f"rule-err-{sm.metric_name}",
                description=f"模拟显示 {sm.metric_name} 在 {sm.node_id} 上可能升至 {sm.post_action_value:.1f}%，超过 10% 安全阈值",
                severity="high",
                probability=0.7,
                affected_components=[sm.node_id],
                trigger_condition=f"{sm.metric_name} > 10%",
            ))

        # P99 latency above 2000ms is dangerous
        if sm.metric_name == "p99_latency_ms" and sm.post_action_value > 2000.0:
            risks.append(RiskItem(
                risk_id=f"rule-lat-{sm.metric_name}",
                description=f"模拟显示 {sm.metric_name} 在 {sm.node_id} 上可能升至 {sm.post_action_value:.0f}ms，超过 2000ms 安全阈值",
                severity="medium",
                probability=0.6,
                affected_components=[sm.node_id],
                trigger_condition=f"{sm.metric_name} > 2000ms",
            ))

        # CPU above 95% is dangerous
        if sm.metric_name == "cpu_usage" and sm.post_action_value > 95.0:
            risks.append(RiskItem(
                risk_id=f"rule-cpu-{sm.metric_name}",
                description=f"模拟显示 {sm.metric_name} 在 {sm.node_id} 上可能升至 {sm.post_action_value:.1f}%，接近资源耗尽",
                severity="medium",
                probability=0.5,
                affected_components=[sm.node_id],
                trigger_condition=f"{sm.metric_name} > 95%",
            ))

    # Check for cross-action risks
    for action in actions:
        action_type = action.get("action", "")
        for car in CROSS_ACTION_RISKS:
            if car["action"] == action_type:
                risks.append(RiskItem(
                    risk_id=f"cross-{action_type}",
                    description=car["risk"],
                    severity=car["severity"],
                    probability=0.7,
                    affected_components=car["affected"],
                    trigger_condition=f"执行 {action_type} 时",
                ))

    return risks


def get_alternative_actions(blocked_action: str) -> List[Dict[str, Any]]:
    """Suggest safer alternatives for a blocked/risky action."""
    alternatives: Dict[str, List[Dict[str, Any]]] = {
        "emergency_stop": [
            {"action": "scale_deployment", "target": "", "command": "先尝试水平扩容降低负载",
             "expected_effect": "增加服务副本数降低单点压力，避免全线停机"},
            {"action": "restart_pod", "target": "", "command": "尝试滚动重启问题 Pod",
             "expected_effect": "通过重启清理异常状态，不影响其他实例"},
        ],
        "mysql_failover": [
            {"action": "kill_query", "target": "", "command": "先尝试 kill 阻塞查询",
             "expected_effect": "终止长时间运行的查询，可能避免主备切换"},
        ],
        "plc_parameter_rollback": [
            {"action": "cnc_parameter_adjust", "target": "", "command": "先校准关联的 CNC 参数",
             "expected_effect": "可能避免直接动 PLC 参数"},
        ],
    }
    return alternatives.get(blocked_action, [])


# ─── Helpers ────────────────────────────────────────────────────

def _trend_label(direction: str) -> str:
    return {
        "improving": "improving",
        "degrading": "degrading",
        "spike_then_recover": "transient_spike",
        "neutral": "stable",
    }.get(direction, "stable")


def _default_for_metric(metric_name: str) -> float:
    """Return a plausible default value for a metric type."""
    defaults = {
        "p99_latency_ms": 80.0,
        "p50_latency_ms": 20.0,
        "avg_latency_ms": 30.0,
        "error_rate": 0.5,
        "cpu_usage": 30.0,
        "memory_usage": 45.0,
        "disk_io_mbps": 50.0,
        "queue_depth": 10.0,
        "vibration_um": 5.0,
        "temperature_c": 35.0,
        "joint_deviation_deg": 0.1,
        "packet_loss_pct": 0.0,
        "comms_latency_ms": 2.0,
        "connection_count": 100.0,
        "blocked_clients": 0.0,
    }
    return defaults.get(metric_name, 1.0)
