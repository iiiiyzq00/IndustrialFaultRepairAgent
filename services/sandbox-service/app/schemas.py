"""
Pydantic models for the Digital Twin Sandbox Service.

The sandbox receives a proposed self-healing plan plus incident context,
simulates applying the actions, and returns a safety verdict.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


# ─── Request ───────────────────────────────────────────────────

class SandboxVerifyRequest(BaseModel):
    """Request from Supervisor to verify a self-healing plan in the sandbox."""
    supervisor_trace_id: str
    incident: Dict[str, Any] = Field(default_factory=dict)
    arbitration_result: Dict[str, Any] = Field(default_factory=dict)
    self_healing_plan: Dict[str, Any] = Field(default_factory=dict)
    expert_results: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    rag_context: Dict[str, Any] = Field(default_factory=dict)


# ─── Simulation output ─────────────────────────────────────────

class SimulatedMetricPoint(BaseModel):
    """One simulated metric data point after an action is applied."""
    metric_name: str = ""
    node_id: str = ""
    pre_action_value: float = 0.0
    post_action_value: float = 0.0
    delta_pct: float = 0.0
    trend: str = "stable"  # improving | stable | degrading
    evaluated_at_step: int = 0


class RiskItem(BaseModel):
    """A single risk identified during sandbox simulation."""
    risk_id: str = ""
    description: str = ""
    severity: str = "low"  # low | medium | high | critical
    probability: float = 0.0  # 0..1
    affected_components: List[str] = Field(default_factory=list)
    trigger_condition: str = ""
    from_historical_case: bool = False
    historical_case_id: str = ""


# ─── Response ──────────────────────────────────────────────────

class SandboxVerifyResponse(BaseModel):
    """Verdict from the sandbox after simulating the proposed actions."""
    supervisor_trace_id: str = ""
    verdict: str = "safe"  # safe | needs_modification | blocked
    confidence: float = 0.0  # 0..1
    safe: bool = True  # convenience: True if verdict == "safe"
    risks: List[RiskItem] = Field(default_factory=list)
    simulated_metrics: List[SimulatedMetricPoint] = Field(default_factory=list)
    alternative_actions: List[Dict[str, Any]] = Field(default_factory=list)
    reasoning: str = ""
    historical_cases_referenced: List[str] = Field(default_factory=list)
    simulation_duration_ms: int = 0
    fallback: bool = False  # True if sandbox degraded gracefully
