"""Action Executor schemas."""

from __future__ import annotations
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class ActionRequest(BaseModel):
    """A single self-healing action to execute."""
    order: int
    action: str  # rollback_deployment, scale_deployment, restart_pod, redis_config_set, mysql_failover, plc_parameter_rollback
    target: str = ""
    command: str = ""
    parameters: Dict[str, Any] = Field(default_factory=dict)
    expected_effect: str = ""
    execution_timeout_seconds: int = 30
    dry_run: bool = False
    risk_level: str = "low"  # low | medium | high


class ActionResult(BaseModel):
    """Result of executing a single action."""
    order: int
    action: str
    target: str
    status: str  # success | failed | skipped | dry_run
    output: Dict[str, Any] = Field(default_factory=dict)
    error: str = ""
    duration_ms: int = 0


class ExecutionRequest(BaseModel):
    """Supervisor or HITL Gateway calls this to execute a self-healing plan."""
    supervisor_trace_id: str
    arbitration_id: str = ""
    actions: List[ActionRequest] = Field(default_factory=list)
    rollback_playbook: Dict[str, Any] = Field(default_factory=dict)
    dry_run: bool = False


class ExecutionResponse(BaseModel):
    supervisor_trace_id: str
    status: str  # success | partial_failure | failed | dry_run
    results: List[ActionResult] = Field(default_factory=list)
    rollback_triggered: bool = False
    total_duration_ms: int = 0
