"""
Pydantic models — shared between Supervisor, Experts, RAG, and HITL.

Mirrors the OpenAPI contract from Phase 2, with all corrections applied.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


# ─── Incident (Flink → Supervisor) ───────────────────────────

class AggregatedAlert(BaseModel):
    alert_id: str
    node_id: str
    node_type: str
    metric_type: str
    current_value: float
    baseline_mean: float
    baseline_std: float
    deviation_sigma: float
    severity: str
    tags: Dict[str, str] = Field(default_factory=dict)
    first_trigger_time: Optional[int] = None
    peak_value: Optional[float] = None


class IncidentEvent(BaseModel):
    incident_id: str
    trigger_time: str  # ISO-8601
    aggregation_window_seconds: int = 300
    priority_score: float = 0.0
    aggregated_alerts: List[AggregatedAlert] = Field(default_factory=list)
    affected_line_profile: str = "general"
    changeover_active: bool = False
    node_id: Optional[str] = None
    metric_group: Optional[str] = None
    alert_count: int = 0
    severity_max: str = "warning"


class IncidentAccepted(BaseModel):
    incident_id: str
    status: str  # accepted | queued | rejected
    supervisor_trace_id: str
    estimated_completion_seconds: int = 75


# ─── RAG ─────────────────────────────────────────────────────

class RAGDocument(BaseModel):
    ticket_id: str
    relevance_score: float
    phenomenon_summary: str = ""
    root_cause_summary: str = ""
    fix_steps: List[str] = Field(default_factory=list)
    fault_category: str = ""
    severity: str = ""
    confidence: float = 0.0
    match_reason: str = ""


class RAGRetrieveRequest(BaseModel):
    query: str
    top_k: int = 10
    filters: Optional[Dict[str, Any]] = None
    retrieval_strategy: str = "hybrid"


class RAGRetrieveResponse(BaseModel):
    query_id: str
    documents: List[RAGDocument] = Field(default_factory=list)
    retrieval_stats: Dict[str, Any] = Field(default_factory=dict)


class RAGContext(BaseModel):
    """Embedded inside DiagnoseRequest — pre-retrieved by Supervisor."""
    documents: List[RAGDocument] = Field(default_factory=list)
    retrieved_at: Optional[str] = None
    retrieval_query: str = ""


# ─── Expert Diagnose ─────────────────────────────────────────

class DiagnoseRequest(BaseModel):
    supervisor_trace_id: str
    incident: IncidentEvent
    domain_hypothesis: str = ""
    rag_context: Optional[RAGContext] = None
    override_rag: bool = False
    max_tool_calls: int = 10
    deadline_ms: int = 25_000


class ToolCallLog(BaseModel):
    tool_name: str
    call_order: int
    latency_ms: int
    success: bool
    summary: str = ""


class Evidence(BaseModel):
    tool_name: str
    output_summary: str
    raw_data_ref: str = ""


class DiagnoseResponse(BaseModel):
    agent_type: str
    trace_id: str
    findings: str
    suspected_root_cause: str
    confidence: float  # 0..1
    evidence: List[Evidence] = Field(default_factory=list)
    tool_call_log: List[ToolCallLog] = Field(default_factory=list)
    rag_documents_used: List[RAGDocument] = Field(default_factory=list)
    suggested_actions: List[Dict[str, Any]] = Field(default_factory=list)


# ─── Arbitration ─────────────────────────────────────────────

class SelfHealingAction(BaseModel):
    order: int
    action: str
    target: str = ""
    command: str = ""
    parameters: Dict[str, Any] = Field(default_factory=dict)
    expected_effect: str = ""
    execution_timeout_seconds: int = 30


class TriggerMetric(BaseModel):
    name: str
    metric_source: str  # app_expert | hbase_direct | k8s_expert | middleware_expert
    tool_name: str = ""  # get_apm_metrics | get_node_metrics | get_redis_info
    service: str = ""
    node_id: str = ""
    aggregation: str = "avg"
    threshold_type: str = "relative_pct_increase"  # relative_pct_increase | absolute_value
    threshold_value: float = 200.0
    consecutive_violations: int = 2


class RollbackPlaybook(BaseModel):
    id: str = ""
    watch_window_seconds: int = 60
    check_interval_seconds: int = 10
    trigger_metrics: List[TriggerMetric] = Field(default_factory=list)
    rollback_commands: List[Dict[str, Any]] = Field(default_factory=list)
    post_rollback_verification: Dict[str, Any] = Field(default_factory=dict)
    on_failure: Dict[str, Any] = Field(default_factory=dict)


class SelfHealingPlan(BaseModel):
    actions: List[SelfHealingAction] = Field(default_factory=list)
    risk_level: str = "low"  # low | medium | high
    requires_approval: bool = False
    rollback_playbook: Optional[RollbackPlaybook] = None


class ExpertOpinionSummary(BaseModel):
    agent_type: str
    finding: str
    confidence: float
    agreed_with_final: bool = True


class ConflictResolution(BaseModel):
    strategy_used: str  # weighted_voting | adversarial_debate | unanimous
    resolution_detail: str = ""
    winning_agent: str = ""


class ArbitrationResult(BaseModel):
    arbitration_id: str
    supervisor_trace_id: str
    unified_root_cause: str
    confidence: float
    impact_scope: Dict[str, Any] = Field(default_factory=dict)
    self_healing_plan: Optional[SelfHealingPlan] = None
    expert_opinions_summary: List[ExpertOpinionSummary] = Field(default_factory=list)
    conflict_resolution: Optional[ConflictResolution] = None


# ─── HITL / Fallback ────────────────────────────────────────

class CreateApprovalRequest(BaseModel):
    supervisor_trace_id: str
    arbitration_id: str
    self_healing_plan: SelfHealingPlan
    risk_level: str
    requested_by: str = "arbitration_agent"
    summary: str = ""
    expires_in_seconds: int = 300


class ApprovalResponse(BaseModel):
    approval_id: str
    status: str  # pending | approved | rejected | expired
    risk_level: str
    required_approvers: int
    current_approvals: int
    approvers: List[Dict[str, Any]] = Field(default_factory=list)
    expires_at: Optional[str] = None
    created_at: str = ""


class FallbackRequest(BaseModel):
    reason: str  # approval_timeout | execution_failed | rollback_triggered | expert_unavailable
    approval_id: str = ""
    fallback_action: str  # notify_oncall | retry_with_alternate_plan | escalate_to_supervisor | manual_only
    context: Dict[str, Any] = Field(default_factory=dict)


class FallbackResponse(BaseModel):
    incident_id: str
    fallback_status: str
    oncall_notified: bool = False
    notification_channels: List[str] = Field(default_factory=list)


# ─── Digital Twin Sandbox ─────────────────────────────────────

class SandboxRiskItem(BaseModel):
    risk_id: str = ""
    description: str = ""
    severity: str = "low"
    probability: float = 0.0
    affected_components: List[str] = Field(default_factory=list)
    trigger_condition: str = ""
    from_historical_case: bool = False
    historical_case_id: str = ""


class SandboxVerdict(BaseModel):
    """Verdict from Digital Twin Sandbox after simulating proposed actions."""
    verdict: str = "safe"  # safe | needs_modification | blocked
    confidence: float = 0.0
    safe: bool = True
    risks: List[SandboxRiskItem] = Field(default_factory=list)
    simulated_metrics: List[Dict[str, Any]] = Field(default_factory=list)
    alternative_actions: List[Dict[str, Any]] = Field(default_factory=list)
    reasoning: str = ""
    historical_cases_referenced: List[str] = Field(default_factory=list)
    simulation_duration_ms: int = 0
    fallback: bool = False


# ─── Supervisor State Machine ────────────────────────────────

class DiagnosisState(BaseModel):
    """Internal state for each incident being diagnosed."""
    trace_id: str
    incident: IncidentEvent
    rag_context: Optional[RAGContext] = None
    expert_results: Dict[str, DiagnoseResponse] = Field(default_factory=dict)
    arbitration_result: Optional[ArbitrationResult] = None
    approval: Optional[ApprovalResponse] = None
    sandbox_verdict: Optional[SandboxVerdict] = None
    execution_status: str = "pending"  # pending | running | observing | success | rollback_triggered | failed | blocked_by_sandbox
    observation_started_at: Optional[str] = None
    observation_log: List[Dict[str, Any]] = Field(default_factory=list)
    created_at: str = ""
    updated_at: str = ""
