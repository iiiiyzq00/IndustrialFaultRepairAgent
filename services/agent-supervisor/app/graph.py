"""
LangGraph-based Supervisor pipeline with SQLite checkpoint persistence.

Key features:
  - StateGraph with 6 nodes, conditional routing, and HITL interrupt
  - SqliteSaver persists state at every node transition
  - Process restart → resumes from last checkpoint automatically
  - JSON backup for disaster recovery
"""

from __future__ import annotations

import os, sys, uuid, time, logging, sqlite3
from typing import Any, Dict, List, Optional, TypedDict, Literal
from datetime import datetime, timezone

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.checkpoint.memory import MemorySaver
from langgraph.types import interrupt, Command

# Path for common module
_PARENT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

from .schemas import IncidentEvent, DiagnoseRequest, DiagnoseResponse, SelfHealingPlan
from . import rag_client, expert_client, arbitrator, self_healer, review_extractor, sandbox_client
from common.notifier import notify_self_healing_result  # noqa: E402

logger = logging.getLogger(__name__)

CHECKPOINT_DB = os.getenv("CHECKPOINT_DB", "data/checkpoints.db")

# ═══════════════════════════════════════════════════════════════
# State
# ═══════════════════════════════════════════════════════════════

class DiagnosisState(TypedDict, total=False):
    incident: Dict[str, Any]
    supervisor_trace_id: str
    rag_context: Dict[str, Any]
    expert_results: Dict[str, Dict[str, Any]]
    arbitration_result: Dict[str, Any]
    risk_level: str
    requires_approval: bool
    approval_id: str
    approval_status: str
    hitl_interrupt_reason: str
    sandbox_verdict: Dict[str, Any]
    execution_status: str
    observation_log: List[Dict[str, Any]]
    created_at: str
    updated_at: str
    phase_timings: Dict[str, float]
    error_message: str

# ═══════════════════════════════════════════════════════════════
# Nodes
# ═══════════════════════════════════════════════════════════════

async def node_rag_prefetch(state: DiagnosisState) -> Dict[str, Any]:
    t0 = time.monotonic()
    logger.info("[%s] Phase 1: RAG pre-retrieval", state["supervisor_trace_id"])
    incident = IncidentEvent(**state["incident"])

    # Build a natural-language query — BGE embeddings work best with clean
    # descriptive Chinese.  Avoid embedding random unique identifiers (node_id)
    # as they dilute the semantic signal.
    severity_cn = _severity_cn(incident.severity_max)
    parts = []
    for a in incident.aggregated_alerts[:3]:
        metric_cn = _metric_description(a.metric_type)
        parts.append(
            f"{metric_cn} 从 {a.baseline_mean} 异常升高至 {a.current_value}"
        )
    # Include node_type and tags for context (skip the random node_id)
    types = list({a.node_type for a in incident.aggregated_alerts})
    tags_all = []
    for a in incident.aggregated_alerts[:2]:
        for k, v in (a.tags or {}).items():
            if v and v not in tags_all:
                tags_all.append(str(v))
    context = " ".join(types + tags_all)
    text = (
        f"{context} 发生 {severity_cn} 告警，"
        f"{' '.join(parts)}，{_metric_group_cn(incident.metric_group)}类故障"
    )
    rag_ctx = await rag_client.pre_retrieve_for_incident(text, incident.affected_line_profile)
    rag_ctx.retrieved_at = datetime.now(timezone.utc).isoformat()
    pt = dict(state.get("phase_timings", {}))
    pt["rag_prefetch"] = round(time.monotonic() - t0, 2)
    return {"rag_context": rag_ctx.model_dump(), "phase_timings": pt,
            "updated_at": datetime.now(timezone.utc).isoformat()}


async def node_dispatch_experts(state: DiagnosisState) -> Dict[str, Any]:
    t0 = time.monotonic()
    logger.info("[%s] Phase 2: Dispatching experts", state["supervisor_trace_id"])
    incident = IncidentEvent(**state["incident"])
    expert_types = _determine_experts(incident)
    hypothesis = _build_hypothesis(incident, state.get("rag_context", {}))
    from .schemas import RAGContext
    rag_obj = RAGContext(**state["rag_context"]) if state.get("rag_context") else None
    req = DiagnoseRequest(supervisor_trace_id=state["supervisor_trace_id"],
                          incident=incident, domain_hypothesis=hypothesis, rag_context=rag_obj)
    results = await expert_client.dispatch_all_parallel(expert_types, req)
    expert_dict = {k: v.model_dump() if v else {"error": "no_response"} for k, v in results.items()}
    pt = dict(state.get("phase_timings", {}))
    pt["dispatch_experts"] = round(time.monotonic() - t0, 2)
    return {"expert_results": expert_dict, "phase_timings": pt,
            "updated_at": datetime.now(timezone.utc).isoformat()}


async def node_arbitrate(state: DiagnosisState) -> Dict[str, Any]:
    t0 = time.monotonic()
    logger.info("[%s] Phase 3: Arbitration", state["supervisor_trace_id"])
    incident = IncidentEvent(**state["incident"])
    expert_results = {}
    for at, d in state.get("expert_results", {}).items():
        if d and "error" not in d:
            try: expert_results[at] = DiagnoseResponse(**d)
            except Exception: pass
    arb_result = await arbitrator.arbitrate(state["supervisor_trace_id"], incident, expert_results)
    plan = arb_result.self_healing_plan
    risk = plan.risk_level if plan else "medium"
    needs_approval = plan.requires_approval if plan else (risk in ("medium", "high"))
    pt = dict(state.get("phase_timings", {}))
    pt["arbitrate"] = round(time.monotonic() - t0, 2)
    return {"arbitration_result": arb_result.model_dump(), "risk_level": risk,
            "requires_approval": needs_approval, "phase_timings": pt,
            "execution_status": "awaiting_approval" if needs_approval else "running",
            "updated_at": datetime.now(timezone.utc).isoformat()}


async def node_hitl_interrupt(state: DiagnosisState) -> Dict[str, Any]:
    logger.info("[%s] Phase 4: HITL interrupt (risk=%s)",
                state["supervisor_trace_id"], state.get("risk_level", "?"))
    arb = state.get("arbitration_result", {})
    plan = arb.get("self_healing_plan", {})
    risk = state.get("risk_level", "medium")

    # On re-entry (after resume), reuse the existing approval id
    approval_id = state.get("approval_id", "")
    if not approval_id:
        try:
            import httpx
            async with httpx.AsyncClient(timeout=5.0) as c:
                resp = await c.post(
                    f"{os.getenv('HITL_GATEWAY_URL', 'http://hitl-gateway:8300')}/api/v1/approvals",
                    json={"supervisor_trace_id": state["supervisor_trace_id"],
                          "arbitration_id": arb.get("arbitration_id", ""),
                          "self_healing_plan": plan, "risk_level": risk,
                          "requested_by": "arbitration_agent",
                          "summary": arb.get("unified_root_cause", "")[:200]},
                    headers={"X-API-Key": os.getenv("API_KEY", "dev-key-change-me")})
                resp.raise_for_status()
                approval_id = resp.json().get("approval_id", "")
        except Exception as e:
            logger.error("Failed to create approval: %s", e)

    decision = interrupt({
        "message": f"HITL approval required (risk={risk})",
        "approval_id": approval_id, "risk_level": risk,
        "summary": arb.get("unified_root_cause", "")[:200]})
    logger.info("[%s] HITL resumed: %s", state["supervisor_trace_id"], decision)
    return {"approval_id": approval_id,
            "approval_status": decision.get("status", "rejected"),
            "execution_status": "running",
            "hitl_interrupt_reason": decision.get("reason", ""),
            "updated_at": datetime.now(timezone.utc).isoformat()}


async def node_sandbox_verify(state: DiagnosisState) -> Dict[str, Any]:
    """Phase 4.5: Digital Twin Sandbox — verify actions before execution.

    Simulates the proposed self-healing actions in a sandboxed environment,
    checks whether they could cause new problems, and returns a safety verdict.

    This node runs for BOTH paths (low-risk auto-heal AND HITL-approved):
      - low-risk:    arbitrate → sandbox_verify → execute_and_observe
      - mid/high:    hitl_interrupt → sandbox_verify → execute_and_observe

    If the sandbox blocks an action, execution is skipped and the incident
    goes directly to review with execution_status="blocked_by_sandbox".
    """
    t0 = time.monotonic()
    logger.info("[%s] Phase 4.5: Sandbox verification", state["supervisor_trace_id"])

    # Skip if HITL was rejected
    if state.get("approval_status") == "rejected":
        logger.info("[%s] HITL rejected — skipping sandbox", state["supervisor_trace_id"])
        return {"execution_status": "rejected",
                "sandbox_verdict": {"verdict": "skipped", "reason": "hitl_rejected"},
                "updated_at": datetime.now(timezone.utc).isoformat()}

    verdict = await sandbox_client.verify_actions(
        trace_id=state["supervisor_trace_id"],
        incident=state.get("incident", {}),
        arbitration_result=state.get("arbitration_result", {}),
        expert_results=state.get("expert_results", {}),
        rag_context=state.get("rag_context", {}),
    )

    pt = dict(state.get("phase_timings", {}))
    pt["sandbox_verify"] = round(time.monotonic() - t0, 2)

    if verdict.verdict == "blocked":
        logger.warning("[%s] SANDBOX BLOCKED: %s", state["supervisor_trace_id"], verdict.reasoning[:200])
        return {
            "sandbox_verdict": verdict.model_dump(),
            "execution_status": "blocked_by_sandbox",
            "phase_timings": pt,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    if verdict.verdict == "needs_modification":
        logger.info("[%s] Sandbox suggests modifications (%d risks, %d alternatives)",
                    state["supervisor_trace_id"], len(verdict.risks), len(verdict.alternative_actions))
        # Use alternative actions if provided and safer
        if verdict.alternative_actions:
            alt_plan = dict(state.get("arbitration_result", {}).get("self_healing_plan", {}))
            alt_plan["actions"] = verdict.alternative_actions
            alt_plan["sandbox_modified"] = True
            arb = dict(state.get("arbitration_result", {}))
            arb["self_healing_plan"] = alt_plan
            return {
                "sandbox_verdict": verdict.model_dump(),
                "arbitration_result": arb,
                "execution_status": "running",
                "phase_timings": pt,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }

    logger.info("[%s] Sandbox approved: %s", state["supervisor_trace_id"], verdict.reasoning[:120])
    return {
        "sandbox_verdict": verdict.model_dump(),
        "execution_status": "running",
        "phase_timings": pt,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


async def node_execute_and_observe(state: DiagnosisState) -> Dict[str, Any]:
    t0 = time.monotonic()
    logger.info("[%s] Phase 5: Execute & Observe", state["supervisor_trace_id"])
    plan_dict = state.get("arbitration_result", {}).get("self_healing_plan", {})
    plan = SelfHealingPlan(**plan_dict) if plan_dict else SelfHealingPlan()
    result = await self_healer.execute_and_watch(
        state["supervisor_trace_id"], plan, plan.rollback_playbook)
    exec_status = result.get("result", "failed")
    exec_duration = round(time.monotonic() - t0, 2)

    # Fire-and-forget notification
    import asyncio as _asyncio
    sandbox_info = state.get("sandbox_verdict", {})
    detail = ""
    if sandbox_info.get("verdict") == "blocked":
        detail = sandbox_info.get("reasoning", "")
    _asyncio.create_task(notify_self_healing_result(
        state["supervisor_trace_id"],
        state.get("incident", {}).get("node_id", "unknown"),
        exec_status,
        [a.model_dump() if hasattr(a, 'model_dump') else a for a in plan.actions],
        exec_duration,
        detail,
    ))

    pt = dict(state.get("phase_timings", {}))
    pt["execute_and_observe"] = exec_duration
    return {"execution_status": exec_status,
            "observation_log": result.get("observation_log", []),
            "phase_timings": pt,
            "updated_at": datetime.now(timezone.utc).isoformat()}


async def node_review(state: DiagnosisState) -> Dict[str, Any]:
    t0 = time.monotonic()
    logger.info("[%s] Phase 6: Review (flywheel)", state["supervisor_trace_id"])
    if state.get("execution_status") not in ("success", "rollback_triggered"):
        return {"updated_at": datetime.now(timezone.utc).isoformat()}
    await review_extractor.run_review(
        trace_id=state["supervisor_trace_id"], incident=state.get("incident", {}),
        expert_results=state.get("expert_results", {}),
        arbitration=state.get("arbitration_result", {}),
        execution_result={"result": state.get("execution_status")},
        observation_log=state.get("observation_log", []))
    pt = dict(state.get("phase_timings", {}))
    pt["review"] = round(time.monotonic() - t0, 2)
    return {"phase_timings": pt, "updated_at": datetime.now(timezone.utc).isoformat()}


# ═══════════════════════════════════════════════════════════════
# Routing
# ═══════════════════════════════════════════════════════════════

def route_after_arbitrate(state: DiagnosisState) -> Literal["hitl_interrupt", "sandbox_verify"]:
    """Low-risk actions skip HITL and go directly to sandbox verification."""
    return "hitl_interrupt" if state.get("requires_approval", False) else "sandbox_verify"


def route_after_sandbox_verify(state: DiagnosisState) -> Literal["execute_and_observe", "review"]:
    """If sandbox blocked the action, skip execution and go to review."""
    if state.get("execution_status") == "blocked_by_sandbox":
        return "review"
    return "execute_and_observe"


# ═══════════════════════════════════════════════════════════════
# Graph Builder
# ═══════════════════════════════════════════════════════════════

_graph = None
_checkpointer_conn = None

async def init_graph():
    """Build graph with AsyncSqliteSaver persistence. Called once at app startup."""
    global _graph, _checkpointer_conn

    db_dir = os.path.dirname(CHECKPOINT_DB)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    import aiosqlite
    _checkpointer_conn = await aiosqlite.connect(CHECKPOINT_DB)
    checkpointer = AsyncSqliteSaver(_checkpointer_conn)
    await checkpointer.setup()
    logger.info("AsyncSqliteSaver initialized: %s", CHECKPOINT_DB)

    builder = StateGraph(DiagnosisState)
    builder.add_node("rag_prefetch", node_rag_prefetch)
    builder.add_node("dispatch_experts", node_dispatch_experts)
    builder.add_node("arbitrate", node_arbitrate)
    builder.add_node("hitl_interrupt", node_hitl_interrupt)
    builder.add_node("sandbox_verify", node_sandbox_verify)
    builder.add_node("execute_and_observe", node_execute_and_observe)
    builder.add_node("review", node_review)

    builder.set_entry_point("rag_prefetch")
    builder.add_edge("rag_prefetch", "dispatch_experts")
    builder.add_edge("dispatch_experts", "arbitrate")
    # Low-risk → sandbox directly; mid/high-risk → HITL first
    builder.add_conditional_edges("arbitrate", route_after_arbitrate,
        {"hitl_interrupt": "hitl_interrupt", "sandbox_verify": "sandbox_verify"})
    # Both paths converge at sandbox_verify
    builder.add_edge("hitl_interrupt", "sandbox_verify")
    # Sandbox → execute or skip (blocked)
    builder.add_conditional_edges("sandbox_verify", route_after_sandbox_verify,
        {"execute_and_observe": "execute_and_observe", "review": "review"})
    builder.add_edge("execute_and_observe", "review")
    builder.add_edge("review", END)

    _graph = builder.compile(checkpointer=checkpointer)
    logger.info("LangGraph compiled: %d nodes, SqliteSaver", len(_graph.nodes))
    return _graph


def get_graph():
    if _graph is None:
        raise RuntimeError("Graph not initialized")
    return _graph


# ═══════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════

def _determine_experts(incident: IncidentEvent) -> list[str]:
    experts = set()
    for a in incident.aggregated_alerts:
        nt = a.node_type.lower()
        if nt in ("server", "container"): experts.update(["k8s", "application"])
        elif nt in ("plc", "cnc", "robotarm", "agv", "edgegw"): experts.update(["network", "application"])
    if (incident.metric_group or "") in ("queue", "latency"): experts.add("middleware")
    return sorted(experts) if experts else ["application", "network"]


def _metric_description(metric_type: str) -> str:
    """Map a metric_type to a short Chinese description for RAG queries."""
    mapping = {
        "p99_latency_ms": "P99 延迟",
        "p50_latency_ms": "P50 延迟",
        "avg_latency_ms": "平均延迟",
        "error_rate": "错误率",
        "cpu_usage": "CPU 使用率",
        "memory_usage": "内存使用率",
        "disk_io_mbps": "磁盘 IO",
        "queue_depth": "队列深度",
        "vibration_mm_s": "振动值",
        "temperature_c": "温度",
        "joint_deviation_deg": "关节偏差",
        "packet_loss_pct": "丢包率",
    }
    return mapping.get(metric_type, metric_type)


def _severity_cn(severity: str) -> str:
    """Map severity to Chinese."""
    return {"minor": "轻微", "warning": "警告", "major": "严重", "critical": "紧急"}.get(
        severity.lower(), severity
    )


def _metric_group_cn(group: str) -> str:
    """Map metric group to Chinese."""
    mapping = {
        "latency": "延迟",
        "resource": "资源",
        "error": "错误",
        "queue": "队列",
        "throughput": "吞吐量",
    }
    return mapping.get(group or "", group or "未知")


def _build_hypothesis(incident: IncidentEvent, rag_ctx: dict) -> str:
    hint = ""
    docs = rag_ctx.get("documents", [])
    if docs: hint = f" 历史案例: {docs[0].get('ticket_id','?')}"
    return f"故障节点={incident.node_id}, 指标组={incident.metric_group}, 严重度={incident.severity_max}.{hint}"
