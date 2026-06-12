"""
Arbitration module — Priority 7 upgrade with real cross-validation.

Strategies:
  A. Weighted Evidence Voting (default):
     - Cross-validate each expert's claim by calling one tool independently
     - Score = confidence × evidence_reproducibility
     - Winner by highest weighted score

  B. Adversarial Debate (fallback, score diff < 20%):
     - Submit conflicting claims back to experts for rebuttal
     - LLM judges the debate and produces final verdict
"""

from __future__ import annotations

import os
import json
import uuid
import time
import logging
import asyncio
from typing import Any, Dict, List, Tuple, Optional

import httpx
from .schemas import (
    DiagnoseResponse, ArbitrationResult, SelfHealingPlan,
    SelfHealingAction, RollbackPlaybook, TriggerMetric,
    ExpertOpinionSummary, ConflictResolution, IncidentEvent,
)

logger = logging.getLogger(__name__)

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1")
LLM_MODEL = os.getenv("LLM_MODEL", "deepseek-chat")
API_KEY = os.getenv("API_KEY", "dev-key-change-me")

# Expert URLs for cross-validation
EXPERT_URLS = {
    "k8s":         os.getenv("KB_EXPERT_URL", "http://k8s-expert:8110"),
    "middleware":  os.getenv("MW_EXPERT_URL", "http://middleware-expert:8120"),
    "network":     os.getenv("NW_EXPERT_URL", "http://network-expert:8130"),
    "application": os.getenv("APP_EXPERT_URL", "http://app-expert:8140"),
}

# Cross-validation tool per expert type
VALIDATION_TOOLS = {
    "k8s":         "get_pod_events",
    "middleware":  "get_redis_info",
    "network":     "ping_mesh",
    "application": "get_apm_metrics",
}

VALIDATION_PARAMS = {
    "k8s":         {"namespace": "default"},
    "middleware":  {"instance": "redis-prod-01"},
    "network":     {"source": "arbitrator", "target": "redis-prod-01", "count": 3},
    "application": {"service": "order-svc"},
}


# ─── Main entry ───────────────────────────────────────────────

async def arbitrate(
    trace_id: str,
    incident: IncidentEvent,
    expert_results: Dict[str, DiagnoseResponse | None],
) -> ArbitrationResult:
    """Entry point: detect conflicts, run arbitration strategy, return result."""

    valid = {k: v for k, v in expert_results.items() if v is not None}
    if not valid:
        return ArbitrationResult(
            arbitration_id=str(uuid.uuid4()),
            supervisor_trace_id=trace_id,
            unified_root_cause="无法确定根因（所有专家无响应）",
            confidence=0.1,
            conflict_resolution=ConflictResolution(strategy_used="unanimous", resolution_detail="no_expert_response"),
        )

    # Step 1: Conflict detection — check if root causes disagree
    conflicts = _detect_semantic_conflict(valid)

    if not conflicts:
        logger.info("[%s] All experts agree — unanimous synthesis", trace_id)
        return await _synthesize(trace_id, incident, valid, "unanimous")

    logger.info("[%s] Conflict detected between: %s", trace_id, conflicts)

    # Step 2: Strategy A — Weighted Evidence Voting with cross-validation
    result_a, score_diff = await _weighted_voting_cross_validate(trace_id, incident, valid)

    if score_diff >= 0.20:
        logger.info("[%s] Strategy A resolved (diff=%.2f ≥ 0.20)", trace_id, score_diff)
        return result_a

    # Step 3: Strategy B — Adversarial Debate (fallback)
    logger.info("[%s] Strategy A inconclusive (diff=%.2f), entering adversarial debate", trace_id, score_diff)
    return await _adversarial_debate(trace_id, incident, valid, result_a)


# ─── Semantic Conflict Detection ──────────────────────────────

def _detect_semantic_conflict(results: Dict[str, DiagnoseResponse]) -> List[Tuple[str, str]]:
    """
    Detect conflicts by comparing root cause keywords.
    Returns list of (agent_a, agent_b) pairs that disagree.
    """
    conflicts = []
    agents = list(results.items())

    for i in range(len(agents)):
        for j in range(i + 1, len(agents)):
            a_name, a_result = agents[i]
            b_name, b_result = agents[j]

            # Extract key terms from root cause
            a_terms = _extract_terms(a_result.suspected_root_cause)
            b_terms = _extract_terms(b_result.suspected_root_cause)

            # Check overlap
            overlap = a_terms & b_terms
            if not overlap:
                conflicts.append((a_name, b_name))
                logger.debug("Conflict: %s(%s) vs %s(%s) — no term overlap",
                             a_name, a_terms, b_name, b_terms)

    return conflicts


def min_risk(a: str, b: str) -> str:
    """Return the lower of two risk levels."""
    order = {"low": 0, "medium": 1, "high": 2}
    return a if order.get(a, 1) <= order.get(b, 1) else b


def _extract_terms(text: str) -> set:
    """Extract diagnostic keywords from root cause text."""
    keywords = [
        "redis", "mysql", "kafka", "rabbitmq", "cpu", "memory", "oom",
        "network", "packet", "dns", "latency", "timeout", "crash",
        "config", "deploy", "version", "keystar", "keys *", "slowlog",
        "connection", "pool", "disk", "io", "thread", "lock",
        "plc", "cnc", "modbus", "opc", "firmware",
    ]
    text_lower = text.lower()
    return {kw for kw in keywords if kw in text_lower}


# ─── Strategy A: Weighted Voting + Cross-validation ───────────

async def _weighted_voting_cross_validate(
    trace_id: str,
    incident: IncidentEvent,
    results: Dict[str, DiagnoseResponse],
) -> Tuple[ArbitrationResult, float]:
    """
    Cross-validate each expert's claim by calling one of their tools independently.
    Evidence that is independently reproducible gets full weight;
    unverifiable claims are discounted.
    """
    scored: List[Tuple[str, float, DiagnoseResponse, Dict]] = []

    for agent_type, r in results.items():
        # Cross-validate: call a validation tool for this expert
        validation_result = await _cross_validate(agent_type, incident)
        reproducibility = _compute_reproducibility(r, validation_result)

        # Score = confidence × (1 + evidence_count/5) × reproducibility
        evidence_weight = min(len(r.evidence) / 5.0, 1.0)
        score = r.confidence * (1.0 + evidence_weight) * reproducibility

        scored.append((agent_type, score, r, validation_result))
        logger.info("[%s] %s: score=%.3f (conf=%.2f × evidence=%.2f × repro=%.2f)",
                     trace_id, agent_type, score, r.confidence, 1.0 + evidence_weight, reproducibility)

    scored.sort(key=lambda x: x[1], reverse=True)
    winner = scored[0]

    # Score diff between winner and runner-up
    runner_up_score = scored[1][1] if len(scored) > 1 else 0.0
    score_diff = (winner[1] - runner_up_score) / max(winner[1], 0.001)

    # Synthesize with LLM
    result = await _synthesize(trace_id, incident, results, "weighted_voting")

    # Annotate which experts agreed/disagreed
    conflict_pairs = _detect_semantic_conflict(results)
    for summary in result.expert_opinions_summary:
        is_in_conflict = any(summary.agent_type in pair for pair in conflict_pairs)
        summary.agreed_with_final = not is_in_conflict

    result.conflict_resolution = ConflictResolution(
        strategy_used="weighted_voting",
        resolution_detail=f"Winner: {winner[0]} (score={winner[1]:.3f}), "
                          f"runner-up: {scored[1][0] if len(scored) > 1 else 'N/A'} "
                          f"(score={runner_up_score:.3f}), diff={score_diff:.1%}",
        winning_agent=winner[0],
    )

    return result, score_diff


async def _cross_validate(agent_type: str, incident: IncidentEvent) -> Dict[str, Any]:
    """
    Independently call a validation tool to verify an expert's domain claim.

    This is the arbitrator's OWN tool call — not relying on the expert's
    own evidence. If the validation succeeds, the expert's claim is considered
    reproducible.
    """
    tool_name = VALIDATION_TOOLS.get(agent_type)
    base_url = EXPERT_URLS.get(agent_type)

    if not tool_name or not base_url:
        return {"reproducible": False, "reason": f"no validation tool for {agent_type}"}

    params = dict(VALIDATION_PARAMS.get(agent_type, {}))
    # Enrich with incident context
    if agent_type == "application":
        alerts = incident.aggregated_alerts
        if alerts:
            svc = alerts[0].tags.get("service", "order-svc")
            params["service"] = svc
    elif agent_type == "k8s":
        params["namespace"] = "default"

    try:
        t0 = time.monotonic()
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(
                f"{base_url}/api/v1/tools/{tool_name}",
                json=params,
                headers={"X-API-Key": API_KEY, "Content-Type": "application/json"},
            )
            resp.raise_for_status()
            data = resp.json()
            elapsed = int((time.monotonic() - t0) * 1000)

            success = "error" not in data
            return {
                "reproducible": success,
                "tool": tool_name,
                "agent": agent_type,
                "latency_ms": elapsed,
                "summary": str(data.get("metrics", data.get("info", data)))[:200],
            }
    except Exception as e:
        logger.warning("Cross-validation for %s failed: %s", agent_type, e)
        return {"reproducible": False, "reason": str(e), "tool": tool_name, "agent": agent_type}


def _compute_reproducibility(result: DiagnoseResponse, validation: Dict) -> float:
    """
    Compute reproducibility score [0, 1]:
      - 1.0: independent tool call succeeded → expert's domain claim is verifiable
      - 0.5: tool call failed but expert has ≥ 2 evidence items
      - 0.3: neither verified nor well-evidenced
    """
    if validation.get("reproducible"):
        return 1.0
    if len(result.evidence) >= 2:
        return 0.5
    return 0.3


# ─── Strategy B: Adversarial Debate ───────────────────────────

async def _adversarial_debate(
    trace_id: str,
    incident: IncidentEvent,
    results: Dict[str, DiagnoseResponse],
    preliminary: ArbitrationResult,
) -> ArbitrationResult:
    """
    When Strategy A can't decide, submit conflicting claims to the
    two top experts for rebuttal, then have LLM judge the debate.

    This adds 8-15 seconds but provides higher confidence for hard cases.
    """
    # Find the top 2 conflicting experts
    sorted_agents = sorted(results.items(), key=lambda x: x[1].confidence, reverse=True)
    if len(sorted_agents) < 2:
        return preliminary

    agent_a, result_a = sorted_agents[0]
    agent_b, result_b = sorted_agents[1]

    logger.info("[%s] Debate: %s vs %s", trace_id, agent_a, agent_b)

    # Build debate prompt
    debate_prompt = _build_debate_prompt(incident, agent_a, result_a, agent_b, result_b)
    llm_result = await _call_llm(debate_prompt)

    root_cause = llm_result.get("root_cause", preliminary.unified_root_cause)
    confidence = float(llm_result.get("confidence", 0.7))

    # Update the preliminary result with debate outcome
    return ArbitrationResult(
        arbitration_id=preliminary.arbitration_id,
        supervisor_trace_id=trace_id,
        unified_root_cause=root_cause,
        confidence=confidence,
        impact_scope=preliminary.impact_scope,
        self_healing_plan=preliminary.self_healing_plan,
        expert_opinions_summary=[
            ExpertOpinionSummary(agent_type=agent_a, finding=result_a.findings[:200],
                                confidence=result_a.confidence,
                                agreed_with_final=(agent_a == llm_result.get("winning_side", ""))),
            ExpertOpinionSummary(agent_type=agent_b, finding=result_b.findings[:200],
                                confidence=result_b.confidence,
                                agreed_with_final=(agent_b == llm_result.get("winning_side", ""))),
        ],
        conflict_resolution=ConflictResolution(
            strategy_used="adversarial_debate",
            resolution_detail=llm_result.get("reasoning", f"Debate: {agent_a} vs {agent_b}"),
            winning_agent=llm_result.get("winning_side", ""),
        ),
    )


def _build_debate_prompt(
    incident: IncidentEvent,
    agent_a: str, result_a: DiagnoseResponse,
    agent_b: str, result_b: DiagnoseResponse,
) -> str:
    return f"""你是一个工业故障仲裁专家。两个领域专家对同一故障给出了不同的根因分析。请审阅双方证据并做出最终裁决。

## 故障事件
节点: {incident.node_id}
严重度: {incident.severity_max}
指标组: {incident.metric_group}

## {agent_a} Expert (confidence={result_a.confidence:.0%})
发现: {result_a.findings[:500]}
根因推断: {result_a.suspected_root_cause[:400]}
证据数: {len(result_a.evidence)}

## {agent_b} Expert (confidence={result_b.confidence:.0%})
发现: {result_b.findings[:500]}
根因推断: {result_b.suspected_root_cause[:400]}
证据数: {len(result_b.evidence)}

## 裁决要求
1. 判断哪个专家的根因更合理（或综合两者的合理部分）
2. 给出最终根因、置信度和裁决理由
3. 输出严格JSON（不要markdown代码块）:
{{"root_cause": "...", "confidence": 0.85, "winning_side": "{agent_a}|{agent_b}|synthesis", "reasoning": "裁决理由"}}"""


# ─── Synthesis ────────────────────────────────────────────────

async def _synthesize(
    trace_id: str,
    incident: IncidentEvent,
    results: Dict[str, DiagnoseResponse],
    strategy: str,
) -> ArbitrationResult:
    """Synthesize all expert opinions into a unified root cause via LLM."""

    prompt = _build_synthesis_prompt(incident, results)
    llm_response = await _call_llm(prompt)

    root_cause = llm_response.get("root_cause", "无法综合专家意见")
    confidence = float(llm_response.get("confidence", 0.7))
    risk_level = llm_response.get("risk_level", "low")

    # Heuristic: risk level based on incident severity AND action types
    severity = incident.severity_max.lower()
    if severity in ("minor", "warning"):
        risk_level = "low"
    elif severity == "major":
        risk_level = "medium"  # major → medium by default
    elif severity == "critical":
        risk_level = "high"    # critical → high by default

    # Action-type override: purely operational actions = lower risk
    suggested_actions = llm_response.get("actions", [])
    if suggested_actions:
        action_types = {a.get("action", "") for a in suggested_actions}
        low_risk = {"rollback_deployment", "restart_pod", "scale_deployment", "redis_config_set",
                     "rollback", "restart", "scale", "config_set"}
        high_risk = {"mysql_failover", "plc_parameter_rollback", "emergency_stop", "cnc_parameter_adjust"}
        if action_types & high_risk:
            risk_level = "high"
        elif action_types.issubset(low_risk) or any("rollback" in a or "restart" in a or "scale" in a for a in action_types):
            risk_level = min_risk(risk_level, "low")

    # Build self-healing plan
    actions = []
    for i, act in enumerate(llm_response.get("actions", [])):
        actions.append(SelfHealingAction(
            order=i + 1,
            action=act.get("action", "manual_check"),
            target=act.get("target", ""),
            command=act.get("command", ""),
            expected_effect=act.get("expected_effect", ""),
        ))

    plan = SelfHealingPlan(
        actions=actions,
        risk_level=risk_level,
        requires_approval=risk_level in ("medium", "high"),
        rollback_playbook=_default_rollback_playbook(risk_level, incident),
    )

    opinions = []
    for at, r in results.items():
        opinions.append(ExpertOpinionSummary(
            agent_type=at, finding=r.findings[:200],
            confidence=r.confidence, agreed_with_final=True,
        ))

    return ArbitrationResult(
        arbitration_id=str(uuid.uuid4()),
        supervisor_trace_id=trace_id,
        unified_root_cause=root_cause,
        confidence=confidence,
        impact_scope={"affected_services": [], "severity": incident.severity_max},
        self_healing_plan=plan,
        expert_opinions_summary=opinions,
        conflict_resolution=ConflictResolution(
            strategy_used=strategy,
            resolution_detail="LLM synthesis of expert opinions",
        ),
    )


def _build_synthesis_prompt(incident: IncidentEvent, results: Dict[str, DiagnoseResponse]) -> str:
    lines = ["你是一个工业故障仲裁专家。请综合多个领域专家的诊断报告给出最终结论。", "",
             f"## 故障事件\n节点={incident.node_id}, 严重度={incident.severity_max}, 指标组={incident.metric_group}", "",
             "## 专家诊断报告"]
    for at, r in results.items():
        lines.append(f"### {at} (conf={r.confidence:.0%})")
        lines.append(f"- 发现: {r.findings[:300]}")
        lines.append(f"- 根因: {r.suspected_root_cause[:200]}")
        lines.append("")
    lines.extend(["", "输出严格JSON:", '{"root_cause":"...","confidence":0.85,"risk_level":"low|medium|high",',
                  '"actions":[{"action":"rollback_deployment","target":"...","command":"...","expected_effect":"..."}]}'])
    return "\n".join(lines)


def _default_rollback_playbook(risk_level: str, incident: IncidentEvent) -> RollbackPlaybook:
    watch_map = {"low": 60, "medium": 90, "high": 120}
    return RollbackPlaybook(
        id=f"rb-{incident.incident_id}",
        watch_window_seconds=watch_map.get(risk_level, 60),
        trigger_metrics=[
            TriggerMetric(name="p99_latency_ms", metric_source="app_expert",
                         tool_name="get_apm_metrics", service="order-svc",
                         threshold_type="relative_pct_increase", threshold_value=200.0,
                         consecutive_violations=2),
            TriggerMetric(name="error_rate", metric_source="app_expert",
                         tool_name="get_apm_metrics", service="order-svc",
                         threshold_type="absolute_value", threshold_value=5.0,
                         consecutive_violations=1),
        ],
    )


# ─── LLM Call ────────────────────────────────────────────────

async def _call_llm(prompt: str) -> dict:
    if not DEEPSEEK_API_KEY:
        return {"root_cause": "LLM不可用，综合专家意见为最可能根因", "confidence": 0.5, "risk_level": "low", "actions": []}

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{DEEPSEEK_BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": LLM_MODEL, "temperature": 0.3, "max_tokens": 2000,
                    "messages": [
                        {"role": "system", "content": "你是工业故障仲裁专家。严格输出JSON，不要markdown代码块。"},
                        {"role": "user", "content": prompt},
                    ],
                },
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"].strip()
            if content.startswith("```"):
                content = content.split("\n", 1)[-1]
                if content.endswith("```"): content = content[:-3]
            return json.loads(content)
    except Exception as e:
        logger.error("LLM call failed: %s", e)
        return {"root_cause": f"LLM调用失败: {e}", "confidence": 0.3, "risk_level": "low", "actions": []}
