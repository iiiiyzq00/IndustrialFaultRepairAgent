"""
Digital Twin Sandbox Service — verifies self-healing actions before execution.

Endpoint:
  POST /api/v1/sandbox/verify  — simulate actions and return safety verdict

Pipeline:
  1. Simulate action effects on system metrics (rule-based)
  2. Query RAG for historical cases where similar actions were applied
  3. Call LLM to synthesize risk assessment
  4. Return verdict: safe | needs_modification | blocked
"""

from __future__ import annotations

import os
import sys
import json
import time
import logging
from typing import Any, Dict, List

import httpx
from fastapi import FastAPI, HTTPException

# Path for common module
_PARENT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

from common.auth import setup_api_key_auth  # noqa: E402

from .schemas import (  # noqa: E402
    SandboxVerifyRequest, SandboxVerifyResponse,
    SimulatedMetricPoint, RiskItem,
)
from . import simulator  # noqa: E402

# ─── Config ────────────────────────────────────────────────────

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [sandbox] %(levelname)s %(message)s",
)
logger = logging.getLogger("sandbox-service")

SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8500"))
API_KEY = os.getenv("API_KEY", "dev-key-change-me")
RAG_SERVICE_URL = os.getenv("RAG_SERVICE_URL", "http://rag-service:8200")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1")
LLM_MODEL = os.getenv("LLM_MODEL", "deepseek-chat")
SANDBOX_TIMEOUT = float(os.getenv("SANDBOX_TIMEOUT_SECONDS", "10.0"))

# ─── App ───────────────────────────────────────────────────────

app = FastAPI(
    title="Industrial Fault Repair — Digital Twin Sandbox",
    version="1.0.0",
)
setup_api_key_auth(app)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "digital-twin-sandbox",
        "simulator_actions": len(simulator.ACTION_EFFECTS),
    }


# ─── Verify endpoint ───────────────────────────────────────────

@app.post("/api/v1/sandbox/verify", response_model=SandboxVerifyResponse)
async def verify(req: SandboxVerifyRequest):
    """
    Verify a self-healing plan in the digital twin sandbox.

    Steps:
      1. Simulate action effects on metrics
      2. Query RAG for historical similar actions
      3. Evaluate rule-based safety
      4. LLM synthesis (with graceful degradation)
    """
    t0 = time.monotonic()
    trace_id = req.supervisor_trace_id
    plan = req.self_healing_plan
    incident = req.incident

    logger.info("[%s] Sandbox verification started — %d actions: %s",
                trace_id, len(plan.get("actions", [])),
                [a.get("action", "?") for a in plan.get("actions", [])])

    # ── Step 1: Simulate metric changes ──
    sim_metrics = simulator.simulate_actions(plan, incident)
    logger.info("[%s] Simulated %d metric points", trace_id, len(sim_metrics))

    # ── Step 2: Rule-based safety evaluation ──
    rule_risks = simulator.evaluate_safety(sim_metrics, plan)
    logger.info("[%s] Rule-based risks: %d", trace_id, len(rule_risks))

    # ── Step 3: Query RAG for historical similar actions ──
    hist_cases = await _query_rag_for_similar_actions(trace_id, plan, incident)

    # ── Step 4: LLM synthesis ──
    llm_verdict = await _llm_evaluate(
        trace_id, plan, incident, sim_metrics, rule_risks, hist_cases,
        req.arbitration_result, req.expert_results,
    )

    # ── Step 5: Merge results ──
    duration_ms = int((time.monotonic() - t0) * 1000)

    response = SandboxVerifyResponse(
        supervisor_trace_id=trace_id,
        verdict=llm_verdict.get("verdict", "safe"),
        confidence=llm_verdict.get("confidence", 0.8),
        safe=llm_verdict.get("verdict", "safe") == "safe",
        risks=_merge_risks(rule_risks, llm_verdict.get("risks", [])),
        simulated_metrics=sim_metrics,
        alternative_actions=llm_verdict.get("alternative_actions", []),
        reasoning=llm_verdict.get("reasoning", "规则评估通过，无显著风险"),
        historical_cases_referenced=[c.get("ticket_id", "") for c in hist_cases],
        simulation_duration_ms=duration_ms,
        fallback=llm_verdict.get("fallback", False),
    )

    logger.info("[%s] Sandbox verdict: %s (confidence=%.0f%%, %dms)",
                trace_id, response.verdict, response.confidence * 100, duration_ms)

    return response


# ─── RAG query ─────────────────────────────────────────────────

async def _query_rag_for_similar_actions(
    trace_id: str,
    plan: Dict[str, Any],
    incident: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Query RAG for historical cases where similar actions were applied."""
    actions = plan.get("actions", [])
    if not actions:
        return []

    # Build a query from the action types and incident context
    action_names = [a.get("action", "") for a in actions[:3]]
    node_id = incident.get("node_id", "")
    severity = incident.get("severity_max", "")

    query = f"{severity} 级别故障在 {node_id} 执行 {' '.join(action_names)} 自愈动作后是否引发新问题"

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(
                f"{RAG_SERVICE_URL}/api/v1/rag/retrieve",
                json={"query": query, "top_k": 3, "filters": None, "retrieval_strategy": "hybrid"},
                headers={"X-API-Key": API_KEY, "Content-Type": "application/json"},
            )
            resp.raise_for_status()
            data = resp.json()
            docs = data.get("documents", [])
            logger.info("[%s] RAG query returned %d historical cases", trace_id, len(docs))
            return docs
    except Exception as e:
        logger.warning("[%s] RAG query for sandbox failed: %s", trace_id, e)
        return []


# ─── LLM evaluation ─────────────────────────────────────────────

async def _llm_evaluate(
    trace_id: str,
    plan: Dict[str, Any],
    incident: Dict[str, Any],
    sim_metrics: List[SimulatedMetricPoint],
    rule_risks: List[RiskItem],
    hist_cases: List[Dict[str, Any]],
    arbitration: Dict[str, Any],
    expert_results: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Call DeepSeek LLM to synthesize a safety verdict.

    Falls back to rule-based decision if LLM is unavailable.
    """
    if not DEEPSEEK_API_KEY:
        logger.info("[%s] No LLM API key — using rule-based verdict", trace_id)
        return _rule_based_verdict(rule_risks, sim_metrics)

    prompt = _build_llm_prompt(plan, incident, sim_metrics, rule_risks, hist_cases, arbitration, expert_results)

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                f"{DEEPSEEK_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": LLM_MODEL,
                    "messages": [
                        {"role": "system", "content": "你是一个工业故障自愈安全审查专家。你的任务是评估自愈动作计划的安全性，判断执行后是否会引发新的故障。始终输出严格JSON，不要markdown代码块。"},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.2,
                    "max_tokens": 2000,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"].strip()
            if content.startswith("```"):
                content = content.split("\n", 1)[-1]
                if content.endswith("```"):
                    content = content[:-3]
            result = json.loads(content)
            result["fallback"] = False
            return result
    except Exception as e:
        logger.warning("[%s] LLM sandbox evaluation failed: %s — falling back to rules", trace_id, e)
        verdict = _rule_based_verdict(rule_risks, sim_metrics)
        verdict["fallback"] = True
        return verdict


def _build_llm_prompt(
    plan: Dict[str, Any],
    incident: Dict[str, Any],
    sim_metrics: List[SimulatedMetricPoint],
    rule_risks: List[RiskItem],
    hist_cases: List[Dict[str, Any]],
    arbitration: Dict[str, Any],
    expert_results: Dict[str, Dict[str, Any]],
) -> str:
    """Build the LLM evaluation prompt."""
    actions = plan.get("actions", [])
    action_lines = "\n".join(
        f"{i+1}. {a.get('action','?')} → target={a.get('target','?')}, "
        f"command={a.get('command','?')}, 预期效果={a.get('expected_effect','?')}"
        for i, a in enumerate(actions)
    )

    metric_lines = "\n".join(
        f"- {sm.metric_name} on {sm.node_id}: {sm.pre_action_value:.1f} → {sm.post_action_value:.1f} "
        f"({sm.delta_pct:+.1f}%, trend={sm.trend})"
        for sm in sim_metrics[:15]
    )

    risk_lines = "\n".join(
        f"- [{r.severity}] {r.description} (probability={r.probability:.0%})"
        for r in rule_risks
    ) if rule_risks else "（无规则级风险）"

    hist_lines = "\n".join(
        f"- {c.get('ticket_id','?')}: {c.get('root_cause_summary','?')[:200]} "
        f"(relevance={c.get('relevance_score',0):.0%})"
        for c in hist_cases[:3]
    ) if hist_cases else "（无相似历史案例）"

    root_cause = arbitration.get("unified_root_cause", "未知")

    return f"""## 故障概述
节点: {incident.get('node_id','?')}
严重度: {incident.get('severity_max','?')}
根因: {root_cause}

## 拟执行的自愈动作
{action_lines}

## 模拟的指标变化
{metric_lines}

## 规则引擎识别的风险
{risk_lines}

## 历史相似动作案例
{hist_lines}

## 评估要求
请综合以上信息，判断该自愈计划的安全性：
1. 动作是否会引发新的故障或指标恶化？
2. 是否存在更安全的替代方案？
3. 与历史案例对比，当前计划是否合理？

请输出严格JSON（不要markdown代码块）:
{{
  "verdict": "safe|needs_modification|blocked",
  "confidence": 0.85,
  "reasoning": "综合评估理由（2-3句话）",
  "risks": [
    {{"risk_id": "risk-1", "description": "...", "severity": "low|medium|high|critical", "probability": 0.5, "affected_components": ["..."], "trigger_condition": "...", "from_historical_case": false, "historical_case_id": ""}}
  ],
  "alternative_actions": [
    {{"action": "...", "target": "...", "command": "...", "expected_effect": "..."}}
  ]
}}

判定标准:
- safe: 所有指标改善或稳定，无新增风险
- needs_modification: 存在可修复的风险，建议调整动作参数或增加前置校验
- blocked: 动作可能造成严重故障，必须阻止执行"""


# ─── Rule-based fallback ────────────────────────────────────────

def _rule_based_verdict(
    rule_risks: List[RiskItem],
    sim_metrics: List[SimulatedMetricPoint],
) -> Dict[str, Any]:
    """Generate a verdict purely from rule-based evaluation (no LLM)."""
    high_critical = [r for r in rule_risks if r.severity in ("high", "critical")]
    medium = [r for r in rule_risks if r.severity == "medium"]

    # Check for degrading metrics
    degrading = [sm for sm in sim_metrics if sm.trend == "degrading"]

    if high_critical:
        verdict = "blocked"
        confidence = 0.9
        reasoning = f"规则引擎检测到 {len(high_critical)} 个高风险项，阻塞执行"
    elif medium or len(degrading) > 2:
        verdict = "needs_modification"
        confidence = 0.7
        reasoning = f"检测到 {len(medium)} 个中风险和 {len(degrading)} 个指标恶化，建议修改计划"
    else:
        verdict = "safe"
        confidence = 0.85
        reasoning = "规则评估通过，所有模拟指标在安全阈值内"

    # Convert RiskItem to dict
    risks_dict = [
        {
            "risk_id": r.risk_id,
            "description": r.description,
            "severity": r.severity,
            "probability": r.probability,
            "affected_components": r.affected_components,
            "trigger_condition": r.trigger_condition,
            "from_historical_case": r.from_historical_case,
            "historical_case_id": r.historical_case_id,
        }
        for r in rule_risks
    ]

    return {
        "verdict": verdict,
        "confidence": confidence,
        "reasoning": reasoning,
        "risks": risks_dict,
        "alternative_actions": [],
        "fallback": True,
    }


# ─── Helpers ────────────────────────────────────────────────────

def _merge_risks(
    rule_risks: List[RiskItem],
    llm_risks: List[Dict[str, Any]],
) -> List[RiskItem]:
    """Merge rule-based risks with LLM-identified risks, deduplicating."""
    seen = set()
    merged = []

    for r in rule_risks:
        if r.risk_id not in seen:
            merged.append(r)
            seen.add(r.risk_id)

    for r_dict in llm_risks:
        rid = r_dict.get("risk_id", "")
        if rid and rid not in seen:
            merged.append(RiskItem(
                risk_id=rid,
                description=r_dict.get("description", ""),
                severity=r_dict.get("severity", "low"),
                probability=r_dict.get("probability", 0.5),
                affected_components=r_dict.get("affected_components", []),
                trigger_condition=r_dict.get("trigger_condition", ""),
                from_historical_case=r_dict.get("from_historical_case", False),
                historical_case_id=r_dict.get("historical_case_id", ""),
            ))
            seen.add(rid)

    return merged


# ─── Main ───────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    logger.info("Starting Digital Twin Sandbox on port %d", SERVICE_PORT)
    uvicorn.run("app.main:app", host="0.0.0.0", port=SERVICE_PORT, reload=True)
