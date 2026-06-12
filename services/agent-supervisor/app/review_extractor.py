"""
After-Action Review Extractor — Experience Flywheel.

Triggered after every fault resolution (success or manual fix).
 1. Collects full diagnosis chain
 2. Calls DeepSeek-V3 to generate a structured Markdown fault case
 3. Pushes the case to RAG Service via /api/v1/rag/upsert

This closes the experience loop: more cases → better RAG → faster diagnosis.
"""

from __future__ import annotations

import os
import json
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

RAG_SERVICE_URL = os.getenv("RAG_SERVICE_URL", "http://rag-service:8200")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1")
LLM_MODEL = os.getenv("LLM_MODEL", "deepseek-chat")
API_KEY = os.getenv("API_KEY", "dev-key-change-me")


async def run_review(
    trace_id: str,
    incident: dict,
    expert_results: Dict[str, dict],
    arbitration: dict,
    execution_result: dict,
    observation_log: list | None = None,
) -> Dict[str, Any]:
    """
    Run the full after-action review pipeline.

    Returns {"status": "ok", "ticket_id": "...", "chroma_id": "..."}
    or {"status": "error", "message": "..."}
    """
    logger.info("[%s] Starting after-action review (flywheel)", trace_id)

    try:
        # ── Step 1: Build context ──
        context = _build_context(trace_id, incident, expert_results, arbitration, execution_result, observation_log)

        # ── Step 2: Generate Markdown case via DeepSeek-V3 ──
        markdown_case = await _generate_case(context)
        if not markdown_case:
            logger.warning("[%s] LLM case generation returned empty — skipping upsert", trace_id)
            return {"status": "error", "message": "LLM generation empty"}

        # ── Step 3: Push to RAG ──
        ticket_id = f"INC-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{uuid.uuid4().hex[:6]}"
        result = await _upsert_to_rag(ticket_id, markdown_case, context)

        logger.info("[%s] After-action review complete: ticket=%s chroma=%s",
                    trace_id, ticket_id, result.get("chroma_id", "?"))

        return {"status": "ok", "ticket_id": ticket_id, "chroma_id": result.get("chroma_id", "")}

    except Exception as e:
        logger.exception("[%s] After-action review failed: %s", trace_id, e)
        return {"status": "error", "message": str(e)}


# ─── Context builder ──────────────────────────────────────────

def _build_context(
    trace_id: str,
    incident: dict,
    expert_results: Dict[str, dict],
    arbitration: dict,
    execution_result: dict,
    observation_log: list | None,
) -> dict:
    """Build structured context for LLM case generation."""

    # Extract key info from incident
    alerts = incident.get("aggregated_alerts", [])
    node_id = incident.get("node_id", "unknown")
    severity = incident.get("severity_max", "unknown")

    # Extract expert findings
    expert_summaries = []
    for agent_type, result in (expert_results or {}).items():
        expert_summaries.append({
            "agent": agent_type,
            "findings": result.get("findings", "")[:500],
            "root_cause": result.get("suspected_root_cause", "")[:300],
            "confidence": result.get("confidence", 0),
            "evidence_count": len(result.get("evidence", [])),
        })

    # Extract arbitration
    root_cause = arbitration.get("unified_root_cause", "")
    arb_confidence = arbitration.get("confidence", 0)
    plan = arbitration.get("self_healing_plan", {})

    # Build tags
    tags = ["flywheel", severity, node_id]
    for alert in alerts[:3]:
        mt = alert.get("metric_type", "")
        if mt and mt not in tags:
            tags.append(mt)

    return {
        "trace_id": trace_id,
        "incident": incident,
        "expert_summaries": expert_summaries,
        "root_cause": root_cause,
        "arbitration_confidence": arb_confidence,
        "self_healing_plan": plan,
        "execution_result": execution_result,
        "observation_log": observation_log or [],
        "affected_line_profile": incident.get("affected_line_profile", "general"),
        "tags": tags,
        "severity": severity,
        "node_id": node_id,
    }


# ─── LLM Case Generation ─────────────────────────────────────

async def _generate_case(context: dict) -> str:
    """Call DeepSeek-V3 to generate a structured Markdown fault case."""

    prompt = _build_extraction_prompt(context)

    if not DEEPSEEK_API_KEY:
        logger.warning("DEEPSEEK_API_KEY not set — generating basic case from context")
        return _basic_case(context)

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{DEEPSEEK_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": LLM_MODEL,
                    "messages": [
                        {"role": "system", "content": "你是一个工业故障复盘专家。请根据诊断数据生成标准化的故障案例Markdown文档。只输出Markdown，不要额外解释。"},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.3,
                    "max_tokens": 3000,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"].strip()
            # Strip code fences if present
            if content.startswith("```"):
                lines = content.split("\n")
                content = "\n".join(lines[1:])
                if content.endswith("```"):
                    content = content[:-3]
            return content.strip()
    except Exception as e:
        logger.error("LLM case generation failed: %s", e)
        return _basic_case(context)


def _build_extraction_prompt(context: dict) -> str:
    """Build the prompt for LLM to generate a fault case."""
    incident = context["incident"]
    alerts = incident.get("aggregated_alerts", [])

    alert_text = "\n".join(
        f"- {a.get('node_id','?')}: {a.get('metric_type','?')}={a.get('current_value','?')} "
        f"(基线={a.get('baseline_mean','?')}, σ={a.get('deviation_sigma','?')})"
        for a in alerts[:5]
    )

    expert_text = "\n".join(
        f"### {e['agent']} Expert (confidence={e['confidence']:.0%})\n"
        f"- 发现: {e['findings'][:300]}\n"
        f"- 根因推断: {e['root_cause'][:200]}"
        for e in context.get("expert_summaries", [])
    )

    plan = context.get("self_healing_plan", {})
    actions = plan.get("actions", [])
    action_text = "\n".join(
        f"{i+1}. {a.get('action','?')}: {a.get('command','')} → {a.get('expected_effect','')}"
        for i, a in enumerate(actions)
    )

    exec_result = context.get("execution_result", {})
    obs_log = context.get("observation_log", [])

    return f"""请根据以下工业故障诊断数据，生成一份标准化的故障复盘Markdown案例。

## 故障概况
- 节点: {context.get('node_id','?')}
- 严重度: {context.get('severity','?')}
- 产线: {context.get('affected_line_profile','general')}

## 告警详情
{alert_text}

## 专家诊断
{expert_text}

## 仲裁结论
- 根因: {context.get('root_cause','?')}
- 置信度: {context.get('arbitration_confidence',0):.0%}

## 自愈动作
{action_text}

## 执行结果
- 状态: {exec_result.get('result','?')}
- 观察日志: {len(obs_log)} 个采样点

## 要求
请生成以下结构的Markdown（直接输出，不要代码块标记）:

---
ticket_id: (留空，系统自动生成)
fault_category: (根据根因推断，格式: domain/subdomain/type)
severity: {context.get('severity','P2')}
---

# 故障现象
(用2-3句话描述：什么时间、什么节点、什么指标异常)

# 排查链条
(分点列出各专家的关键发现)

# 根因
(综合根因分析，1-2句话)

# 修复步骤
(分步骤列出具体操作和命令)

# 经验教训
(1-2条可操作的建议或规则)
"""


def _basic_case(context: dict) -> str:
    """Generate a basic Markdown case without LLM (fallback)."""
    incident = context.get("incident", {})
    alerts = incident.get("aggregated_alerts", [])

    alert_lines = "\n".join(
        f"- {a.get('node_id','?')}: {a.get('metric_type','?')} = {a.get('current_value','?')} "
        f"(σ={a.get('deviation_sigma','?')})"
        for a in alerts[:3]
    )

    expert_lines = "\n".join(
        f"- **{e['agent']}** (confidence={e['confidence']:.0%}): {e['findings'][:200]}"
        for e in context.get("expert_summaries", [])
    )

    plan = context.get("self_healing_plan", {})
    actions = plan.get("actions", [])
    action_lines = "\n".join(
        f"{i+1}. `{a.get('command', a.get('action','?'))}` → {a.get('expected_effect','')}"
        for i, a in enumerate(actions)
    )

    return f"""---
ticket_id: AUTO
fault_category: auto/generated/{context.get('severity','unknown').lower()}
severity: {context.get('severity','P2')}
---

# 故障现象
{context.get('node_id','?')} 触发 {context.get('severity','?')} 级别告警。

告警指标:
{alert_lines}

# 排查链条
{expert_lines}

# 根因
{context.get('root_cause','LLM不可用，根因未自动提炼')}

# 修复步骤
{action_lines}

# 经验教训
- 本案例由系统自动生成（LLM API Key未配置），建议配置 DEEPSEEK_API_KEY 以获得更高质量的复盘文档。
"""


# ─── RAG Upsert ───────────────────────────────────────────────

async def _upsert_to_rag(ticket_id: str, content: str, context: dict) -> dict:
    """Push the generated case to ChromaDB via RAG Service."""
    metadata = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "resolved_at": datetime.now(timezone.utc).isoformat(),
        "fault_category": _infer_category(context),
        "severity": context.get("severity", "P2"),
        "affected_services": context.get("affected_services", []),
        "affected_nodes": [context.get("node_id", "")],
        "confidence": context.get("arbitration_confidence", 0.8),
        "risk_level": context.get("self_healing_plan", {}).get("risk_level", "low"),
        "fix_success": context.get("execution_result", {}).get("result") == "success",
        "tags": context.get("tags", ["flywheel"]),
        "line_profile": context.get("affected_line_profile", "general"),
        "source": "after_action_review",
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{RAG_SERVICE_URL}/api/v1/rag/upsert",
                json={
                    "ticket_id": ticket_id,
                    "content": content,
                    "metadata": metadata,
                },
                headers={"X-API-Key": API_KEY, "Content-Type": "application/json"},
            )
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        logger.error("RAG upsert failed: %s", e)
        return {"error": str(e)}


def _infer_category(context: dict) -> str:
    """Infer fault category from context."""
    root_cause = (context.get("root_cause") or "").lower()
    incident = context.get("incident", {})
    mg = incident.get("metric_group", "")

    if "redis" in root_cause: return "middleware/redis/performance"
    if "mysql" in root_cause: return "middleware/mysql/performance"
    if "kafka" in root_cause: return "middleware/kafka/lag"
    if "oom" in root_cause or "memory" in root_cause: return "k8s/oom"
    if "crash" in root_cause: return "k8s/crash_loop"
    if "network" in root_cause or "packet" in root_cause: return "network/packet_loss"
    if "dns" in root_cause: return "network/dns_failure"
    if "cnc" in root_cause or "plc" in root_cause: return "industrial/hardware"
    if mg == "latency": return "application/latency"
    if mg == "error": return "application/error"
    return "general/unknown"
