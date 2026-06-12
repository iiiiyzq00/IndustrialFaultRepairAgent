"""DeepSeek-V3 API client for expert reasoning."""

from __future__ import annotations

import os
import json
import logging
from typing import Any, Dict, List

import httpx

logger = logging.getLogger(__name__)

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1")
LLM_MODEL = os.getenv("LLM_MODEL", "deepseek-chat")


async def reason(
    agent_type: str,
    incident: dict,
    domain_hypothesis: str,
    rag_context: List[dict],
    tool_results: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Call DeepSeek-V3 with the full context (incident + RAG + tool results)
    and ask it to produce a structured diagnosis.
    """
    if not DEEPSEEK_API_KEY:
        logger.warning("DEEPSEEK_API_KEY not set — returning mock diagnosis")
        return _mock_diagnosis(agent_type, tool_results)

    prompt = _build_reasoning_prompt(agent_type, incident, domain_hypothesis, rag_context, tool_results)

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
                        {"role": "system", "content": _system_prompt(agent_type)},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.3,
                    "max_tokens": 2000,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"]

            # Parse JSON from LLM response
            content = content.strip()
            if content.startswith("```"):
                lines = content.split("\n")
                content = "\n".join(lines[1:])
                if content.endswith("```"):
                    content = content[:-3]

            return json.loads(content)

    except Exception as e:
        logger.error("LLM reasoning failed: %s", e)
        return _mock_diagnosis(agent_type, tool_results)


def _system_prompt(agent_type: str) -> str:
    prompts = {
        "k8s": "你是一个 Kubernetes 集群诊断专家。你掌握 kubectl、事件查询和节点监控技能。请根据工具调用结果给出根因推断，始终输出严格JSON。",
        "middleware": "你是一个中间件（Redis/MySQL/Kafka/RabbitMQ）诊断专家。请根据慢日志、连接数和配置信息推断根因，始终输出严格JSON。",
        "network": "你是一个工业网络诊断专家。请根据 ping、traceroute、DNS 解析和服务网格指标推断根因，始终输出严格JSON。",
        "application": "你是一个应用性能诊断专家。请根据链路追踪、日志和 APM 指标推断根因，始终输出严格JSON。",
    }
    return prompts.get(agent_type, "你是一个工业故障诊断专家，请输出严格JSON。")


def _build_reasoning_prompt(
    agent_type: str,
    incident: dict,
    hypothesis: str,
    rag_context: List[dict],
    tool_results: List[Dict[str, Any]],
) -> str:
    lines = [
        f"你是一个{agent_type}领域诊断专家。请根据以下信息给出本领域的诊断结论。",
        "",
        "## 故障概况",
        json.dumps(incident, ensure_ascii=False, indent=2),
        "",
        f"## 主控假设\n{hypothesis}",
        "",
        "## 历史相似案例 (RAG)",
    ]
    for doc in rag_context[:3]:
        lines.append(f"- [{doc.get('ticket_id', '?')}] {doc.get('root_cause_summary', '')[:200]}")
        lines.append(f"  相似度: {doc.get('relevance_score', 0):.0%}")

    lines.append("")
    lines.append("## 工具调用结果")
    for tr in tool_results:
        lines.append(f"### {tr['tool_name']} (成功={tr['success']})")
        lines.append(f"```\n{json.dumps(tr.get('output', {}), ensure_ascii=False, indent=2)[:2000]}\n```")

    lines.extend([
        "",
        "请输出严格JSON（不要markdown代码块标记）:",
        "{",
        '  "findings": "本领域观察到的具体现象",',
        '  "suspected_root_cause": "本领域根因推断",',
        '  "confidence": 0.85,',
        '  "suggested_actions": [{"action": "rollback_deployment", "target": "...", "risk_level": "low"}],',
        '  "needs_more_investigation": false',
        "}",
    ])
    return "\n".join(lines)


def _mock_diagnosis(agent_type: str, tool_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Return a mock diagnosis for testing without an LLM API key."""
    return {
        "findings": f"[Mock] {agent_type} expert collected {len(tool_results)} tool results",
        "suspected_root_cause": f"[Mock] {agent_type} root cause — LLM API key not configured",
        "confidence": 0.5,
        "suggested_actions": [{"action": "manual_check", "target": "unknown", "risk_level": "medium"}],
        "needs_more_investigation": True,
    }
