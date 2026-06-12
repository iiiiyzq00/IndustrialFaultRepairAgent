"""
Async HTTP client for the Digital Twin Sandbox Service.

Follows the same pattern as rag_client.py — lightweight async wrapper
with configurable timeout and graceful degradation on failure.
"""

from __future__ import annotations

import os
import logging
from typing import Any, Dict

import httpx
from .schemas import SandboxVerdict, SandboxRiskItem

logger = logging.getLogger(__name__)

SANDBOX_SERVICE_URL = os.getenv("SANDBOX_SERVICE_URL", "http://sandbox-service:8500")
API_KEY = os.getenv("API_KEY", "dev-key-change-me")
TIMEOUT = float(os.getenv("SANDBOX_VERIFY_TIMEOUT_SECONDS", "10.0"))


async def verify_actions(
    trace_id: str,
    incident: Dict[str, Any],
    arbitration_result: Dict[str, Any],
    expert_results: Dict[str, Dict[str, Any]],
    rag_context: Dict[str, Any],
) -> SandboxVerdict:
    """
    Call the sandbox service to verify a self-healing plan before execution.

    On any failure (timeout, connection error, HTTP error), returns a
    safe-default verdict (fail-open) so the pipeline is not blocked.
    """
    plan = arbitration_result.get("self_healing_plan", {})

    # Skip sandbox if there are no actions to verify
    if not plan.get("actions"):
        logger.info("[%s] No actions to verify — skipping sandbox", trace_id)
        return SandboxVerdict(
            verdict="safe", confidence=1.0, safe=True,
            reasoning="无动作需要验证", fallback=False,
        )

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(TIMEOUT, connect=5.0, read=TIMEOUT, write=5.0),
        ) as client:
            resp = await client.post(
                f"{SANDBOX_SERVICE_URL}/api/v1/sandbox/verify",
                json={
                    "supervisor_trace_id": trace_id,
                    "incident": incident,
                    "arbitration_result": arbitration_result,
                    "self_healing_plan": plan,
                    "expert_results": expert_results,
                    "rag_context": rag_context,
                },
                headers={
                    "X-API-Key": API_KEY,
                    "Content-Type": "application/json",
                },
            )
            resp.raise_for_status()
            data = resp.json()

            verdict = SandboxVerdict(**data)
            logger.info(
                "[%s] Sandbox verdict: %s (confidence=%.0f%%, %dms)%s",
                trace_id, verdict.verdict, verdict.confidence * 100,
                verdict.simulation_duration_ms,
                " [FALLBACK]" if verdict.fallback else "",
            )

            if verdict.risks:
                for r in verdict.risks[:3]:
                    logger.info("[%s]   risk: [%s] %s", trace_id, r.severity, r.description[:120])

            return verdict

    except httpx.TimeoutException:
        logger.warning("[%s] Sandbox timed out after %.1fs — fail-open (safe default)", trace_id, TIMEOUT)
        return _fail_open_verdict("sandbox timeout")

    except httpx.ConnectError:
        logger.warning("[%s] Sandbox service unreachable — fail-open (safe default)", trace_id)
        return _fail_open_verdict("sandbox unreachable")

    except Exception as e:
        logger.warning("[%s] Sandbox call failed: %s — fail-open (safe default)", trace_id, e)
        return _fail_open_verdict(f"sandbox error: {e}")


def _fail_open_verdict(reason: str) -> SandboxVerdict:
    """Return a safe-default verdict when the sandbox is unavailable.

    Fail-open: we let the action proceed but mark fallback=True for audit.
    The observation window still provides a safety net after execution.
    """
    return SandboxVerdict(
        verdict="safe",
        confidence=0.5,
        safe=True,
        reasoning=f"沙盒服务不可用（{reason}），降级为直接执行（fail-open）。观察窗口仍会提供安全保障",
        fallback=True,
        risks=[SandboxRiskItem(
            risk_id="sandbox-unavailable",
            description=f"沙盒服务不可用: {reason}，跳过预验证",
            severity="low",
            probability=0.3,
        )],
    )
