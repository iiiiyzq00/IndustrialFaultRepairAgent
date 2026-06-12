"""Fallback handler — invoked when HITL times out or execution fails."""

from __future__ import annotations

import os
import logging
from typing import Dict, Any

import httpx

logger = logging.getLogger(__name__)

API_KEY = os.getenv("API_KEY", "dev-key-change-me")
DINGTALK_WEBHOOK = os.getenv("DINGTALK_WEBHOOK_URL", "")


async def handle_fallback(
    incident_id: str,
    reason: str,
    fallback_action: str,
    context: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """
    Handle fallback scenarios:
      - notify_oncall: send IM notification to on-call engineer
      - retry_with_alternate_plan: attempt a degraded self-healing approach
      - escalate_to_supervisor: escalate to human supervisor
      - manual_only: mark as requiring full manual intervention
    """
    context = context or {}
    logger.warning("Fallback triggered: incident=%s reason=%s action=%s",
                   incident_id, reason, fallback_action)

    result = {
        "incident_id": incident_id,
        "fallback_status": "acknowledged",
        "oncall_notified": False,
        "notification_channels": [],
    }

    if fallback_action == "notify_oncall":
        await _notify_oncall(incident_id, reason, context)
        result["oncall_notified"] = True
        result["notification_channels"] = _enabled_channels()

    elif fallback_action == "retry_with_alternate_plan":
        logger.info("Attempting alternate self-healing plan for %s", incident_id)
        result["fallback_status"] = "retry_initiated"

    elif fallback_action in ("escalate_to_supervisor", "manual_only"):
        await _notify_oncall(incident_id, reason, context)
        result["oncall_notified"] = True
        result["fallback_status"] = "escalated"
        result["notification_channels"] = _enabled_channels()

    return result


async def _notify_oncall(incident_id: str, reason: str, context: Dict[str, Any]) -> None:
    """Send notification to on-call engineer via DingTalk/WeChat Work."""
    if not DINGTALK_WEBHOOK:
        logger.warning("DINGTALK_WEBHOOK_URL not set — skipping IM notification")
        return

    message = {
        "msgtype": "markdown",
        "markdown": {
            "title": f"故障自愈审批超时 - {incident_id}",
            "text": (
                f"## ⚠️ 故障自愈审批超时\n\n"
                f"- **Incident ID**: {incident_id}\n"
                f"- **原因**: {reason}\n"
                f"- **上下文**: {context}\n"
                f"- **状态**: 等待人工介入\n\n"
                f"[查看详情](http://hitl-frontend:3000/approvals/{incident_id})"
            ),
        },
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(DINGTALK_WEBHOOK, json=message)
            logger.info("On-call notification sent for %s", incident_id)
    except Exception as e:
        logger.error("Failed to send on-call notification: %s", e)


def _enabled_channels() -> list[str]:
    channels = ["web_panel"]
    if DINGTALK_WEBHOOK:
        channels.append("dingtalk")
    return channels
