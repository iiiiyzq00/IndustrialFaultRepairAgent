"""
Notification module — DingTalk / WeCom (企业微信) webhook sender.

Usage:
  from services.common.notifier import send_dingtalk, send_wecom

  await send_dingtalk("故障自愈通知", "order-svc 自愈成功，P99延迟已恢复")
  await send_wecom("故障自愈告警", "order-svc 回滚触发，需人工介入")

All functions are async and fail silently — a notification failure
must never block the main diagnosis pipeline.
"""

from __future__ import annotations

import os
import json
import logging
import httpx
from datetime import datetime, timezone

logger = logging.getLogger("notifier")

DINGTALK_WEBHOOK_URL = os.getenv("DINGTALK_WEBHOOK_URL", "")
WECOM_WEBHOOK_URL = os.getenv("WECOM_WEBHOOK_URL", "")

# ─── Public API ──────────────────────────────────────────────────


async def send_dingtalk(
    title: str,
    content: str,
    msg_type: str = "markdown",
    webhook_url: str = "",
) -> bool:
    """
    Send a DingTalk (钉钉) robot notification via webhook.

    Args:
        title: Message title
        content: Message body (Markdown format)
        msg_type: "text" or "markdown"
        webhook_url: Override the default webhook URL

    Returns:
        True if sent successfully, False otherwise.
    """
    url = webhook_url or DINGTALK_WEBHOOK_URL
    if not url:
        logger.debug("DingTalk webhook URL not configured — skipping notification")
        return False

    if msg_type == "markdown":
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": title,
                "text": f"## {title}\n\n{content}\n\n> 发送时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n> 系统: 工业故障自愈 Multi-Agent",
            },
        }
    else:
        payload = {
            "msgtype": "text",
            "text": {"content": f"[{title}] {content}"},
        }

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            result = resp.json()
            if result.get("errcode") == 0:
                logger.info("DingTalk notification sent: %s", title)
                return True
            else:
                logger.warning("DingTalk returned error: %s", result.get("errmsg", "unknown"))
                return False
    except Exception as e:
        logger.warning("DingTalk notification failed: %s", e)
        return False


async def send_wecom(
    title: str,
    content: str,
    webhook_url: str = "",
) -> bool:
    """
    Send a WeCom (企业微信) robot notification via webhook.

    Args:
        title: Message title
        content: Message body
        webhook_url: Override the default webhook URL

    Returns:
        True if sent successfully, False otherwise.
    """
    url = webhook_url or WECOM_WEBHOOK_URL
    if not url:
        logger.debug("WeCom webhook URL not configured — skipping notification")
        return False

    payload = {
        "msgtype": "markdown",
        "markdown": {
            "content": f"## {title}\n{content}\n<font color=\"comment\">工业故障自愈 Multi-Agent | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}</font>",
        },
    }

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            result = resp.json()
            if result.get("errcode") == 0:
                logger.info("WeCom notification sent: %s", title)
                return True
            else:
                logger.warning("WeCom returned error: %s", result.get("errmsg", "unknown"))
                return False
    except Exception as e:
        logger.warning("WeCom notification failed: %s", e)
        return False


async def notify_self_healing_result(
    trace_id: str,
    node_id: str,
    result: str,
    actions: list,
    duration_s: float,
    detail: str = "",
) -> None:
    """
    Convenience function: send notification for a self-healing execution result.

    Automatically chooses the right severity and channel.
    """
    action_summary = ", ".join(a.get("action", "?") for a in actions[:3])

    if result == "success":
        title = f"✅ 自愈成功 — {node_id}"
        content = (
            f"**节点**: {node_id}\n"
            f"**动作**: {action_summary}\n"
            f"**耗时**: {duration_s:.1f}s\n"
            f"**诊断ID**: {trace_id}\n"
            + (f"**详情**: {detail}" if detail else "")
        )
    elif result == "rollback_triggered":
        title = f"⚠️ 自动回滚触发 — {node_id}"
        content = (
            f"**节点**: {node_id}\n"
            f"**动作**: {action_summary}\n"
            f"**耗时**: {duration_s:.1f}s\n"
            f"**诊断ID**: {trace_id}\n"
            f"**状态**: 观察窗口检测到指标恶化，已自动回滚\n"
            + (f"**详情**: {detail}" if detail else "")
        )
    elif result == "blocked_by_sandbox":
        title = f"🛡️ 沙盒阻塞 — {node_id}"
        content = (
            f"**节点**: {node_id}\n"
            f"**动作**: {action_summary}\n"
            f"**诊断ID**: {trace_id}\n"
            f"**状态**: 数字孪生沙盒判定动作不安全，已阻塞执行\n"
            + (f"**详情**: {detail}" if detail else "")
        )
    else:
        title = f"❌ 自愈失败 — {node_id}"
        content = (
            f"**节点**: {node_id}\n"
            f"**动作**: {action_summary}\n"
            f"**耗时**: {duration_s:.1f}s\n"
            f"**诊断ID**: {trace_id}\n"
            f"**状态**: {result}\n"
            + (f"**详情**: {detail}" if detail else "")
        )

    await send_dingtalk(title, content)
    await send_wecom(title, content)


async def notify_hitl_approval(
    approval_id: str,
    trace_id: str,
    risk_level: str,
    summary: str,
    status: str,
) -> None:
    """
    Convenience function: send notification for HITL approval events.
    """
    if status == "pending":
        title = f"🔔 待审批 — {risk_level.upper()} 风险自愈动作"
        content = (
            f"**审批ID**: {approval_id}\n"
            f"**诊断ID**: {trace_id}\n"
            f"**风险等级**: {risk_level}\n"
            f"**摘要**: {summary[:200]}\n"
            f"**操作**: 请登录审批面板确认或拒绝"
        )
    elif status == "approved":
        title = f"✅ 审批通过 — {risk_level.upper()} 风险动作"
        content = (
            f"**审批ID**: {approval_id}\n"
            f"**诊断ID**: {trace_id}\n"
            f"**风险等级**: {risk_level}\n"
            f"**状态**: 已批准，即将执行"
        )
    elif status == "rejected":
        title = f"❌ 审批拒绝 — {risk_level.upper()} 风险动作"
        content = (
            f"**审批ID**: {approval_id}\n"
            f"**诊断ID**: {trace_id}\n"
            f"**风险等级**: {risk_level}\n"
            f"**状态**: 已拒绝"
        )
    elif status == "expired":
        title = f"⏰ 审批超时 — {risk_level.upper()} 风险动作"
        content = (
            f"**审批ID**: {approval_id}\n"
            f"**诊断ID**: {trace_id}\n"
            f"**风险等级**: {risk_level}\n"
            f"**状态**: 已超时，触发降级策略"
        )
    else:
        return

    await send_dingtalk(title, content)
    await send_wecom(title, content)
