"""
HITL (Human-in-the-Loop) Gateway.

Manages the approval workflow for self-healing actions:
  - Low risk: auto-executed (never reaches here)
  - Medium risk: requires 1 human approval
  - High risk: requires 2 human approvals

Endpoints:
  POST   /api/v1/approvals               — create approval
  GET    /api/v1/approvals/pending       — list pending
  GET    /api/v1/approvals/{id}          — get approval detail
  POST   /api/v1/approvals/{id}/approve  — approve
  POST   /api/v1/approvals/{id}/reject   — reject
  WS     /api/v1/approvals/ws            — WebSocket for real-time push

Timeout scanning:
  APScheduler runs every 30s, marks expired approvals, and calls
  Supervisor's /incident/{id}/fallback endpoint.
"""

from __future__ import annotations

import os
import sys
import uuid
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict

import httpx
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

# Path for common module
_PARENT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

from common.auth import setup_api_key_auth  # noqa: E402
from common.metrics import setup_metrics  # noqa: E402
from common.notifier import notify_hitl_approval  # noqa: E402

from .schemas import (  # noqa: E402
    CreateApprovalRequest, Approval, ApproveRequest, RejectRequest, ApproverAction,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [hitl] %(levelname)s %(message)s",
)
logger = logging.getLogger("hitl-gateway")

SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8300"))
SUPERVISOR_URL = os.getenv("SUPERVISOR_URL", "http://agent-supervisor:8100")
API_KEY = os.getenv("API_KEY", "dev-key-change-me")
APPROVAL_TIMEOUT_MEDIUM = int(os.getenv("APPROVAL_TIMEOUT_MEDIUM_SECONDS", "300"))
APPROVAL_TIMEOUT_HIGH = int(os.getenv("APPROVAL_TIMEOUT_HIGH_SECONDS", "600"))

# In-memory store (MVP)
_approvals: Dict[str, Approval] = {}
# WebSocket connections
_ws_clients: list[WebSocket] = []

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Industrial Fault Repair — HITL Gateway",
    version="1.0.0",
)
setup_api_key_auth(app)
setup_metrics(app)


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

_scheduler = BackgroundScheduler()


def _scan_expired():
    """Scan for expired approvals and trigger fallback."""
    now = datetime.now(timezone.utc)
    expired_ids = []

    for aid, approval in _approvals.items():
        if approval.status != "pending":
            continue
        expires_at = datetime.fromisoformat(approval.expires_at)
        if now >= expires_at:
            approval.status = "expired"
            expired_ids.append(aid)
            logger.warning("Approval %s expired (risk=%s, trace=%s)",
                           aid, approval.risk_level, approval.supervisor_trace_id)
            # Send notification
            _send_approval_notification(approval, "expired")
            # Trigger fallback on Supervisor
            _trigger_fallback(approval)

    for aid in expired_ids:
        _broadcast({"type": "approval_expired", "approval_id": aid})


def _trigger_fallback(approval: Approval):
    """Call Supervisor's fallback endpoint."""
    fallback_action = "notify_oncall" if approval.risk_level == "medium" else "escalate_to_supervisor"
    try:
        # Fire-and-forget HTTP call
        httpx.post(
            f"{SUPERVISOR_URL}/api/v1/incident/{approval.supervisor_trace_id}/fallback",
            json={
                "reason": "approval_timeout",
                "approval_id": approval.approval_id,
                "fallback_action": fallback_action,
                "context": {"risk_level": approval.risk_level},
            },
            headers={"X-API-Key": API_KEY},
            timeout=10.0,
        )
    except Exception as e:
        logger.error("Fallback trigger failed for %s: %s", approval.approval_id, e)


@app.on_event("startup")
def start_scheduler():
    _scheduler.add_job(
        _scan_expired,
        IntervalTrigger(seconds=30),
        id="expiry-scanner",
        name="Scan expired approvals every 30s",
        replace_existing=True,
    )
    _scheduler.start()
    logger.info("APScheduler started: scanning every 30s")
    logger.info("Timeout config: medium=%ds, high=%ds", APPROVAL_TIMEOUT_MEDIUM, APPROVAL_TIMEOUT_HIGH)


@app.on_event("shutdown")
def stop_scheduler():
    _scheduler.shutdown(wait=False)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok", "pending_approvals": sum(1 for a in _approvals.values() if a.status == "pending")}


@app.post("/api/v1/approvals")
async def create_approval(req: CreateApprovalRequest):
    """Create a new approval (called by Supervisor for medium/high risk actions)."""
    if req.risk_level not in ("medium", "high"):
        raise HTTPException(status_code=400, detail="Only medium/high risk actions require approval")

    required = 1 if req.risk_level == "medium" else 2

    # Determine timeout
    timeout_s = APPROVAL_TIMEOUT_MEDIUM if req.risk_level == "medium" else APPROVAL_TIMEOUT_HIGH
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=timeout_s)).isoformat()

    approval = Approval(
        approval_id=str(uuid.uuid4()),
        supervisor_trace_id=req.supervisor_trace_id,
        arbitration_id=req.arbitration_id,
        risk_level=req.risk_level,
        required_approvers=required,
        summary=req.summary,
        self_healing_plan=req.self_healing_plan,
        expires_at=expires_at,
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    _approvals[approval.approval_id] = approval

    logger.info("Approval created: id=%s risk=%s required=%d expires=%s",
                approval.approval_id, req.risk_level, required, expires_at)

    # Push to WebSocket clients
    _broadcast({"type": "new_approval", "approval": _approval_to_dict(approval)})

    # Send notification
    import asyncio
    asyncio.create_task(notify_hitl_approval(
        approval.approval_id, approval.supervisor_trace_id,
        approval.risk_level, approval.summary, "pending"))

    return _approval_to_dict(approval)


@app.get("/api/v1/approvals/pending")
def list_pending(risk_level: str | None = None, trace_id: str | None = None, limit: int = 20):
    """List pending approvals (for the web panel)."""
    items = []
    for a in _approvals.values():
        if a.status != "pending":
            continue
        if risk_level and a.risk_level != risk_level:
            continue
        if trace_id and a.supervisor_trace_id != trace_id:
            continue
        items.append(_approval_to_dict(a))
        if len(items) >= limit:
            break
    return {"total": len(items), "items": items}


@app.get("/api/v1/approvals/{approval_id}")
def get_approval(approval_id: str):
    """Get a single approval by ID."""
    a = _approvals.get(approval_id)
    if not a:
        raise HTTPException(status_code=404, detail="Approval not found")
    return _approval_to_dict(a)


@app.post("/api/v1/approvals/{approval_id}/approve")
def approve(approval_id: str, req: ApproveRequest):
    """Approve a pending approval."""
    a = _approvals.get(approval_id)
    if not a:
        raise HTTPException(status_code=404, detail="Approval not found")
    if a.status != "pending":
        raise HTTPException(status_code=409, detail=f"Approval already {a.status}")

    # Check for duplicate approver
    for approver in a.approvers:
        if approver.user_id == req.user_id:
            raise HTTPException(status_code=409, detail="User already voted")

    action = ApproverAction(
        user_id=req.user_id,
        action="approved",
        comment=req.comment,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )
    a.approvers.append(action)
    a.current_approvals += 1

    if a.current_approvals >= a.required_approvers:
        a.status = "approved"
        logger.info("Approval %s APPROVED (%d/%d)", approval_id, a.current_approvals, a.required_approvers)
        _send_approval_notification(a, "approved")
        # Trigger execution via Supervisor (call back)
        _notify_supervisor_approved(a)

    _broadcast({"type": "approval_updated", "approval": _approval_to_dict(a)})

    return {"approval_id": approval_id, "status": a.status, "action_executed": a.status == "approved"}


@app.post("/api/v1/approvals/{approval_id}/reject")
def reject(approval_id: str, req: RejectRequest):
    """Reject a pending approval."""
    a = _approvals.get(approval_id)
    if not a:
        raise HTTPException(status_code=404, detail="Approval not found")
    if a.status != "pending":
        raise HTTPException(status_code=409, detail=f"Approval already {a.status}")

    a.status = "rejected"
    a.approvers.append(ApproverAction(
        user_id=req.user_id,
        action="rejected",
        comment=req.reason,
        timestamp=datetime.now(timezone.utc).isoformat(),
    ))

    logger.info("Approval %s REJECTED by %s: %s", approval_id, req.user_id, req.reason)

    # Notify Supervisor that action was rejected
    _notify_supervisor_rejected(a, req.reason)

    _broadcast({"type": "approval_updated", "approval": _approval_to_dict(a)})

    return _approval_to_dict(a)


# ---------------------------------------------------------------------------
# WebSocket
# ---------------------------------------------------------------------------

@app.websocket("/api/v1/approvals/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    _ws_clients.append(ws)
    logger.info("WebSocket client connected (total=%d)", len(_ws_clients))
    try:
        while True:
            await ws.receive_text()  # keep-alive
    except WebSocketDisconnect:
        _ws_clients.remove(ws)
        logger.info("WebSocket client disconnected (total=%d)", len(_ws_clients))


def _broadcast(message: dict):
    """Send a JSON message to all connected WebSocket clients."""
    dead = []
    for ws in _ws_clients:
        try:
            # Use asyncio.create_task since this may be called from sync context
            import asyncio
            asyncio.create_task(ws.send_json(message))
        except Exception:
            dead.append(ws)
    for ws in dead:
        _ws_clients.remove(ws)


def _send_approval_notification(approval, status: str):
    """Fire-and-forget notification for approval events."""
    try:
        import asyncio
        asyncio.create_task(notify_hitl_approval(
            approval.approval_id,
            approval.supervisor_trace_id,
            approval.risk_level,
            approval.summary,
            status,
        ))
    except Exception as e:
        logger.warning("Failed to send approval notification: %s", e)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _approval_to_dict(a: Approval) -> dict:
    d = a.model_dump()
    d["self_healing_plan"] = a.self_healing_plan  # already dict
    return d


def _notify_supervisor_approved(a: Approval):
    """Notify Supervisor to resume the LangGraph pipeline (HITL approved)."""
    try:
        resp = httpx.post(
            f"{SUPERVISOR_URL}/api/v1/incident/{a.supervisor_trace_id}/resume",
            json={
                "status": "approved",
                "approval_id": a.approval_id,
                "user_id": "hitl-gateway",
                "reason": "Approved by HITL gateway",
            },
            headers={"X-API-Key": API_KEY},
            timeout=10.0,
        )
        logger.info("Supervisor resume response: %s", resp.status_code)
    except Exception as e:
        logger.error("Failed to notify Supervisor of approval: %s", e)


def _notify_supervisor_rejected(a: Approval, reason: str):
    """Notify Supervisor that the HITL approval was rejected."""
    # Try resume endpoint first (for LangGraph), fall back to /fallback
    try:
        resp = httpx.post(
            f"{SUPERVISOR_URL}/api/v1/incident/{a.supervisor_trace_id}/resume",
            json={
                "status": "rejected",
                "approval_id": a.approval_id,
                "user_id": "hitl-gateway",
                "reason": reason,
            },
            headers={"X-API-Key": API_KEY},
            timeout=10.0,
        )
        logger.info("Supervisor resume (rejected) response: %s", resp.status_code)
    except Exception as e:
        logger.error("Failed to notify Supervisor of rejection via resume: %s", e)
        # Fallback
        try:
            httpx.post(
                f"{SUPERVISOR_URL}/api/v1/incident/{a.supervisor_trace_id}/fallback",
                json={
                    "reason": "manual_rejection",
                    "approval_id": a.approval_id,
                    "fallback_action": "manual_only",
                    "context": {"rejection_reason": reason},
                },
                headers={"X-API-Key": API_KEY},
                timeout=10.0,
            )
        except Exception as e2:
            logger.error("Fallback also failed: %s", e2)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=SERVICE_PORT, reload=True)
