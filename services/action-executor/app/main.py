"""
Action Executor Service — executes self-healing actions after HITL approval.

POST /api/v1/execute       — execute a self-healing plan
GET  /health                — health check
GET  /api/v1/actions        — list available actions
"""

from __future__ import annotations

import os
import sys
import time
import logging
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException

_PARENT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

from common.auth import setup_api_key_auth  # noqa: E402

from .schemas import (  # noqa: E402
    ExecutionRequest, ExecutionResponse, ActionResult, ActionRequest,
)
from .executor import execute_action, get_risk_level, HANDLERS  # noqa: E402

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [executor] %(levelname)s %(message)s",
)
logger = logging.getLogger("action-executor")

SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8400"))
API_KEY = os.getenv("API_KEY", "dev-key-change-me")
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

if DRY_RUN:
    logger.warning("DRY_RUN mode enabled — all actions will be simulated")

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Industrial Fault Repair — Action Executor",
    version="1.0.0",
)
setup_api_key_auth(app)


# ---------------------------------------------------------------------------
# Health & Info
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {
        "status": "ok",
        "dry_run": DRY_RUN,
        "available_actions": len(HANDLERS),
    }


@app.get("/api/v1/actions")
def list_actions():
    """List all available action types with risk levels."""
    return {
        "actions": [
            {"name": name, "risk_level": info[1]}
            for name, info in HANDLERS.items()
        ]
    }


# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------

@app.post("/api/v1/execute", response_model=ExecutionResponse)
async def execute(req: ExecutionRequest):
    """
    Execute a self-healing plan.

    Called by:
      - Supervisor (for low-risk auto-executed actions)
      - HITL Gateway callback (after approval for medium/high risk)

    Actions are executed sequentially in order.
    """
    logger.info("[%s] Executing %d actions (dry_run=%s, risk=%s)",
                req.supervisor_trace_id, len(req.actions), req.dry_run or DRY_RUN,
                [get_risk_level(a.action) for a in req.actions])

    t0 = time.monotonic()
    results: List[ActionResult] = []
    all_success = True

    for action in req.actions:
        logger.info("  → Action %d/%d: %s on %s",
                    action.order, len(req.actions), action.action, action.target)

        result = execute_action(
            action.action,
            {**action.parameters, "target": action.target, "command": action.command},
            dry_run=req.dry_run or DRY_RUN,
        )

        ar = ActionResult(
            order=action.order,
            action=action.action,
            target=action.target,
            status=result["status"],
            output=result.get("output", {}),
            error=result.get("error", ""),
            duration_ms=result.get("duration_ms", 0),
        )
        results.append(ar)

        if result["status"] == "failed":
            all_success = False
            logger.error("  → Action %d FAILED: %s", action.order, result.get("error", "unknown"))
        else:
            logger.info("  → Action %d OK (%dms)", action.order, result.get("duration_ms", 0))

    total_ms = int((time.monotonic() - t0) * 1000)

    status = "success" if all_success else "partial_failure"
    if req.dry_run or DRY_RUN:
        status = "dry_run"

    logger.info("[%s] Execution complete: status=%s, %d actions, %dms",
                req.supervisor_trace_id, status, len(results), total_ms)

    return ExecutionResponse(
        supervisor_trace_id=req.supervisor_trace_id,
        status=status,
        results=results,
        rollback_triggered=False,
        total_duration_ms=total_ms,
    )


# ---------------------------------------------------------------------------
# Single action (for quick tests)
# ---------------------------------------------------------------------------

@app.post("/api/v1/execute/single")
async def execute_single(action: ActionRequest):
    """Execute a single action (convenience endpoint for testing)."""
    result = execute_action(
        action.action,
        {**action.parameters, "target": action.target},
        dry_run=action.dry_run or DRY_RUN,
    )
    return {
        "action": action.action,
        "target": action.target,
        **result,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=SERVICE_PORT, reload=True)
