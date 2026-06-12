"""
Agent Supervisor — LangGraph-powered with SQLite checkpoint persistence.

All runtime state is stored in LangGraph's state graph and checkpointed
at every node transition. Survives process restart.

Endpoints:
  POST /api/v1/incident                  ← Flink webhook
  POST /api/v1/incident/{id}/resume      ← HITL approval callback (LangGraph resume)
  POST /api/v1/incident/{id}/fallback    ← HITL timeout / execution failure
  GET  /health                            ← health check
  GET  /api/v1/diagnosis/{trace_id}       ← query diagnosis state from checkpoint
"""

from __future__ import annotations

import os
import sys
import uuid
import logging
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from langgraph.types import Command
from langgraph.errors import GraphInterrupt

# Path for common module
_PARENT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

from common.auth import setup_api_key_auth  # noqa: E402
from common.metrics import setup_metrics, DIAGNOSIS_TOTAL, DIAGNOSIS_CONFIDENCE, ACTIVE_DIAGNOSES  # noqa: E402

from .schemas import (  # noqa: E402
    IncidentEvent, IncidentAccepted,
    FallbackRequest, FallbackResponse,
)
from . import fallback  # noqa: E402

# ── LangGraph ──────────────────────────────────────────────────
from .graph import init_graph, get_graph, DiagnosisState, CHECKPOINT_DB  # noqa: E402

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [supervisor] %(levelname)s %(message)s",
)
logger = logging.getLogger("supervisor")

SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8100"))

# ── Built at startup via lifespan ─────────────────────────────
_diagnosis_graph = None

# Store active configs for resume
_active_configs: Dict[str, Dict[str, Any]] = {}


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize LangGraph with SQLite checkpointer on startup."""
    global _diagnosis_graph
    logger.info("Initializing LangGraph with SQLite checkpointer...")
    _diagnosis_graph = await init_graph()
    logger.info("LangGraph ready")
    yield
    logger.info("Shutting down LangGraph")

app = FastAPI(
    title="Industrial Fault Repair — Agent Supervisor (LangGraph)",
    version="2.0.0",
    lifespan=lifespan,
)
setup_api_key_auth(app)
setup_metrics(app)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    try:
        g = get_graph()
        cp = type(g.checkpointer).__name__ if g.checkpointer else "none"
    except Exception:
        cp = "initializing"
    return {
        "status": "ok",
        "active_configs": len(_active_configs),
        "service": "agent-supervisor-langgraph",
        "checkpointer": cp,
        "db": CHECKPOINT_DB if CHECKPOINT_DB else "memory",
    }


# ---------------------------------------------------------------------------
# Incident Webhook (Flink → Supervisor)
# ---------------------------------------------------------------------------

@app.post("/api/v1/incident", response_model=IncidentAccepted)
async def create_incident(incident: IncidentEvent):
    """
    Receive aggregated incident from Flink. Kick off the LangGraph pipeline.

    The graph auto-persists state at each node. If the process restarts
    mid-pipeline, the graph resumes from the last checkpoint automatically.
    """
    trace_id = f"trace-{uuid.uuid4().hex[:12]}"
    logger.info("Incident received: id=%s trace=%s priority=%.1f",
                incident.incident_id, trace_id, incident.priority_score)

    # Check for duplicate active diagnoses (and purge stale _active_configs entries)
    for tid, cfg in list(_active_configs.items()):
        try:
            state = await get_graph().aget_state(cfg)
            if not state or not state.values:
                _active_configs.pop(tid, None)  # stale — no checkpoint
                continue
            node_id = state.values.get("incident", {}).get("node_id", "")
            exec_status = state.values.get("execution_status", "")
            # Terminal states shouldn't stick around
            if exec_status in ("success", "failed", "rollback_triggered"):
                _active_configs.pop(tid, None)
                continue
            if node_id == incident.node_id and exec_status in ("", "running", "observing", "awaiting_approval"):
                return JSONResponse(
                    status_code=429,
                    content={"error": {"code": "INCIDENT_DUPLICATE",
                                       "message": f"Active diagnosis for node {incident.node_id}"}},
                )
        except Exception:
            pass

    DIAGNOSIS_TOTAL.labels(status="accepted").inc()
    ACTIVE_DIAGNOSES.inc()

    # Build initial state
    initial_state: Dict[str, Any] = {
        "incident": incident.model_dump(),
        "supervisor_trace_id": trace_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "execution_status": "running",
        "expert_results": {},
        "observation_log": [],
        "phase_timings": {},
    }

    # Launch graph in background
    config = {"configurable": {"thread_id": trace_id}}
    _active_configs[trace_id] = config
    asyncio.create_task(_run_graph(trace_id, initial_state, config))

    return IncidentAccepted(
        incident_id=incident.incident_id,
        status="accepted",
        supervisor_trace_id=trace_id,
        estimated_completion_seconds=75,
    )


async def _run_graph(trace_id: str, initial_state, config: Dict):
    """Run the LangGraph in background, updating metrics on completion.

    ``initial_state`` can be a plain dict (cold start) or a ``Command``
    object (resume after HITL interrupt).

    LangGraph 1.2.x catches ``GraphInterrupt`` internally — ``ainvoke``
    returns with ``execution_status="awaiting_approval"`` when paused.
    Resume via second ``ainvoke(Command(resume=...), config)``.
    """
    try:
        graph = get_graph()
        final_state = await graph.ainvoke(initial_state, config)
        exec_status = final_state.get("execution_status", "failed")
        logger.info("[%s] LangGraph done: status=%s", trace_id, exec_status)

        if exec_status == "awaiting_approval":
            # Paused at interrupt() — checkpoint saved, keep config alive
            return

        # Terminal — record metrics and clean up
        DIAGNOSIS_TOTAL.labels(status=exec_status).inc()
        ACTIVE_DIAGNOSES.dec()
        _active_configs.pop(trace_id, None)
        arb = final_state.get("arbitration_result", {})
        if arb.get("confidence"):
            DIAGNOSIS_CONFIDENCE.observe(arb["confidence"])

    except Exception as e:
        logger.exception("[%s] LangGraph pipeline crashed: %s", trace_id, e)
        DIAGNOSIS_TOTAL.labels(status="failed").inc()
        ACTIVE_DIAGNOSES.dec()
        _active_configs.pop(trace_id, None)


# ---------------------------------------------------------------------------
# Resume Endpoint (HITL approval callback)
# ---------------------------------------------------------------------------

@app.post("/api/v1/incident/{trace_id}/resume")
async def resume_diagnosis(trace_id: str, decision: Dict[str, Any]):
    """
    Resume a HITL-interrupted LangGraph.

    Called by HITL Gateway when a human approves or rejects.
    Passes the decision to LangGraph's Command(resume=...).
    """
    config = {"configurable": {"thread_id": trace_id}}

    try:
        # Check current state (async for AsyncSqliteSaver)
        state = await get_graph().aget_state(config)
        if not state or not state.values:
            raise HTTPException(status_code=404, detail="Diagnosis not found or already completed")

        current = state.values
        if current.get("execution_status") not in ("running", "", "awaiting_approval"):
            raise HTTPException(status_code=409, detail=f"Diagnosis already {current.get('execution_status')}")

        logger.info("[%s] Resuming from HITL interrupt with decision: %s", trace_id, decision)

        # Resume graph — Command(resume=...) is passed as the input to ainvoke
        cmd = Command(resume=decision)
        _active_configs[trace_id] = config
        asyncio.create_task(_run_graph(trace_id, cmd, config))

        return {"trace_id": trace_id, "status": "resumed", "decision": decision}

    except HTTPException:
        raise
    except Exception as e:
        logger.error("[%s] Resume failed: %s", trace_id, e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Fallback Endpoint
# ---------------------------------------------------------------------------

@app.post("/api/v1/incident/{incident_id}/fallback", response_model=FallbackResponse)
async def incident_fallback(incident_id: str, req: FallbackRequest):
    """Handle HITL timeout or execution failure."""
    logger.warning("Fallback for incident=%s: reason=%s action=%s",
                   incident_id, req.reason, req.fallback_action)

    result = await fallback.handle_fallback(
        incident_id, req.reason, req.fallback_action, req.context
    )
    return FallbackResponse(**result)


# ---------------------------------------------------------------------------
# Diagnosis State Query (reads from LangGraph checkpoint)
# ---------------------------------------------------------------------------

@app.get("/api/v1/diagnosis/{trace_id}")
async def get_diagnosis(trace_id: str):
    """Query the state of an ongoing or completed diagnosis from LangGraph checkpoint."""
    config = {"configurable": {"thread_id": trace_id}}

    try:
        state = await get_graph().aget_state(config)
        if not state or not state.values:
            raise HTTPException(status_code=404, detail="Diagnosis not found")

        return state.values

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# List active diagnoses
# ---------------------------------------------------------------------------

@app.get("/api/v1/diagnoses")
async def list_diagnoses(limit: int = 20):
    """List recent diagnoses from checkpoint store."""
    items = []
    for tid in list(_active_configs.keys())[:limit]:
        try:
            config = {"configurable": {"thread_id": tid}}
            state = await get_graph().aget_state(config)
            if state and state.values:
                items.append({
                    "trace_id": tid,
                    "execution_status": state.values.get("execution_status", "?"),
                    "node_id": state.values.get("incident", {}).get("node_id", "?"),
                })
        except Exception:
            pass
    return {"total": len(items), "items": items}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=SERVICE_PORT, reload=True)
