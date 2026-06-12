"""
Expert Worker — Shared codebase for all four expert types.

Differentiated by AGENT_TYPE env var: k8s | middleware | network | application

POST /api/v1/agent/{agent_type}/diagnose  — 接收主控的诊断请求
POST /api/v1/tools/{tool_name}             — 暴露 MCP 工具（主控观察窗口用）
GET  /health                                — 健康检查
GET  /api/v1/tools                          — 列出本专家的 MCP 工具
"""

from __future__ import annotations

import os
import sys
import uuid
import logging
import time
from typing import Any, Dict, List

import httpx
from fastapi import FastAPI, HTTPException

# Path for common module
_PARENT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

from common.auth import setup_api_key_auth  # noqa: E402
from common.metrics import setup_metrics  # noqa: E402

from .tools.base import execute_tool, get_tool_list  # noqa: E402
from .tools.k8s_tools import init_k8s_tools  # noqa: E402
from .tools.middleware_tools import init_middleware_tools  # noqa: E402
from .tools.network_tools import init_network_tools  # noqa: E402
from .tools.app_tools import init_app_tools  # noqa: E402
from .llm_client import reason  # noqa: E402

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("expert-worker")

AGENT_TYPE = os.getenv("AGENT_TYPE", "application").strip().lower()
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8110"))
API_KEY = os.getenv("API_KEY", "dev-key-change-me")
MOCK_BASE_URL = os.getenv("MOCK_BASE_URL", "")

if AGENT_TYPE not in ("k8s", "middleware", "network", "application"):
    logger.fatal("Invalid AGENT_TYPE: %s. Must be one of: k8s, middleware, network, application", AGENT_TYPE)
    sys.exit(1)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title=f"Industrial Fault Repair — {AGENT_TYPE.upper()} Expert Worker",
    version="1.0.0",
)
setup_api_key_auth(app)
setup_metrics(app)

# Initialize tools for this agent type
_init_map = {"k8s": init_k8s_tools, "middleware": init_middleware_tools,
             "network": init_network_tools, "application": init_app_tools}
_init_map.get(AGENT_TYPE, lambda: None)()
TOOLS = get_tool_list(AGENT_TYPE)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {
        "status": "ok",
        "agent_type": AGENT_TYPE,
        "tools_count": len(TOOLS),
    }


# ---------------------------------------------------------------------------
# MCP Tool listing
# ---------------------------------------------------------------------------

@app.get("/api/v1/tools")
def list_tools():
    """Return the MCP tool definitions for this expert."""
    return {"agent_type": AGENT_TYPE, "tools": TOOLS}


# ---------------------------------------------------------------------------
# MCP Tool execution
# ---------------------------------------------------------------------------

@app.post("/api/v1/tools/{tool_name}")
async def execute_tool_endpoint(tool_name: str, params: Dict[str, Any] | None = None):
    """
    Execute a named MCP tool. Uses real clients when available,
    falls back to mock HTTP when MOCK_BASE_URL is set.
    Called by the expert's reasoning loop or by Supervisor during observation windows.
    """
    params = params or {}
    result = await execute_tool(AGENT_TYPE, tool_name, params)
    if "error" in result and "_mode" not in result:
        raise HTTPException(status_code=502, detail=result.get("error", "Tool execution failed"))
    return result


# ---------------------------------------------------------------------------
# Diagnose endpoint (Supervisor → Expert)
# ---------------------------------------------------------------------------

@app.post("/api/v1/agent/{agent_type}/diagnose")
async def diagnose(agent_type: str, req: Dict[str, Any]):
    """
    Receive a diagnosis request from the Supervisor, run MCP tools,
    reason with LLM, and return a DiagnoseResponse.
    """
    if agent_type != AGENT_TYPE:
        raise HTTPException(status_code=400, detail=f"This worker is {AGENT_TYPE}, not {agent_type}")

    trace_id = req.get("supervisor_trace_id", str(uuid.uuid4()))
    incident = req.get("incident", {})
    hypothesis = req.get("domain_hypothesis", "")
    rag_context = req.get("rag_context", {}).get("documents", [])
    override_rag = req.get("override_rag", False)
    max_tool_calls = req.get("max_tool_calls", 10)

    logger.info("[%s] Diagnose request received (override_rag=%s, tools=%d)",
                trace_id, override_rag, len(TOOLS))

    # ── Step 1: Execute MCP tools (real or mock) ──
    tool_results: List[Dict[str, Any]] = []
    call_order = 0

    for tool in TOOLS[:max_tool_calls]:
        call_order += 1
        t0 = time.monotonic()
        tool_name = tool["name"]
        try:
            params = _build_tool_params(tool, incident)
            output = await execute_tool(AGENT_TYPE, tool_name, params)
            success = "error" not in output

            latency = int((time.monotonic() - t0) * 1000)
            tool_results.append({
                "tool_name": tool_name, "call_order": call_order,
                "latency_ms": latency, "success": success,
                "output": output,
            })
            mode = output.get("_mode", "?")
            logger.debug("[%s] Tool %s → success (%dms, %s)", trace_id, tool_name, latency, mode)

        except Exception as e:
            latency = int((time.monotonic() - t0) * 1000)
            tool_results.append({
                "tool_name": tool_name, "call_order": call_order,
                "latency_ms": latency, "success": False,
                "error": str(e),
            })
            logger.warning("[%s] Tool %s → failed (%dms): %s", trace_id, tool_name, latency, e)

    # ── Step 2: Reason with LLM ──
    llm_result = await reason(AGENT_TYPE, incident, hypothesis, rag_context, tool_results)

    # ── Step 3: Build response ──
    evidence = []
    for tr in tool_results:
        if tr["success"]:
            evidence.append({
                "tool_name": tr["tool_name"],
                "output_summary": str(tr.get("output", {}))[:500],
                "raw_data_ref": f"tool://{AGENT_TYPE}/{tr['tool_name']}",
            })

    response = {
        "agent_type": AGENT_TYPE,
        "trace_id": trace_id,
        "findings": llm_result.get("findings", ""),
        "suspected_root_cause": llm_result.get("suspected_root_cause", ""),
        "confidence": llm_result.get("confidence", 0.5),
        "evidence": evidence,
        "tool_call_log": [
            {"tool_name": tr["tool_name"], "call_order": tr["call_order"],
             "latency_ms": tr["latency_ms"], "success": tr["success"],
             "summary": str(tr.get("output", tr.get("error", "")))[:200]}
            for tr in tool_results
        ],
        "rag_documents_used": rag_context[:3],
        "suggested_actions": llm_result.get("suggested_actions", []),
    }

    logger.info("[%s] Diagnosis complete: confidence=%.0f%% tools_ok=%d/%d",
                trace_id, response["confidence"] * 100,
                sum(1 for t in tool_results if t["success"]), len(tool_results))

    return response


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_tool_params(tool: Dict[str, Any], incident: Dict[str, Any]) -> Dict[str, Any]:
    """Build reasonable default parameters for a tool from incident context."""
    params: Dict[str, Any] = {}

    schema = tool.get("inputSchema", {}).get("properties", {})
    for prop_name, prop_schema in schema.items():
        default = prop_schema.get("default")
        if default is not None:
            params[prop_name] = default

    # Try to enrich from incident
    node_id = incident.get("node_id", "")
    alerts = incident.get("aggregated_alerts", [])
    if alerts:
        alert = alerts[0]
        if "node_name" in schema:
            params["node_name"] = alert.get("node_id", node_id)
        if "service" in schema:
            params["service"] = alert.get("tags", {}).get("service", "order-svc")
        if "namespace" in schema:
            params["namespace"] = "prod"

    if "source" in schema:
        params["source"] = node_id or "order-svc"
    if "target" in schema:
        params["target"] = "redis-prod-01.prod.svc.cluster.local"

    return params


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    logger.info("Starting %s expert worker on port %d", AGENT_TYPE, SERVICE_PORT)
    uvicorn.run("app.main:app", host="0.0.0.0", port=SERVICE_PORT, reload=True)
