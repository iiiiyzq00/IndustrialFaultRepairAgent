"""
Base tool infrastructure: dual-mode execution (real client vs mock HTTP).
"""

from __future__ import annotations

import os
import logging
import time
from typing import Any, Callable, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

MOCK_BASE_URL = os.getenv("MOCK_BASE_URL", "")
API_KEY = os.getenv("API_KEY", "dev-key-change-me")

# Global tool registry per agent type
_tool_registry: Dict[str, Dict[str, Any]] = {}


def register_tools(agent_type: str, tools: List[Dict[str, Any]]) -> None:
    """Register all tools for an agent type."""
    _tool_registry[agent_type] = {t["name"]: t for t in tools}
    logger.info("Registered %d tools for %s", len(tools), agent_type)


def get_tool_list(agent_type: str) -> List[Dict[str, Any]]:
    """Return the public-facing tool list (schema only, no execute fn)."""
    tools = _tool_registry.get(agent_type, {})
    return [
        {
            "name": t["name"],
            "description": t["description"],
            "inputSchema": t.get("inputSchema", {}),
        }
        for t in tools.values()
    ]


async def execute_tool(agent_type: str, tool_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute a named tool for the given agent type.

    If MOCK_BASE_URL is set → mock HTTP call.
    Otherwise → call the real execute function.
    """
    tools = _tool_registry.get(agent_type, {})
    tool = tools.get(tool_name)
    if not tool:
        return {"error": f"Tool '{tool_name}' not found for {agent_type}"}

    # Prefer real execution
    real_fn: Optional[Callable] = tool.get("real_fn")
    if real_fn and not MOCK_BASE_URL:
        t0 = time.monotonic()
        try:
            result = real_fn(params)
            # Handle both sync and async
            if hasattr(result, "__await__"):
                result = await result
            elapsed = int((time.monotonic() - t0) * 1000)
            if isinstance(result, dict):
                result["_execution_ms"] = elapsed
                result["_mode"] = "real"
            return result
        except Exception as e:
            logger.error("Real tool %s/%s failed: %s", agent_type, tool_name, e)
            return {"error": str(e), "_mode": "real"}

    # Fallback to mock HTTP
    t0 = time.monotonic()
    try:
        endpoint = tool.get("endpoint", "")
        method = tool.get("method", "GET")
        base_url = MOCK_BASE_URL or "http://localhost:9002"
        url = _resolve_url(base_url, endpoint, params)

        async with httpx.AsyncClient(timeout=5.0) as client:
            if method == "GET":
                resp = await client.get(url, headers={"X-API-Key": API_KEY})
            else:
                resp = await client.post(url, json=params, headers={"X-API-Key": API_KEY})
            resp.raise_for_status()
            elapsed = int((time.monotonic() - t0) * 1000)
            result = resp.json()
            result["_execution_ms"] = elapsed
            result["_mode"] = "mock"
            return result
    except Exception as e:
        elapsed = int((time.monotonic() - t0) * 1000)
        logger.warning("Mock tool %s/%s failed: %s", agent_type, tool_name, e)
        return {"error": str(e), "_execution_ms": elapsed, "_mode": "mock"}


def _resolve_url(base: str, endpoint: str, params: Dict[str, Any]) -> str:
    """Replace {param} placeholders in endpoint with actual values."""
    for key, value in params.items():
        endpoint = endpoint.replace(f"{{{key}}}", str(value))
    return f"{base}{endpoint}"
