"""Async HTTP clients for dispatching to expert workers."""

from __future__ import annotations

import os
import logging
import asyncio
import httpx
from .schemas import DiagnoseRequest, DiagnoseResponse

logger = logging.getLogger(__name__)

API_KEY = os.getenv("API_KEY", "dev-key-change-me")
EXPERT_TIMEOUT = float(os.getenv("EXPERT_TIMEOUT_SECONDS", "25.0"))

EXPERT_URLS = {
    "k8s":          os.getenv("KB_EXPERT_URL",  "http://k8s-expert:8110"),
    "middleware":   os.getenv("MW_EXPERT_URL",  "http://middleware-expert:8120"),
    "network":      os.getenv("NW_EXPERT_URL",  "http://network-expert:8130"),
    "application":  os.getenv("APP_EXPERT_URL", "http://app-expert:8140"),
}


async def dispatch_expert(agent_type: str, req: DiagnoseRequest) -> DiagnoseResponse | None:
    """Send a diagnose request to one expert. Returns None on timeout or error."""
    url = EXPERT_URLS.get(agent_type)
    if not url:
        logger.error("Unknown agent_type: %s", agent_type)
        return None

    endpoint = f"{url}/api/v1/agent/{agent_type}/diagnose"

    try:
        async with httpx.AsyncClient(timeout=EXPERT_TIMEOUT) as client:
            resp = await client.post(
                endpoint,
                json=req.model_dump(),
                headers={"X-API-Key": API_KEY, "Accept": "application/json"},
            )
            resp.raise_for_status()
            return DiagnoseResponse(**resp.json())
    except httpx.TimeoutException:
        logger.warning("Expert %s timed out after %.1fs", agent_type, EXPERT_TIMEOUT)
        return None
    except Exception as e:
        logger.error("Expert %s call failed: %s", agent_type, e)
        return None


async def dispatch_all(agent_types: list[str], req: DiagnoseRequest) -> dict[str, DiagnoseResponse | None]:
    """Dispatch to multiple experts in parallel."""
    tasks = {at: dispatch_expert(at, req) for at in agent_types}
    results = {}
    for at, task in tasks.items():
        results[at] = await task  # asyncio.gather would be better for true parallelism
    return results


async def dispatch_all_parallel(agent_types: list[str], req: DiagnoseRequest) -> dict[str, DiagnoseResponse | None]:
    """Dispatch to multiple experts truly in parallel using asyncio.gather."""
    async def _dispatch(at: str) -> tuple[str, DiagnoseResponse | None]:
        return at, await dispatch_expert(at, req)

    coros = [_dispatch(at) for at in agent_types]
    gathered = await asyncio.gather(*coros, return_exceptions=True)

    results: dict[str, DiagnoseResponse | None] = {}
    for item in gathered:
        if isinstance(item, Exception):
            logger.error("Expert dispatch exception: %s", item)
        else:
            at, resp = item
            results[at] = resp
    return results
