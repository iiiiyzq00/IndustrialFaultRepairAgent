"""
Redis / Middleware Mock API endpoints.

Provides fake slowlog, config, and info endpoints that mimic real Redis
diagnostic commands.
"""

import logging
from fastapi import APIRouter, Query, HTTPException
from ..scenario_loader import ScenarioRegistry

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["redis-mock"])

_redis_registry: ScenarioRegistry | None = None


def init_redis_routes(registry: ScenarioRegistry) -> None:
    global _redis_registry
    _redis_registry = registry


def _data() -> dict:
    if _redis_registry is None:
        raise HTTPException(status_code=503, detail="Redis mock registry not initialised")
    return _redis_registry.get_active()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/slowlog")
def get_redis_slowlog(
    top_n: int = Query(20, description="Number of slow log entries to return"),
    minutes: int = Query(30, description="Lookback window in minutes"),
    instance: str = Query("redis-prod-01", description="Redis instance identifier"),
):
    """
    Return the most recent slowlog entries.

    Equivalent to:  SLOWLOG GET <top_n>
    """
    data = _data()
    slowlog = data.get("slowlog", [])
    return {
        "instance": instance,
        "slowlog_entries": slowlog[:top_n],
        "count": len(slowlog[:top_n]),
        "scenario": _redis_registry.active_name if _redis_registry else "unknown",
    }


@router.get("/config")
def get_redis_config(
    key: str = Query(None, description="Specific config key to retrieve"),
    instance: str = Query("redis-prod-01"),
):
    """
    Return Redis configuration.

    Equivalent to:  CONFIG GET <key>
    """
    data = _data()
    config = data.get("config", {})
    if key:
        return {
            "instance": instance,
            "key": key,
            "value": config.get(key, "(not set)"),
        }
    return {
        "instance": instance,
        "config": config,
        "scenario": _redis_registry.active_name if _redis_registry else "unknown",
    }


@router.get("/info")
def get_redis_info(instance: str = Query("redis-prod-01")):
    """
    Return Redis INFO output (key server metrics).

    Equivalent to:  INFO
    """
    data = _data()
    return {
        "instance": instance,
        "info": data.get("info", {}),
        "scenario": _redis_registry.active_name if _redis_registry else "unknown",
    }
