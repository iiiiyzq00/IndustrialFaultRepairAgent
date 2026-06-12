"""
Mock Services — Unified entry point.

Environment variables:
    MOCK_TYPE      : k8s | redis | network   (required)
    SERVICE_PORT   : listen port (default 9002)
    SCENARIO_PATH  : directory containing YAML files (default /app/scenarios)
    API_KEY        : shared dev key (default "dev-key-change-me")

Each instance registers only the routes relevant to its MOCK_TYPE.
Shared scenario-management endpoints (/scenario) are always available.
"""

from __future__ import annotations

import os
import sys
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

# --- Make common/ importable when running inside the container ---
# In the Docker image the common module is copied alongside the app,
# but during local dev we add the parent's parent to sys.path.
_PARENT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

from common.auth import setup_api_key_auth  # noqa: E402

from .scenario_loader import ScenarioRegistry  # noqa: E402

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("mock-services")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MOCK_TYPE = os.getenv("MOCK_TYPE", "").strip().lower()
if MOCK_TYPE not in ("k8s", "redis", "network"):
    logger.fatal("MOCK_TYPE must be one of: k8s, redis, network.  Got: '%s'", MOCK_TYPE)
    sys.exit(1)

SERVICE_PORT = int(os.getenv("SERVICE_PORT", "9002"))

# ---------------------------------------------------------------------------
# Scenario registry (shared across routes + scenario-management endpoints)
# ---------------------------------------------------------------------------

_scenario_registry = ScenarioRegistry(MOCK_TYPE)


# ---------------------------------------------------------------------------
# Route registration (only the matching type)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load scenarios on startup."""
    _scenario_registry.load()
    logger.info("Mock service started: type=%s port=%d", MOCK_TYPE, SERVICE_PORT)
    yield


app = FastAPI(
    title=f"Industrial Fault Repair — {MOCK_TYPE.upper()} Mock API",
    version="1.0.0",
    lifespan=lifespan,
)

# --- Auth ---
setup_api_key_auth(app)

# --- Domain routes ---
if MOCK_TYPE == "k8s":
    from .routes.k8s import router as k8s_router, init_k8s_routes

    init_k8s_routes(_scenario_registry)
    app.include_router(k8s_router)

elif MOCK_TYPE == "redis":
    from .routes.redis import router as redis_router, init_redis_routes

    init_redis_routes(_scenario_registry)
    app.include_router(redis_router)

elif MOCK_TYPE == "network":
    from .routes.network import router as network_router, init_network_routes

    init_network_routes(_scenario_registry)
    app.include_router(network_router)


# ---------------------------------------------------------------------------
# Scenario-management endpoints (available on all mock types)
# ---------------------------------------------------------------------------


@app.get("/scenario")
def list_scenarios():
    """Return available scenario names and the active one."""
    return {
        "mock_type": MOCK_TYPE,
        "active": _scenario_registry.active_name,
        "available": _scenario_registry.list_scenarios(),
    }


@app.post("/scenario/{name}")
def switch_scenario(name: str):
    """Switch the active scenario at runtime (for fault-injection tests)."""
    ok = _scenario_registry.set_active(name)
    if not ok:
        raise HTTPException(
            status_code=404,
            detail=f"Scenario '{name}' not found. Available: {_scenario_registry.list_scenarios()}",
        )
    return {
        "mock_type": MOCK_TYPE,
        "active": name,
        "message": f"Scenario switched to '{name}'",
    }


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@app.get("/health")
def health():
    return {
        "status": "ok",
        "mock_type": MOCK_TYPE,
        "scenario": _scenario_registry.active_name,
    }


# ---------------------------------------------------------------------------
# Main (for direct python invocation)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=SERVICE_PORT, reload=True)
