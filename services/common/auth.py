"""
API Key Authentication Middleware for all services.

Usage in any FastAPI service:
    from common.auth import create_api_key_middleware, APIKeyAuthBackend

    app = FastAPI()
    app.add_middleware(APIKeyAuthMiddleware)  # simple usage
    # OR for granular control:
    setup_api_key_auth(app)

All services read API_KEY from environment variable.
Development default: "dev-key-change-me"
Optional: SERVICE_API_KEYS (JSON map) for per-service key allowlists.
"""

import os
import json
import logging
from typing import Callable, Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SKIP_PATHS: tuple = (
    "/metrics",   # Prometheus scraping
    "/health",
    "/healthz",
    "/ready",
    "/docs",
    "/openapi.json",
    "/redoc",
)

API_KEY_HEADER = "X-API-Key"

DEFAULT_DEV_KEY = "dev-key-change-me"


def get_api_key() -> str:
    """Return the API key this service accepts."""
    return os.getenv("API_KEY", DEFAULT_DEV_KEY)


def get_service_allowlist() -> dict:
    """
    Return a dict of {service_name: api_key} for per-service allowlisting.

    Set SERVICE_API_KEYS as a JSON string env var, e.g.:
        SERVICE_API_KEYS='{"k8s-expert":"key1","supervisor":"key2"}'

    If not set, every caller presenting the service's own API_KEY is accepted.
    """
    raw = os.getenv("SERVICE_API_KEYS", "")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("SERVICE_API_KEYS is not valid JSON; ignoring allowlist.")
        return {}


def verify_api_key(key: Optional[str]) -> bool:
    """
    Return True if *key* is a valid API key for this service.

    Order of checks:
    1. key must be non-empty.
    2. If allowlist configured -> key must be in allowlist values.
    3. Else key must equal the service's own API_KEY.
    """
    if not key:
        return False

    allowlist = get_service_allowlist()
    if allowlist:
        return key in allowlist.values()

    return key == get_api_key()


# ---------------------------------------------------------------------------
# Starlette Middleware
# ---------------------------------------------------------------------------

class APIKeyAuthMiddleware(BaseHTTPMiddleware):
    """
    Simple middleware that requires X-API-Key on every request.

    Installation:
        app = FastAPI()
        app.add_middleware(APIKeyAuthMiddleware)
    """

    async def dispatch(self, request: Request, call_next: Callable):
        # Skip health / docs endpoints
        if request.url.path.startswith(SKIP_PATHS):
            return await call_next(request)

        api_key = request.headers.get(API_KEY_HEADER)
        if not verify_api_key(api_key):
            if not api_key:
                detail = "Missing X-API-Key header"
                status_code = 401
            else:
                detail = "Invalid X-API-Key"
                status_code = 403

            logger.warning(
                "Auth rejected path=%s remote=%s reason=%s",
                request.url.path,
                request.client.host if request.client else "unknown",
                detail,
            )
            return JSONResponse(
                status_code=status_code,
                content={
                    "error": {
                        "code": "UNAUTHORIZED" if status_code == 401 else "FORBIDDEN",
                        "message": detail,
                    }
                },
            )

        return await call_next(request)


# ---------------------------------------------------------------------------
# Helper: fast setup
# ---------------------------------------------------------------------------

def setup_api_key_auth(app) -> None:
    """Add the API-key middleware to a FastAPI/Starlette app."""
    app.add_middleware(APIKeyAuthMiddleware)
    logger.info("API Key authentication middleware installed (header: %s)", API_KEY_HEADER)


# ---------------------------------------------------------------------------
# Dependency-injection variant (for per-route control)
# ---------------------------------------------------------------------------

from fastapi import Header, HTTPException


def require_api_key(x_api_key: Optional[str] = Header(None, alias=API_KEY_HEADER)) -> str:
    """
    FastAPI dependency.  Use as:

        @app.get("/protected")
        def protected_endpoint(api_key: str = Depends(require_api_key)):
            ...
    """
    if not verify_api_key(x_api_key):
        if not x_api_key:
            raise HTTPException(status_code=401, detail="Missing X-API-Key header")
        raise HTTPException(status_code=403, detail="Invalid X-API-Key")
    return x_api_key
