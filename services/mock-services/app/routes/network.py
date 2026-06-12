"""
Network Mock API endpoints.

Provides fake ping, traceroute, DNS resolution, and service-mesh metrics.
"""

import logging
from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel, Field
from ..scenario_loader import ScenarioRegistry

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["network-mock"])

_network_registry: ScenarioRegistry | None = None


class PingRequest(BaseModel):
    source: str = Field(..., description="Source host / pod name")
    target: str = Field(..., description="Target host / pod name")
    count: int = Field(10, ge=1, le=100, description="Number of ping probes")


class TracerouteRequest(BaseModel):
    source: str = Field(...)
    target: str = Field(...)


def init_network_routes(registry: ScenarioRegistry) -> None:
    global _network_registry
    _network_registry = registry


def _data() -> dict:
    if _network_registry is None:
        raise HTTPException(status_code=503, detail="Network mock registry not initialised")
    return _network_registry.get_active()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/ping")
def ping_mesh(body: PingRequest):
    """
    Simulate a ping from *source* to *target*.

    Returns rtt stats and packet loss derived from the active scenario.
    """
    data = _data()
    ping_config = data.get("ping", {})
    # Each scenario can provide per-target overrides under 'ping.targets.<target>'
    target_overrides = ping_config.get("targets", {}).get(body.target, {})

    default_rtt = ping_config.get("default_rtt_ms", 1.0)
    default_loss = ping_config.get("default_loss_pct", 0.0)
    jitter = ping_config.get("jitter_ms", 0.3)

    rtt_base = target_overrides.get("rtt_ms", default_rtt)
    loss = target_overrides.get("loss_pct", default_loss)

    # Simulate per-ping rtt
    import random
    rtts = [max(0.1, rtt_base + random.uniform(-jitter, jitter)) for _ in range(body.count)]
    _ = (rtts, loss)  # referenced below

    return {
        "source": body.source,
        "target": body.target,
        "count": body.count,
        "min_ms": round(min(rtts), 2),
        "avg_ms": round(sum(rtts) / len(rtts), 2),
        "max_ms": round(max(rtts), 2),
        "loss_pct": round(loss, 1),
        "scenario": _network_registry.active_name if _network_registry else "unknown",
    }


@router.post("/traceroute")
def trace_route(body: TracerouteRequest):
    """Simulate a traceroute, returning per-hop RTT."""
    data = _data()
    hops = data.get("traceroute", {}).get("hops", _default_hops(body.target))
    return {
        "source": body.source,
        "target": body.target,
        "hops": hops,
        "scenario": _network_registry.active_name if _network_registry else "unknown",
    }


@router.get("/dns/resolve")
def check_dns_resolution(
    hostname: str = Query(..., description="FQDN to resolve"),
):
    """Simulate DNS resolution."""
    data = _data()
    dns_entries = data.get("dns", {})
    entry = dns_entries.get(hostname, {"resolved": True, "ips": ["10.0.0.1"], "ttl_ms": 12})
    return {
        "hostname": hostname,
        **entry,
        "scenario": _network_registry.active_name if _network_registry else "unknown",
    }


@router.get("/svc-mesh-metrics")
def get_svc_mesh_metrics(
    namespace: str = Query("prod"),
    service: str = Query("redis-prod-01"),
    minutes: int = Query(10),
):
    """Simulate service mesh sidecar metrics."""
    data = _data()
    mesh = data.get("svc_mesh", {})
    svc_data = mesh.get(service, {})
    return {
        "namespace": namespace,
        "service": service,
        "request_rate": svc_data.get("request_rate", 850),
        "error_rate_5xx": svc_data.get("error_rate_5xx", 0.01),
        "latency_p99_ms": svc_data.get("latency_p99_ms", 5.0),
        "scenario": _network_registry.active_name if _network_registry else "unknown",
    }


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _default_hops(target: str) -> list[dict]:
    return [
        {"hop": 1, "addr": "10.0.0.1", "rtt_ms": 0.5},
        {"hop": 2, "addr": "10.0.0.2", "rtt_ms": 1.2},
        {"hop": 3, "addr": target, "rtt_ms": 2.0},
    ]
