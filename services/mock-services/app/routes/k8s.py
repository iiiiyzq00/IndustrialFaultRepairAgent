"""
K8s Mock API endpoints.

Exposes a subset of the Kubernetes API surface that the K8s Expert Agent
needs for diagnosis.  All responses are driven by the active scenario in
the YAML scenario registry.
"""

import time
import logging
from fastapi import APIRouter, Path, Query, HTTPException
from ..scenario_loader import ScenarioRegistry

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["k8s-mock"])

# Populated by main.py at startup
_k8s_registry: ScenarioRegistry | None = None


def init_k8s_routes(registry: ScenarioRegistry) -> None:
    global _k8s_registry
    _k8s_registry = registry


def _data() -> dict:
    """Return the currently-active scenario data."""
    if _k8s_registry is None:
        raise HTTPException(status_code=503, detail="K8s mock registry not initialised")
    return _k8s_registry.get_active()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/namespaces/{namespace}/pods")
def get_pods(
    namespace: str = Path(..., description="K8s namespace"),
    pod_name_prefix: str = Query(None, description="Filter by pod name prefix"),
):
    """
    Return a list of Pod objects in *namespace*.

    Equivalent to:  kubectl get pods -n <namespace> -o json
    """
    data = _data()
    pods = data.get("pods", [])

    if pod_name_prefix:
        pods = [p for p in pods if p.get("name", "").startswith(pod_name_prefix)]

    return {
        "namespace": namespace,
        "pods": pods,
        "count": len(pods),
        "scenario": _k8s_registry.active_name if _k8s_registry else "unknown",
    }


@router.get("/namespaces/{namespace}/events")
def get_pod_events(
    namespace: str = Path(...),
    pod_name: str = Query(None, description="Filter by pod name"),
    minutes: int = Query(30, description="Lookback window in minutes"),
):
    """Return K8s Events, optionally scoped to a specific pod."""
    data = _data()
    events = data.get("events", [])

    if pod_name:
        events = [e for e in events if e.get("pod") == pod_name]

    return {
        "namespace": namespace,
        "events": events,
        "count": len(events),
        "scenario": _k8s_registry.active_name if _k8s_registry else "unknown",
    }


@router.get("/namespaces/{namespace}/resource-quota")
def get_resource_quota(namespace: str = Path(...)):
    """Return resource quota and current usage for *namespace*."""
    data = _data()
    return {
        "namespace": namespace,
        **data.get("resource_quota", {}),
        "scenario": _k8s_registry.active_name if _k8s_registry else "unknown",
    }


@router.get("/namespaces/{namespace}/deployments/{deployment}/history")
def get_deploy_history(
    namespace: str = Path(...),
    deployment: str = Path(...),
    count: int = Query(5, description="Number of recent revisions"),
):
    """Return recent deployment revision history."""
    data = _data()
    history = data.get("deploy_history", [])
    return {
        "namespace": namespace,
        "deployment": deployment,
        "revisions": history[:count],
        "scenario": _k8s_registry.active_name if _k8s_registry else "unknown",
    }


@router.get("/nodes/{node_name}/metrics")
def get_node_metrics(
    node_name: str = Path(...),
    metrics: str = Query("cpu,mem", description="Comma-separated metric names"),
):
    """Return node-level metrics (cpu/mem/disk_io)."""
    data = _data()
    node_data = data.get("node_metrics", {}).get(node_name, {})
    requested = [m.strip() for m in metrics.split(",")]
    return {
        "node_name": node_name,
        "metrics": {k: v for k, v in node_data.items() if k in requested},
        "scenario": _k8s_registry.active_name if _k8s_registry else "unknown",
    }
