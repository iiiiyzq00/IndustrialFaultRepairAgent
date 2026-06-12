"""
K8s Expert Tools — real Kubernetes client (kind cluster) with mock fallback.

Requires: pip install kubernetes
Connectivity: reads KUBECONFIG (default ~/.kube/config) or in-cluster config.
"""

from __future__ import annotations

import os
import logging
from typing import Any, Dict

from .base import register_tools

logger = logging.getLogger(__name__)

# Try importing kubernetes client (optional — graceful degradation)
try:
    from kubernetes import client, config
    _HAS_K8S = True
    try:
        config.load_kube_config()  # use default ~/.kube/config
        _k8s_configured = True
    except Exception:
        try:
            config.load_incluster_config()
            _k8s_configured = True
        except Exception:
            _k8s_configured = False
            logger.warning("Cannot load K8s config — real k8s tools unavailable")
except ImportError:
    _HAS_K8S = False
    _k8s_configured = False
    logger.warning("kubernetes package not installed — real k8s tools unavailable")


# ─── Real implementations ─────────────────────────────────────

def _real_get_pods(params: Dict[str, Any]) -> Dict[str, Any]:
    namespace = params.get("namespace", "prod")
    prefix = params.get("pod_name_prefix", "")

    v1 = client.CoreV1Api()
    pod_list = v1.list_namespaced_pod(namespace)

    pods = []
    for p in pod_list.items:
        name = p.metadata.name
        if prefix and not name.startswith(prefix):
            continue
        container_statuses = p.status.container_statuses or []
        restarts = sum(c.restart_count for c in container_statuses)
        status = p.status.phase

        pods.append({
            "name": name,
            "namespace": namespace,
            "status": status,
            "ready": _pod_ready(p),
            "restarts": restarts,
            "cpu_m": _get_container_resource(p, "cpu", "requests") or "?",
            "memory": _get_container_resource(p, "memory", "requests") or "?",
            "node": p.spec.node_name or "",
            "version": p.metadata.labels.get("version", "") if p.metadata.labels else "",
        })

    return {"namespace": namespace, "pods": pods, "count": len(pods)}


def _real_get_events(params: Dict[str, Any]) -> Dict[str, Any]:
    namespace = params.get("namespace", "prod")
    pod_name = params.get("pod_name", "")

    v1 = client.CoreV1Api()
    events = v1.list_namespaced_event(namespace)

    result = []
    for e in events.items:
        if pod_name and e.involved_object.name != pod_name:
            continue
        result.append({
            "time": str(e.last_timestamp or e.event_time or ""),
            "type": e.type or "",
            "reason": e.reason or "",
            "message": e.message or "",
            "pod": e.involved_object.name,
        })

    return {"namespace": namespace, "events": result, "count": len(result)}


def _real_get_deploy_history(params: Dict[str, Any]) -> Dict[str, Any]:
    namespace = params.get("namespace", "prod")
    deployment_name = params.get("deployment", "")

    v1 = client.AppsV1Api()
    try:
        deploy = v1.read_namespaced_deployment(deployment_name, namespace)
        # List replica sets to infer history
        rs_list = v1.list_namespaced_replica_set(namespace)

        revisions = []
        for rs in rs_list.items:
            if rs.metadata.owner_references:
                for ref in rs.metadata.owner_references:
                    if ref.name == deployment_name:
                        revisions.append({
                            "version": rs.metadata.annotations.get("deployment.kubernetes.io/revision", "") if rs.metadata.annotations else "",
                            "deployed_at": str(rs.metadata.creation_timestamp or ""),
                            "image": _first_container_image(rs),
                            "trigger": "CI-CD",
                            "status": "active" if rs.status.replicas and rs.status.replicas > 0 else "superseded",
                        })
        return {"namespace": namespace, "deployment": deployment_name, "revisions": revisions[:5]}
    except client.ApiException as e:
        return {"error": f"K8s API error: {e.status} {e.reason}", "revisions": []}


def _real_get_resource_quota(params: Dict[str, Any]) -> Dict[str, Any]:
    namespace = params.get("namespace", "prod")

    v1 = client.CoreV1Api()
    try:
        quotas = v1.list_namespaced_resource_quota(namespace)
        result: Dict[str, Any] = {}
        for q in quotas.items:
            if q.status and q.status.hard:
                for resource, hard_val in q.status.hard.items():
                    used_val = q.status.used.get(resource, "0") if q.status.used else "0"
                    result[resource] = f"hard={hard_val}, used={used_val}"
        return {"namespace": namespace, **result}
    except client.ApiException as e:
        return {"namespace": namespace, "error": str(e.reason)}


def _real_get_node_metrics(params: Dict[str, Any]) -> Dict[str, Any]:
    node_name = params.get("node_name", "")

    v1 = client.CoreV1Api()
    try:
        node = v1.read_node(node_name)
        # Node status.allocatable has cpu/memory
        allocatable = node.status.allocatable or {}
        capacity = node.status.capacity or {}
        conditions = {c.type: c.status for c in (node.status.conditions or [])}

        return {
            "node_name": node_name,
            "metrics": {
                "cpu": str(allocatable.get("cpu", "?")),
                "mem": str(allocatable.get("memory", "?")),
                "disk_io": "N/A (use metrics-server)",
            },
            "ready": conditions.get("Ready", "Unknown"),
        }
    except client.ApiException as e:
        return {"node_name": node_name, "error": str(e.reason)}


# ─── Helpers ──────────────────────────────────────────────────

def _pod_ready(pod) -> bool:
    if not pod.status.conditions:
        return False
    for c in pod.status.conditions:
        if c.type == "Ready" and c.status == "True":
            return True
    return False


def _get_container_resource(pod, resource: str, field: str) -> str:
    """Extract resource from first container's requests/limits."""
    for container in (pod.spec.containers or []):
        res = getattr(container.resources, field, None) if container.resources else None
        if res and resource in res:
            return str(res[resource])
    return ""


def _first_container_image(rs) -> str:
    if rs.spec.template.spec.containers:
        return rs.spec.template.spec.containers[0].image
    return ""


# ─── Tool definitions ─────────────────────────────────────────

K8S_TOOLS = [
    {
        "name": "get_pod_status",
        "description": "查询 K8s namespace 中 Pod 的运行状态、资源用量和 restart 次数（真实 kube-api）",
        "inputSchema": {
            "type": "object",
            "properties": {
                "namespace": {"type": "string", "default": "default"},
                "pod_name_prefix": {"type": "string"},
            },
            "required": ["namespace"],
        },
        "endpoint": "/api/v1/namespaces/{namespace}/pods",
        "method": "GET",
        "real_fn": _real_get_pods,
    },
    {
        "name": "get_pod_events",
        "description": "查询 K8s Events（真实 kube-api）",
        "inputSchema": {
            "type": "object",
            "properties": {
                "namespace": {"type": "string", "default": "default"},
                "pod_name": {"type": "string"},
                "minutes": {"type": "integer", "default": 30},
            },
            "required": ["namespace"],
        },
        "endpoint": "/api/v1/namespaces/{namespace}/events",
        "method": "GET",
        "real_fn": _real_get_events,
    },
    {
        "name": "get_deploy_history",
        "description": "查询 Deployment 的最近发布历史（真实 kube-api）",
        "inputSchema": {
            "type": "object",
            "properties": {
                "namespace": {"type": "string", "default": "default"},
                "deployment": {"type": "string"},
                "count": {"type": "integer", "default": 5},
            },
            "required": ["namespace", "deployment"],
        },
        "endpoint": "/api/v1/namespaces/{namespace}/deployments/{deployment}/history",
        "method": "GET",
        "real_fn": _real_get_deploy_history,
    },
    {
        "name": "get_resource_quota",
        "description": "查询 namespace 的资源配额和使用情况（真实 kube-api）",
        "inputSchema": {
            "type": "object",
            "properties": {"namespace": {"type": "string", "default": "default"}},
            "required": ["namespace"],
        },
        "endpoint": "/api/v1/namespaces/{namespace}/resource-quota",
        "method": "GET",
        "real_fn": _real_get_resource_quota,
    },
    {
        "name": "get_node_metrics",
        "description": "查询 K8s 节点的 CPU/内存/状态（真实 kube-api）",
        "inputSchema": {
            "type": "object",
            "properties": {
                "node_name": {"type": "string"},
                "metrics": {"type": "string", "default": "cpu,mem"},
            },
            "required": ["node_name"],
        },
        "endpoint": "/api/v1/nodes/{node_name}/metrics",
        "method": "GET",
        "real_fn": _real_get_node_metrics,
    },
]


def init_k8s_tools() -> None:
    register_tools("k8s", K8S_TOOLS)
    if _HAS_K8S and _k8s_configured:
        logger.info("K8s tools initialized with REAL kubernetes client (kind cluster)")
    else:
        logger.info("K8s tools initialized (mock mode — set MOCK_BASE_URL or install kubernetes)")
