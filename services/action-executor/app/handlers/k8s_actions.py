"""
K8s self-healing actions: rollback, scale, restart.

Uses the kubernetes Python client. Falls back to subprocess kubectl if client unavailable.
"""

from __future__ import annotations

import os
import logging
import subprocess
from typing import Any, Dict

logger = logging.getLogger(__name__)

try:
    from kubernetes import client, config
    _HAS_K8S_CLIENT = True
    try:
        config.load_kube_config()
    except Exception:
        try:
            config.load_incluster_config()
        except Exception:
            pass
except ImportError:
    _HAS_K8S_CLIENT = False

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"


def rollback_deployment(params: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
    """kubectl rollout undo deployment/<name> -n <namespace>"""
    namespace = params.get("namespace", "prod")
    deployment = params.get("deployment", params.get("target", ""))
    revision = params.get("revision", "")

    if dry_run or DRY_RUN:
        return _dry_result("rollback_deployment", f"{namespace}/{deployment} to rev={revision or 'previous'}")

    cmd = ["kubectl", "rollout", "undo", f"deployment/{deployment}", "-n", namespace]
    if revision:
        cmd.extend(["--to-revision", str(revision)])

    return _run_kubectl(cmd, f"rollback {namespace}/{deployment}")


def scale_deployment(params: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
    """kubectl scale deployment/<name> --replicas=N -n <namespace>"""
    namespace = params.get("namespace", "prod")
    deployment = params.get("deployment", params.get("target", ""))
    replicas = params.get("replicas", 3)

    if dry_run or DRY_RUN:
        return _dry_result("scale_deployment", f"{namespace}/{deployment} → {replicas} replicas")

    cmd = ["kubectl", "scale", f"deployment/{deployment}", f"--replicas={replicas}", "-n", namespace]
    return _run_kubectl(cmd, f"scale {namespace}/{deployment} to {replicas}")


def restart_pod(params: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
    """kubectl delete pod <name> -n <namespace> (triggers recreation via ReplicaSet)"""
    namespace = params.get("namespace", "prod")
    pod_name = params.get("pod", params.get("target", ""))

    if dry_run or DRY_RUN:
        return _dry_result("restart_pod", f"{namespace}/{pod_name}")

    cmd = ["kubectl", "delete", "pod", pod_name, "-n", namespace, "--grace-period=30"]
    return _run_kubectl(cmd, f"restart pod {namespace}/{pod_name}")


def _run_kubectl(cmd: list, description: str) -> Dict[str, Any]:
    """Execute a kubectl command and return structured result."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            logger.info("K8s action SUCCESS: %s", description)
            return {"success": True, "command": " ".join(cmd), "stdout": result.stdout.strip(), "stderr": result.stderr.strip()}
        else:
            logger.error("K8s action FAILED: %s — %s", description, result.stderr.strip())
            return {"success": False, "command": " ".join(cmd), "error": result.stderr.strip()}
    except subprocess.TimeoutExpired:
        return {"success": False, "error": "timeout after 30s"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def scale_down(params: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
    """kubectl scale deployment/<name> --replicas=N -n <namespace> (scale DOWN)"""
    namespace = params.get("namespace", "prod")
    deployment = params.get("deployment", params.get("target", ""))
    replicas = params.get("replicas", 2)

    if dry_run or DRY_RUN:
        return _dry_result("scale_down", f"{namespace}/{deployment} → {replicas} replicas")

    cmd = ["kubectl", "scale", f"deployment/{deployment}", f"--replicas={replicas}", "-n", namespace]
    return _run_kubectl(cmd, f"scale down {namespace}/{deployment} to {replicas}")


def _dry_result(action: str, target: str) -> Dict[str, Any]:
    logger.info("[DRY RUN] %s on %s", action, target)
    return {"success": True, "dry_run": True, "action": action, "target": target}
