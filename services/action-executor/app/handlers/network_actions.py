"""
Network self-healing actions: traffic shift, DNS failover.

MEDIUM risk — typically require single HITL approval.
"""

from __future__ import annotations

import os
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"


def network_traffic_shift(params: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
    """
    MEDIUM RISK — switch traffic from a degraded link to a backup link.

    Expected params:
      - source_service: service to redirect traffic from
      - target_link: backup link / gateway to switch to
      - reason: why the shift is needed

    In production: update service mesh (Istio/Linkerd) VirtualService/DestinationRule
    or modify load balancer routing rules.
    """
    source = params.get("source_service", params.get("target", ""))
    target_link = params.get("target_link", "backup-gateway")
    reason = params.get("reason", "link_degradation")

    if dry_run or DRY_RUN:
        return {"success": True, "dry_run": True, "source": source, "target_link": target_link}

    logger.warning("NETWORK TRAFFIC SHIFT (SIMULATED): %s → %s (reason=%s)", source, target_link, reason)

    return {
        "success": True,
        "simulated": True,
        "source_service": source,
        "target_link": target_link,
        "reason": reason,
        "action": "network_traffic_shift",
        "note": "Production would update Istio VirtualService / AWS Route53 / Nginx upstream"
    }


def dns_failover(params: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
    """
    LOW RISK — switch DNS resolution to a backup resolver.

    Expected params:
      - hostname: the failing hostname
      - primary_resolver: current (failing) resolver
      - backup_resolver: backup DNS server
    """
    hostname = params.get("hostname", params.get("target", ""))
    backup = params.get("backup_resolver", "8.8.8.8")
    primary = params.get("primary_resolver", "10.0.0.53")

    if dry_run or DRY_RUN:
        return {"success": True, "dry_run": True, "hostname": hostname, "backup_resolver": backup}

    logger.warning("DNS FAILOVER (SIMULATED): %s resolver: %s → %s", hostname, primary, backup)

    return {
        "success": True,
        "simulated": True,
        "hostname": hostname,
        "primary_resolver": primary,
        "backup_resolver": backup,
        "action": "dns_failover",
        "note": "Production would update CoreDNS ConfigMap / resolv.conf"
    }
