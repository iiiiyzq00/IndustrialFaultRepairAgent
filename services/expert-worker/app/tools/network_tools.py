"""
Network Expert Tools — real network probing with mock fallback.

Uses: ping3 (ICMP), system traceroute, dnspython (DNS), httpx (HTTP).
"""

from __future__ import annotations

import os
import logging
import subprocess
import random
from typing import Any, Dict

from .base import register_tools

logger = logging.getLogger(__name__)

# ─── ping3 ────────────────────────────────────────────────────

try:
    import ping3
    _HAS_PING3 = True
except ImportError:
    _HAS_PING3 = False
    logger.warning("ping3 not installed — ping will use mock fallback")

# ─── dnspython ────────────────────────────────────────────────

try:
    import dns.resolver
    _HAS_DNS = True
except ImportError:
    _HAS_DNS = False
    logger.warning("dnspython not installed — DNS will use mock fallback")


# ─── Real implementations ─────────────────────────────────────

def _real_ping(params: Dict[str, Any]) -> Dict[str, Any]:
    source = params.get("source", "")
    target = params.get("target", "")
    count = params.get("count", 5)

    if not _HAS_PING3:
        return _system_ping(target, count)

    rtts = []
    lost = 0
    for _ in range(count):
        try:
            rtt = ping3.ping(target, timeout=2, unit="ms")
            if rtt is not None and rtt > 0:
                rtts.append(rtt)
            else:
                lost += 1
        except Exception:
            lost += 1

    return {
        "source": source or "expert-worker",
        "target": target,
        "count": count,
        "min_ms": round(min(rtts), 2) if rtts else -1,
        "avg_ms": round(sum(rtts) / len(rtts), 2) if rtts else -1,
        "max_ms": round(max(rtts), 2) if rtts else -1,
        "loss_pct": round(lost / count * 100, 1),
    }


def _system_ping(target: str, count: int) -> Dict[str, Any]:
    """Fallback: use system ping command."""
    try:
        result = subprocess.run(
            ["ping", "-c", str(count), "-W", "2", target],
            capture_output=True, text=True, timeout=10,
        )
        # Parse loss from output
        output = result.stdout
        loss = 0.0
        if "packet loss" in output:
            # "10 packets transmitted, 8 received, 20% packet loss"
            for line in output.split("\n"):
                if "packet loss" in line:
                    try:
                        loss = float(line.split("%")[0].split()[-1])
                    except Exception:
                        pass
        return {
            "target": target, "count": count,
            "avg_ms": -1, "loss_pct": loss,
            "raw": output[-500:],
        }
    except Exception as e:
        return {"target": target, "error": str(e), "loss_pct": 100.0}


def _real_traceroute(params: Dict[str, Any]) -> Dict[str, Any]:
    target = params.get("target", "")
    try:
        result = subprocess.run(
            ["traceroute", "-n", "-m", "15", "-w", "2", target],
            capture_output=True, text=True, timeout=15,
        )
        hops = []
        for line in result.stdout.split("\n")[1:]:
            parts = line.strip().split()
            if len(parts) >= 2 and parts[0].isdigit():
                hop = int(parts[0])
                addr = parts[1] if parts[1] != "*" else "*"
                rtt = -1
                for p in parts[2:]:
                    try:
                        rtt = float(p)
                        break
                    except ValueError:
                        continue
                hops.append({"hop": hop, "addr": addr, "rtt_ms": rtt})
        return {"target": target, "hops": hops}
    except Exception as e:
        return {"target": target, "error": str(e), "hops": [
            {"hop": 1, "addr": "10.0.0.1", "rtt_ms": 0.5},
            {"hop": 2, "addr": "10.0.0.2", "rtt_ms": 1.2},
            {"hop": 3, "addr": target, "rtt_ms": 2.0},
        ]}


def _real_dns_resolve(params: Dict[str, Any]) -> Dict[str, Any]:
    hostname = params.get("hostname", "")
    if not _HAS_DNS:
        # Fallback: socket
        import socket
        try:
            ips = socket.gethostbyname_ex(hostname)[2]
            return {"hostname": hostname, "resolved": True, "ips": ips, "ttl_ms": 0}
        except Exception as e:
            return {"hostname": hostname, "resolved": False, "ips": [], "error": str(e)}

    try:
        answers = dns.resolver.resolve(hostname, "A")
        ips = [str(r) for r in answers]
        return {"hostname": hostname, "resolved": True, "ips": ips, "ttl_ms": answers.rrset.ttl * 1000 if answers.rrset else 0}
    except Exception as e:
        return {"hostname": hostname, "resolved": False, "ips": [], "error": str(e)}


def _real_svc_mesh_metrics(params: Dict[str, Any]) -> Dict[str, Any]:
    """Query real service mesh metrics. For MVP, returns synthetic but realistic values."""
    # In production: query Prometheus / Istio / Linkerd metrics API
    # For now: generate realistic values based on params
    service = params.get("service", "")
    return {
        "service": service,
        "request_rate": random.randint(100, 1500),
        "error_rate_5xx": round(random.uniform(0, 5), 2),
        "latency_p99_ms": round(random.uniform(2, 200), 1),
    }


# ─── Tool definitions ─────────────────────────────────────────

NETWORK_TOOLS = [
    {
        "name": "ping_mesh",
        "description": "从 source 到 target 执行 ICMP ping 探测 (真实 ping3/system ping)",
        "inputSchema": {
            "type": "object",
            "properties": {
                "source": {"type": "string"},
                "target": {"type": "string"},
                "count": {"type": "integer", "default": 5},
            },
            "required": ["source", "target"],
        },
        "endpoint": "/api/v1/ping",
        "method": "POST",
        "real_fn": _real_ping,
    },
    {
        "name": "trace_route",
        "description": "从 source 到 target 执行路由追踪 (真实 traceroute)",
        "inputSchema": {
            "type": "object",
            "properties": {
                "source": {"type": "string"},
                "target": {"type": "string"},
            },
            "required": ["source", "target"],
        },
        "endpoint": "/api/v1/traceroute",
        "method": "POST",
        "real_fn": _real_traceroute,
    },
    {
        "name": "check_dns",
        "description": "DNS 解析查询 (真实 dns.resolver / socket)",
        "inputSchema": {
            "type": "object",
            "properties": {"hostname": {"type": "string"}},
            "required": ["hostname"],
        },
        "endpoint": "/api/v1/dns/resolve",
        "method": "GET",
        "real_fn": _real_dns_resolve,
    },
    {
        "name": "get_svc_mesh_metrics",
        "description": "查询服务网格侧车指标 (真实 Prometheus/Istio 或合成)",
        "inputSchema": {
            "type": "object",
            "properties": {
                "namespace": {"type": "string", "default": "prod"},
                "service": {"type": "string"},
                "minutes": {"type": "integer", "default": 10},
            },
            "required": ["service"],
        },
        "endpoint": "/api/v1/svc-mesh-metrics",
        "method": "GET",
        "real_fn": _real_svc_mesh_metrics,
    },
]


def init_network_tools() -> None:
    register_tools("network", NETWORK_TOOLS)
    mode = "REAL" if (_HAS_PING3 or _HAS_DNS) and not os.getenv("MOCK_BASE_URL") else "mock"
    logger.info("Network tools initialized (%s mode)", mode)
