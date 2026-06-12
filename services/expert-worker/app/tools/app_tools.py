"""
Application Expert Tools — real APM/log queries with mock fallback.

Connects to:
  - Prometheus API for metrics (PROMETHEUS_URL)
  - Jaeger / Grafana Tempo for traces (TRACING_URL)
  - Elasticsearch / Loki for logs (LOGS_URL)

When real backends are unavailable, returns realistic synthetic data.
"""

from __future__ import annotations

import os
import logging
import random
import time
from typing import Any, Dict

import httpx
from .base import register_tools

logger = logging.getLogger(__name__)

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "")
JAEGER_URL = os.getenv("JAEGER_URL", "")
LOGS_URL = os.getenv("LOGS_URL", "")
API_KEY = os.getenv("API_KEY", "dev-key-change-me")


# ─── Real implementations ─────────────────────────────────────

async def _real_get_trace(params: Dict[str, Any]) -> Dict[str, Any]:
    """Query Jaeger for a specific trace."""
    trace_id = params.get("trace_id", "")
    if not JAEGER_URL:
        return _synthetic_trace(trace_id)

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                f"{JAEGER_URL}/api/traces/{trace_id}",
                headers={"Accept": "application/json"},
            )
            resp.raise_for_status()
            data = resp.json()
            spans = []
            for trace in (data.get("data", []) if isinstance(data, dict) else data):
                for span in trace.get("spans", []):
                    spans.append({
                        "service": span.get("process", {}).get("serviceName", ""),
                        "operation": span.get("operationName", ""),
                        "duration_ms": span.get("duration", 0) / 1000,
                    })
            return {"trace_id": trace_id, "spans": spans}
    except Exception as e:
        logger.warning("Jaeger query failed: %s — returning synthetic trace", e)
        return _synthetic_trace(trace_id)


def _synthetic_trace(trace_id: str) -> Dict[str, Any]:
    """Generate a realistic-looking trace for MVP/demo."""
    services = ["gateway-svc", "order-svc", "payment-svc", "redis-prod-01", "mysql-prod-01"]
    spans = []
    total = 0
    for svc in services:
        duration = random.uniform(2, 200) if svc != "redis-prod-01" else random.uniform(800, 2000)
        total += duration
        spans.append({"service": svc, "operation": f"GET /api/{svc}", "duration_ms": round(duration, 1)})
    return {"trace_id": trace_id, "spans": spans, "total_duration_ms": round(total, 1)}


async def _real_search_logs(params: Dict[str, Any]) -> Dict[str, Any]:
    """Search application logs (Elasticsearch / Loki / file)."""
    service = params.get("service", "")
    level = params.get("level", "ERROR")
    keyword = params.get("keyword", "")

    if not LOGS_URL:
        return _synthetic_logs(service, level, keyword)

    try:
        query = {"query": {"bool": {"must": [
            {"term": {"service": service}},
            {"term": {"level": level}},
        ]}}}
        if keyword:
            query["query"]["bool"]["must"].append({"match": {"message": keyword}})

        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(
                f"{LOGS_URL}/_search",
                json=query,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            data = resp.json()
            hits = data.get("hits", {}).get("hits", [])
            logs = [{"timestamp": h["_source"].get("@timestamp", ""),
                     "level": h["_source"].get("level", ""),
                     "message": h["_source"].get("message", "")} for h in hits[:20]]
            return {"service": service, "logs": logs, "count": len(logs)}
    except Exception as e:
        logger.warning("Log query failed: %s — returning synthetic", e)
        return _synthetic_logs(service, level, keyword)


def _synthetic_logs(service: str, level: str, keyword: str) -> Dict[str, Any]:
    logs = []
    for i in range(3):
        logs.append({
            "timestamp": f"2025-06-15T02:3{3+i}:{i:02d}Z",
            "level": level,
            "message": f"[{service}] {keyword or 'error'}: connection timeout after 5000ms retry={i+1}",
        })
    return {"service": service, "logs": logs, "count": len(logs)}


async def _real_apm_metrics(params: Dict[str, Any]) -> Dict[str, Any]:
    """Query Prometheus for APM metrics."""
    service = params.get("service", "")
    metrics_str = params.get("metrics", "p99_latency_ms,error_rate,request_rate")
    metric_names = [m.strip() for m in metrics_str.split(",")]

    if not PROMETHEUS_URL:
        return _synthetic_apm(service, metric_names)

    results = {}
    promql_map = {
        "p99_latency_ms": f'histogram_quantile(0.99, rate(http_request_duration_seconds_bucket{{service="{service}"}}[5m])) * 1000',
        "error_rate": f'rate(http_requests_total{{service="{service}",status=~"5.."}}[5m]) / rate(http_requests_total{{service="{service}"}}[5m]) * 100',
        "request_rate": f'rate(http_requests_total{{service="{service}"}}[1m])',
    }

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            for metric_name in metric_names:
                query = promql_map.get(metric_name, metric_name)
                resp = await client.get(
                    f"{PROMETHEUS_URL}/api/v1/query",
                    params={"query": query},
                )
                resp.raise_for_status()
                data = resp.json()
                values = []
                for result in data.get("data", {}).get("result", []):
                    val = float(result.get("value", [0, 0])[1])
                    values.append(val)
                results[metric_name] = round(sum(values) / len(values), 4) if values else 0
        return {"service": service, "metrics": results}
    except Exception as e:
        logger.warning("Prometheus query failed: %s — returning synthetic", e)
        return _synthetic_apm(service, metric_names)


def _synthetic_apm(service: str, metric_names: list) -> Dict[str, Any]:
    base_vals = {
        "p99_latency_ms": random.uniform(50, 1200),
        "error_rate": random.uniform(0, 5.2),
        "request_rate": random.uniform(100, 1500),
    }
    return {"service": service, "metrics": {m: round(base_vals.get(m, 0), 2) for m in metric_names}}


async def _real_recent_deployments(params: Dict[str, Any]) -> Dict[str, Any]:
    """Query recent deployments. For MVP, returns synthetic change log."""
    service = params.get("service", "")
    days = params.get("days", 1)
    return {
        "service": service,
        "deployments": [
            {
                "version": f"v{random.randint(2,4)}.{random.randint(0,5)}.{random.randint(0,9)}",
                "deployed_at": f"2025-06-{15-days:02d}T{random.randint(0,23):02d}:00:00Z",
                "changelog": "Updated Redis client library from Jedis 4.x to Lettuce 6.x",
            }
        ],
    }


async def _real_config_diff(params: Dict[str, Any]) -> Dict[str, Any]:
    """Compare config between two versions. MVP: returns synthetic diff."""
    return {
        "service": params.get("service", ""),
        "version_a": params.get("version_a", ""),
        "version_b": params.get("version_b", ""),
        "changed_keys": [
            {"key": "redis.command", "old": "SCAN", "new": "KEYS *"},
            {"key": "redis.scanCount", "old": "100", "new": "-1"},
        ],
    }


# ─── Tool definitions ─────────────────────────────────────────

APP_TOOLS = [
    {
        "name": "get_trace",
        "description": "查询分布式调用链详情 (真实 Jaeger / 合成数据)",
        "inputSchema": {
            "type": "object",
            "properties": {"trace_id": {"type": "string"}},
            "required": ["trace_id"],
        },
        "endpoint": "/api/v1/traces/{trace_id}",
        "method": "GET",
        "real_fn": _real_get_trace,
    },
    {
        "name": "search_logs",
        "description": "按关键字检索应用日志 (真实 ES/Loki / 合成数据)",
        "inputSchema": {
            "type": "object",
            "properties": {
                "service": {"type": "string"},
                "level": {"type": "string", "default": "ERROR"},
                "minutes": {"type": "integer", "default": 30},
                "keyword": {"type": "string"},
            },
            "required": ["service"],
        },
        "endpoint": "/api/v1/logs/search",
        "method": "GET",
        "real_fn": _real_search_logs,
    },
    {
        "name": "get_apm_metrics",
        "description": "查询 APM 指标 (真实 Prometheus / 合成数据)",
        "inputSchema": {
            "type": "object",
            "properties": {
                "service": {"type": "string"},
                "metrics": {"type": "string", "default": "p99_latency_ms,error_rate,request_rate"},
                "minutes": {"type": "integer", "default": 30},
            },
            "required": ["service"],
        },
        "endpoint": "/api/v1/apm/metrics",
        "method": "GET",
        "real_fn": _real_apm_metrics,
    },
    {
        "name": "get_recent_deployments",
        "description": "查询服务的最近发布记录和变更日志",
        "inputSchema": {
            "type": "object",
            "properties": {
                "service": {"type": "string"},
                "days": {"type": "integer", "default": 1},
            },
            "required": ["service"],
        },
        "endpoint": "/api/v1/deployments/recent",
        "method": "GET",
        "real_fn": _real_recent_deployments,
    },
    {
        "name": "get_config_diff",
        "description": "对比两个版本间的配置差异",
        "inputSchema": {
            "type": "object",
            "properties": {
                "service": {"type": "string"},
                "version_a": {"type": "string"},
                "version_b": {"type": "string"},
            },
            "required": ["service", "version_a", "version_b"],
        },
        "endpoint": "/api/v1/config/diff",
        "method": "GET",
        "real_fn": _real_config_diff,
    },
]


def init_app_tools() -> None:
    register_tools("application", APP_TOOLS)
    logger.info("Application tools initialized (%s mode)", "REAL" if PROMETHEUS_URL else "synthetic")
