"""
Shared Prometheus metrics for all services.

Usage in any FastAPI service:
    from common.metrics import setup_metrics, DIAGNOSIS_TOTAL, DIAGNOSIS_DURATION

    app = FastAPI()
    setup_metrics(app)  # adds /metrics endpoint

    @app.post("/incident")
    async def create_incident():
        DIAGNOSIS_TOTAL.labels(status="accepted").inc()
        with DIAGNOSIS_DURATION.time():
            ...
"""

from __future__ import annotations

import os
import time
import logging
from typing import Any, Dict

try:
    from prometheus_client import Counter, Histogram, Gauge, Info, generate_latest, CONTENT_TYPE_LATEST
    from prometheus_client import CollectorRegistry, multiprocess
    _HAS_PROMETHEUS = True
except ImportError:
    _HAS_PROMETHEUS = False

logger = logging.getLogger(__name__)

SERVICE_NAME = os.getenv("SERVICE_NAME", os.getenv("AGENT_TYPE", "unknown"))

# ─── Registry ──────────────────────────────────────────────────

_registry = None
if _HAS_PROMETHEUS:
    _registry = CollectorRegistry()

# ─── Metric Definitions ────────────────────────────────────────

def _make_counter(name, desc, labels=None):
    if _HAS_PROMETHEUS:
        return Counter(name, desc, labels or [], registry=_registry)
    return _DummyMetric()

def _make_histogram(name, desc, labels=None, buckets=None):
    if _HAS_PROMETHEUS:
        return Histogram(name, desc, labels or [], buckets=buckets, registry=_registry)
    return _DummyMetric()

def _make_gauge(name, desc, labels=None):
    if _HAS_PROMETHEUS:
        return Gauge(name, desc, labels or [], registry=_registry)
    return _DummyMetric()


class _DummyMetric:
    """No-op metric when prometheus_client is not installed."""
    def labels(self, **kwargs): return self
    def inc(self, amount=1): pass
    def set(self, value): pass
    def observe(self, value): pass
    def time(self): return _DummyTimer()
    def __enter__(self): return self
    def __exit__(self, *args): pass

class _DummyTimer:
    def __enter__(self): return self
    def __exit__(self, *args): pass


# ─── Diagnosis Metrics (Supervisor) ────────────────────────────

DIAGNOSIS_TOTAL = _make_counter(
    "ifr_diagnosis_total", "Total number of diagnoses",
    ["status"]  # accepted, running, success, failed, rollback_triggered
)

DIAGNOSIS_DURATION = _make_histogram(
    "ifr_diagnosis_duration_seconds", "Diagnosis pipeline duration",
    ["risk_level"],
    buckets=[5, 10, 20, 30, 45, 60, 90, 120, 180, 300]
)

DIAGNOSIS_CONFIDENCE = _make_histogram(
    "ifr_diagnosis_confidence", "Arbitration confidence score",
    buckets=[0.1, 0.3, 0.5, 0.7, 0.8, 0.9, 0.95, 0.99]
)

ACTIVE_DIAGNOSES = _make_gauge(
    "ifr_active_diagnoses", "Currently active diagnoses"
)

DIAGNOSIS_MTTR = _make_histogram(
    "ifr_diagnosis_mttr_seconds", "Mean Time To Resolve",
    buckets=[30, 60, 90, 120, 180, 300, 600]
)

# ─── Expert Metrics ────────────────────────────────────────────

EXPERT_TOOL_CALLS = _make_counter(
    "ifr_expert_tool_calls_total", "Total tool calls by expert",
    ["agent_type", "tool_name", "status"]
)

EXPERT_TOOL_DURATION = _make_histogram(
    "ifr_expert_tool_duration_seconds", "Tool call duration",
    ["agent_type", "tool_name"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1, 2, 5]
)

EXPERT_DIAGNOSIS_TOTAL = _make_counter(
    "ifr_expert_diagnosis_total", "Expert diagnosis count",
    ["agent_type", "status"]
)

# ─── RAG Metrics ───────────────────────────────────────────────

RAG_RETRIEVAL_DURATION = _make_histogram(
    "ifr_rag_retrieval_duration_seconds", "RAG retrieval duration",
    buckets=[0.1, 0.5, 1, 2, 3, 5, 10]
)

RAG_DOCUMENT_COUNT = _make_gauge(
    "ifr_rag_document_count", "Total documents in RAG corpus"
)

RAG_UPSERT_TOTAL = _make_counter(
    "ifr_rag_upsert_total", "Total documents upserted",
    ["source"]
)

# ─── HITL Metrics ──────────────────────────────────────────────

HITL_APPROVAL_TOTAL = _make_counter(
    "ifr_hitl_approval_total", "Total HITL approvals",
    ["risk_level", "status"]  # status: pending, approved, rejected, expired
)

HITL_APPROVAL_DURATION = _make_histogram(
    "ifr_hitl_approval_duration_seconds", "Time from creation to resolution",
    ["risk_level"],
    buckets=[30, 60, 120, 300, 600, 1200]
)

# ─── Action Executor Metrics ───────────────────────────────────

ACTION_EXECUTION_TOTAL = _make_counter(
    "ifr_action_execution_total", "Total action executions",
    ["action_type", "status"]
)

ACTION_EXECUTION_DURATION = _make_histogram(
    "ifr_action_execution_duration_seconds", "Action execution duration",
    ["action_type"],
    buckets=[1, 5, 10, 30, 60, 120]
)

# ─── FastAPI Integration ───────────────────────────────────────

def setup_metrics(app) -> None:
    """Add /metrics endpoint and request-count middleware to a FastAPI app."""
    if not _HAS_PROMETHEUS:
        logger.warning("prometheus_client not installed — /metrics unavailable")
        return

    from fastapi import Request, Response

    REQUEST_COUNT = _make_counter(
        "ifr_http_requests_total", "Total HTTP requests",
        ["method", "endpoint", "status"]
    )
    REQUEST_DURATION = _make_histogram(
        "ifr_http_request_duration_seconds", "HTTP request duration",
        ["method", "endpoint"],
        buckets=[0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10]
    )

    @app.middleware("http")
    async def metrics_middleware(request: Request, call_next):
        if request.url.path == "/metrics":
            return await call_next(request)

        start = time.monotonic()
        response = await call_next(request)
        duration = time.monotonic() - start

        endpoint = request.url.path
        method = request.method
        status = response.status_code

        REQUEST_COUNT.labels(method=method, endpoint=endpoint, status=str(status)).inc()
        REQUEST_DURATION.labels(method=method, endpoint=endpoint).observe(duration)

        return response

    @app.get("/metrics")
    async def metrics():
        return Response(content=generate_latest(_registry), media_type=CONTENT_TYPE_LATEST)

    logger.info("Prometheus /metrics endpoint enabled for service=%s", SERVICE_NAME)
