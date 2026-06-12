#!/usr/bin/env python3
"""Integration test helpers — API wrappers, timing, assertions."""

from __future__ import annotations
import sys, os, json, time, argparse
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import httpx

BASE = "http://localhost"
API_KEY = "dev-key-change-me"
AUTH = {"X-API-Key": API_KEY}
TIMEOUT = 10.0

class TestContext:
    """Holds state across test steps."""
    def __init__(self):
        self.events: List[Dict] = []
        self.start_time: Optional[float] = None
        self.results: Dict[str, Any] = {}

    def record(self, step: str, data: Dict = None):
        entry = {"step": step, "timestamp": datetime.now(timezone.utc).isoformat(),
                 "elapsed_s": round(time.monotonic() - (self.start_time or time.monotonic()), 1)}
        if data: entry.update(data)
        self.events.append(entry)
        print(f"  [{entry['elapsed_s']:6.1f}s] {step}")

    def assert_true(self, condition, msg):
        if not condition: raise AssertionError(msg)
        print(f"         ✅ {msg}")

async def health_check(service: str, port: int) -> bool:
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get(f"{BASE}:{port}/health", headers=AUTH)
            return r.status_code == 200
    except Exception: return False

async def activate_scenario(name: str) -> Dict:
    async with httpx.AsyncClient(timeout=TIMEOUT) as c:
        r = await c.post(f"{BASE}:9005/scenarios/{name}/activate", headers=AUTH)
        return r.json()

async def set_mock_scenario(mock: str, name: str) -> Dict:
    ports = {"k8s": 9002, "redis": 9003, "network": 9004}
    async with httpx.AsyncClient(timeout=TIMEOUT) as c:
        r = await c.post(f"{BASE}:{ports[mock]}/scenario/{name}", headers=AUTH)
        return r.json()

async def trigger_incident(payload: Dict) -> Dict:
    async with httpx.AsyncClient(timeout=TIMEOUT) as c:
        r = await c.post(f"{BASE}:8100/api/v1/incident", json=payload, headers=AUTH)
        return r.json()

async def get_diagnosis(trace_id: str) -> Dict:
    async with httpx.AsyncClient(timeout=TIMEOUT) as c:
        r = await c.get(f"{BASE}:8100/api/v1/diagnosis/{trace_id}", headers=AUTH)
        return r.json()

async def wait_for_status(trace_id: str, target_statuses: List[str], max_wait: int = 120) -> Dict:
    deadline = time.monotonic() + max_wait
    while time.monotonic() < deadline:
        d = await get_diagnosis(trace_id)
        status = d.get("execution_status", "unknown")
        if status in target_statuses:
            return d
        await asyncio.sleep(2)
    raise TimeoutError(f"Status did not reach {target_statuses} within {max_wait}s")

async def approve(approval_id: str) -> Dict:
    async with httpx.AsyncClient(timeout=TIMEOUT) as c:
        r = await c.post(f"{BASE}:8300/api/v1/approvals/{approval_id}/approve",
                         json={"user_id": "test-engineer", "comment": "Integration test auto-approval"},
                         headers=AUTH)
        return r.json()

async def get_pending_approvals() -> List[Dict]:
    async with httpx.AsyncClient(timeout=TIMEOUT) as c:
        r = await c.get(f"{BASE}:8300/api/v1/approvals/pending", headers=AUTH)
        return r.json().get("items", [])

async def rag_doc_count() -> int:
    async with httpx.AsyncClient(timeout=TIMEOUT) as c:
        r = await c.get(f"{BASE}:8200/health", headers=AUTH)
        return r.json().get("total_documents", 0)

async def rag_retrieve(query: str, top_k: int = 5) -> List[Dict]:
    async with httpx.AsyncClient(timeout=10.0) as c:
        r = await c.post(f"{BASE}:8200/api/v1/rag/retrieve",
                         json={"query": query, "top_k": top_k}, headers=AUTH)
        return r.json().get("documents", [])

def build_incident(node_id="order-svc", metric_type="p99_latency_ms",
                   current=1200.0, baseline=80.0, std=15.0, sigma=5.2,
                   severity="critical", tags=None, **kwargs) -> Dict:
    import uuid
    tags = tags or {"service": node_id}
    return {
        "incident_id": f"test-{uuid.uuid4().hex[:8]}",
        "trigger_time": datetime.now(timezone.utc).isoformat(),
        "aggregation_window_seconds": 300,
        "priority_score": 82.5,
        "aggregated_alerts": [{
            "alert_id": f"alert-{uuid.uuid4().hex[:6]}",
            "node_id": node_id, "node_type": "Container",
            "metric_type": metric_type, "current_value": current,
            "baseline_mean": baseline, "baseline_std": std,
            "deviation_sigma": sigma, "severity": severity,
            "tags": tags,
        }],
        "affected_line_profile": "general",
        "node_id": node_id, "metric_group": "latency",
        "alert_count": 1, "severity_max": severity,
        **kwargs,
    }

import asyncio
if __name__ == "__main__":
    print("Test helpers loaded.")
