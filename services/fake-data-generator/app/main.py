"""
Fake Data Generator — Entry point.

Periodically generates industrial metrics and alert events, sending them
to Kafka topics.  Supports:
  - YAML-based anomaly scenario injection (loaded at startup, switchable via HTTP)
  - Configurable time acceleration (>1 to simulate hours in minutes)
  - Line-profile-aware node registry (50+ nodes, 3 production lines)
  - Graceful shutdown on SIGTERM/SIGINT

Environment variables:
  KAFKA_BOOTSTRAP_SERVERS   : Kafka broker (default kafka:9092)
  TOPIC_METRICS             : topic for metric stream (default industrial-metrics)
  TOPIC_ALERTS              : topic for alert stream (default industrial-alerts)
  INTERVAL_MS               : tick interval in ms (default 1000)
  TIME_MULTIPLIER           : >1 for simulation acceleration (default 1.0)
  ANOMALY_CONFIG_PATH       : path to anomaly_scenarios.yaml
  ANOMALY_INJECTION_ENABLED : "true" or "false"
  ANOMALY_AUTO_START        : scenario name to auto-activate on startup
  PRODUCTION_LINE_PROFILES  : comma-separated list (default all)
  API_PORT                  : HTTP management port (default 9005)
"""

from __future__ import annotations

import os
import sys
import signal
import logging
import time
import threading
from datetime import datetime, timezone
from pathlib import Path

import yaml
from fastapi import FastAPI, HTTPException

# Path wrangling
_PARENT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

from common.auth import setup_api_key_auth  # noqa: E402

from .node_registry import get_all_nodes, get_nodes_by_line  # noqa: E402
from .metric_generator import MetricGenerator, AnomalyInjector  # noqa: E402
from .kafka_producer import MetricProducer  # noqa: E402

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [fake-gen] %(levelname)s %(message)s",
)
logger = logging.getLogger("fake-data-generator")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_METRICS = os.getenv("TOPIC_METRICS", "industrial-metrics")
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "industrial-alerts")
INTERVAL_MS = int(os.getenv("INTERVAL_MS", "1000"))
TIME_MULTIPLIER = float(os.getenv("TIME_MULTIPLIER", "1.0"))
ANOMALY_CONFIG = os.getenv("ANOMALY_CONFIG_PATH", "/app/anomaly_scenarios.yaml")
ANOMALY_ENABLED = os.getenv("ANOMALY_INJECTION_ENABLED", "true").lower() == "true"
ANOMALY_AUTO_START = os.getenv("ANOMALY_AUTO_START", "")
API_PORT = int(os.getenv("API_PORT", "9005"))
LINE_PROFILES = os.getenv("PRODUCTION_LINE_PROFILES", "")

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------

_stop_event = threading.Event()
_generator: MetricGenerator | None = None
_producer: MetricProducer | None = None
_injector: AnomalyInjector | None = None
_stats = {"messages_sent": 0, "ticks": 0, "started_at": None, "last_tick_at": None}

# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

def _load_anomaly_scenarios(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        logger.warning("Anomaly config not found at %s — no injection rules loaded", path)
        return {}
    with open(p, "r") as fh:
        raw = yaml.safe_load(fh)
    return raw.get("scenarios", {}) if raw else {}


def _select_nodes() -> list:
    """Select nodes based on PRODUCTION_LINE_PROFILES env var."""
    profiles = [p.strip() for p in LINE_PROFILES.split(",") if p.strip()]
    if not profiles:
        return get_all_nodes()
    selected = []
    for p in profiles:
        selected.extend(get_nodes_by_line(p))
    return selected if selected else get_all_nodes()


# ---------------------------------------------------------------------------
# Producer loop (runs in a daemon thread)
# ---------------------------------------------------------------------------

def _producer_loop() -> None:
    global _stats
    _stats["started_at"] = datetime.now(timezone.utc).isoformat()

    # Kafka producer (with retries)
    prod = MetricProducer(KAFKA_BOOTSTRAP, TOPIC_METRICS, TOPIC_ALERTS)
    _producer_ref = prod  # keep reference for shutdown flush

    # Injector
    inj = AnomalyInjector()
    if ANOMALY_ENABLED:
        scenarios = _load_anomaly_scenarios(ANOMALY_CONFIG)
        inj.load_scenarios(scenarios)
        if ANOMALY_AUTO_START and ANOMALY_AUTO_START in scenarios:
            inj.activate_scenario(ANOMALY_AUTO_START)
    global _injector
    _injector = inj

    # Generator
    nodes = _select_nodes()
    gen = MetricGenerator(nodes, inj, time_multiplier=TIME_MULTIPLIER)
    global _generator
    _generator = gen

    logger.info("Starting generation loop: %d nodes, interval=%dms, multiplier=%.1fx",
                len(nodes), INTERVAL_MS, TIME_MULTIPLIER)

    while not _stop_event.is_set():
        loop_start = time.monotonic()

        try:
            sim_ts = datetime.now(timezone.utc)
            points = gen.tick(sim_ts)
            prod.send_metrics(points)
            _stats["messages_sent"] += len(points)
            _stats["ticks"] += 1
            _stats["last_tick_at"] = sim_ts.isoformat()

            # Periodically check if any metric would trigger an alert
            # (simplified: send a synthetic alert every ~50 ticks for testing)
            if gen._tick_count % 50 == 0 and gen._tick_count > 0:
                _maybe_generate_alert(prod, points, sim_ts)

        except Exception:
            logger.exception("Error in generation loop")

        # Sleep for the configured interval, accounting for processing time
        elapsed = (time.monotonic() - loop_start) * 1000
        sleep_ms = max(0, INTERVAL_MS / TIME_MULTIPLIER - elapsed)
        _stop_event.wait(sleep_ms / 1000.0)

    prod.flush()
    prod.close()
    logger.info("Producer loop stopped. Total messages: %d", _stats["messages_sent"])


def _maybe_generate_alert(prod, points, ts):
    """Generate a synthetic alert based on anomaly injector state."""
    if _injector is None or not _injector.active_scenario_name:
        return

    # Pick a point from an anomalous node
    anomalous = [p for p in points if p.get("anomaly_active")]
    if not anomalous:
        return
    pt = anomalous[0]
    alert = {
        "alert_id": f"alert-{int(ts.timestamp() * 1000)}",
        "timestamp": int(ts.timestamp() * 1000),
        "node_id": pt["node_id"],
        "node_type": pt["node_type"],
        "metric_type": pt["metric_type"],
        "current_value": pt["metric_value"],
        "severity": pt["severity"],
        "anomaly_scenario": _injector.active_scenario_name,
        "tags": pt["tags"],
    }
    prod.send_alert(alert)
    logger.info("Sent synthetic alert: %s", alert["alert_id"])


# ---------------------------------------------------------------------------
# HTTP management API
# ---------------------------------------------------------------------------

mgmt_app = FastAPI(title="Fake Data Generator — Management API", version="1.0.0")
setup_api_key_auth(mgmt_app)


@mgmt_app.get("/health")
def health():
    return {"status": "ok", "stats": _stats}


@mgmt_app.get("/scenarios")
def list_scenarios():
    if _injector is None:
        return {"scenarios": []}
    return {"scenarios": list(_injector._scenarios.keys()), "active": _injector.active_scenario_name}


@mgmt_app.post("/scenarios/{name}/activate")
def activate_scenario(name: str):
    if _injector is None:
        raise HTTPException(status_code=503, detail="Generator not started yet")
    ok = _injector.activate_scenario(name)
    if not ok:
        raise HTTPException(status_code=404, detail=f"Scenario '{name}' not found")
    return {"message": f"Scenario '{name}' activated", "active": _injector.active_scenario_name}


@mgmt_app.post("/scenarios/deactivate")
def deactivate_scenario():
    if _injector is None:
        raise HTTPException(status_code=503, detail="Generator not started yet")
    _injector.deactivate_all()
    return {"message": "All scenarios deactivated"}


@mgmt_app.get("/stats")
def stats():
    return _stats


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _signal_handler(signum, frame):
    logger.info("Received signal %s, shutting down...", signum)
    _stop_event.set()


def main():
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    # Start producer in background thread
    thread = threading.Thread(target=_producer_loop, daemon=True, name="producer-loop")
    thread.start()

    # Start management API
    import uvicorn
    uvicorn.run(mgmt_app, host="0.0.0.0", port=API_PORT, log_level="info")


if __name__ == "__main__":
    main()
