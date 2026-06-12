"""
Industrial node registry.

Defines the topology of a virtual factory floor with three production lines:
  - precision_machining  (CNC mills, PLCs — sensitive to vibration/temperature)
  - packaging             (conveyors, AGVs — throughput-oriented)
  - assembly              (robot arms, edge gateways — moderate)

Each node emits 10 metrics on every tick (1 Hz default).
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import List, Dict, Optional

# ---------------------------------------------------------------------------
# Metric definitions per node_type
# ---------------------------------------------------------------------------

METRIC_TEMPLATES: Dict[str, List[dict]] = {
    "CNC": [
        {"name": "spindle_speed_rpm",   "unit": "rpm",    "baseline": 12000, "std": 200,  "min": 0,    "max": 24000},
        {"name": "vibration_um",        "unit": "μm",     "baseline": 2.5,  "std": 0.5,  "min": 0,    "max": 20},
        {"name": "temperature_c",       "unit": "°C",     "baseline": 55.0, "std": 2.0,  "min": 0,    "max": 120},
        {"name": "power_consumption_w", "unit": "W",      "baseline": 3500, "std": 200,  "min": 0,    "max": 8000},
        {"name": "tool_wear_um",        "unit": "μm",     "baseline": 0.3,  "std": 0.05, "min": 0,    "max": 5},
        {"name": "coolant_flow_lpm",    "unit": "L/min",  "baseline": 8.0,  "std": 0.3,  "min": 0,    "max": 15},
        {"name": "feed_rate_mm_min",    "unit": "mm/min", "baseline": 500,  "std": 30,   "min": 0,    "max": 1000},
        {"name": "error_rate",          "unit": "%",      "baseline": 0.01, "std": 0.01, "min": 0,    "max": 100},
        {"name": "queue_depth",         "unit": "count",  "baseline": 2,    "std": 1,    "min": 0,    "max": 50},
        {"name": "cycle_time_ms",       "unit": "ms",     "baseline": 850,  "std": 30,   "min": 100,  "max": 5000},
    ],
    "PLC": [
        {"name": "cpu_usage",           "unit": "%",      "baseline": 35.0, "std": 5.0,  "min": 0,    "max": 100},
        {"name": "mem_usage",           "unit": "%",      "baseline": 45.0, "std": 3.0,  "min": 0,    "max": 100},
        {"name": "scan_cycle_ms",       "unit": "ms",     "baseline": 10.0, "std": 1.0,  "min": 0,    "max": 100},
        {"name": "comms_latency_ms",    "unit": "ms",     "baseline": 2.0,  "std": 0.5,  "min": 0,    "max": 200},
        {"name": "error_rate",          "unit": "%",      "baseline": 0.0,  "std": 0.0,  "min": 0,    "max": 100},
        {"name": "io_point_count",      "unit": "count",  "baseline": 128,  "std": 0.0,  "min": 0,    "max": 512},
        {"name": "program_cycle_us",    "unit": "μs",     "baseline": 500,  "std": 50,   "min": 0,    "max": 5000},
        {"name": "firmware_version",    "unit": "version","baseline": 2.1,  "std": 0.0,  "min": 0,    "max": 10},
        {"name": "temperature_c",       "unit": "°C",     "baseline": 40.0, "std": 2.0,  "min": 0,    "max": 85},
        {"name": "power_consumption_w", "unit": "W",      "baseline": 30,   "std": 2,    "min": 0,    "max": 100},
    ],
    "RobotArm": [
        {"name": "joint_angle_0_deg",   "unit": "deg",    "baseline": 45.0, "std": 0.5,  "min": -180, "max": 180},
        {"name": "joint_torque_nm",     "unit": "Nm",     "baseline": 12.0, "std": 1.0,  "min": 0,    "max": 50},
        {"name": "gripper_pressure_bar","unit": "bar",    "baseline": 5.0,  "std": 0.2,  "min": 0,    "max": 10},
        {"name": "cycle_time_ms",       "unit": "ms",     "baseline": 2200, "std": 100,  "min": 500,  "max": 10000},
        {"name": "temperature_c",       "unit": "°C",     "baseline": 42.0, "std": 1.5,  "min": 0,    "max": 90},
        {"name": "power_consumption_w", "unit": "W",      "baseline": 800,  "std": 50,   "min": 0,    "max": 2000},
        {"name": "error_rate",          "unit": "%",      "baseline": 0.02, "std": 0.02, "min": 0,    "max": 100},
        {"name": "vibration_um",        "unit": "μm",     "baseline": 1.5,  "std": 0.3,  "min": 0,    "max": 15},
        {"name": "queue_depth",         "unit": "count",  "baseline": 1,    "std": 0.5,  "min": 0,    "max": 30},
        {"name": "speed_mm_s",          "unit": "mm/s",   "baseline": 500,  "std": 20,   "min": 0,    "max": 1500},
    ],
    "AGV": [
        {"name": "battery_pct",         "unit": "%",      "baseline": 75.0, "std": 5.0,  "min": 0,    "max": 100},
        {"name": "speed_m_s",           "unit": "m/s",    "baseline": 1.2,  "std": 0.1,  "min": 0,    "max": 2.5},
        {"name": "position_x_m",        "unit": "m",      "baseline": 50.0, "std": 10.0, "min": 0,    "max": 200},
        {"name": "obstacle_distance_m", "unit": "m",      "baseline": 5.0,  "std": 2.0,  "min": 0,    "max": 50},
        {"name": "wifi_signal_dbm",     "unit": "dBm",    "baseline": -45,  "std": 5,    "min": -90,  "max": -20},
        {"name": "motor_temp_c",        "unit": "°C",     "baseline": 35.0, "std": 2.0,  "min": 0,    "max": 80},
        {"name": "error_rate",          "unit": "%",      "baseline": 0.01, "std": 0.01, "min": 0,    "max": 100},
        {"name": "payload_kg",          "unit": "kg",     "baseline": 0.0,  "std": 0.0,  "min": 0,    "max": 500},
        {"name": "queue_depth",         "unit": "count",  "baseline": 0,    "std": 0,    "min": 0,    "max": 20},
        {"name": "battery_cycles",      "unit": "count",  "baseline": 320,  "std": 0.0,  "min": 0,    "max": 1000},
    ],
    "EdgeGW": [
        {"name": "cpu_usage",           "unit": "%",      "baseline": 25.0, "std": 5.0,  "min": 0,    "max": 100},
        {"name": "mem_usage",           "unit": "%",      "baseline": 40.0, "std": 4.0,  "min": 0,    "max": 100},
        {"name": "net_in_mbps",         "unit": "Mbps",   "baseline": 80.0, "std": 10.0, "min": 0,    "max": 1000},
        {"name": "net_out_mbps",        "unit": "Mbps",   "baseline": 60.0, "std": 8.0,  "min": 0,    "max": 1000},
        {"name": "msg_queue_depth",     "unit": "count",  "baseline": 5,    "std": 3,    "min": 0,    "max": 500},
        {"name": "disk_usage",          "unit": "%",      "baseline": 35.0, "std": 2.0,  "min": 0,    "max": 100},
        {"name": "error_rate",          "unit": "%",      "baseline": 0.0,  "std": 0.0,  "min": 0,    "max": 100},
        {"name": "connected_devices",   "unit": "count",  "baseline": 12,   "std": 0,    "min": 0,    "max": 100},
        {"name": "temperature_c",       "unit": "°C",     "baseline": 38.0, "std": 2.0,  "min": 0,    "max": 85},
        {"name": "uptime_hours",        "unit": "h",      "baseline": 720,  "std": 0.0,  "min": 0,    "max": 8760},
    ],
    "Server": [
        {"name": "cpu_usage",           "unit": "%",      "baseline": 22.0, "std": 5.0,  "min": 0,    "max": 100},
        {"name": "mem_usage",           "unit": "%",      "baseline": 35.0, "std": 4.0,  "min": 0,    "max": 100},
        {"name": "disk_io_mbps",        "unit": "MB/s",   "baseline": 120,  "std": 20,   "min": 0,    "max": 500},
        {"name": "p99_latency_ms",      "unit": "ms",     "baseline": 45.0, "std": 5.0,  "min": 0,    "max": 5000},
        {"name": "request_rate",        "unit": "rps",    "baseline": 250,  "std": 30,   "min": 0,    "max": 5000},
        {"name": "error_rate",          "unit": "%",      "baseline": 0.01, "std": 0.01, "min": 0,    "max": 100},
        {"name": "connection_count",    "unit": "count",  "baseline": 85,   "std": 10,   "min": 0,    "max": 5000},
        {"name": "disk_usage",          "unit": "%",      "baseline": 42.0, "std": 2.0,  "min": 0,    "max": 100},
        {"name": "queue_depth",         "unit": "count",  "baseline": 3,    "std": 2,    "min": 0,    "max": 200},
        {"name": "thread_pool_active",  "unit": "count",  "baseline": 18,   "std": 3,    "min": 0,    "max": 200},
    ],
    "Container": [
        {"name": "cpu_usage",           "unit": "%",      "baseline": 30.0, "std": 8.0,  "min": 0,    "max": 100},
        {"name": "mem_usage",           "unit": "%",      "baseline": 45.0, "std": 5.0,  "min": 0,    "max": 100},
        {"name": "p99_latency_ms",      "unit": "ms",     "baseline": 80.0, "std": 10.0, "min": 0,    "max": 5000},
        {"name": "request_rate",        "unit": "rps",    "baseline": 150,  "std": 20,   "min": 0,    "max": 3000},
        {"name": "error_rate",          "unit": "%",      "baseline": 0.01, "std": 0.02, "min": 0,    "max": 100},
        {"name": "restart_count",       "unit": "count",  "baseline": 0,    "std": 0,    "min": 0,    "max": 100},
        {"name": "connection_pool_used","unit": "count",  "baseline": 25,   "std": 5,    "min": 0,    "max": 100},
        {"name": "disk_io_kbps",        "unit": "KB/s",   "baseline": 500,  "std": 100,  "min": 0,    "max": 10000},
        {"name": "queue_depth",         "unit": "count",  "baseline": 2,    "std": 1,    "min": 0,    "max": 100},
        {"name": "gc_pause_ms",         "unit": "ms",     "baseline": 15.0, "std": 5.0,  "min": 0,    "max": 5000},
    ],
}

# ---------------------------------------------------------------------------
# Node instances per production line
# ---------------------------------------------------------------------------

@dataclass
class Node:
    node_id: str
    node_type: str
    line_profile: str
    zone: str
    tags: Dict[str, str] = field(default_factory=dict)


def _build_nodes() -> List[Node]:
    """Build the full node registry (~50 nodes across 3 lines)."""
    nodes: List[Node] = []

    # --- Precision Machining Line (L1) ---
    for i in range(1, 6):
        nodes.append(Node(f"CNC-Mill-L1-{i:02d}", "CNC", "precision_machining", "machining",
                          {"line": "L1", "zone": "machining", "cell": f"cell-{i}"}))
    for i in range(1, 4):
        nodes.append(Node(f"PLC-L1-{i:02d}", "PLC", "precision_machining", "control",
                          {"line": "L1", "zone": "control"}))
    for i in range(1, 3):
        nodes.append(Node(f"Robot-L1-{i:02d}", "RobotArm", "precision_machining", "handling",
                          {"line": "L1", "zone": "handling"}))
    nodes.append(Node("EdgeGW-L1-01", "EdgeGW", "precision_machining", "edge", {"line": "L1"}))

    # --- Packaging Line (L2) ---
    for i in range(1, 4):
        nodes.append(Node(f"AGV-L2-{i:02d}", "AGV", "packaging", "logistics",
                          {"line": "L2", "zone": "logistics"}))
    for i in range(1, 5):
        nodes.append(Node(f"PLC-L2-{i:02d}", "PLC", "packaging", "control",
                          {"line": "L2", "zone": "control"}))
    for i in range(1, 3):
        nodes.append(Node(f"Robot-L2-{i:02d}", "RobotArm", "packaging", "palletizing",
                          {"line": "L2", "zone": "palletizing"}))
    nodes.append(Node("EdgeGW-L2-01", "EdgeGW", "packaging", "edge", {"line": "L2"}))

    # --- Assembly Line (L3) ---
    for i in range(1, 5):
        nodes.append(Node(f"Robot-L3-{i:02d}", "RobotArm", "assembly", "assembly",
                          {"line": "L3", "zone": "assembly"}))
    for i in range(1, 3):
        nodes.append(Node(f"PLC-L3-{i:02d}", "PLC", "assembly", "control",
                          {"line": "L3", "zone": "control"}))
    for i in range(1, 3):
        nodes.append(Node(f"AGV-L3-{i:02d}", "AGV", "assembly", "logistics",
                          {"line": "L3", "zone": "logistics"}))
    nodes.append(Node("EdgeGW-L3-01", "EdgeGW", "assembly", "edge", {"line": "L3"}))

    # --- Shared IT Infrastructure (Servers + Containers) ---
    for i in range(1, 4):
        nodes.append(Node(f"k8s-node-0{i}", "Server", "general", "k8s-cluster",
                          {"line": "shared", "zone": "k8s"}))
    services = [
        ("order-svc", "Container", "general", "k8s-pod"),
        ("payment-svc", "Container", "general", "k8s-pod"),
        ("inventory-svc", "Container", "general", "k8s-pod"),
        ("gateway-svc", "Container", "general", "k8s-pod"),
        ("analytics-svc", "Container", "general", "k8s-pod"),
        ("notification-svc", "Container", "general", "k8s-pod"),
    ]
    for svc_name, svc_type, svc_line, svc_zone in services:
        nodes.append(Node(svc_name, svc_type, svc_line, svc_zone,
                          {"line": "shared", "service": svc_name}))
    # Middleware nodes
    nodes.append(Node("redis-prod-01", "Server", "general", "middleware",
                      {"line": "shared", "service": "redis"}))
    nodes.append(Node("mysql-prod-01", "Server", "general", "middleware",
                      {"line": "shared", "service": "mysql"}))
    nodes.append(Node("kafka-prod-01", "Server", "general", "middleware",
                      {"line": "shared", "service": "kafka"}))

    return nodes


NODE_REGISTRY: List[Node] = _build_nodes()

# Pre-compute lookup maps
NODES_BY_LINE: Dict[str, List[Node]] = {}
for node in NODE_REGISTRY:
    NODES_BY_LINE.setdefault(node.line_profile, []).append(node)


def get_all_nodes() -> List[Node]:
    return NODE_REGISTRY


def get_nodes_by_line(line: str) -> List[Node]:
    return NODES_BY_LINE.get(line, [])


def metric_templates_for(node_type: str) -> List[dict]:
    """Return the metric definitions for a given node_type."""
    return METRIC_TEMPLATES.get(node_type, METRIC_TEMPLATES["Server"])
