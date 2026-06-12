"""
Scenario orchestration for the 52-scenario benchmark.

Maps 20 anomaly generator scenarios × 32 mock service scenarios
into a comprehensive test matrix. Prioritizes high-impact cross-category
combinations for a realistic regression suite.
"""

from __future__ import annotations

from typing import Any, Dict, List

# ─── Scenario Matrix ─────────────────────────────────────────────
# Each entry: (name, anomaly_scenario, mock_scenarios, expected_category, expected_risk)

BENCHMARK_SCENARIOS: List[Dict[str, Any]] = [
    # ── Redis / Latency (6) ──
    {"name": "redis_keys_latency",     "anomaly": "latency_spike",       "mocks": {"redis": "slow_query",    "k8s": "oom"},            "category": "middleware/redis/performance", "risk": "low"},
    {"name": "redis_memory_pressure",  "anomaly": "latency_spike",       "mocks": {"redis": "memory_pressure","k8s": "default"},       "category": "middleware/redis/performance", "risk": "low"},
    {"name": "redis_connection_storm", "anomaly": "connection_storm",    "mocks": {"redis": "connection_storm","k8s": "default"},      "category": "middleware/redis/performance", "risk": "medium"},
    {"name": "redis_maxmemory_evict",  "anomaly": "connection_storm",    "mocks": {"redis": "maxmemory_eviction","k8s": "default"},    "category": "middleware/redis/performance", "risk": "low"},
    {"name": "redis_replica_lag",      "anomaly": "latency_spike",       "mocks": {"redis": "replica_lag",     "k8s": "default"},      "category": "middleware/redis/performance", "risk": "low"},
    {"name": "redis_cluster_migration","anomaly": "latency_spike",       "mocks": {"redis": "cluster_resharding","k8s": "default"},   "category": "middleware/redis/performance", "risk": "medium"},

    # ── MySQL (4) ──
    {"name": "mysql_conn_exhaustion",  "anomaly": "db_connection_exhaustion","mocks": {"redis": "default",       "k8s": "oom"},            "category": "middleware/mysql/performance", "risk": "high"},
    {"name": "mysql_slow_query",       "anomaly": "db_connection_exhaustion","mocks": {"redis": "default",       "k8s": "crash_loop"},      "category": "middleware/mysql/performance", "risk": "medium"},
    {"name": "mysql_disk_io",          "anomaly": "db_connection_exhaustion","mocks": {"redis": "default",       "k8s": "default"},         "category": "middleware/mysql/performance", "risk": "low"},
    {"name": "mysql_sentinel_switch",  "anomaly": "db_connection_exhaustion","mocks": {"redis": "sentinel_failover","k8s": "default"},     "category": "middleware/mysql/connection_leak","risk": "high"},

    # ── Kafka (4) ──
    {"name": "kafka_lag_spike",        "anomaly": "kafka_consumer_lag",  "mocks": {"redis": "default",       "k8s": "oom"},            "category": "middleware/kafka/lag", "risk": "medium"},
    {"name": "kafka_queue_buildup",    "anomaly": "kafka_consumer_lag",  "mocks": {"redis": "default",       "k8s": "default"},         "category": "middleware/kafka/lag", "risk": "low"},
    {"name": "kafka_broker_pressure",  "anomaly": "kafka_consumer_lag",  "mocks": {"redis": "memory_pressure","k8s": "default"},       "category": "middleware/kafka/lag", "risk": "medium"},
    {"name": "kafka_client_storm",     "anomaly": "kafka_consumer_lag",  "mocks": {"redis": "client_connection_storm","k8s": "default"},"category": "middleware/kafka/lag","risk": "medium"},

    # ── K8s OOM (6) ──
    {"name": "k8s_oom_redis",          "anomaly": "oom_cascade",         "mocks": {"redis": "slow_query",    "k8s": "oom"},            "category": "k8s/oom", "risk": "high"},
    {"name": "k8s_oom_cpu_throttle",   "anomaly": "cpu_throttling",      "mocks": {"redis": "default",       "k8s": "oom"},            "category": "k8s/oom", "risk": "high"},
    {"name": "k8s_oom_node_full",      "anomaly": "node_disk_full",      "mocks": {"redis": "default",       "k8s": "oom"},            "category": "k8s/oom", "risk": "high"},
    {"name": "k8s_crash_loop",         "anomaly": "oom_cascade",         "mocks": {"redis": "default",       "k8s": "crash_loop"},      "category": "k8s/crash_loop", "risk": "high"},
    {"name": "k8s_crash_backoff",      "anomaly": "oom_cascade",         "mocks": {"redis": "default",       "k8s": "crash_loop_backoff"},"category": "k8s/crash_loop","risk": "high"},
    {"name": "k8s_image_pull_backoff", "anomaly": "oom_cascade",         "mocks": {"redis": "default",       "k8s": "image_pull_backoff"},"category": "k8s/crash_loop","risk": "high"},

    # ── K8s Deployment (4) ──
    {"name": "k8s_deploy_hpa",         "anomaly": "memory_leak_slow",    "mocks": {"redis": "default",       "k8s": "hpa_thrashing"},   "category": "k8s/deployment/rollback", "risk": "medium"},
    {"name": "k8s_node_not_ready",     "anomaly": "oom_cascade",         "mocks": {"redis": "default",       "k8s": "node_not_ready"},   "category": "k8s/oom", "risk": "critical"},
    {"name": "k8s_pvc_full",           "anomaly": "node_disk_full",      "mocks": {"redis": "default",       "k8s": "pvc_full"},         "category": "k8s/deployment/rollback", "risk": "medium"},
    {"name": "k8s_stale_endpoints",    "anomaly": "oom_cascade",         "mocks": {"redis": "default",       "k8s": "stale_endpoints"},   "category": "k8s/deployment/rollback", "risk": "medium"},

    # ── Network (8) ──
    {"name": "net_packet_loss",        "anomaly": "network_degradation", "mocks": {"network": "packet_loss",    "redis": "default"},   "category": "network/packet_loss", "risk": "medium"},
    {"name": "net_dns_failure",        "anomaly": "dns_timeout",         "mocks": {"network": "dns_failure",    "redis": "default"},   "category": "network/dns_failure", "risk": "medium"},
    {"name": "net_partition",          "anomaly": "network_degradation", "mocks": {"network": "network_partition","redis": "default"},  "category": "network/packet_loss", "risk": "high"},
    {"name": "net_proxy_timeout",      "anomaly": "gateway_throttling",  "mocks": {"network": "proxy_timeout",   "redis": "default"},   "category": "network/packet_loss", "risk": "medium"},
    {"name": "net_firewall_change",    "anomaly": "network_degradation", "mocks": {"network": "firewall_rule_change","redis": "default"},"category": "network/packet_loss","risk": "medium"},
    {"name": "net_ssl_expiry",         "anomaly": "ntp_time_skew",       "mocks": {"network": "ssl_expiry",      "redis": "default"},   "category": "network/dns_failure", "risk": "high"},
    {"name": "net_bandwidth_sat",      "anomaly": "network_degradation", "mocks": {"network": "bandwidth_saturation","redis": "default"},"category": "network/packet_loss","risk": "medium"},
    {"name": "net_dns_poisoning",      "anomaly": "dns_timeout",         "mocks": {"network": "dns_cache_poisoning","redis": "default"},"category": "network/dns_failure","risk": "high"},

    # ── Application (4) ──
    {"name": "app_gateway_throttle",   "anomaly": "gateway_throttling",  "mocks": {"redis": "default",       "k8s": "default"},         "category": "application/latency", "risk": "low"},
    {"name": "app_config_drift",       "anomaly": "config_drift",        "mocks": {"redis": "default",       "k8s": "default"},         "category": "general/config_drift", "risk": "medium"},
    {"name": "app_ntp_skew",           "anomaly": "ntp_time_skew",       "mocks": {"redis": "default",       "k8s": "default"},         "category": "general/config_drift", "risk": "medium"},
    {"name": "app_lb_failure",         "anomaly": "load_balancer_failure","mocks": {"redis": "default",       "k8s": "hpa_thrashing"},   "category": "application/latency", "risk": "high"},

    # ── Industrial / PLC / CNC (8) ──
    {"name": "plc_comms_timeout",      "anomaly": "plc_comms_timeout",   "mocks": {"redis": "default",       "k8s": "default"},         "category": "industrial/plc/comms", "risk": "high"},
    {"name": "cnc_tool_wear",          "anomaly": "cnc_tool_wear",       "mocks": {"redis": "default",       "k8s": "default"},         "category": "industrial/cnc/precision","risk": "high"},
    {"name": "agv_battery_critical",   "anomaly": "agv_battery_critical","mocks": {"redis": "default",       "k8s": "default"},         "category": "industrial/agv/battery","risk": "medium"},
    {"name": "multi_line_failure",     "anomaly": "multi_line_failure",  "mocks": {"redis": "default",       "k8s": "default"},         "category": "industrial/plc/comms","risk": "critical"},

    # ── Mixed / Edge cases (6) ──
    {"name": "mixed_disk_pressure",    "anomaly": "disk_pressure",        "mocks": {"redis": "default",       "k8s": "oom"},            "category": "general/disk_full", "risk": "medium"},
    {"name": "mixed_queue_buildup",    "anomaly": "queue_buildup",        "mocks": {"redis": "connection_storm","k8s": "crash_loop"},   "category": "middleware/kafka/lag", "risk": "medium"},
    {"name": "mixed_memory_leak",      "anomaly": "memory_leak_slow",    "mocks": {"redis": "memory_pressure","k8s": "hpa_thrashing"},  "category": "k8s/oom", "risk": "high"},
    {"name": "mixed_dns_timeout",      "anomaly": "dns_timeout",         "mocks": {"network": "dns_failure",    "k8s": "default"},      "category": "network/dns_failure","risk": "medium"},
    {"name": "mixed_init_failure",     "anomaly": "oom_cascade",         "mocks": {"k8s": "init_container_failure","redis": "default"},  "category": "k8s/crash_loop","risk": "high"},
    {"name": "mixed_keyspace_storm",   "anomaly": "connection_storm",    "mocks": {"redis": "keyspace_notification_storm","k8s":"default"},"category":"middleware/redis/performance","risk":"medium"},

    # ── Additional edge cases (+6) ──
    {"name": "edge_mtu_mismatch",      "anomaly": "network_degradation", "mocks": {"network": "mtu_mismatch",        "redis": "default"},   "category": "network/packet_loss","risk":"medium"},
    {"name": "edge_tcp_retransmit",    "anomaly": "network_degradation", "mocks": {"network": "tcp_retransmit_storm", "redis": "default"},  "category": "network/packet_loss","risk":"medium"},
    {"name": "edge_bgp_hijack",        "anomaly": "dns_timeout",         "mocks": {"network": "bgp_route_hijack",     "redis": "default"},  "category": "network/dns_failure","risk":"high"},
    {"name": "edge_redis_sentinel",    "anomaly": "connection_storm",    "mocks": {"redis": "sentinel_failover",       "k8s": "default"},    "category": "middleware/redis/performance","risk":"high"},
    {"name": "edge_cnc_overheat",      "anomaly": "cnc_tool_wear",       "mocks": {"redis": "default",                 "k8s": "crash_loop"}, "category": "industrial/cnc/precision","risk":"critical"},
    {"name": "edge_agv_signal_weak",   "anomaly": "agv_battery_critical","mocks": {"redis": "default",                 "k8s": "hpa_thrashing"},"category":"industrial/agv/battery","risk":"medium"},
]

# Total: 52 scenarios
assert len(BENCHMARK_SCENARIOS) == 52, f"Expected 52, got {len(BENCHMARK_SCENARIOS)}"
