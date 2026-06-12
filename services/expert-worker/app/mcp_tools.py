"""
MCP (Model Context Protocol) tool definitions per agent type.

Each tool maps to a mock service endpoint. In production, MOCK_BASE_URL
is replaced with the real service URL.

Tool definitions follow the MCP specification:
  name, description, inputSchema (JSON Schema)
"""

from __future__ import annotations

import os
import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

MOCK_BASE_URL = os.getenv("MOCK_BASE_URL", "")

# ─── Tool Registries per Agent Type ──────────────────────────

TOOLS: Dict[str, List[Dict[str, Any]]] = {
    "k8s": [
        {
            "name": "get_pod_status",
            "description": "查询 K8s namespace 中 Pod 的运行状态、资源用量和 restart 次数",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "namespace": {"type": "string", "default": "prod"},
                    "pod_name_prefix": {"type": "string", "description": "按 Pod 名称前缀过滤"},
                },
                "required": ["namespace"],
            },
            "endpoint": "/api/v1/namespaces/{namespace}/pods",
            "method": "GET",
        },
        {
            "name": "get_pod_events",
            "description": "查询 K8s Events，按 namespace 和可选 pod 名称过滤",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "namespace": {"type": "string", "default": "prod"},
                    "pod_name": {"type": "string"},
                    "minutes": {"type": "integer", "default": 30},
                },
                "required": ["namespace"],
            },
            "endpoint": "/api/v1/namespaces/{namespace}/events",
            "method": "GET",
        },
        {
            "name": "get_deploy_history",
            "description": "查询 Deployment 的最近发布历史",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "namespace": {"type": "string", "default": "prod"},
                    "deployment": {"type": "string"},
                    "count": {"type": "integer", "default": 5},
                },
                "required": ["namespace", "deployment"],
            },
            "endpoint": "/api/v1/namespaces/{namespace}/deployments/{deployment}/history",
            "method": "GET",
        },
        {
            "name": "get_resource_quota",
            "description": "查询 namespace 的资源配额和使用情况",
            "inputSchema": {
                "type": "object",
                "properties": {"namespace": {"type": "string", "default": "prod"}},
                "required": ["namespace"],
            },
            "endpoint": "/api/v1/namespaces/{namespace}/resource-quota",
            "method": "GET",
        },
        {
            "name": "get_node_metrics",
            "description": "查询 K8s 节点的 CPU/内存/磁盘指标",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "node_name": {"type": "string"},
                    "metrics": {"type": "string", "default": "cpu,mem,disk_io"},
                },
                "required": ["node_name"],
            },
            "endpoint": "/api/v1/nodes/{node_name}/metrics",
            "method": "GET",
        },
    ],
    "middleware": [
        {
            "name": "get_redis_slowlog",
            "description": "查询 Redis 慢日志，返回最近 N 条执行时间超过阈值的命令",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "top_n": {"type": "integer", "default": 20},
                    "minutes": {"type": "integer", "default": 30},
                    "instance": {"type": "string", "default": "redis-prod-01"},
                },
                "required": [],
            },
            "endpoint": "/api/v1/slowlog",
            "method": "GET",
        },
        {
            "name": "get_redis_config",
            "description": "查询 Redis 运行配置参数",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "key": {"type": "string", "description": "特定配置 key"},
                    "instance": {"type": "string", "default": "redis-prod-01"},
                },
                "required": [],
            },
            "endpoint": "/api/v1/config",
            "method": "GET",
        },
        {
            "name": "get_redis_info",
            "description": "查询 Redis INFO 输出（连接数、内存、ops 等）",
            "inputSchema": {
                "type": "object",
                "properties": {"instance": {"type": "string", "default": "redis-prod-01"}},
                "required": [],
            },
            "endpoint": "/api/v1/info",
            "method": "GET",
        },
    ],
    "network": [
        {
            "name": "ping_mesh",
            "description": "从 source 到 target 执行 ping 探测，返回 RTT 和丢包率",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "source": {"type": "string"},
                    "target": {"type": "string"},
                    "count": {"type": "integer", "default": 10},
                },
                "required": ["source", "target"],
            },
            "endpoint": "/api/v1/ping",
            "method": "POST",
        },
        {
            "name": "trace_route",
            "description": "从 source 到 target 执行路由追踪",
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
        },
        {
            "name": "check_dns",
            "description": "解析 DNS 主机名",
            "inputSchema": {
                "type": "object",
                "properties": {"hostname": {"type": "string"}},
                "required": ["hostname"],
            },
            "endpoint": "/api/v1/dns/resolve",
            "method": "GET",
        },
        {
            "name": "get_svc_mesh_metrics",
            "description": "查询服务网格 sidecar 指标（请求率、错误率、延迟）",
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
        },
    ],
    "application": [
        {
            "name": "get_trace",
            "description": "根据 trace_id 查询分布式调用链详情",
            "inputSchema": {
                "type": "object",
                "properties": {"trace_id": {"type": "string"}},
                "required": ["trace_id"],
            },
            "endpoint": "/api/v1/traces/{trace_id}",
            "method": "GET",
        },
        {
            "name": "search_logs",
            "description": "按关键字和时间范围检索应用日志",
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
        },
        {
            "name": "get_apm_metrics",
            "description": "查询 APM 时序指标（P99 延迟、错误率、请求率）",
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
        },
        {
            "name": "get_config_diff",
            "description": "对比两个版本之间的配置差异",
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
        },
    ],
}


def get_tools(agent_type: str) -> List[Dict[str, Any]]:
    """Return the MCP tool list for a given agent type."""
    return TOOLS.get(agent_type, [])


def resolve_endpoint(tool: Dict[str, Any], params: Dict[str, Any]) -> str:
    """Resolve a tool endpoint with path parameters."""
    endpoint = tool["endpoint"]
    for key, value in params.items():
        endpoint = endpoint.replace(f"{{{key}}}", str(value))
    base = MOCK_BASE_URL or "http://localhost:9002"
    return f"{base}{endpoint}"
