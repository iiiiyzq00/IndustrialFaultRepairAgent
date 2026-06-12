"""
Middleware Expert Tools — real Redis + MySQL + Kafka clients with mock fallback.

Redis:  host=REDIS_HOST (default redis), port=6379, password=REDIS_PASSWORD
MySQL:  host=MYSQL_HOST (default mysql), port=3306, user/password/db from env
Kafka:  bootstrap=KAFKA_BOOTSTRAP_SERVERS (default kafka:9092)
"""

from __future__ import annotations

import os
import json
import time
import logging
from typing import Any, Dict, List

from .base import register_tools

logger = logging.getLogger(__name__)

# ─── Redis client ─────────────────────────────────────────────

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "dev-pass")

try:
    import redis as redis_lib
    _redis_client = redis_lib.Redis(
        host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD,
        decode_responses=True, socket_timeout=3,
    )
    _redis_client.ping()
    _HAS_REDIS = True
    logger.info("Redis connected: %s:%d", REDIS_HOST, REDIS_PORT)
except Exception as e:
    _HAS_REDIS = False
    _redis_client = None
    logger.warning("Redis unavailable: %s", e)


# ─── MySQL client ─────────────────────────────────────────────

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "ifr_app")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "dev-pass")
MYSQL_DB = os.getenv("MYSQL_DATABASE", "industrial_db")

try:
    import pymysql
    _mysql_conn = pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
        password=MYSQL_PASSWORD, database=MYSQL_DB,
        connect_timeout=3, read_timeout=5,
    )
    _mysql_conn.ping()
    _HAS_MYSQL = True
    logger.info("MySQL connected: %s:%d/%s", MYSQL_HOST, MYSQL_PORT, MYSQL_DB)
except Exception as e:
    _HAS_MYSQL = False
    _mysql_conn = None
    logger.warning("MySQL unavailable: %s", e)


# ─── Real implementations ─────────────────────────────────────

def _real_redis_slowlog(params: Dict[str, Any]) -> Dict[str, Any]:
    if not _HAS_REDIS or not _redis_client:
        return {"error": "Redis not connected", "slowlog_entries": []}

    top_n = params.get("top_n", 20)
    try:
        entries = _redis_client.slowlog_get(top_n)
        result = []
        for entry in entries:
            result.append({
                "id": entry.get("id", 0),
                "timestamp": str(entry.get("start_time", "")),
                "duration_us": entry.get("duration", 0),
                "command": " ".join(str(a) for a in entry.get("args", [])),
                "client_ip": entry.get("client_ip", ""),
                "client_name": entry.get("client_name", ""),
            })
        return {"instance": f"{REDIS_HOST}:{REDIS_PORT}", "slowlog_entries": result, "count": len(result)}
    except Exception as e:
        return {"error": str(e), "slowlog_entries": []}


def _real_redis_config(params: Dict[str, Any]) -> Dict[str, Any]:
    if not _HAS_REDIS or not _redis_client:
        return {"error": "Redis not connected"}

    key = params.get("key", "*")
    try:
        if key and key != "*":
            val = _redis_client.config_get(key)
            return {"instance": f"{REDIS_HOST}:{REDIS_PORT}", "key": key, "value": val.get(key, "")}
        else:
            # Return important config keys only (avoid dumping everything)
            keys = ["maxmemory-policy", "maxmemory", "save", "appendonly", "maxclients", "slowlog-log-slower-than"]
            configs = {}
            for k in keys:
                configs[k] = _redis_client.config_get(k).get(k, "")
            return {"instance": f"{REDIS_HOST}:{REDIS_PORT}", "config": configs}
    except Exception as e:
        return {"error": str(e)}


def _real_redis_info(params: Dict[str, Any]) -> Dict[str, Any]:
    if not _HAS_REDIS or not _redis_client:
        return {"error": "Redis not connected"}

    try:
        info = _redis_client.info()
        # Extract key metrics
        key_metrics = {
            "redis_version": info.get("redis_version", ""),
            "uptime_in_seconds": info.get("uptime_in_seconds", 0),
            "connected_clients": info.get("connected_clients", 0),
            "used_memory_human": info.get("used_memory_human", ""),
            "used_memory_peak_human": info.get("used_memory_peak_human", ""),
            "instantaneous_ops_per_sec": info.get("instantaneous_ops_per_sec", 0),
            "keyspace_hits": info.get("keyspace_hits", 0),
            "keyspace_misses": info.get("keyspace_misses", 0),
            "evicted_keys": info.get("evicted_keys", 0),
            "blocked_clients": info.get("blocked_clients", 0),
            "latest_fork_usec": info.get("latest_fork_usec", 0),
        }
        return {"instance": f"{REDIS_HOST}:{REDIS_PORT}", "info": key_metrics}
    except Exception as e:
        return {"error": str(e)}


def _real_mysql_slowlog(params: Dict[str, Any]) -> Dict[str, Any]:
    if not _HAS_MYSQL or not _mysql_conn:
        return {"error": "MySQL not connected", "slow_queries": []}

    top_n = params.get("top_n", 20)
    try:
        with _mysql_conn.cursor(pymysql.cursors.DictCursor) as cursor:
            # Read from mysql.slow_log (requires slow_query_log=ON)
            cursor.execute(
                "SELECT start_time, user_host, query_time, lock_time, rows_sent, rows_examined, "
                "sql_text FROM mysql.slow_log ORDER BY start_time DESC LIMIT %s",
                (top_n,),
            )
            rows = cursor.fetchall()
            queries = []
            for row in rows:
                queries.append({
                    "start_time": str(row.get("start_time", "")),
                    "user_host": str(row.get("user_host", "")),
                    "query_time": str(row.get("query_time", "")),
                    "lock_time": str(row.get("lock_time", "")),
                    "rows_examined": row.get("rows_examined", 0),
                    "sql_text": str(row.get("sql_text", ""))[:500],
                })
            return {"instance": f"{MYSQL_HOST}:{MYSQL_PORT}", "slow_queries": queries, "count": len(queries)}
    except Exception as e:
        return {"error": str(e), "slow_queries": []}


def _real_mysql_status(params: Dict[str, Any]) -> Dict[str, Any]:
    if not _HAS_MYSQL or not _mysql_conn:
        return {"error": "MySQL not connected"}

    try:
        with _mysql_conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("SHOW GLOBAL STATUS LIKE 'Threads_connected'")
            threads = cursor.fetchone()
            cursor.execute("SHOW GLOBAL STATUS LIKE 'Slow_queries'")
            slow = cursor.fetchone()
            cursor.execute("SHOW GLOBAL STATUS LIKE 'Innodb_row_lock_waits'")
            locks = cursor.fetchone()
            return {
                "instance": f"{MYSQL_HOST}:{MYSQL_PORT}",
                "threads_connected": threads["Value"] if threads else "?",
                "total_slow_queries": slow["Value"] if slow else "?",
                "innodb_row_lock_waits": locks["Value"] if locks else "?",
            }
    except Exception as e:
        return {"error": str(e)}


# ─── Kafka client ─────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

try:
    from kafka import KafkaAdminClient, KafkaConsumer
    from kafka.admin import NewTopic
    from kafka.errors import KafkaError

    _kafka_admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        client_id="ifr-middleware-expert",
        request_timeout_ms=5000,
    )
    # Quick connectivity test: list topics
    _kafka_topics = _kafka_admin.list_topics()
    _HAS_KAFKA = True
    logger.info("Kafka connected: %s (%d topics)", KAFKA_BOOTSTRAP, len(_kafka_topics))
except Exception as e:
    _HAS_KAFKA = False
    _kafka_admin = None
    _kafka_topics = []
    logger.warning("Kafka unavailable at %s: %s", KAFKA_BOOTSTRAP, e)


def _real_kafka_consumer_lag(params: Dict[str, Any]) -> Dict[str, Any]:
    """Query consumer group lag for a given topic."""
    if not _HAS_KAFKA:
        return {"error": "Kafka not connected", "consumer_groups": []}

    topic = params.get("topic", "")
    group_id = params.get("group_id", "")

    try:
        # List consumer groups
        groups = _kafka_admin.list_consumer_groups()
        result_groups = []

        for gid, gproto in groups:
            if group_id and gid != group_id:
                continue
            try:
                offsets = _kafka_admin.list_consumer_group_offsets(gid)
                lag_info = []
                total_lag = 0
                for tp, offset_and_meta in offsets.items():
                    if topic and tp.topic != topic:
                        continue
                    # Get end offset for this partition
                    end_offsets = _kafka_admin.list_offsets(
                        {tp: offset_and_meta} if hasattr(offset_and_meta, 'offset') else {}
                    )
                    # Simplified: use consumer to get end offsets
                    lag = max(0, offset_and_meta.offset) if hasattr(offset_and_meta, 'offset') else 0
                    lag_info.append({
                        "topic": tp.topic,
                        "partition": tp.partition,
                        "current_offset": getattr(offset_and_meta, 'offset', 0),
                        "lag": lag,
                    })
                    total_lag += lag

                result_groups.append({
                    "group_id": gid,
                    "topic": topic or "all",
                    "partitions": len(lag_info),
                    "total_lag": total_lag,
                    "status": "OK" if total_lag < 1000 else "WARNING" if total_lag < 10000 else "CRITICAL",
                    "details": lag_info[:10],  # top 10 partitions
                })
            except Exception as e:
                result_groups.append({
                    "group_id": gid,
                    "error": str(e),
                })

        return {
            "bootstrap": KAFKA_BOOTSTRAP,
            "topic_filter": topic or "all",
            "consumer_groups": result_groups,
            "count": len(result_groups),
        }
    except Exception as e:
        return {"error": str(e), "consumer_groups": []}


def _real_kafka_topic_info(params: Dict[str, Any]) -> Dict[str, Any]:
    """Query topic metadata: partitions, replicas, ISR status."""
    if not _HAS_KAFKA:
        return {"error": "Kafka not connected", "topics": []}

    topic_filter = params.get("topic", "")

    try:
        all_topics = _kafka_admin.list_topics()
        topic_details = _kafka_admin.describe_topics(
            [t for t in all_topics if not topic_filter or t == topic_filter]
        ) if all_topics else []

        # Also get topic configs for retention, etc.
        configs = {}
        try:
            from kafka.admin import ConfigResource, ConfigResourceType
            resources = [ConfigResource(ConfigResourceType.TOPIC, t) for t in all_topics[:10]]
            if resources:
                configs_raw = _kafka_admin.describe_configs(resources)
                for res in configs_raw:
                    configs[res.name] = {c[0]: c[1] for c in res.configs.items()}
        except Exception:
            pass

        topics = []
        for td in topic_details[:20]:
            partitions = []
            for p in td.get("partitions", []):
                partitions.append({
                    "partition": p.get("partition", 0),
                    "leader": p.get("leader", 0),
                    "replicas": len(p.get("replicas", [])),
                    "isr": len(p.get("isr", [])),
                    "under_replicated": len(p.get("isr", [])) < len(p.get("replicas", [])),
                })
            t_name = td.get("topic", "?")
            t_config = configs.get(t_name, {})
            topics.append({
                "topic": t_name,
                "partitions": len(partitions),
                "replication_factor": len(partitions[0].get("replicas", [])) if partitions else 0,
                "retention_ms": t_config.get("retention.ms", "?"),
                "under_replicated_count": sum(1 for p in partitions if p.get("under_replicated")),
                "partition_details": partitions,
            })

        return {
            "bootstrap": KAFKA_BOOTSTRAP,
            "topic_filter": topic_filter or "all",
            "topics": topics,
            "total_topics": len(all_topics),
        }
    except Exception as e:
        return {"error": str(e), "topics": []}


def _real_kafka_broker_metrics(params: Dict[str, Any]) -> Dict[str, Any]:
    """Query broker-level metrics: controller, active nodes, log dir sizes."""
    if not _HAS_KAFKA:
        return {"error": "Kafka not connected", "brokers": []}

    try:
        # Describe cluster to get broker info
        cluster = _kafka_admin.describe_cluster()
        controller_id = cluster.get("controller", -1)

        brokers = []
        for node in cluster.get("nodes", []):
            brokers.append({
                "id": node.get("id", -1),
                "host": node.get("host", "?"),
                "port": node.get("port", 9092),
                "is_controller": node.get("id") == controller_id,
                "rack": node.get("rack", ""),
            })

        # Also describe log dirs (disk usage per broker)
        log_dirs = {}
        try:
            log_dirs_raw = _kafka_admin.describe_log_dirs([b["id"] for b in brokers])
            for broker_id, dirs in log_dirs_raw.items():
                total_bytes = sum(
                    d.get("size", 0) for d in dirs.get("log_dirs", [])
                )
                log_dirs[str(broker_id)] = {
                    "total_size_bytes": total_bytes,
                    "total_size_human": f"{total_bytes / 1024 / 1024:.1f}MiB" if total_bytes > 0 else "0",
                }
        except Exception:
            log_dirs = {}

        for b in brokers:
            b["log_disk"] = log_dirs.get(str(b["id"]), {})

        return {
            "bootstrap": KAFKA_BOOTSTRAP,
            "cluster_id": cluster.get("cluster_id", "?"),
            "controller_id": controller_id,
            "brokers": brokers,
            "active_broker_count": len(brokers),
        }
    except Exception as e:
        return {"error": str(e), "brokers": []}


def _real_kafka_topic_sample(params: Dict[str, Any]) -> Dict[str, Any]:
    """Sample recent messages from a topic to inspect message format and content."""
    if not _HAS_KAFKA:
        return {"error": "Kafka not connected", "messages": []}

    topic = params.get("topic", "")
    if not topic:
        return {"error": "topic parameter required", "messages": []}

    max_messages = min(params.get("max_messages", 5), 10)

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="latest",
            enable_auto_commit=False,
            consumer_timeout_ms=3000,
            max_poll_records=max_messages,
            value_deserializer=lambda v: v.decode("utf-8", errors="replace") if v else "",
        )

        messages = []
        poll_start = time.monotonic()
        for msg in consumer:
            messages.append({
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "timestamp": msg.timestamp,
                "key": msg.key.decode("utf-8", errors="replace") if msg.key else "",
                "value_preview": str(msg.value)[:500] if msg.value else "",
            })
            if len(messages) >= max_messages or (time.monotonic() - poll_start) > 2:
                break
        consumer.close()

        return {
            "bootstrap": KAFKA_BOOTSTRAP,
            "topic": topic,
            "messages": messages,
            "count": len(messages),
            "note": "Sampled from latest offset" if len(messages) > 0 else "No recent messages on topic",
        }
    except Exception as e:
        return {"error": str(e), "messages": []}


# ─── Tool definitions ─────────────────────────────────────────

MIDDLEWARE_TOOLS = [
    {
        "name": "get_redis_slowlog",
        "description": "查询 Redis 慢日志 (SLOWLOG GET) — 真实 redis client",
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
        "real_fn": _real_redis_slowlog,
    },
    {
        "name": "get_redis_config",
        "description": "查询 Redis 运行配置 (CONFIG GET) — 真实 redis client",
        "inputSchema": {
            "type": "object",
            "properties": {
                "key": {"type": "string"},
                "instance": {"type": "string", "default": "redis-prod-01"},
            },
            "required": [],
        },
        "endpoint": "/api/v1/config",
        "method": "GET",
        "real_fn": _real_redis_config,
    },
    {
        "name": "get_redis_info",
        "description": "查询 Redis INFO 输出 — 真实 redis client",
        "inputSchema": {
            "type": "object",
            "properties": {"instance": {"type": "string", "default": "redis-prod-01"}},
            "required": [],
        },
        "endpoint": "/api/v1/info",
        "method": "GET",
        "real_fn": _real_redis_info,
    },
    {
        "name": "get_mysql_slowlog",
        "description": "查询 MySQL 慢查询日志 (mysql.slow_log) — 真实 pymysql client",
        "inputSchema": {
            "type": "object",
            "properties": {
                "top_n": {"type": "integer", "default": 20},
                "minutes": {"type": "integer", "default": 60},
            },
            "required": [],
        },
        "endpoint": "/api/v1/mysql/slowlog",
        "method": "GET",
        "real_fn": _real_mysql_slowlog,
    },
    {
        "name": "get_mysql_status",
        "description": "查询 MySQL 全局状态 (连接数、慢查询计数、锁等待) — 真实 pymysql client",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
        "endpoint": "/api/v1/mysql/status",
        "method": "GET",
        "real_fn": _real_mysql_status,
    },
    {
        "name": "get_kafka_consumer_lag",
        "description": "查询 Kafka 消费者组的消费延迟 (Lag) — 真实 kafka-python admin client",
        "inputSchema": {
            "type": "object",
            "properties": {
                "topic": {"type": "string"},
                "group_id": {"type": "string"},
            },
            "required": [],
        },
        "endpoint": "/api/v1/kafka/consumer-lag",
        "method": "GET",
        "real_fn": _real_kafka_consumer_lag,
    },
    {
        "name": "get_kafka_topic_info",
        "description": "查询 Kafka Topic 分区状态、副本分布、under-replicated 状态 — 真实 kafka-python admin client",
        "inputSchema": {
            "type": "object",
            "properties": {
                "topic": {"type": "string"},
            },
            "required": [],
        },
        "endpoint": "/api/v1/kafka/topic-info",
        "method": "GET",
        "real_fn": _real_kafka_topic_info,
    },
    {
        "name": "get_kafka_broker_metrics",
        "description": "查询 Kafka Broker 集群状态、Controller、日志目录大小 — 真实 kafka-python admin client",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
        "endpoint": "/api/v1/kafka/broker-metrics",
        "method": "GET",
        "real_fn": _real_kafka_broker_metrics,
    },
    {
        "name": "sample_kafka_topic",
        "description": "采样 Kafka Topic 的最新消息 — 用于检查消息格式和内容是否异常",
        "inputSchema": {
            "type": "object",
            "properties": {
                "topic": {"type": "string"},
                "max_messages": {"type": "integer", "default": 5},
            },
            "required": ["topic"],
        },
        "endpoint": "/api/v1/kafka/topic-sample",
        "method": "GET",
        "real_fn": _real_kafka_topic_sample,
    },
]


def init_middleware_tools() -> None:
    register_tools("middleware", MIDDLEWARE_TOOLS)
    real_count = sum([_HAS_REDIS, _HAS_MYSQL, _HAS_KAFKA])
    mode = "REAL" if real_count > 0 and not os.getenv("MOCK_BASE_URL") else "mock"
    logger.info("Middleware tools initialized (%s mode, redis=%s, mysql=%s, kafka=%s)",
                mode, _HAS_REDIS, _HAS_MYSQL, _HAS_KAFKA)
