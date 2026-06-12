"""
Middleware self-healing actions: Redis config, MySQL failover, Kafka operations.
"""

from __future__ import annotations

import os
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "dev-pass")
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "dev-pass")
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"


def redis_config_set(params: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
    """
    Low-risk Redis config changes. E.g.:
      CONFIG SET maxmemory-policy allkeys-lru
      CONFIG SET slowlog-log-slower-than 20000
    """
    key = params.get("key", "")
    value = params.get("value", "")

    if not key:
        return {"success": False, "error": "Missing 'key' parameter"}

    if dry_run or DRY_RUN:
        logger.info("[DRY RUN] redis_config_set %s=%s", key, value)
        return {"success": True, "dry_run": True, "key": key, "value": value}

    try:
        import redis as redis_lib
        client = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD,
                                 decode_responses=True, socket_timeout=5)
        old_value = client.config_get(key).get(key, "")
        client.config_set(key, value)
        new_value = client.config_get(key).get(key, "")

        logger.info("Redis CONFIG SET %s: %s → %s", key, old_value, new_value)
        return {"success": True, "key": key, "old_value": old_value, "new_value": new_value}
    except Exception as e:
        logger.error("Redis CONFIG SET failed: %s", e)
        return {"success": False, "error": str(e)}


def mysql_failover(params: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
    """
    HIGH RISK — requires HITL dual approval.
    Execute MySQL master-slave failover.
    """
    action = params.get("action", "promote_replica")
    target = params.get("target", "")

    if dry_run or DRY_RUN:
        logger.info("[DRY RUN] mysql_failover %s → %s", action, target)
        return {"success": True, "dry_run": True, "action": action, "target": target}

    try:
        import pymysql
        conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
                               password=MYSQL_PASSWORD, connect_timeout=5)

        with conn.cursor() as cursor:
            if action == "promote_replica":
                cursor.execute("STOP SLAVE;")
                cursor.execute("RESET SLAVE ALL;")
                cursor.execute("SET GLOBAL read_only = OFF;")
                logger.warning("MySQL failover executed: promoted %s to master", MYSQL_HOST)
                return {"success": True, "action": "promote_replica", "host": MYSQL_HOST}

            elif action == "set_readonly":
                cursor.execute("SET GLOBAL read_only = ON;")
                return {"success": True, "action": "set_readonly"}

        conn.close()
    except Exception as e:
        logger.error("MySQL failover failed: %s", e)
        return {"success": False, "error": str(e)}


def mysql_kill_query(params: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
    """Kill a long-running MySQL query by ID."""
    query_id = params.get("query_id", params.get("target", ""))

    if dry_run or DRY_RUN:
        return {"success": True, "dry_run": True, "query_id": query_id}

    try:
        import pymysql
        conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
                               password=MYSQL_PASSWORD, connect_timeout=5)
        with conn.cursor() as cursor:
            cursor.execute(f"KILL {int(query_id)}")
        conn.close()
        logger.info("Killed MySQL query %s", query_id)
        return {"success": True, "killed_query_id": query_id}
    except Exception as e:
        return {"success": False, "error": str(e)}
