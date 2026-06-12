"""
HBase client wrapper — time-series metric storage.

Provides write_metrics() for bulk metric archival and query_metrics()
for time-range queries. Uses happybase (Thrift gateway) to connect.

Environment:
  HBASE_HOST: HBase Thrift server host (default: hbase)
  HBASE_PORT: HBase Thrift server port (default: 9090)
"""

from __future__ import annotations

import os
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("hbase-client")

HBASE_HOST = os.getenv("HBASE_HOST", "hbase")
HBASE_PORT = int(os.getenv("HBASE_PORT", "9090"))
METRICS_TABLE = os.getenv("HBASE_METRICS_TABLE", "industrial_metrics")

_HAS_HBASE = False
_connection = None

try:
    import happybase
    _conn = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, timeout=10000)
    # Ensure table exists
    tables = set(t.decode() if isinstance(t, bytes) else t for t in _conn.tables())
    if METRICS_TABLE.encode() in _conn.tables() or METRICS_TABLE in tables:
        logger.info("HBase connected: %s:%d, table '%s' exists", HBASE_HOST, HBASE_PORT, METRICS_TABLE)
    else:
        _conn.create_table(
            METRICS_TABLE,
            {"m": dict(max_versions=168)},  # 7 days × 24 hours of hourly snapshots
        )
        logger.info("HBase connected: %s:%d, created table '%s'", HBASE_HOST, HBASE_PORT, METRICS_TABLE)
    _connection = _conn
    _HAS_HBASE = True
except ImportError:
    logger.warning("happybase not installed — HBase storage unavailable (pip install happybase)")
except Exception as e:
    logger.warning("HBase connection failed: %s — metrics will not be archived", e)


# ─── Public API ──────────────────────────────────────────────────

def write_metrics_batch(metrics: List[Dict[str, Any]]) -> int:
    """
    Write a batch of metric data points to HBase.

    Row key format: {node_id}:{metric_type}:{timestamp_minute}
    Column family: "m" (metrics)
    Columns: value, unit, severity, tags

    Returns: number of rows written (0 if HBase unavailable)
    """
    if not _HAS_HBASE or _connection is None:
        return 0

    try:
        table = _connection.table(METRICS_TABLE)
        batch = table.batch()
        count = 0

        for m in metrics:
            node_id = m.get("node_id", "unknown")
            metric_type = m.get("metric_type", "unknown")
            ts = m.get("timestamp", 0)
            # Row key: node_id:metric_type:minute_bucket
            minute_bucket = ts // 60000 if ts else int(datetime.now(timezone.utc).timestamp() // 60)
            row_key = f"{node_id}:{metric_type}:{minute_bucket}".encode()

            data = {
                b"m:value": str(m.get("metric_value", 0)).encode(),
                b"m:unit": str(m.get("unit", "")).encode(),
                b"m:severity": str(m.get("severity", "normal")).encode(),
                b"m:node_type": str(m.get("node_type", "")).encode(),
                b"m:line_profile": str(m.get("line_profile", "general")).encode(),
                b"m:ts": str(ts).encode(),
            }
            batch.put(row_key, data)
            count += 1

        batch.send()
        if count > 0:
            logger.debug("HBase: wrote %d metric rows to %s", count, METRICS_TABLE)
        return count
    except Exception as e:
        logger.warning("HBase write failed: %s", e)
        return 0


def query_metrics(
    node_id: str,
    metric_type: str = "",
    start_minute: int = 0,
    end_minute: int = 0,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """
    Query historical metrics from HBase.

    Args:
        node_id: Node ID to query
        metric_type: Optional metric type filter (prefix match)
        start_minute: Start of time range (epoch minute)
        end_minute: End of time range (epoch minute)
        limit: Max rows to return

    Returns: list of metric dicts (empty if HBase unavailable)
    """
    if not _HAS_HBASE or _connection is None:
        return []

    try:
        table = _connection.table(METRICS_TABLE)
        prefix = f"{node_id}:{metric_type}" if metric_type else f"{node_id}:"
        start_key = f"{prefix}:{start_minute}".encode() if start_minute else f"{prefix}:".encode()
        end_key = f"{prefix}:{end_minute}z".encode() if end_minute else f"{prefix};".encode()

        results = []
        for key, data in table.scan(row_start=start_key, row_stop=end_key, limit=limit):
            row_key = key.decode() if isinstance(key, bytes) else key
            results.append({
                "row_key": row_key,
                "value": _decode(data.get(b"m:value", b"")),
                "unit": _decode(data.get(b"m:unit", b"")),
                "severity": _decode(data.get(b"m:severity", b"normal")),
                "node_type": _decode(data.get(b"m:node_type", b"")),
                "ts": _decode(data.get(b"m:ts", b"0")),
            })

        logger.debug("HBase: queried %s* → %d rows", prefix, len(results))
        return results
    except Exception as e:
        logger.warning("HBase query failed: %s", e)
        return []


def get_table_stats() -> Dict[str, Any]:
    """Return table statistics for monitoring."""
    if not _HAS_HBASE or _connection is None:
        return {"status": "unavailable", "host": HBASE_HOST, "port": HBASE_PORT}

    try:
        table = _connection.table(METRICS_TABLE)
        regions = len(table.regions()) if hasattr(table, 'regions') else 1
        return {
            "status": "connected",
            "host": HBASE_HOST,
            "port": HBASE_PORT,
            "table": METRICS_TABLE,
            "regions": regions,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ─── Helpers ────────────────────────────────────────────────────

def _decode(val) -> str:
    return val.decode() if isinstance(val, bytes) else str(val)
