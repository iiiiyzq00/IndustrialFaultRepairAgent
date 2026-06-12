"""
Kafka producer wrapper.

Sends industrial-metrics and industrial-alerts to Kafka topics.
Uses kafka-python for simplicity (no C dependencies).
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logger = logging.getLogger(__name__)


class MetricProducer:
    """Sends metric data points to Kafka."""

    def __init__(
        self,
        bootstrap_servers: str,
        metrics_topic: str = "industrial-metrics",
        alerts_topic: str = "industrial-alerts",
        max_retries: int = 10,
        retry_backoff: float = 2.0,
    ):
        self.metrics_topic = metrics_topic
        self.alerts_topic = alerts_topic

        for attempt in range(1, max_retries + 1):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks=1,
                    compression_type="gzip",
                    max_block_ms=10000,
                )
                logger.info("Connected to Kafka at %s", bootstrap_servers)
                break
            except NoBrokersAvailable:
                if attempt == max_retries:
                    raise
                logger.warning("Kafka not ready (attempt %d/%d), retrying in %.1fs...",
                               attempt, max_retries, retry_backoff)
                time.sleep(retry_backoff)

    def send_metrics(self, points: List[Dict[str, Any]]) -> None:
        """Send a batch of metric data points."""
        for point in points:
            key = f"{point['node_id']}:{point['metric_type']}"
            self.producer.send(self.metrics_topic, key=key, value=point)
        # Don't flush per tick — let the producer batch internally

    def send_alert(self, alert: Dict[str, Any]) -> None:
        """Send a single alert event."""
        key = alert.get("node_id", "unknown")
        self.producer.send(self.alerts_topic, key=key, value=alert)

    def flush(self) -> None:
        self.producer.flush()

    def close(self) -> None:
        self.producer.close(timeout=5)
