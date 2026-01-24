"""
Kafka producer for indicator events.
"""

import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


class IndicatorProducer:
    """Produces indicator events to Kafka."""

    def __init__(self, brokers: list[str], topic: str):
        """
        Initialize the Kafka producer.

        Args:
            brokers: List of Kafka broker addresses.
            topic: Topic to publish indicator events to.
        """
        self.brokers = brokers
        self.topic = topic
        self._producer: Optional[KafkaProducer] = None

    def connect(self) -> bool:
        """
        Connect to Kafka brokers.

        Returns:
            True if connection successful, False otherwise.
        """
        try:
            logger.info(f"Connecting to Kafka brokers: {self.brokers}")

            self._producer = KafkaProducer(
                bootstrap_servers=self.brokers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",  # Wait for all replicas to acknowledge
                retries=3,
                retry_backoff_ms=1000,
            )

            logger.info("Successfully connected to Kafka producer")
            return True

        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False

    def publish_indicator(
        self,
        symbol: str,
        timestamp: datetime,
        indicators: Dict[str, Any],
        data_quality: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Publish an indicator event to Kafka.

        Args:
            symbol: Stock symbol.
            timestamp: Timestamp of the indicators.
            indicators: Dictionary of indicator values.
            data_quality: Optional data quality metadata from freshness check.

        Returns:
            True if published successfully, False otherwise.
        """
        if not self._producer:
            raise RuntimeError("Kafka producer not connected")

        try:
            event = {
                "event_type": "INDICATOR_UPDATE",
                "source": "analytics-service",
                "timestamp": timestamp.isoformat() + "Z",
                "schema_version": "1.1",  # Bumped version for data_quality field
                "data": {
                    "symbol": symbol,
                    "time": timestamp.isoformat() + "Z",
                    "indicators": indicators,
                },
            }

            # Include data quality metadata if available
            if data_quality:
                event["data"]["data_quality"] = data_quality

            # Use symbol as key for partitioning
            future = self._producer.send(
                self.topic,
                key=symbol,
                value=event,
            )

            # Wait for send to complete (with timeout)
            record_metadata = future.get(timeout=10)

            logger.debug(
                f"Published indicator event for {symbol} to "
                f"{record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}"
            )
            return True

        except KafkaError as e:
            logger.error(f"Failed to publish indicator event for {symbol}: {e}")
            return False

    def close(self):
        """Close the Kafka producer connection."""
        if self._producer:
            try:
                self._producer.flush()
                self._producer.close()
                logger.info("Kafka producer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")
