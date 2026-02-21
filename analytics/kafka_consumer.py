"""
Kafka consumer for quote events.
"""

import json
import logging
import time
from typing import Callable, Optional
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

# Reconnection constants
MAX_RECONNECT_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 2


class QuoteConsumer:
    """Consumes quote events from Kafka."""

    def __init__(
        self,
        brokers: list[str],
        topic: str,
        consumer_group: str,
        message_handler: Callable[[dict], None],
    ):
        """
        Initialize the Kafka consumer.

        Args:
            brokers: List of Kafka broker addresses.
            topic: Topic to consume from.
            consumer_group: Consumer group ID.
            message_handler: Function to call for each message.
        """
        self.brokers = brokers
        self.topic = topic
        self.consumer_group = consumer_group
        self.message_handler = message_handler
        self._consumer: Optional[KafkaConsumer] = None
        self._running = False

    def connect(self) -> bool:
        """
        Connect to Kafka brokers.

        Returns:
            True if connection successful, False otherwise.
        """
        try:
            logger.info(f"Connecting to Kafka brokers: {self.brokers}")
            logger.info(f"Topic: {self.topic}, Consumer Group: {self.consumer_group}")

            self._consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.brokers,
                group_id=self.consumer_group,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                session_timeout_ms=30000,
                request_timeout_ms=40000,
            )

            logger.info("Successfully connected to Kafka consumer")
            return True

        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False

    def _consume_loop(self):
        """Run the inner consume loop. Raises on unrecoverable errors."""
        while self._running:
            messages = self._consumer.poll(timeout_ms=1000)
            if not messages:
                continue

            for tp, records in messages.items():
                for message in records:
                    try:
                        try:
                            event = json.loads(message.value.decode('utf-8'))
                        except (json.JSONDecodeError, UnicodeDecodeError) as e:
                            logger.error(f'Failed to decode message: {e}')
                            continue
                        self.message_handler(event)
                    except Exception as e:
                        logger.error(f"Error processing message: {e}", exc_info=True)

            try:
                self._consumer.commit()
            except KafkaError as e:
                logger.error(f"Failed to commit offsets: {e}")

    def _reconnect(self) -> bool:
        """
        Attempt to reconnect to Kafka with exponential backoff.

        Returns:
            True if reconnection succeeded, False if all retries exhausted.
        """
        backoff = INITIAL_BACKOFF_SECONDS
        for attempt in range(1, MAX_RECONNECT_RETRIES + 1):
            if not self._running:
                return False
            logger.warning(
                f"Kafka reconnect attempt {attempt}/{MAX_RECONNECT_RETRIES} "
                f"in {backoff}s..."
            )
            time.sleep(backoff)
            if not self._running:
                return False
            # Close stale consumer before reconnecting
            if self._consumer:
                try:
                    self._consumer.close()
                except Exception:
                    pass
                self._consumer = None
            if self.connect():
                logger.info(f"Kafka reconnected on attempt {attempt}")
                return True
            backoff *= 2
        return False

    def start(self):
        """Start consuming messages with automatic reconnection."""
        if not self._consumer:
            raise RuntimeError("Consumer not connected. Call connect() first.")

        logger.info(f"Starting to consume messages from topic: {self.topic}")
        self._running = True

        try:
            while self._running:
                try:
                    self._consume_loop()
                except KeyboardInterrupt:
                    logger.info("Consumer interrupted by user")
                    return
                except Exception as e:
                    if not self._running:
                        return
                    logger.error(f"Consumer error: {e}", exc_info=True)
                    if not self._reconnect():
                        logger.error(
                            f"Failed to reconnect after {MAX_RECONNECT_RETRIES} "
                            f"attempts, giving up"
                        )
                        raise
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")

    def close(self):
        """Close the Kafka consumer."""
        self._running = False
        if self._consumer:
            try:
                self._consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")
