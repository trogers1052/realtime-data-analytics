"""
Redis client for reading data freshness status.
"""

import json
import logging
from typing import Dict, Optional

import redis

logger = logging.getLogger(__name__)


# Redis key constants (must match market-data-ingestion)
INGESTION_STATUS_KEY = "ingestion:status"
SYMBOL_FRESHNESS_KEY_PREFIX = "ingestion:symbol:"


class IngestionStatus:
    """Overall ingestion service status."""

    def __init__(self, data: dict):
        self.status = data.get("status", "unknown")
        self.is_market_hours = data.get("is_market_hours", False)
        self.last_heartbeat = data.get("last_heartbeat", "")
        self.symbols_tracked = data.get("symbols_tracked", 0)
        self.bars_received = data.get("bars_received", 0)
        self.bars_inserted = data.get("bars_inserted", 0)
        self.backfill_pending = data.get("backfill_pending", 0)
        self.error_message = data.get("error_message", "")

    def is_healthy(self) -> bool:
        """Check if the ingestion service is healthy."""
        return self.status == "running" and not self.error_message


class SymbolFreshness:
    """Data freshness status for a specific symbol."""

    def __init__(self, data: dict):
        self.symbol = data.get("symbol", "")
        self.status = data.get("status", "unknown")  # current, stale, backfilling, error, no_data
        self.last_bar_time = data.get("last_bar_time", "")
        self.bar_count = data.get("bar_count", 0)
        self.backfill_status = data.get("backfill_status", "")
        self.backfill_start = data.get("backfill_start", "")
        self.backfill_end = data.get("backfill_end", "")
        self.coverage_percent = data.get("coverage_percent", 0.0)
        self.gaps_detected = data.get("gaps_detected", 0)
        self.last_updated = data.get("last_updated", "")
        self.minutes_stale = data.get("minutes_stale", 0)
        self.is_ready = data.get("is_ready", False)

    def to_dict(self) -> dict:
        """Convert to dictionary for inclusion in events."""
        return {
            "status": self.status,
            "is_ready": self.is_ready,
            "bar_count": self.bar_count,
            "minutes_stale": self.minutes_stale,
            "backfill_status": self.backfill_status,
            "coverage_percent": self.coverage_percent,
            "gaps_detected": self.gaps_detected,
        }


class FreshnessClient:
    """Client for checking data freshness from Redis."""

    def __init__(self, host: str, port: int, password: str = "", db: int = 0):
        """
        Initialize the Redis client.

        Args:
            host: Redis host.
            port: Redis port.
            password: Redis password (optional).
            db: Redis database number.
        """
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self._client: Optional[redis.Redis] = None

    def connect(self) -> bool:
        """
        Connect to Redis.

        Returns:
            True if connection successful, False otherwise.
        """
        try:
            logger.info(f"Connecting to Redis at {self.host}:{self.port}")

            self._client = redis.Redis(
                host=self.host,
                port=self.port,
                password=self.password if self.password else None,
                db=self.db,
                decode_responses=True,
            )

            # Test connection
            self._client.ping()
            logger.info("Connected to Redis for freshness checks")
            return True

        except redis.RedisError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return False

    def close(self):
        """Close the Redis connection."""
        if self._client:
            self._client.close()
            logger.info("Redis connection closed")

    def get_ingestion_status(self) -> Optional[IngestionStatus]:
        """
        Get the overall ingestion service status.

        Returns:
            IngestionStatus or None if not available.
        """
        if not self._client:
            return None

        try:
            data = self._client.get(INGESTION_STATUS_KEY)
            if data:
                return IngestionStatus(json.loads(data))
            return None
        except Exception as e:
            logger.warning(f"Failed to get ingestion status: {e}")
            return None

    def get_symbol_freshness(self, symbol: str) -> Optional[SymbolFreshness]:
        """
        Get data freshness status for a specific symbol.

        Args:
            symbol: Stock symbol.

        Returns:
            SymbolFreshness or None if not available.
        """
        if not self._client:
            return None

        try:
            key = f"{SYMBOL_FRESHNESS_KEY_PREFIX}{symbol}:freshness"
            data = self._client.get(key)
            if data:
                return SymbolFreshness(json.loads(data))
            return None
        except Exception as e:
            logger.warning(f"Failed to get freshness for {symbol}: {e}")
            return None

    def get_all_symbol_freshness(self) -> Dict[str, SymbolFreshness]:
        """
        Get freshness data for all tracked symbols.

        Returns:
            Dictionary mapping symbol to SymbolFreshness.
        """
        if not self._client:
            return {}

        try:
            pattern = f"{SYMBOL_FRESHNESS_KEY_PREFIX}*:freshness"
            keys = self._client.keys(pattern)

            result = {}
            for key in keys:
                data = self._client.get(key)
                if data:
                    freshness = SymbolFreshness(json.loads(data))
                    result[freshness.symbol] = freshness

            return result
        except Exception as e:
            logger.warning(f"Failed to get all freshness data: {e}")
            return {}

    def is_symbol_ready(self, symbol: str) -> tuple[bool, str]:
        """
        Check if a symbol's data is ready for analytics.

        Args:
            symbol: Stock symbol.

        Returns:
            Tuple of (is_ready, reason_if_not_ready)
        """
        freshness = self.get_symbol_freshness(symbol)

        if freshness is None:
            return False, "no freshness data available"

        if not freshness.is_ready:
            reasons = []
            if freshness.backfill_status != "completed":
                reasons.append(f"backfill {freshness.backfill_status}")
            if freshness.bar_count < 200:
                reasons.append(f"only {freshness.bar_count} bars")
            if freshness.minutes_stale > 10:
                reasons.append(f"{freshness.minutes_stale} minutes stale")
            if freshness.status == "error":
                reasons.append("data error")

            return False, "; ".join(reasons) if reasons else "data not ready"

        return True, ""

    def is_ingestion_healthy(self) -> tuple[bool, str]:
        """
        Check if the ingestion service is healthy.

        Returns:
            Tuple of (is_healthy, reason_if_not_healthy)
        """
        status = self.get_ingestion_status()

        if status is None:
            return False, "ingestion status not available (service may be down)"

        if not status.is_healthy():
            return False, f"ingestion status: {status.status}, error: {status.error_message}"

        return True, ""
