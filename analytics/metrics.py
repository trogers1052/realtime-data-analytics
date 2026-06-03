"""Prometheus metrics for analytics-service.

The Prometheus shim (real ``prometheus_client`` with a transparent no-op
fallback) lives in :mod:`trading_commons.metrics`. This module only declares
the analytics-specific metric objects and the service-local
``start_metrics_server`` (default port 9091).
"""

import logging
import os

from trading_commons.metrics import (
    Counter,
    Gauge,
    Histogram,
    NoOpMetric,
    _HAS_PROMETHEUS,
    start_http_server,
)

logger = logging.getLogger(__name__)

_DEFAULT_PORT = 9091

# Mirror the library's availability flag under this module's historical name
# (tests monkeypatch ``metrics._PROMETHEUS_AVAILABLE``).
_PROMETHEUS_AVAILABLE = _HAS_PROMETHEUS

# Re-export the no-op metric type under the name the tests look for.
_NoOpMetric = NoOpMetric

# ---------------------------------------------------------------------------
# Metric definitions
# ---------------------------------------------------------------------------

QUOTES_RECEIVED = Counter(
    "analytics_quotes_received_total",
    "Quotes consumed from Kafka",
    ["symbol"],
)

QUOTES_REJECTED = Counter(
    "analytics_quotes_rejected_total",
    "Invalid quotes rejected (bad OHLCV, stale, etc.)",
    ["reason"],
)

INDICATORS_CALCULATED = Counter(
    "analytics_indicators_calculated_total",
    "Indicator calculations completed",
    ["symbol"],
)

INDICATOR_CALC_DURATION = Histogram(
    "analytics_indicator_calculation_duration_seconds",
    "Time spent calculating indicators",
)

KAFKA_PUBLISH = Counter(
    "analytics_kafka_publish_total",
    "Indicators published to Kafka",
    ["status"],
)

SYMBOLS_TRACKED = Gauge(
    "analytics_symbols_tracked",
    "Number of symbols currently in the price buffer",
)

PRICE_BUFFER_SIZE = Gauge(
    "analytics_price_buffer_size",
    "Total bars across all symbols in the price buffer",
)

REDIS_ERRORS = Counter(
    "analytics_redis_errors_total",
    "Redis connection or operation failures",
)


def start_metrics_server() -> None:
    """Start Prometheus metrics HTTP server on METRICS_PORT (default 9091)."""
    if not _PROMETHEUS_AVAILABLE:
        logger.warning("prometheus_client not installed — metrics endpoint disabled")
        return
    port = int(os.environ.get("METRICS_PORT", str(_DEFAULT_PORT)))
    start_http_server(port)
    logger.info(f"Metrics server listening on :{port}/metrics")
