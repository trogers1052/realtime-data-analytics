"""Prometheus metrics for analytics-service."""

import logging
import os

logger = logging.getLogger(__name__)

_DEFAULT_PORT = 9091

# ---------------------------------------------------------------------------
# Metric definitions
# ---------------------------------------------------------------------------
# Guarded behind a try/except so the service can still start if
# prometheus_client is not installed (metrics will simply be no-ops).
# ---------------------------------------------------------------------------

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server

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

    _PROMETHEUS_AVAILABLE = True

except ImportError:
    _PROMETHEUS_AVAILABLE = False
    start_http_server = None  # type: ignore[assignment]

    # Provide no-op stand-ins so instrumented code doesn't need guards
    class _NoOpMetric:
        """Dummy metric that silently discards all operations."""
        def inc(self, *a, **kw): pass
        def dec(self, *a, **kw): pass
        def set(self, *a, **kw): pass
        def observe(self, *a, **kw): pass
        def labels(self, **kw): return self

    QUOTES_RECEIVED = _NoOpMetric()  # type: ignore[assignment]
    QUOTES_REJECTED = _NoOpMetric()  # type: ignore[assignment]
    INDICATORS_CALCULATED = _NoOpMetric()  # type: ignore[assignment]
    INDICATOR_CALC_DURATION = _NoOpMetric()  # type: ignore[assignment]
    KAFKA_PUBLISH = _NoOpMetric()  # type: ignore[assignment]
    SYMBOLS_TRACKED = _NoOpMetric()  # type: ignore[assignment]
    PRICE_BUFFER_SIZE = _NoOpMetric()  # type: ignore[assignment]
    REDIS_ERRORS = _NoOpMetric()  # type: ignore[assignment]


def start_metrics_server() -> None:
    """Start Prometheus metrics HTTP server on METRICS_PORT (default 9091)."""
    if not _PROMETHEUS_AVAILABLE:
        logger.warning("prometheus_client not installed — metrics endpoint disabled")
        return
    port = int(os.environ.get("METRICS_PORT", str(_DEFAULT_PORT)))
    start_http_server(port)
    logger.info(f"Metrics server listening on :{port}/metrics")
