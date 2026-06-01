"""Tests for the metrics module: start_metrics_server and the no-op fallback."""

from unittest.mock import patch

import analytics.metrics as metrics


class TestStartMetricsServer:
    def test_starts_server_when_prometheus_available(self, monkeypatch):
        monkeypatch.setattr(metrics, "_PROMETHEUS_AVAILABLE", True)
        called = {}

        def fake_start(port):
            called["port"] = port

        monkeypatch.setattr(metrics, "start_http_server", fake_start)
        monkeypatch.delenv("METRICS_PORT", raising=False)
        metrics.start_metrics_server()
        assert called["port"] == metrics._DEFAULT_PORT

    def test_respects_env_port(self, monkeypatch):
        monkeypatch.setattr(metrics, "_PROMETHEUS_AVAILABLE", True)
        captured = {}
        monkeypatch.setattr(metrics, "start_http_server", lambda p: captured.setdefault("p", p))
        monkeypatch.setenv("METRICS_PORT", "9999")
        metrics.start_metrics_server()
        assert captured["p"] == 9999

    def test_noop_when_prometheus_unavailable(self, monkeypatch):
        monkeypatch.setattr(metrics, "_PROMETHEUS_AVAILABLE", False)
        # Should simply return without trying to bind a port
        metrics.start_metrics_server()


class TestNoOpMetric:
    def test_noop_metric_operations(self):
        # The _NoOpMetric class only exists when prometheus_client is missing
        # (the except-ImportError branch). When prometheus IS installed the
        # name is absent, so we skip rather than fail.
        cls = getattr(metrics, "_NoOpMetric", None)
        if cls is None:
            import pytest
            pytest.skip("prometheus_client installed; _NoOpMetric not defined")
        m = cls()
        assert m.labels(symbol="X") is m
        m.inc()
        m.dec()
        m.set(5)
        m.observe(1.2)


class TestMetricObjectsExist:
    def test_real_metrics_are_importable(self):
        # Whichever branch ran, these names must exist and support .inc/.labels.
        assert hasattr(metrics, "QUOTES_RECEIVED")
        assert hasattr(metrics, "REDIS_ERRORS")
        assert hasattr(metrics, "KAFKA_PUBLISH")
