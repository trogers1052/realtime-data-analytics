"""Tests for FreshnessClient connection and query logic with mocked redis."""

import json
from unittest.mock import MagicMock, patch

import redis

from analytics.redis_client import FreshnessClient, SYMBOL_FRESHNESS_KEY_PREFIX


def _client_with_mock():
    fc = FreshnessClient(host="localhost", port=6379)
    mock_redis = MagicMock()
    fc._client = mock_redis
    return fc, mock_redis


# ---------------------------------------------------------------------------
# connect / close
# ---------------------------------------------------------------------------

class TestConnect:
    def test_connect_success(self):
        fc = FreshnessClient(host="h", port=1, password="secret", db=2)
        mock_redis = MagicMock()
        with patch("analytics.redis_client.redis.Redis", return_value=mock_redis) as R:
            assert fc.connect() is True
            mock_redis.ping.assert_called_once()
            # password passed through
            assert R.call_args.kwargs["password"] == "secret"

    def test_connect_no_password_passes_none(self):
        fc = FreshnessClient(host="h", port=1, password="")
        mock_redis = MagicMock()
        with patch("analytics.redis_client.redis.Redis", return_value=mock_redis) as R:
            fc.connect()
            assert R.call_args.kwargs["password"] is None

    def test_connect_failure_returns_false(self):
        fc = FreshnessClient(host="h", port=1)
        with patch("analytics.redis_client.redis.Redis", side_effect=redis.RedisError("down")):
            assert fc.connect() is False

    def test_close_with_client(self):
        fc, mock = _client_with_mock()
        fc.close()
        mock.close.assert_called_once()

    def test_close_without_client(self):
        FreshnessClient(host="h", port=1).close()  # no raise


# ---------------------------------------------------------------------------
# get_ingestion_status / get_symbol_freshness
# ---------------------------------------------------------------------------

class TestGetters:
    def test_get_ingestion_status_none_when_no_client(self):
        assert FreshnessClient("h", 1).get_ingestion_status() is None

    def test_get_ingestion_status_parses(self):
        fc, mock = _client_with_mock()
        mock.get.return_value = json.dumps({"status": "running", "error_message": ""})
        status = fc.get_ingestion_status()
        assert status is not None
        assert status.is_healthy() is True

    def test_get_ingestion_status_none_when_missing(self):
        fc, mock = _client_with_mock()
        mock.get.return_value = None
        assert fc.get_ingestion_status() is None

    def test_get_ingestion_status_handles_exception(self):
        fc, mock = _client_with_mock()
        mock.get.side_effect = Exception("bad json")
        assert fc.get_ingestion_status() is None

    def test_get_symbol_freshness_none_when_no_client(self):
        assert FreshnessClient("h", 1).get_symbol_freshness("AAPL") is None

    def test_get_symbol_freshness_parses(self):
        fc, mock = _client_with_mock()
        mock.get.return_value = json.dumps({"symbol": "AAPL", "is_ready": True, "bar_count": 300})
        f = fc.get_symbol_freshness("AAPL")
        assert f.symbol == "AAPL"
        assert f.is_ready is True

    def test_get_symbol_freshness_none_when_missing(self):
        fc, mock = _client_with_mock()
        mock.get.return_value = None
        assert fc.get_symbol_freshness("AAPL") is None

    def test_get_symbol_freshness_handles_exception(self):
        fc, mock = _client_with_mock()
        mock.get.side_effect = Exception("err")
        assert fc.get_symbol_freshness("AAPL") is None


# ---------------------------------------------------------------------------
# get_all_symbol_freshness
# ---------------------------------------------------------------------------

class TestGetAllSymbolFreshness:
    def test_empty_when_no_client(self):
        assert FreshnessClient("h", 1).get_all_symbol_freshness() == {}

    def test_returns_mapping(self):
        fc, mock = _client_with_mock()
        key_a = f"{SYMBOL_FRESHNESS_KEY_PREFIX}AAPL:freshness"
        key_b = f"{SYMBOL_FRESHNESS_KEY_PREFIX}MSFT:freshness"
        mock.keys.return_value = [key_a, key_b]
        mock.get.side_effect = [
            json.dumps({"symbol": "AAPL"}),
            json.dumps({"symbol": "MSFT"}),
        ]
        result = fc.get_all_symbol_freshness()
        assert set(result.keys()) == {"AAPL", "MSFT"}

    def test_handles_exception(self):
        fc, mock = _client_with_mock()
        mock.keys.side_effect = Exception("err")
        assert fc.get_all_symbol_freshness() == {}


# ---------------------------------------------------------------------------
# is_symbol_ready
# ---------------------------------------------------------------------------

class TestIsSymbolReady:
    def test_no_freshness_data(self):
        fc, mock = _client_with_mock()
        mock.get.return_value = None
        ready, reason = fc.is_symbol_ready("AAPL")
        assert ready is False
        assert "no freshness data" in reason

    def test_ready(self):
        fc, mock = _client_with_mock()
        mock.get.return_value = json.dumps({
            "symbol": "AAPL", "is_ready": True, "bar_count": 300,
            "backfill_status": "completed", "minutes_stale": 0, "status": "current",
        })
        ready, reason = fc.is_symbol_ready("AAPL")
        assert ready is True
        assert reason == ""

    def test_not_ready_reports_all_reasons(self):
        fc, mock = _client_with_mock()
        mock.get.return_value = json.dumps({
            "symbol": "AAPL", "is_ready": False, "bar_count": 50,
            "backfill_status": "running", "minutes_stale": 30, "status": "error",
        })
        ready, reason = fc.is_symbol_ready("AAPL")
        assert ready is False
        assert "backfill running" in reason
        assert "only 50 bars" in reason
        assert "30 minutes stale" in reason
        assert "data error" in reason


# ---------------------------------------------------------------------------
# publish_indicators
# ---------------------------------------------------------------------------

class TestPublishIndicators:
    def test_false_when_no_client(self):
        assert FreshnessClient("h", 1).publish_indicators("AAPL", {}) is False

    def test_success(self):
        fc, mock = _client_with_mock()
        ok = fc.publish_indicators("AAPL", {"RSI_14": 50.0})
        assert ok is True
        args, kwargs = mock.set.call_args
        assert args[0] == "indicators:AAPL"
        assert kwargs["ex"] == 600

    def test_failure_returns_false(self):
        fc, mock = _client_with_mock()
        mock.set.side_effect = Exception("boom")
        assert fc.publish_indicators("AAPL", {"RSI_14": 50.0}) is False


# ---------------------------------------------------------------------------
# is_ingestion_healthy
# ---------------------------------------------------------------------------

class TestIsIngestionHealthy:
    def test_status_unavailable(self):
        fc, mock = _client_with_mock()
        mock.get.return_value = None
        ok, reason = fc.is_ingestion_healthy()
        assert ok is False
        assert "not available" in reason

    def test_unhealthy_status(self):
        fc, mock = _client_with_mock()
        mock.get.return_value = json.dumps({"status": "stopped", "error_message": "x"})
        ok, reason = fc.is_ingestion_healthy()
        assert ok is False
        assert "ingestion status" in reason

    def test_healthy(self):
        fc, mock = _client_with_mock()
        mock.get.return_value = json.dumps({"status": "running", "error_message": ""})
        ok, reason = fc.is_ingestion_healthy()
        assert ok is True
        assert reason == ""
