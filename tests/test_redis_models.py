"""Tests for the pure data-model classes in analytics.redis_client."""

import pytest
from analytics.redis_client import (
    IngestionStatus,
    SymbolFreshness,
    INGESTION_STATUS_KEY,
    SYMBOL_FRESHNESS_KEY_PREFIX,
)


# ---------------------------------------------------------------------------
# IngestionStatus
# ---------------------------------------------------------------------------

class TestIngestionStatus:
    def test_from_full_data(self):
        data = {
            "status": "running",
            "is_market_hours": True,
            "last_heartbeat": "2026-02-20T14:30:00Z",
            "symbols_tracked": 25,
            "bars_received": 5000,
            "bars_inserted": 4800,
            "backfill_pending": 2,
            "error_message": "",
        }
        s = IngestionStatus(data)
        assert s.status == "running"
        assert s.is_market_hours is True
        assert s.last_heartbeat == "2026-02-20T14:30:00Z"
        assert s.symbols_tracked == 25
        assert s.bars_received == 5000
        assert s.bars_inserted == 4800
        assert s.backfill_pending == 2
        assert s.error_message == ""

    def test_defaults_for_empty_dict(self):
        s = IngestionStatus({})
        assert s.status == "unknown"
        assert s.is_market_hours is False
        assert s.last_heartbeat == ""
        assert s.symbols_tracked == 0
        assert s.bars_received == 0
        assert s.bars_inserted == 0
        assert s.backfill_pending == 0
        assert s.error_message == ""

    def test_is_healthy_true(self):
        s = IngestionStatus({"status": "running", "error_message": ""})
        assert s.is_healthy() is True

    def test_is_healthy_false_when_not_running(self):
        s = IngestionStatus({"status": "stopped", "error_message": ""})
        assert s.is_healthy() is False

    def test_is_healthy_false_when_error(self):
        s = IngestionStatus({"status": "running", "error_message": "connection lost"})
        assert s.is_healthy() is False

    def test_is_healthy_false_default(self):
        s = IngestionStatus({})
        assert s.is_healthy() is False


# ---------------------------------------------------------------------------
# SymbolFreshness
# ---------------------------------------------------------------------------

class TestSymbolFreshness:
    def test_from_full_data(self):
        data = {
            "symbol": "AAPL",
            "status": "current",
            "last_bar_time": "2026-02-20T14:30:00Z",
            "bar_count": 1000,
            "backfill_status": "completed",
            "backfill_start": "2026-01-01T00:00:00Z",
            "backfill_end": "2026-02-20T00:00:00Z",
            "coverage_percent": 98.5,
            "gaps_detected": 2,
            "last_updated": "2026-02-20T14:30:05Z",
            "minutes_stale": 1,
            "is_ready": True,
        }
        f = SymbolFreshness(data)
        assert f.symbol == "AAPL"
        assert f.status == "current"
        assert f.bar_count == 1000
        assert f.backfill_status == "completed"
        assert f.coverage_percent == 98.5
        assert f.gaps_detected == 2
        assert f.minutes_stale == 1
        assert f.is_ready is True

    def test_defaults_for_empty_dict(self):
        f = SymbolFreshness({})
        assert f.symbol == ""
        assert f.status == "unknown"
        assert f.last_bar_time == ""
        assert f.bar_count == 0
        assert f.backfill_status == ""
        assert f.coverage_percent == 0.0
        assert f.gaps_detected == 0
        assert f.minutes_stale == 0
        assert f.is_ready is False

    def test_to_dict(self):
        data = {
            "symbol": "GOOG",
            "status": "stale",
            "is_ready": False,
            "bar_count": 500,
            "minutes_stale": 7,
            "backfill_status": "completed",
            "coverage_percent": 95.0,
            "gaps_detected": 1,
        }
        f = SymbolFreshness(data)
        d = f.to_dict()
        assert d == {
            "status": "stale",
            "is_ready": False,
            "bar_count": 500,
            "minutes_stale": 7,
            "backfill_status": "completed",
            "coverage_percent": 95.0,
            "gaps_detected": 1,
        }

    def test_to_dict_does_not_include_symbol(self):
        f = SymbolFreshness({"symbol": "X"})
        assert "symbol" not in f.to_dict()


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

class TestConstants:
    def test_ingestion_status_key(self):
        assert INGESTION_STATUS_KEY == "ingestion:status"

    def test_symbol_freshness_key_prefix(self):
        assert SYMBOL_FRESHNESS_KEY_PREFIX == "ingestion:symbol:"
