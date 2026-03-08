"""Tests for AnalyticsService pure logic — eviction, event handling, OHLCV validation."""

import sys
import time
from collections import deque
from datetime import datetime, timezone
from types import ModuleType
from unittest.mock import patch, MagicMock

import pytest

# ---------------------------------------------------------------------------
# Stub pandas/pandas_ta BEFORE importing analytics.service, which pulls in
# pandas at module level.  The stubs are minimal — just enough for the
# service module to import and for handle_quote_event to build DataFrames.
# ---------------------------------------------------------------------------
if "pandas" not in sys.modules:
    _pd = ModuleType("pandas")

    class _FakeDataFrame:
        def __init__(self, data=None):
            self._data = data or {}

        def sort_values(self, *a, **kw):
            return self

        def __len__(self):
            if self._data:
                return len(next(iter(self._data.values())))
            return 0

        def __getitem__(self, key):
            if key in self._data:
                return _FakeSeries(self._data[key])
            raise KeyError(key)

    class _FakeSeries:
        def __init__(self, data=None):
            self._data = data or []
            self.iloc = self
            self.empty = len(self._data) == 0

        def __getitem__(self, idx):
            return self._data[idx]

    _pd.DataFrame = _FakeDataFrame
    _pd.Series = _FakeSeries
    _pd.isna = lambda x: x != x
    sys.modules["pandas"] = _pd

if "pandas_ta" not in sys.modules:
    sys.modules["pandas_ta"] = ModuleType("pandas_ta")
# ---------------------------------------------------------------------------

from analytics.config import Settings
from analytics.service import (
    AnalyticsService,
    MAX_FRESHNESS_ENTRIES,
    MAX_PRICE_BUFFER_SYMBOLS,
    EVICTION_MAX_AGE_SECONDS,
)


def _make_settings(**overrides):
    defaults = dict(
        check_data_freshness=False,
        enable_postgres_storage=False,
        load_historical_data=False,
    )
    defaults.update(overrides)
    return Settings(**defaults)


def _make_service(**overrides):
    return AnalyticsService(_make_settings(**overrides))


# ---------------------------------------------------------------------------
# Eviction: freshness warnings
# ---------------------------------------------------------------------------

class TestEvictStaleFreshnessWarnings:
    def test_noop_when_under_limit(self):
        svc = _make_service()
        for i in range(MAX_FRESHNESS_ENTRIES):
            svc._freshness_warnings[f"SYM{i}"] = (f"key{i}", time.monotonic())
        svc._evict_stale_freshness_warnings()
        assert len(svc._freshness_warnings) == MAX_FRESHNESS_ENTRIES

    def test_evicts_old_entries_when_over_limit(self):
        svc = _make_service()
        old = time.monotonic() - EVICTION_MAX_AGE_SECONDS - 1
        for i in range(MAX_FRESHNESS_ENTRIES + 50):
            svc._freshness_warnings[f"SYM{i}"] = (f"key{i}", old)
        svc._evict_stale_freshness_warnings()
        assert len(svc._freshness_warnings) == 0

    def test_lru_eviction_when_all_recent(self):
        svc = _make_service()
        now = time.monotonic()
        for i in range(MAX_FRESHNESS_ENTRIES + 20):
            svc._freshness_warnings[f"SYM{i}"] = (f"key{i}", now + i * 0.001)
        svc._evict_stale_freshness_warnings()
        assert len(svc._freshness_warnings) == MAX_FRESHNESS_ENTRIES
        # The oldest 20 should have been evicted; the newest should remain
        assert f"SYM{MAX_FRESHNESS_ENTRIES + 19}" in svc._freshness_warnings

    def test_mixed_old_and_recent(self):
        svc = _make_service()
        old = time.monotonic() - EVICTION_MAX_AGE_SECONDS - 1
        now = time.monotonic()
        # 180 old + 30 recent = 210 total (over 200 limit)
        for i in range(180):
            svc._freshness_warnings[f"OLD{i}"] = (f"k", old)
        for i in range(30):
            svc._freshness_warnings[f"NEW{i}"] = (f"k", now)
        svc._evict_stale_freshness_warnings()
        # Old entries removed; 30 recent remain (under limit)
        assert len(svc._freshness_warnings) == 30


# ---------------------------------------------------------------------------
# Eviction: price buffers
# ---------------------------------------------------------------------------

class TestEvictStalePriceBuffers:
    def test_noop_when_under_limit(self):
        svc = _make_service()
        for i in range(MAX_PRICE_BUFFER_SYMBOLS):
            svc.price_buffer[f"SYM{i}"] = deque()
            svc._buffer_last_access[f"SYM{i}"] = time.monotonic()
        svc._evict_stale_price_buffers()
        assert len(svc.price_buffer) == MAX_PRICE_BUFFER_SYMBOLS

    def test_evicts_old_entries_when_over_limit(self):
        svc = _make_service()
        old = time.monotonic() - EVICTION_MAX_AGE_SECONDS - 1
        for i in range(MAX_PRICE_BUFFER_SYMBOLS + 10):
            svc.price_buffer[f"SYM{i}"] = deque()
            svc._buffer_last_access[f"SYM{i}"] = old
        svc._evict_stale_price_buffers()
        assert len(svc.price_buffer) == 0
        assert len(svc._buffer_last_access) == 0

    def test_lru_eviction_when_all_recent(self):
        svc = _make_service()
        now = time.monotonic()
        for i in range(MAX_PRICE_BUFFER_SYMBOLS + 5):
            svc.price_buffer[f"SYM{i}"] = deque()
            svc._buffer_last_access[f"SYM{i}"] = now + i * 0.001
        svc._evict_stale_price_buffers()
        assert len(svc.price_buffer) == MAX_PRICE_BUFFER_SYMBOLS

    def test_access_dict_stays_in_sync(self):
        svc = _make_service()
        old = time.monotonic() - EVICTION_MAX_AGE_SECONDS - 1
        for i in range(MAX_PRICE_BUFFER_SYMBOLS + 10):
            svc.price_buffer[f"SYM{i}"] = deque()
            svc._buffer_last_access[f"SYM{i}"] = old
        svc._evict_stale_price_buffers()
        assert set(svc.price_buffer.keys()) == set(svc._buffer_last_access.keys())


# ---------------------------------------------------------------------------
# handle_quote_event — event parsing and validation
# ---------------------------------------------------------------------------

class TestHandleQuoteEvent:
    def _valid_event(self, symbol="AAPL", ts="2026-02-20T14:30:00Z",
                     open_=150.0, high=152.0, low=149.0, close=151.0, volume=1000):
        return {
            "event_type": "QUOTE_UPDATE",
            "data": {
                "symbol": symbol,
                "time": ts,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            },
        }

    def test_ignores_non_quote_event(self):
        svc = _make_service()
        svc.handle_quote_event({"event_type": "OTHER"})
        assert len(svc.price_buffer) == 0

    def test_ignores_missing_symbol(self):
        svc = _make_service()
        svc.handle_quote_event({"event_type": "QUOTE_UPDATE", "data": {}})
        assert len(svc.price_buffer) == 0

    def test_ignores_missing_time(self):
        svc = _make_service()
        svc.handle_quote_event({
            "event_type": "QUOTE_UPDATE",
            "data": {"symbol": "X"},
        })
        assert len(svc.price_buffer) == 0

    def test_ignores_invalid_timestamp(self):
        svc = _make_service()
        svc.handle_quote_event(self._valid_event(ts="not-a-date"))
        assert len(svc.price_buffer) == 0

    def test_rejects_zero_close(self):
        svc = _make_service()
        svc.handle_quote_event(self._valid_event(close=0))
        assert len(svc.price_buffer) == 0

    def test_rejects_negative_open(self):
        svc = _make_service()
        svc.handle_quote_event(self._valid_event(open_=-1))
        assert len(svc.price_buffer) == 0

    def test_rejects_high_less_than_low(self):
        svc = _make_service()
        svc.handle_quote_event(self._valid_event(high=100, low=200))
        assert len(svc.price_buffer) == 0

    def test_appends_valid_event(self):
        svc = _make_service()
        svc.handle_quote_event(self._valid_event())
        assert "AAPL" in svc.price_buffer
        assert len(svc.price_buffer["AAPL"]) == 1
        record = svc.price_buffer["AAPL"][0]
        assert record["close"] == 151.0
        assert record["volume"] == 1000

    def test_dedup_rejects_same_timestamp(self):
        svc = _make_service()
        svc.handle_quote_event(self._valid_event(ts="2026-02-20T14:30:00Z"))
        svc.handle_quote_event(self._valid_event(ts="2026-02-20T14:30:00Z"))
        assert len(svc.price_buffer["AAPL"]) == 1

    def test_dedup_rejects_older_timestamp(self):
        svc = _make_service()
        svc.handle_quote_event(self._valid_event(ts="2026-02-20T14:31:00Z"))
        svc.handle_quote_event(self._valid_event(ts="2026-02-20T14:30:00Z"))
        assert len(svc.price_buffer["AAPL"]) == 1

    def test_accepts_newer_timestamp(self):
        svc = _make_service()
        svc.handle_quote_event(self._valid_event(ts="2026-02-20T14:30:00Z"))
        svc.handle_quote_event(self._valid_event(ts="2026-02-20T14:31:00Z"))
        assert len(svc.price_buffer["AAPL"]) == 2

    def test_different_symbols_get_separate_buffers(self):
        svc = _make_service()
        svc.handle_quote_event(self._valid_event(symbol="AAPL"))
        svc.handle_quote_event(self._valid_event(symbol="GOOG"))
        assert "AAPL" in svc.price_buffer
        assert "GOOG" in svc.price_buffer

    def test_triggers_calculation_at_min_bars(self):
        svc = _make_service(min_bars_for_calculation=3)
        with patch.object(svc, "calculate_and_publish_indicators") as mock_calc:
            for i in range(3):
                svc.handle_quote_event(self._valid_event(
                    ts=f"2026-02-20T14:{30 + i:02d}:00Z",
                ))
            assert mock_calc.call_count == 1
            mock_calc.assert_called_with("AAPL")

    def test_no_calculation_below_min_bars(self):
        svc = _make_service(min_bars_for_calculation=5)
        with patch.object(svc, "calculate_and_publish_indicators") as mock_calc:
            for i in range(4):
                svc.handle_quote_event(self._valid_event(
                    ts=f"2026-02-20T14:{30 + i:02d}:00Z",
                ))
            mock_calc.assert_not_called()

    def test_buffer_respects_maxlen(self):
        svc = _make_service(min_bars_for_calculation=2)
        # max_buffer_size = min_bars * 2 = 4
        with patch.object(svc, "calculate_and_publish_indicators"):
            for i in range(10):
                svc.handle_quote_event(self._valid_event(
                    ts=f"2026-02-20T14:{30 + i:02d}:00Z",
                ))
        assert len(svc.price_buffer["AAPL"]) == 4  # 2 * 2

    def test_exception_does_not_propagate(self):
        svc = _make_service()
        # Malformed event should not raise
        svc.handle_quote_event({"event_type": "QUOTE_UPDATE", "data": {"symbol": "X", "time": "2026-02-20T14:30:00Z", "open": "not_a_number"}})
        # Should not crash — exception caught internally


# ---------------------------------------------------------------------------
# max_buffer_size derived from settings
# ---------------------------------------------------------------------------

class TestMaxBufferSize:
    def test_default(self):
        svc = _make_service()
        assert svc.max_buffer_size == 400  # 200 * 2

    def test_custom(self):
        svc = _make_service(min_bars_for_calculation=50)
        assert svc.max_buffer_size == 100  # 50 * 2
