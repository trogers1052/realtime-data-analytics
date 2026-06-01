"""Tests for IndicatorRepository and MarketDataRepository with mocked psycopg2.

All database I/O is mocked — no live Postgres/TimescaleDB connection is made.
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from analytics.database import IndicatorRepository
from analytics.market_data_db import MarketDataRepository


def _make_pool(conn):
    """Build a fake SimpleConnectionPool returning the given connection."""
    pool = MagicMock()
    pool.getconn.return_value = conn
    return pool


# ---------------------------------------------------------------------------
# IndicatorRepository.connect
# ---------------------------------------------------------------------------

class TestIndicatorRepositoryConnect:
    def test_connect_success(self):
        repo = IndicatorRepository("postgresql://x")
        with patch("analytics.database.SimpleConnectionPool", return_value=MagicMock()) as P:
            assert repo.connect() is True
            assert repo.pool is not None
            P.assert_called_once()

    def test_connect_failure_returns_false(self):
        repo = IndicatorRepository("postgresql://x")
        with patch("analytics.database.SimpleConnectionPool", side_effect=Exception("boom")):
            assert repo.connect() is False
            assert repo.pool is None


# ---------------------------------------------------------------------------
# IndicatorRepository.store_indicators
# ---------------------------------------------------------------------------

class TestStoreIndicators:
    def test_raises_when_not_connected(self):
        repo = IndicatorRepository("postgresql://x")
        with pytest.raises(RuntimeError, match="not connected"):
            repo.store_indicators("AAPL", datetime.now(), {"RSI_14": 50.0})

    def test_store_success(self):
        repo = IndicatorRepository("postgresql://x")
        conn = MagicMock()
        conn.closed = 0
        repo.pool = _make_pool(conn)

        with patch("analytics.database.execute_values") as ev:
            ok = repo.store_indicators(
                "AAPL", datetime(2026, 1, 1), {"RSI_14": 50.0, "close": 150.0}
            )

        assert ok is True
        ev.assert_called_once()
        # Two indicator rows were prepared
        rows = ev.call_args[0][2]
        assert len(rows) == 2
        assert rows[0][0] == "AAPL"
        assert rows[0][4] == "1min"
        conn.commit.assert_called_once()
        repo.pool.putconn.assert_called_once()

    def test_store_empty_indicators_returns_true_without_insert(self):
        repo = IndicatorRepository("postgresql://x")
        conn = MagicMock()
        conn.closed = 0
        repo.pool = _make_pool(conn)
        with patch("analytics.database.execute_values") as ev:
            ok = repo.store_indicators("AAPL", datetime.now(), {})
        assert ok is True
        ev.assert_not_called()

    def test_store_rolls_back_and_returns_false_on_error(self):
        repo = IndicatorRepository("postgresql://x")
        conn = MagicMock()
        conn.closed = 0
        repo.pool = _make_pool(conn)
        with patch("analytics.database.execute_values", side_effect=Exception("db down")):
            ok = repo.store_indicators("AAPL", datetime.now(), {"RSI_14": 50.0})
        assert ok is False
        conn.rollback.assert_called_once()


# ---------------------------------------------------------------------------
# IndicatorRepository.get_price_history
# ---------------------------------------------------------------------------

class TestGetPriceHistory:
    def test_raises_when_not_connected(self):
        repo = IndicatorRepository("postgresql://x")
        with pytest.raises(RuntimeError, match="not connected"):
            repo.get_price_history("AAPL")

    def test_returns_records(self):
        repo = IndicatorRepository("postgresql://x")
        conn = MagicMock()
        conn.closed = 0
        cursor = conn.cursor.return_value
        cursor.fetchall.return_value = [
            (datetime(2026, 1, 1), 10.0, 11.0, 9.0, 10.5, 1000),
            (datetime(2026, 1, 2), 10.5, 12.0, 10.0, 11.5, None),
        ]
        repo.pool = _make_pool(conn)

        records = repo.get_price_history("AAPL", limit=2)
        assert len(records) == 2
        assert records[0]["open"] == 10.0
        assert records[0]["volume"] == 1000
        # None volume -> 0
        assert records[1]["volume"] == 0

    def test_returns_none_on_error(self):
        repo = IndicatorRepository("postgresql://x")
        conn = MagicMock()
        conn.closed = 0
        conn.cursor.side_effect = Exception("query failed")
        repo.pool = _make_pool(conn)
        assert repo.get_price_history("AAPL") is None


# ---------------------------------------------------------------------------
# IndicatorRepository.close
# ---------------------------------------------------------------------------

class TestIndicatorRepositoryClose:
    def test_close_with_pool(self):
        repo = IndicatorRepository("postgresql://x")
        repo.pool = MagicMock()
        repo.close()
        repo.pool.closeall.assert_called_once()

    def test_close_without_pool_noop(self):
        repo = IndicatorRepository("postgresql://x")
        repo.close()  # should not raise


# ---------------------------------------------------------------------------
# MarketDataRepository
# ---------------------------------------------------------------------------

class TestMarketDataRepositoryConnect:
    def test_connect_success(self):
        repo = MarketDataRepository("postgresql://x")
        with patch("analytics.market_data_db.SimpleConnectionPool", return_value=MagicMock()):
            assert repo.connect() is True

    def test_connect_failure(self):
        repo = MarketDataRepository("postgresql://x")
        with patch("analytics.market_data_db.SimpleConnectionPool", side_effect=Exception("x")):
            assert repo.connect() is False


class TestGetMonitoredSymbols:
    def test_raises_when_not_connected(self):
        repo = MarketDataRepository("postgresql://x")
        with pytest.raises(RuntimeError):
            repo.get_monitored_symbols()

    def test_returns_symbols(self):
        repo = MarketDataRepository("postgresql://x")
        conn = MagicMock()
        conn.closed = 0
        conn.cursor.return_value.fetchall.return_value = [("AAPL",), ("MSFT",)]
        repo.pool = _make_pool(conn)
        assert repo.get_monitored_symbols() == ["AAPL", "MSFT"]

    def test_returns_empty_on_error(self):
        repo = MarketDataRepository("postgresql://x")
        conn = MagicMock()
        conn.closed = 0
        conn.cursor.side_effect = Exception("boom")
        repo.pool = _make_pool(conn)
        assert repo.get_monitored_symbols() == []


class TestGetHistoricalBars:
    def test_raises_when_not_connected(self):
        repo = MarketDataRepository("postgresql://x")
        with pytest.raises(RuntimeError):
            repo.get_historical_bars("AAPL")

    def test_returns_chronological_bars(self):
        repo = MarketDataRepository("postgresql://x")
        conn = MagicMock()
        conn.closed = 0
        # Rows come back DESC; repo reverses to chronological
        conn.cursor.return_value.fetchall.return_value = [
            (datetime(2026, 1, 2), 11.0, 12.0, 10.0, 11.5, 200, 11.2, 5),
            (datetime(2026, 1, 1), 10.0, 11.0, 9.0, 10.5, None, 10.2, 3),
        ]
        repo.pool = _make_pool(conn)
        bars = repo.get_historical_bars("AAPL", limit=2)
        assert len(bars) == 2
        # Reversed -> oldest first
        assert bars[0]["time"] == datetime(2026, 1, 1)
        assert bars[0]["volume"] == 0  # None -> 0
        assert bars[1]["close"] == 11.5

    def test_returns_empty_on_error(self):
        repo = MarketDataRepository("postgresql://x")
        conn = MagicMock()
        conn.closed = 0
        conn.cursor.side_effect = Exception("x")
        repo.pool = _make_pool(conn)
        assert repo.get_historical_bars("AAPL") == []


class TestGetLatestBarTime:
    def test_raises_when_not_connected(self):
        repo = MarketDataRepository("postgresql://x")
        with pytest.raises(RuntimeError):
            repo.get_latest_bar_time("AAPL")

    def test_returns_timestamp(self):
        repo = MarketDataRepository("postgresql://x")
        conn = MagicMock()
        conn.closed = 0
        ts = datetime(2026, 1, 5)
        conn.cursor.return_value.fetchone.return_value = (ts,)
        repo.pool = _make_pool(conn)
        assert repo.get_latest_bar_time("AAPL") == ts

    def test_returns_none_when_no_data(self):
        repo = MarketDataRepository("postgresql://x")
        conn = MagicMock()
        conn.closed = 0
        conn.cursor.return_value.fetchone.return_value = (None,)
        repo.pool = _make_pool(conn)
        assert repo.get_latest_bar_time("AAPL") is None

    def test_returns_none_on_error(self):
        repo = MarketDataRepository("postgresql://x")
        conn = MagicMock()
        conn.closed = 0
        conn.cursor.side_effect = Exception("x")
        repo.pool = _make_pool(conn)
        assert repo.get_latest_bar_time("AAPL") is None


class TestMarketDataRepositoryClose:
    def test_close_with_pool(self):
        repo = MarketDataRepository("postgresql://x")
        repo.pool = MagicMock()
        repo.close()
        repo.pool.closeall.assert_called_once()

    def test_close_without_pool(self):
        repo = MarketDataRepository("postgresql://x")
        repo.close()
