"""Tests for AnalyticsService orchestration: initialize, load_historical_data,
calculate_and_publish_indicators, start, shutdown.

All external clients (Kafka, Postgres, Redis) are mocked.  Real pandas is used.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from analytics.config import Settings
from analytics.service import AnalyticsService


def _settings(**overrides):
    base = dict(
        db_password="x",
        market_data_db_password="x",
        min_bars_for_calculation=3,
        check_data_freshness=False,
        enable_postgres_storage=False,
        load_historical_data=False,
    )
    base.update(overrides)
    return Settings(**base)


# ---------------------------------------------------------------------------
# initialize
# ---------------------------------------------------------------------------

class TestInitialize:
    def test_minimal_initialize_success(self):
        svc = AnalyticsService(_settings())
        with patch("analytics.service.IndicatorProducer") as P, \
             patch("analytics.service.QuoteConsumer") as C:
            P.return_value.connect.return_value = True
            C.return_value.connect.return_value = True
            assert svc.initialize() is True
            assert svc.producer is not None
            assert svc.consumer is not None

    def test_producer_ready_before_historical_load(self):
        """Regression: the warm-up batch in load_historical_data() publishes
        indicators, so the producer must already exist when it runs — otherwise
        calculate_and_publish_indicators() silently drops the deep-history
        indicators context-service needs (SMA_200 → trend/breadth/temperature)."""
        svc = AnalyticsService(_settings(load_historical_data=True))
        with patch("analytics.service.IndicatorProducer") as P, \
             patch("analytics.service.QuoteConsumer") as C, \
             patch("analytics.service.MarketDataRepository") as M:
            P.return_value.connect.return_value = True
            C.return_value.connect.return_value = True
            M.return_value.connect.return_value = True

            captured = {}

            def _spy_load():
                captured["producer_set"] = svc.producer is not None

            svc.load_historical_data = _spy_load

            assert svc.initialize() is True
            assert captured.get("producer_set") is True, \
                "producer must be initialized BEFORE load_historical_data()"

    def test_producer_connect_failure(self):
        svc = AnalyticsService(_settings())
        with patch("analytics.service.IndicatorProducer") as P, \
             patch("analytics.service.QuoteConsumer"):
            P.return_value.connect.return_value = False
            assert svc.initialize() is False

    def test_consumer_connect_failure(self):
        svc = AnalyticsService(_settings())
        with patch("analytics.service.IndicatorProducer") as P, \
             patch("analytics.service.QuoteConsumer") as C:
            P.return_value.connect.return_value = True
            C.return_value.connect.return_value = False
            assert svc.initialize() is False

    def test_redis_enabled_connect_success(self):
        svc = AnalyticsService(_settings(check_data_freshness=True))
        with patch("analytics.service.FreshnessClient") as F, \
             patch("analytics.service.IndicatorProducer") as P, \
             patch("analytics.service.QuoteConsumer") as C:
            F.return_value.connect.return_value = True
            F.return_value.is_ingestion_healthy.return_value = (True, "")
            P.return_value.connect.return_value = True
            C.return_value.connect.return_value = True
            assert svc.initialize() is True
            assert svc.freshness_client is not None

    def test_redis_connect_failure_continues(self):
        svc = AnalyticsService(_settings(check_data_freshness=True))
        with patch("analytics.service.FreshnessClient") as F, \
             patch("analytics.service.IndicatorProducer") as P, \
             patch("analytics.service.QuoteConsumer") as C:
            F.return_value.connect.return_value = False
            P.return_value.connect.return_value = True
            C.return_value.connect.return_value = True
            assert svc.initialize() is True
            assert svc.freshness_client is None

    def test_redis_unhealthy_ingestion_warns_but_continues(self):
        svc = AnalyticsService(_settings(check_data_freshness=True))
        with patch("analytics.service.FreshnessClient") as F, \
             patch("analytics.service.IndicatorProducer") as P, \
             patch("analytics.service.QuoteConsumer") as C:
            F.return_value.connect.return_value = True
            F.return_value.is_ingestion_healthy.return_value = (False, "down")
            P.return_value.connect.return_value = True
            C.return_value.connect.return_value = True
            assert svc.initialize() is True

    def test_postgres_enabled_connect_failure(self):
        svc = AnalyticsService(_settings(enable_postgres_storage=True))
        with patch("analytics.service.IndicatorRepository") as R, \
             patch("analytics.service.IndicatorProducer"), \
             patch("analytics.service.QuoteConsumer"):
            R.return_value.connect.return_value = False
            assert svc.initialize() is False

    def test_historical_enabled_connect_failure_continues(self):
        svc = AnalyticsService(_settings(load_historical_data=True))
        with patch("analytics.service.MarketDataRepository") as M, \
             patch("analytics.service.IndicatorProducer") as P, \
             patch("analytics.service.QuoteConsumer") as C:
            M.return_value.connect.return_value = False
            P.return_value.connect.return_value = True
            C.return_value.connect.return_value = True
            assert svc.initialize() is True
            assert svc.market_data_repo is None

    def test_historical_enabled_connect_success_loads(self):
        svc = AnalyticsService(_settings(load_historical_data=True))
        with patch("analytics.service.MarketDataRepository") as M, \
             patch("analytics.service.IndicatorProducer") as P, \
             patch("analytics.service.QuoteConsumer") as C, \
             patch.object(AnalyticsService, "load_historical_data") as load:
            M.return_value.connect.return_value = True
            P.return_value.connect.return_value = True
            C.return_value.connect.return_value = True
            assert svc.initialize() is True
            load.assert_called_once()


# ---------------------------------------------------------------------------
# load_historical_data
# ---------------------------------------------------------------------------

def _bar(ts, price=100.0):
    return {
        "time": ts, "open": price, "high": price + 1,
        "low": price - 1, "close": price, "volume": 1000,
    }


class TestLoadHistoricalData:
    def test_noop_without_repo(self):
        svc = AnalyticsService(_settings())
        svc.load_historical_data()  # no raise, no repo

    def test_no_symbols(self):
        svc = AnalyticsService(_settings())
        svc.market_data_repo = MagicMock()
        svc.market_data_repo.get_monitored_symbols.return_value = []
        svc.load_historical_data()
        svc.market_data_repo.get_historical_bars.assert_not_called()

    def test_loads_bars_and_triggers_calc(self):
        svc = AnalyticsService(_settings(min_bars_for_calculation=3))
        svc.market_data_repo = MagicMock()
        svc.market_data_repo.get_monitored_symbols.return_value = ["AAPL", "MSFT"]
        bars = [_bar(datetime(2026, 1, i + 1)) for i in range(5)]
        # AAPL has bars, MSFT has none
        svc.market_data_repo.get_historical_bars.side_effect = [bars, []]
        with patch.object(svc, "calculate_and_publish_indicators") as calc:
            svc.load_historical_data()
            assert "AAPL" in svc.price_buffer
            calc.assert_called_once_with("AAPL")

    def test_handles_exception(self):
        svc = AnalyticsService(_settings())
        svc.market_data_repo = MagicMock()
        svc.market_data_repo.get_monitored_symbols.side_effect = Exception("boom")
        svc.load_historical_data()  # no raise


# ---------------------------------------------------------------------------
# calculate_and_publish_indicators
# ---------------------------------------------------------------------------

class TestCalculateAndPublish:
    def _svc_with_buffer(self, n=5):
        from collections import deque
        svc = AnalyticsService(_settings(min_bars_for_calculation=3))
        buf = deque(maxlen=100)
        for i in range(n):
            buf.append(_bar(datetime(2026, 1, 1, 0, i)))
        svc.price_buffer["AAPL"] = buf
        return svc

    def test_publishes_when_indicators_present(self):
        svc = self._svc_with_buffer()
        svc.producer = MagicMock()
        svc.producer.publish_indicator.return_value = True
        with patch("analytics.service.calculate_all_indicators",
                   return_value={"close": 100.0, "RSI_14": 50.0}):
            svc.calculate_and_publish_indicators("AAPL")
        svc.producer.publish_indicator.assert_called_once()

    def test_no_indicators_returns_early(self):
        svc = self._svc_with_buffer()
        svc.producer = MagicMock()
        with patch("analytics.service.calculate_all_indicators", return_value={}):
            svc.calculate_and_publish_indicators("AAPL")
        svc.producer.publish_indicator.assert_not_called()

    def test_stores_in_repository(self):
        svc = self._svc_with_buffer()
        svc.settings = _settings(min_bars_for_calculation=3, enable_postgres_storage=True)
        svc.producer = MagicMock()
        svc.producer.publish_indicator.return_value = True
        svc.repository = MagicMock()
        with patch("analytics.service.calculate_all_indicators",
                   return_value={"close": 100.0}):
            svc.calculate_and_publish_indicators("AAPL")
        svc.repository.store_indicators.assert_called_once()

    def test_freshness_warning_logged_when_not_ready(self):
        svc = self._svc_with_buffer()
        svc.producer = MagicMock()
        svc.producer.publish_indicator.return_value = True
        fc = MagicMock()
        freshness = MagicMock()
        freshness.is_ready = False
        freshness.status = "stale"
        freshness.to_dict.return_value = {"is_ready": False, "status": "stale"}
        fc.get_symbol_freshness.return_value = freshness
        fc.is_symbol_ready.return_value = (False, "only 50 bars")
        svc.freshness_client = fc
        with patch("analytics.service.calculate_all_indicators",
                   return_value={"close": 100.0}):
            svc.calculate_and_publish_indicators("AAPL")
        # Warning recorded for the symbol
        assert "AAPL" in svc._freshness_warnings
        fc.publish_indicators.assert_called_once()

    def test_no_freshness_data_logged_once(self):
        svc = self._svc_with_buffer()
        svc.producer = MagicMock()
        svc.producer.publish_indicator.return_value = True
        fc = MagicMock()
        fc.get_symbol_freshness.return_value = None
        svc.freshness_client = fc
        with patch("analytics.service.calculate_all_indicators",
                   return_value={"close": 100.0}):
            svc.calculate_and_publish_indicators("AAPL")
        assert svc._freshness_warnings["AAPL"][0] == "no_data"

    def test_exception_is_caught(self):
        svc = self._svc_with_buffer()
        with patch("analytics.service.calculate_all_indicators",
                   side_effect=Exception("calc boom")):
            svc.calculate_and_publish_indicators("AAPL")  # no raise


# ---------------------------------------------------------------------------
# start / shutdown
# ---------------------------------------------------------------------------

class TestStartShutdown:
    def test_start_delegates_to_consumer(self):
        svc = AnalyticsService(_settings())
        svc.consumer = MagicMock()
        svc.start()
        svc.consumer.start.assert_called_once()

    def test_start_keyboard_interrupt_swallowed(self):
        svc = AnalyticsService(_settings())
        svc.consumer = MagicMock()
        svc.consumer.start.side_effect = KeyboardInterrupt()
        svc.start()  # no raise

    def test_start_reraises_other_errors(self):
        svc = AnalyticsService(_settings())
        svc.consumer = MagicMock()
        svc.consumer.start.side_effect = RuntimeError("boom")
        with pytest.raises(RuntimeError):
            svc.start()

    def test_shutdown_closes_all(self):
        svc = AnalyticsService(_settings())
        svc.consumer = MagicMock()
        svc.freshness_client = MagicMock()
        svc.repository = MagicMock()
        svc.market_data_repo = MagicMock()
        svc.producer = MagicMock()
        svc.shutdown()
        svc.consumer.close.assert_called_once()
        svc.freshness_client.close.assert_called_once()
        svc.repository.close.assert_called_once()
        svc.market_data_repo.close.assert_called_once()
        svc.producer.close.assert_called_once()

    def test_shutdown_swallows_close_errors(self):
        svc = AnalyticsService(_settings())
        svc.consumer = MagicMock()
        svc.freshness_client = MagicMock()
        svc.freshness_client.close.side_effect = Exception("x")
        svc.repository = MagicMock()
        svc.repository.close.side_effect = Exception("x")
        svc.market_data_repo = MagicMock()
        svc.market_data_repo.close.side_effect = Exception("x")
        svc.producer = MagicMock()
        svc.producer.close.side_effect = Exception("x")
        svc.shutdown()  # no raise

    def test_shutdown_with_nothing_initialized(self):
        svc = AnalyticsService(_settings())
        svc.shutdown()  # no raise
