"""Tests for analytics.config — Settings class and its validators/properties."""

import os
import pytest
from analytics.config import Settings


# ---------------------------------------------------------------------------
# parse_period_list validator
# ---------------------------------------------------------------------------

class TestParsePeriodList:
    """Covers the @field_validator('sma_periods', 'ema_periods') path."""

    def test_list_of_ints_passthrough(self):
        s = Settings(sma_periods=[10, 20], ema_periods=[5, 10])
        assert s.sma_periods == [10, 20]
        assert s.ema_periods == [5, 10]

    def test_json_string_parsed(self):
        s = Settings(sma_periods="[10, 30, 50]", ema_periods="[7]")
        assert s.sma_periods == [10, 30, 50]
        assert s.ema_periods == [7]

    def test_comma_separated_string_parsed(self):
        s = Settings(sma_periods="10, 30, 50", ema_periods="7, 14")
        assert s.sma_periods == [10, 30, 50]
        assert s.ema_periods == [7, 14]

    def test_single_value_json(self):
        s = Settings(sma_periods="[200]")
        assert s.sma_periods == [200]

    def test_bare_integer_string_raises(self):
        """A bare integer string like "200" is parsed by json.loads as int 200,
        which is not iterable.  This is a known edge case in the validator."""
        with pytest.raises(TypeError):
            Settings(sma_periods="200")

    def test_list_of_string_ints(self):
        s = Settings(sma_periods=["10", "20", "50"])
        assert s.sma_periods == [10, 20, 50]

    def test_defaults(self):
        s = Settings()
        assert s.sma_periods == [20, 50, 200]
        assert s.ema_periods == [9, 21]


# ---------------------------------------------------------------------------
# Property methods
# ---------------------------------------------------------------------------

class TestKafkaBrokerList:
    def test_single_broker(self):
        s = Settings(kafka_brokers="localhost:9092")
        assert s.kafka_broker_list == ["localhost:9092"]

    def test_multiple_brokers(self):
        s = Settings(kafka_brokers="broker1:9092, broker2:9092, broker3:9092")
        assert s.kafka_broker_list == ["broker1:9092", "broker2:9092", "broker3:9092"]

    def test_default_broker(self):
        s = Settings()
        assert s.kafka_broker_list == ["localhost:19092"]


class TestDatabaseUrl:
    def test_default_url(self):
        s = Settings()
        assert s.database_url == "postgresql://trader:test@localhost:5432/trading_platform"

    def test_custom_url(self):
        s = Settings(
            db_user="admin", db_password="secret",
            db_host="db.prod", db_port=5433, db_name="mydb",
        )
        assert s.database_url == "postgresql://admin:secret@db.prod:5433/mydb"


class TestMarketDataDatabaseUrl:
    def test_default_url(self):
        s = Settings()
        assert s.market_data_database_url == "postgresql://ingestor:test@localhost:5432/stock_db"

    def test_custom_url(self):
        s = Settings(
            market_data_db_user="reader", market_data_db_password="pass",
            market_data_db_host="md.host", market_data_db_port=5434,
            market_data_db_name="market",
        )
        assert s.market_data_database_url == "postgresql://reader:pass@md.host:5434/market"


class TestSmaPeriodsListProperty:
    def test_returns_sma_periods(self):
        s = Settings(sma_periods=[10, 20])
        assert s.sma_periods_list == [10, 20]
        assert s.sma_periods_list is s.sma_periods


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

class TestSettingsDefaults:
    def test_indicator_defaults(self):
        s = Settings()
        assert s.rsi_period == 14
        assert s.macd_fast == 12
        assert s.macd_slow == 26
        assert s.macd_signal == 9
        assert s.bb_period == 20
        assert s.bb_std_dev == 2.0
        assert s.atr_period == 14
        assert s.stoch_k == 14
        assert s.stoch_d == 3
        assert s.stoch_smooth_k == 3
        assert s.adx_period == 14

    def test_processing_defaults(self):
        s = Settings()
        assert s.batch_size == 100
        assert s.enable_postgres_storage is True
        assert s.min_bars_for_calculation == 200

    def test_redis_defaults(self):
        s = Settings()
        assert s.redis_host == "localhost"
        assert s.redis_port == 6379
        assert s.redis_password == ""
        assert s.redis_db == 0
        assert s.check_data_freshness is True

    def test_historical_defaults(self):
        s = Settings()
        assert s.load_historical_data is True
        assert s.historical_bars_limit == 500
