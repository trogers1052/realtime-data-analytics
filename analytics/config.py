"""
Configuration management for Analytics Service.
"""

import json
from typing import ClassVar, List

from pydantic import Field, field_validator
from pydantic_settings import SettingsConfigDict
from trading_commons.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    """Application settings loaded from environment variables.

    Subclasses :class:`trading_commons.config.BaseServiceSettings`, inheriting
    the shared Kafka/Redis/Telegram blocks, Docker-secrets support, the
    ``redis_url`` property, ``kafka_broker_list`` and the env > YAML > defaults
    ``from_yaml`` loader.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Field names that may be supplied via Docker secrets (/run/secrets/<name>).
    SECRET_FIELDS: ClassVar[tuple[str, ...]] = (
        "db_password",
        "market_data_db_password",
        "redis_password",
        "telegram_bot_token",
        "telegram_chat_id",
    )

    # Kafka configuration (kafka_brokers inherited from base)
    kafka_consumer_group: str = Field("analytics-service", description="Kafka consumer group")
    kafka_input_topic: str = Field("stock.quotes.realtime", description="Kafka topic for price events")
    kafka_output_topic: str = Field("stock.indicators", description="Kafka topic for indicator events")

    # Database configuration (for storing indicators)
    db_host: str = Field("localhost", description="PostgreSQL host")
    db_port: int = Field(5432, description="PostgreSQL port")
    db_user: str = Field("trader", description="PostgreSQL user")
    db_password: str = Field(..., description="PostgreSQL password (required)")  # No default - must be set via env
    db_name: str = Field("trading_platform", description="PostgreSQL database name")

    # Market Data Database (for loading historical bars)
    market_data_db_host: str = Field("localhost", description="Market data PostgreSQL host")
    market_data_db_port: int = Field(5432, description="Market data PostgreSQL port")
    market_data_db_user: str = Field("ingestor", description="Market data PostgreSQL user")
    market_data_db_password: str = Field(..., description="Market data PostgreSQL password (required)")  # No default - must be set via env
    market_data_db_name: str = Field("stock_db", description="Market data PostgreSQL database name")

    # Historical data loading
    load_historical_data: bool = Field(True, description="Load historical bars on startup")
    historical_bars_limit: int = Field(500, description="Number of historical bars to load per symbol")

    # Indicator calculation settings
    min_bars_for_calculation: int = Field(200, description="Minimum bars needed for SMA_200 calculation")
    rsi_period: int = Field(14, description="RSI period")
    macd_fast: int = Field(12, description="MACD fast period")
    macd_slow: int = Field(26, description="MACD slow period")
    macd_signal: int = Field(9, description="MACD signal period")
    sma_periods: List[int] = Field(default=[20, 50, 200], description="SMA periods to calculate")
    bb_period: int = Field(20, description="Bollinger Bands period")
    bb_std_dev: float = Field(2.0, description="Bollinger Bands standard deviation")
    atr_period: int = Field(14, description="ATR period")
    ema_periods: List[int] = Field(default=[9, 21], description="EMA periods to calculate")
    stoch_k: int = Field(14, description="Stochastic %K period")
    stoch_d: int = Field(3, description="Stochastic %D period")
    stoch_smooth_k: int = Field(3, description="Stochastic %K smoothing")
    adx_period: int = Field(14, description="ADX period")

    # Processing settings
    batch_size: int = Field(100, description="Number of events to process before calculating indicators")
    enable_postgres_storage: bool = Field(True, description="Store indicators in PostgreSQL")

    # Redis configuration (host/port/db inherited from base; override password
    # default to empty string for backwards-compatible behaviour)
    redis_password: str = Field("", description="Redis password (optional)")
    check_data_freshness: bool = Field(True, description="Check data freshness before calculating")

    @field_validator('sma_periods', 'ema_periods', mode='before')
    @classmethod
    def parse_period_list(cls, v):
        """Parse period lists from string or list."""
        if isinstance(v, list):
            return [int(x) for x in v]
        if isinstance(v, str):
            # Try JSON first
            try:
                parsed = json.loads(v)
                return [int(x) for x in parsed]
            except json.JSONDecodeError:
                # Fall back to comma-separated
                return [int(x.strip()) for x in v.split(',')]
        return v

    @property
    def sma_periods_list(self) -> List[int]:
        """Get SMA periods as a list of integers."""
        return self.sma_periods

    @property
    def database_url(self) -> str:
        """Get PostgreSQL connection URL for trading_platform database."""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

    @property
    def market_data_database_url(self) -> str:
        """Get PostgreSQL connection URL for market_data database."""
        return f"postgresql://{self.market_data_db_user}:{self.market_data_db_password}@{self.market_data_db_host}:{self.market_data_db_port}/{self.market_data_db_name}"
