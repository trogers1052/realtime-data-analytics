"""
Configuration management for Analytics Service.
"""

import json
from pydantic_settings import BaseSettings
from pydantic import Field, field_validator
from typing import List


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Kafka configuration
    kafka_brokers: str = Field("localhost:19092", description="Kafka broker addresses (comma-separated)")
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
    sma_periods: str = Field("[20,50,200]", description="SMA periods to calculate (comma-separated or JSON list)")
    bb_period: int = Field(20, description="Bollinger Bands period")
    bb_std_dev: float = Field(2.0, description="Bollinger Bands standard deviation")
    atr_period: int = Field(14, description="ATR period")

    # Processing settings
    batch_size: int = Field(100, description="Number of events to process before calculating indicators")
    enable_postgres_storage: bool = Field(True, description="Store indicators in PostgreSQL")

    @field_validator('sma_periods', mode='before')
    @classmethod
    def parse_sma_periods(cls, v):
        """Parse SMA periods from string or list."""
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            # Try JSON first
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                # Fall back to comma-separated
                return [int(x.strip()) for x in v.split(',')]
        return v

    @property
    def sma_periods_list(self) -> List[int]:
        """Get SMA periods as a list of integers."""
        if isinstance(self.sma_periods, str):
            try:
                return json.loads(self.sma_periods)
            except json.JSONDecodeError:
                return [int(x.strip()) for x in self.sma_periods.split(',')]
        return self.sma_periods

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

    @property
    def kafka_broker_list(self) -> List[str]:
        """Convert comma-separated brokers string to list."""
        return [b.strip() for b in self.kafka_brokers.split(",")]

    @property
    def database_url(self) -> str:
        """Get PostgreSQL connection URL for trading_platform database."""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

    @property
    def market_data_database_url(self) -> str:
        """Get PostgreSQL connection URL for market_data database."""
        return f"postgresql://{self.market_data_db_user}:{self.market_data_db_password}@{self.market_data_db_host}:{self.market_data_db_port}/{self.market_data_db_name}"
