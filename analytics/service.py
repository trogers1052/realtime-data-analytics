"""
Analytics Service - Main processing logic.
"""

import logging
import time
from datetime import datetime
from collections import deque
from typing import Dict, List, Tuple
import pandas as pd

from .config import Settings
from .indicators import calculate_all_indicators
from .kafka_consumer import QuoteConsumer
from .kafka_producer import IndicatorProducer
from .database import IndicatorRepository
from .market_data_db import MarketDataRepository
from .redis_client import FreshnessClient

logger = logging.getLogger(__name__)

# Memory eviction constants
MAX_FRESHNESS_ENTRIES = 200
MAX_PRICE_BUFFER_SYMBOLS = 200
EVICTION_MAX_AGE_SECONDS = 30 * 60  # 30 minutes


class AnalyticsService:
    """Main analytics service that processes quote events and calculates indicators."""

    def __init__(self, settings: Settings):
        """
        Initialize the analytics service.

        Args:
            settings: Application settings.
        """
        self.settings = settings
        self.consumer: QuoteConsumer = None
        self.producer: IndicatorProducer = None
        self.repository: IndicatorRepository = None
        self.market_data_repo: MarketDataRepository = None
        self.freshness_client: FreshnessClient = None

        self.max_buffer_size = settings.min_bars_for_calculation * 2
        self.price_buffer: Dict[str, deque] = {}
        # Tracks last update time per symbol in price_buffer (monotonic seconds)
        self._buffer_last_access: Dict[str, float] = {}

        # value = (warning_key, monotonic_timestamp)
        self._freshness_warnings: Dict[str, Tuple[str, float]] = {}

    def _evict_stale_freshness_warnings(self):
        """Remove freshness warnings older than EVICTION_MAX_AGE_SECONDS."""
        if len(self._freshness_warnings) <= MAX_FRESHNESS_ENTRIES:
            return
        now = time.monotonic()
        stale_keys = [
            k for k, (_, ts) in self._freshness_warnings.items()
            if now - ts > EVICTION_MAX_AGE_SECONDS
        ]
        for k in stale_keys:
            del self._freshness_warnings[k]
        # If still over limit after time-based eviction, drop oldest entries
        if len(self._freshness_warnings) > MAX_FRESHNESS_ENTRIES:
            sorted_keys = sorted(
                self._freshness_warnings,
                key=lambda k: self._freshness_warnings[k][1],
            )
            for k in sorted_keys[: len(self._freshness_warnings) - MAX_FRESHNESS_ENTRIES]:
                del self._freshness_warnings[k]

    def _evict_stale_price_buffers(self):
        """Remove price buffer entries for symbols not seen in 30 minutes."""
        if len(self.price_buffer) <= MAX_PRICE_BUFFER_SYMBOLS:
            return
        now = time.monotonic()
        stale_keys = [
            k for k, ts in self._buffer_last_access.items()
            if now - ts > EVICTION_MAX_AGE_SECONDS
        ]
        for k in stale_keys:
            self.price_buffer.pop(k, None)
            self._buffer_last_access.pop(k, None)
        # If still over limit, drop least recently accessed
        if len(self.price_buffer) > MAX_PRICE_BUFFER_SYMBOLS:
            sorted_keys = sorted(
                self._buffer_last_access,
                key=lambda k: self._buffer_last_access[k],
            )
            for k in sorted_keys[: len(self.price_buffer) - MAX_PRICE_BUFFER_SYMBOLS]:
                self.price_buffer.pop(k, None)
                self._buffer_last_access.pop(k, None)

    def initialize(self) -> bool:
        """
        Initialize all connections.

        Returns:
            True if all connections successful, False otherwise.
        """
        # Initialize Redis client (for checking data freshness)
        if self.settings.check_data_freshness:
            self.freshness_client = FreshnessClient(
                host=self.settings.redis_host,
                port=self.settings.redis_port,
                password=self.settings.redis_password,
                db=self.settings.redis_db,
            )
            if not self.freshness_client.connect():
                logger.warning("Failed to connect to Redis, continuing without freshness checks")
                self.freshness_client = None
            else:
                # Check ingestion service health on startup
                is_healthy, reason = self.freshness_client.is_ingestion_healthy()
                if is_healthy:
                    logger.info("Ingestion service is healthy")
                else:
                    logger.warning(f"Ingestion service may have issues: {reason}")

        # Initialize database repository (for storing indicators)
        if self.settings.enable_postgres_storage:
            self.repository = IndicatorRepository(self.settings.database_url)
            if not self.repository.connect():
                logger.error("Failed to connect to database")
                return False

        # Initialize market data repository (for loading historical bars)
        if self.settings.load_historical_data:
            self.market_data_repo = MarketDataRepository(self.settings.market_data_database_url)
            if not self.market_data_repo.connect():
                logger.warning("Failed to connect to market data database, continuing without historical data")
                self.market_data_repo = None
            else:
                # Load historical data for monitored symbols
                self.load_historical_data()

        # Initialize Kafka producer
        self.producer = IndicatorProducer(
            brokers=self.settings.kafka_broker_list,
            topic=self.settings.kafka_output_topic,
        )
        if not self.producer.connect():
            logger.error("Failed to connect to Kafka producer")
            return False

        # Initialize Kafka consumer
        self.consumer = QuoteConsumer(
            brokers=self.settings.kafka_broker_list,
            topic=self.settings.kafka_input_topic,
            consumer_group=self.settings.kafka_consumer_group,
            message_handler=self.handle_quote_event,
        )
        if not self.consumer.connect():
            logger.error("Failed to connect to Kafka consumer")
            return False

        logger.info("Analytics service initialized successfully")
        return True

    def load_historical_data(self):
        """
        Load historical 1-minute bars for all monitored symbols.
        This pre-populates the price buffer so indicators can be calculated immediately.
        """
        if not self.market_data_repo:
            return

        try:
            logger.info("Loading historical data for monitored symbols...")
            symbols = self.market_data_repo.get_monitored_symbols()

            if not symbols:
                logger.info("No monitored symbols found")
                return

            total_bars_loaded = 0
            for symbol in symbols:
                bars = self.market_data_repo.get_historical_bars(
                    symbol=symbol,
                    limit=self.settings.historical_bars_limit,
                )

                if bars:
                    if symbol not in self.price_buffer:
                        self.price_buffer[symbol] = deque(maxlen=self.max_buffer_size)
                    self.price_buffer[symbol].extend(bars)
                    self._buffer_last_access[symbol] = time.monotonic()
                    total_bars_loaded += len(bars)
                    logger.info(f"Loaded {len(bars)} historical bars for {symbol}")

                    # Calculate indicators immediately if we have enough data
                    if len(bars) >= self.settings.min_bars_for_calculation:
                        logger.info(f"Calculating indicators for {symbol} from historical data...")
                        self.calculate_and_publish_indicators(symbol)
                else:
                    logger.debug(f"No historical data found for {symbol}")

            logger.info(f"Historical data loading complete: {total_bars_loaded} total bars loaded")

        except Exception as e:
            logger.error(f"Error loading historical data: {e}", exc_info=True)

    def handle_quote_event(self, event: dict):
        """
        Handle a quote event from Kafka.

        Args:
            event: Quote event dictionary.
        """
        try:
            # Extract data from event
            if event.get('event_type') != 'QUOTE_UPDATE':
                logger.debug(f"Ignoring non-quote event: {event.get('event_type')}")
                return

            data = event.get('data', {})
            symbol = data.get('symbol')
            if not symbol:
                logger.warning("Quote event missing symbol")
                return

            # Parse timestamp
            time_str = data.get('time')
            if not time_str:
                logger.warning(f"Quote event for {symbol} missing time")
                return

            try:
                timestamp = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            except Exception as e:
                logger.warning(f"Failed to parse timestamp {time_str}: {e}")
                return

            # Extract price data
            price_record = {
                'time': timestamp,
                'open': float(data.get('open', 0)),
                'high': float(data.get('high', 0)),
                'low': float(data.get('low', 0)),
                'close': float(data.get('close', 0)),
                'volume': int(data.get('volume', 0)),
            }

            # Validate OHLCV: reject bars with non-positive OHLC or high < low
            ohlc_values = (price_record['open'], price_record['high'],
                           price_record['low'], price_record['close'])
            if any(v <= 0 for v in ohlc_values):
                logger.warning(
                    f"Rejecting quote for {symbol}: non-positive OHLC values "
                    f"(O={price_record['open']}, H={price_record['high']}, "
                    f"L={price_record['low']}, C={price_record['close']})"
                )
                return
            if price_record['high'] < price_record['low']:
                logger.warning(
                    f"Rejecting quote for {symbol}: high ({price_record['high']}) "
                    f"< low ({price_record['low']})"
                )
                return

            # Check if we already have this timestamp (avoid duplicates)
            if symbol in self.price_buffer and self.price_buffer[symbol]:
                latest_time = self.price_buffer[symbol][-1]['time']
                if timestamp <= latest_time:
                    logger.debug(f"Skipping duplicate/old quote for {symbol}: {timestamp} <= {latest_time}")
                    return

            if symbol not in self.price_buffer:
                self._evict_stale_price_buffers()
                self.price_buffer[symbol] = deque(maxlen=self.max_buffer_size)
            self.price_buffer[symbol].append(price_record)
            self._buffer_last_access[symbol] = time.monotonic()

            # Calculate indicators if we have enough data
            if len(self.price_buffer[symbol]) >= self.settings.min_bars_for_calculation:
                self.calculate_and_publish_indicators(symbol)

        except Exception as e:
            logger.error(f"Error handling quote event: {e}", exc_info=True)

    def calculate_and_publish_indicators(self, symbol: str):
        """
        Calculate indicators for a symbol and publish/store them.

        Args:
            symbol: Stock symbol.
        """
        try:
            # Check data freshness if enabled
            data_quality = None
            if self.freshness_client:
                freshness = self.freshness_client.get_symbol_freshness(symbol)
                if freshness:
                    data_quality = freshness.to_dict()

                    # Log warning if data is not ready (but still calculate)
                    if not freshness.is_ready:
                        warning_key = f"{symbol}:{freshness.status}"
                        existing = self._freshness_warnings.get(symbol)
                        if existing is None or existing[0] != warning_key:
                            self._evict_stale_freshness_warnings()
                            self._freshness_warnings[symbol] = (warning_key, time.monotonic())
                            is_ready, reason = self.freshness_client.is_symbol_ready(symbol)
                            logger.warning(f"Data quality warning for {symbol}: {reason}")
                else:
                    # No freshness data available - log once
                    if symbol not in self._freshness_warnings:
                        self._evict_stale_freshness_warnings()
                        self._freshness_warnings[symbol] = ("no_data", time.monotonic())
                        logger.warning(f"No freshness data available for {symbol}")

            # Convert buffer to DataFrame
            df = pd.DataFrame(self.price_buffer[symbol])
            df = df.sort_values('time')  # Ensure chronological order

            # Calculate indicators
            indicators = calculate_all_indicators(
                df,
                rsi_period=self.settings.rsi_period,
                macd_fast=self.settings.macd_fast,
                macd_slow=self.settings.macd_slow,
                macd_signal=self.settings.macd_signal,
                sma_periods=self.settings.sma_periods_list,
                ema_periods=self.settings.ema_periods,
                bb_period=self.settings.bb_period,
                bb_std_dev=self.settings.bb_std_dev,
                atr_period=self.settings.atr_period,
                stoch_k=self.settings.stoch_k,
                stoch_d=self.settings.stoch_d,
                stoch_smooth_k=self.settings.stoch_smooth_k,
                adx_period=self.settings.adx_period,
            )

            if not indicators:
                logger.debug(f"No indicators calculated for {symbol} (insufficient data)")
                return

            # Get latest timestamp
            latest_timestamp = df['time'].iloc[-1]

            # Publish to Kafka (include data quality metadata)
            if self.producer:
                self.producer.publish_indicator(
                    symbol=symbol,
                    timestamp=latest_timestamp,
                    indicators=indicators,
                    data_quality=data_quality,
                )

            # Publish to Redis for downstream consumers (trading-journal risk metrics)
            if self.freshness_client:
                self.freshness_client.publish_indicators(symbol, indicators)

            # Store in database
            if self.repository and self.settings.enable_postgres_storage:
                self.repository.store_indicators(
                    symbol=symbol,
                    timestamp=latest_timestamp,
                    indicators=indicators,
                )

            # Log with quality status
            quality_status = "ready" if (data_quality and data_quality.get("is_ready")) else "warning"
            logger.info(f"Calculated indicators for {symbol}: {len(indicators)} indicators (data: {quality_status})")

        except Exception as e:
            logger.error(f"Error calculating indicators for {symbol}: {e}", exc_info=True)

    def start(self):
        """Start the analytics service."""
        logger.info("Starting analytics service...")
        logger.info(f"Consuming from: {self.settings.kafka_input_topic}")
        logger.info(f"Publishing to: {self.settings.kafka_output_topic}")
        logger.info(f"Min bars for calculation: {self.settings.min_bars_for_calculation}")

        try:
            self.consumer.start()
        except KeyboardInterrupt:
            logger.info("Service interrupted by user")
        except Exception as e:
            logger.error(f"Service error: {e}", exc_info=True)
            raise

    def shutdown(self):
        """Shutdown the service gracefully.

        Order: stop consumer loop, close external resources (Redis, Postgres),
        then close Kafka connections last.
        """
        logger.info("Shutting down analytics service...")

        # 1. Stop the consumer loop (idempotent if already stopped by signal)
        if self.consumer:
            self.consumer.close()

        # 2. Close external resources first (Redis, Postgres)
        if self.freshness_client:
            try:
                self.freshness_client.close()
            except Exception as e:
                logger.error(f"Error closing Redis: {e}")

        if self.repository:
            try:
                self.repository.close()
            except Exception as e:
                logger.error(f"Error closing database: {e}")

        if self.market_data_repo:
            try:
                self.market_data_repo.close()
            except Exception as e:
                logger.error(f"Error closing market data database: {e}")

        # 3. Close Kafka producer last (flush pending messages)
        if self.producer:
            try:
                self.producer.close()
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")

        logger.info("Analytics service stopped")
