"""
Analytics Service - Main processing logic.
"""

import logging
from datetime import datetime
from collections import defaultdict
from typing import Dict, List
import pandas as pd

from .config import Settings
from .indicators import calculate_all_indicators
from .kafka_consumer import QuoteConsumer
from .kafka_producer import IndicatorProducer
from .database import IndicatorRepository
from .market_data_db import MarketDataRepository

logger = logging.getLogger(__name__)


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

        # In-memory storage for price data per symbol
        # Key: symbol, Value: list of price records
        self.price_buffer: Dict[str, List[Dict]] = defaultdict(list)
        self.max_buffer_size = settings.min_bars_for_calculation * 2  # Keep 2x minimum

    def initialize(self) -> bool:
        """
        Initialize all connections.

        Returns:
            True if all connections successful, False otherwise.
        """
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
                    # Add to buffer
                    self.price_buffer[symbol] = bars
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

            # Check if we already have this timestamp (avoid duplicates)
            if symbol in self.price_buffer and self.price_buffer[symbol]:
                latest_time = self.price_buffer[symbol][-1]['time']
                if timestamp <= latest_time:
                    logger.debug(f"Skipping duplicate/old quote for {symbol}: {timestamp} <= {latest_time}")
                    return

            # Add to buffer
            self.price_buffer[symbol].append(price_record)

            # Trim buffer if too large
            if len(self.price_buffer[symbol]) > self.max_buffer_size:
                self.price_buffer[symbol] = self.price_buffer[symbol][-self.max_buffer_size:]

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
                bb_period=self.settings.bb_period,
                bb_std_dev=self.settings.bb_std_dev,
                atr_period=self.settings.atr_period,
            )

            if not indicators:
                logger.debug(f"No indicators calculated for {symbol} (insufficient data)")
                return

            # Get latest timestamp
            latest_timestamp = df['time'].iloc[-1]

            # Publish to Kafka
            if self.producer:
                self.producer.publish_indicator(
                    symbol=symbol,
                    timestamp=latest_timestamp,
                    indicators=indicators,
                )

            # Store in database
            if self.repository and self.settings.enable_postgres_storage:
                self.repository.store_indicators(
                    symbol=symbol,
                    timestamp=latest_timestamp,
                    indicators=indicators,
                )

            logger.info(f"Calculated and published indicators for {symbol}: {len(indicators)} indicators")

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
        """Shutdown the service gracefully."""
        logger.info("Shutting down analytics service...")

        if self.consumer:
            self.consumer.close()

        if self.producer:
            self.producer.close()

        if self.repository:
            self.repository.close()

        if self.market_data_repo:
            self.market_data_repo.close()

        logger.info("Analytics service stopped")
