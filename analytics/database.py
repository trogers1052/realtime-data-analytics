"""
Database operations for storing indicators.
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.pool import SimpleConnectionPool

logger = logging.getLogger(__name__)


class IndicatorRepository:
    """Repository for storing technical indicators in PostgreSQL."""

    def __init__(self, connection_url: str):
        """
        Initialize the repository.

        Args:
            connection_url: PostgreSQL connection URL.
        """
        self.connection_url = connection_url
        self.pool: Optional[SimpleConnectionPool] = None

    def connect(self) -> bool:
        """
        Create connection pool.

        Returns:
            True if connection successful, False otherwise.
        """
        try:
            logger.info("Connecting to PostgreSQL database")
            self.pool = SimpleConnectionPool(
                minconn=1,
                maxconn=5,
                dsn=self.connection_url,
            )
            logger.info("Successfully connected to PostgreSQL")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False

    def store_indicators(
        self,
        symbol: str,
        timestamp: datetime,
        indicators: Dict[str, Any],
    ) -> bool:
        """
        Store indicators in the database.

        Args:
            symbol: Stock symbol.
            timestamp: Timestamp of the indicators.
            indicators: Dictionary of indicator values.

        Returns:
            True if stored successfully, False otherwise.
        """
        if not self.pool:
            raise RuntimeError("Database not connected")

        conn = None
        try:
            try:
                conn = self.pool.getconn()
            except psycopg2.pool.PoolError as e:
                logger.error(f'Connection pool exhausted: {e}')
                raise
            cursor = conn.cursor()

            # Prepare indicator rows for insertion
            rows = []
            for indicator_type, value in indicators.items():
                rows.append((
                    symbol,
                    timestamp,
                    indicator_type,
                    value,
                    '1min',  # timeframe
                ))

            if not rows:
                return True

            # Insert indicators using ON CONFLICT to handle duplicates
            query = """
                INSERT INTO technical_indicators (symbol, date, indicator_type, value, timeframe)
                VALUES %s
                ON CONFLICT (symbol, date, indicator_type, timeframe)
                DO UPDATE SET value = EXCLUDED.value
            """

            execute_values(cursor, query, rows)
            conn.commit()

            logger.debug(f"Stored {len(rows)} indicators for {symbol}")
            return True

        except Exception as e:
            logger.error(f"Failed to store indicators for {symbol}: {e}", exc_info=True)
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                self.pool.putconn(conn)

    def get_price_history(
        self,
        symbol: str,
        limit: int = 200,
    ) -> Optional[list]:
        """
        Get price history for a symbol from the trading_platform database.

        Note: This assumes price_data_daily table exists in trading_platform DB.
        For real-time data, we'll use the quote events from Kafka.

        Args:
            symbol: Stock symbol.
            limit: Maximum number of records to return.

        Returns:
            List of price records or None if error.
        """
        if not self.pool:
            raise RuntimeError("Database not connected")

        conn = None
        try:
            try:
                conn = self.pool.getconn()
            except psycopg2.pool.PoolError as e:
                logger.error(f'Connection pool exhausted: {e}')
                raise
            cursor = conn.cursor()

            # Query price_data_daily table
            query = """
                SELECT date, open, high, low, close, volume
                FROM price_data_daily
                WHERE symbol = %s
                ORDER BY date DESC
                LIMIT %s
            """

            cursor.execute(query, (symbol, limit))
            rows = cursor.fetchall()

            # Convert to list of dicts
            records = []
            for row in rows:
                records.append({
                    'date': row[0],
                    'open': float(row[1]),
                    'high': float(row[2]),
                    'low': float(row[3]),
                    'close': float(row[4]),
                    'volume': int(row[5]) if row[5] else 0,
                })

            return records

        except Exception as e:
            logger.error(f"Failed to get price history for {symbol}: {e}")
            return None
        finally:
            if conn:
                self.pool.putconn(conn)

    def close(self):
        """Close the connection pool."""
        if self.pool:
            self.pool.closeall()
            logger.info("Database connection pool closed")
