"""
Database operations for loading historical market data.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import psycopg2
from psycopg2.pool import SimpleConnectionPool

logger = logging.getLogger(__name__)


class MarketDataRepository:
    """Repository for loading historical market data from TimescaleDB."""

    def __init__(self, connection_url: str):
        """
        Initialize the repository.

        Args:
            connection_url: PostgreSQL connection URL for market_data database.
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
            logger.info("Connecting to market data database")
            self.pool = SimpleConnectionPool(
                minconn=1,
                maxconn=3,
                dsn=self.connection_url,
                connect_timeout=10,
                options="-c statement_timeout=30000",
            )
            logger.info("Successfully connected to market data database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to market data database: {e}")
            return False

    def get_monitored_symbols(self) -> List[str]:
        """
        Get list of monitored symbols from market_data database.

        Returns:
            List of symbol strings.
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

            query = """
                SELECT DISTINCT symbol
                FROM monitored_symbols
                WHERE enabled = true
                ORDER BY symbol
            """

            cursor.execute(query)
            rows = cursor.fetchall()

            symbols = [row[0] for row in rows]
            logger.info(f"Found {len(symbols)} monitored symbols: {symbols}")
            return symbols

        except Exception as e:
            logger.error(f"Failed to get monitored symbols: {e}")
            return []
        finally:
            if conn:
                try:
                    self.pool.putconn(conn, close=conn.closed != 0)
                except Exception:
                    pass

    def get_historical_bars(
        self,
        symbol: str,
        limit: int = 500,
    ) -> List[Dict]:
        """
        Get historical 1-minute bars for a symbol.

        Args:
            symbol: Stock symbol.
            limit: Maximum number of bars to return.

        Returns:
            List of price records with keys: time, open, high, low, close, volume
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

            # Query ohlcv_1min table (TimescaleDB hypertable)
            query = """
                SELECT time, open, high, low, close, volume, vwap, trade_count
                FROM ohlcv_1min
                WHERE symbol = %s
                ORDER BY time DESC
                LIMIT %s
            """

            cursor.execute(query, (symbol, limit))
            rows = cursor.fetchall()

            # Convert to list of dicts (reverse to chronological order)
            records = []
            for row in reversed(rows):  # Reverse to get chronological order
                records.append({
                    'time': row[0],  # datetime
                    'open': float(row[1]),
                    'high': float(row[2]),
                    'low': float(row[3]),
                    'close': float(row[4]),
                    'volume': int(row[5]) if row[5] else 0,
                })

            logger.info(f"Loaded {len(records)} historical bars for {symbol}")
            return records

        except Exception as e:
            logger.error(f"Failed to get historical bars for {symbol}: {e}", exc_info=True)
            return []
        finally:
            if conn:
                try:
                    self.pool.putconn(conn, close=conn.closed != 0)
                except Exception:
                    pass

    def get_latest_bar_time(self, symbol: str) -> Optional[datetime]:
        """
        Get the timestamp of the latest bar for a symbol.

        Args:
            symbol: Stock symbol.

        Returns:
            Latest bar timestamp or None if no data.
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

            query = """
                SELECT MAX(time)
                FROM ohlcv_1min
                WHERE symbol = %s
            """

            cursor.execute(query, (symbol,))
            row = cursor.fetchone()

            if row and row[0]:
                return row[0]
            return None

        except Exception as e:
            logger.error(f"Failed to get latest bar time for {symbol}: {e}")
            return None
        finally:
            if conn:
                try:
                    self.pool.putconn(conn, close=conn.closed != 0)
                except Exception:
                    pass

    def close(self):
        """Close the connection pool."""
        if self.pool:
            self.pool.closeall()
            logger.info("Market data database connection pool closed")
