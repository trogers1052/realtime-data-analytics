"""
Technical Indicator Calculations using pandas-ta.

Calculates RSI, MACD, SMA, Bollinger Bands, ATR, and other indicators.
"""

import pandas as pd
import pandas_ta as ta
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


def calculate_rsi(prices: pd.Series, period: int = 14) -> pd.Series:
    """Calculate Relative Strength Index."""
    return ta.rsi(prices, length=period)


def calculate_macd(
    prices: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9
) -> Dict[str, pd.Series]:
    """Calculate MACD, Signal, and Histogram."""
    macd_data = ta.macd(prices, fast=fast, slow=slow, signal=signal)
    
    return {
        "macd": macd_data[f"MACD_{fast}_{slow}_{signal}"],
        "signal": macd_data[f"MACDs_{fast}_{slow}_{signal}"],
        "histogram": macd_data[f"MACDh_{fast}_{slow}_{signal}"],
    }


def calculate_sma(prices: pd.Series, period: int) -> pd.Series:
    """Calculate Simple Moving Average."""
    return ta.sma(prices, length=period)


def calculate_ema(prices: pd.Series, period: int) -> pd.Series:
    """Calculate Exponential Moving Average."""
    return ta.ema(prices, length=period)


def calculate_bollinger_bands(
    prices: pd.Series,
    period: int = 20,
    std_dev: float = 2.0
) -> Dict[str, pd.Series]:
    """Calculate Bollinger Bands."""
    bb = ta.bbands(prices, length=period, std=std_dev)

    # Find columns dynamically (pandas-ta naming varies by version)
    upper_col = [c for c in bb.columns if c.startswith('BBU_')][0]
    middle_col = [c for c in bb.columns if c.startswith('BBM_')][0]
    lower_col = [c for c in bb.columns if c.startswith('BBL_')][0]

    return {
        "upper": bb[upper_col],
        "middle": bb[middle_col],
        "lower": bb[lower_col],
    }


def calculate_atr(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    period: int = 14
) -> pd.Series:
    """Calculate Average True Range."""
    return ta.atr(high=high, low=low, close=close, length=period)


def calculate_volume_sma(volume: pd.Series, period: int = 20) -> pd.Series:
    """Calculate Simple Moving Average of Volume."""
    return ta.sma(volume, length=period)


def calculate_all_indicators(
    df: pd.DataFrame,
    rsi_period: int = 14,
    macd_fast: int = 12,
    macd_slow: int = 26,
    macd_signal: int = 9,
    sma_periods: list = [20, 50, 200],
    bb_period: int = 20,
    bb_std_dev: float = 2.0,
    atr_period: int = 14,
    volume_sma_period: int = 20,
) -> Dict[str, Any]:
    """
    Calculate all technical indicators for a stock.

    Args:
        df: DataFrame with columns: time, open, high, low, close, volume
        rsi_period: RSI period (default: 14)
        macd_fast: MACD fast period (default: 12)
        macd_slow: MACD slow period (default: 26)
        macd_signal: MACD signal period (default: 9)
        sma_periods: List of SMA periods (default: [20, 50, 200])
        bb_period: Bollinger Bands period (default: 20)
        bb_std_dev: Bollinger Bands standard deviation (default: 2.0)
        atr_period: ATR period (default: 14)
        volume_sma_period: Volume SMA period (default: 20)

    Returns:
        Dictionary of indicator names to latest values (or None if insufficient data)
    """
    if len(df) < max(sma_periods):
        logger.warning(f"Insufficient data: {len(df)} bars, need at least {max(sma_periods)}")
        return {}

    indicators = {}

    try:
        # =================================================================
        # PRICE AND VOLUME (needed by decision-engine rules)
        # =================================================================
        indicators['close'] = float(df['close'].iloc[-1])
        indicators['volume'] = float(df['volume'].iloc[-1])

        # Volume SMA (for volume confirmation in rules)
        if 'volume' in df.columns and len(df) >= volume_sma_period:
            volume_sma = calculate_volume_sma(df['volume'], period=volume_sma_period)
            if not volume_sma.empty and not pd.isna(volume_sma.iloc[-1]):
                indicators['volume_sma_20'] = float(volume_sma.iloc[-1])

        # =================================================================
        # TECHNICAL INDICATORS
        # =================================================================

        # RSI
        rsi = calculate_rsi(df['close'], period=rsi_period)
        if not rsi.empty and not pd.isna(rsi.iloc[-1]):
            indicators['RSI_14'] = float(rsi.iloc[-1])

        # MACD
        macd_data = calculate_macd(df['close'], fast=macd_fast, slow=macd_slow, signal=macd_signal)
        if not macd_data['macd'].empty and not pd.isna(macd_data['macd'].iloc[-1]):
            indicators['MACD'] = float(macd_data['macd'].iloc[-1])
            indicators['MACD_SIGNAL'] = float(macd_data['signal'].iloc[-1])
            indicators['MACD_HISTOGRAM'] = float(macd_data['histogram'].iloc[-1])

        # SMAs
        for period in sma_periods:
            if len(df) >= period:
                sma = calculate_sma(df['close'], period=period)
                if not sma.empty and not pd.isna(sma.iloc[-1]):
                    indicators[f'SMA_{period}'] = float(sma.iloc[-1])

        # Bollinger Bands
        bb = calculate_bollinger_bands(df['close'], period=bb_period, std_dev=bb_std_dev)
        if not bb['upper'].empty and not pd.isna(bb['upper'].iloc[-1]):
            indicators['BB_UPPER'] = float(bb['upper'].iloc[-1])
            indicators['BB_MIDDLE'] = float(bb['middle'].iloc[-1])
            indicators['BB_LOWER'] = float(bb['lower'].iloc[-1])

        # ATR
        atr = calculate_atr(df['high'], df['low'], df['close'], period=atr_period)
        if not atr.empty and not pd.isna(atr.iloc[-1]):
            indicators['ATR_14'] = float(atr.iloc[-1])

    except Exception as e:
        logger.error(f"Error calculating indicators: {e}", exc_info=True)
        return {}

    return indicators
