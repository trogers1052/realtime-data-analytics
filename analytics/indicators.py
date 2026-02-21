"""
Technical Indicator Calculations using pandas-ta.

Calculates RSI, MACD, SMA, Bollinger Bands, ATR, and other indicators.
"""

import math

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


def calculate_stochastic(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    k: int = 14,
    d: int = 3,
    smooth_k: int = 3,
) -> Dict[str, pd.Series]:
    """Calculate Stochastic Oscillator (%K and %D)."""
    stoch = ta.stoch(high=high, low=low, close=close, k=k, d=d, smooth_k=smooth_k)
    return {
        "k": stoch[f"STOCHk_{k}_{d}_{smooth_k}"],
        "d": stoch[f"STOCHd_{k}_{d}_{smooth_k}"],
    }


def calculate_adx(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    length: int = 14,
) -> Dict[str, pd.Series]:
    """Calculate Average Directional Index (ADX) with directional components."""
    adx_data = ta.adx(high=high, low=low, close=close, length=length)
    return {
        "adx": adx_data[f"ADX_{length}"],
        "dmp": adx_data[f"DMP_{length}"],
        "dmn": adx_data[f"DMN_{length}"],
    }


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
    ema_periods: list = [9, 21],
    bb_period: int = 20,
    bb_std_dev: float = 2.0,
    atr_period: int = 14,
    stoch_k: int = 14,
    stoch_d: int = 3,
    stoch_smooth_k: int = 3,
    adx_period: int = 14,
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
        # close is the anchor value — if it's invalid everything downstream
        # is meaningless, so treat that as a hard failure.
        # =================================================================
        close_val = float(df['close'].iloc[-1])
        if not math.isfinite(close_val) or close_val <= 0:
            logger.error(
                f"Invalid close price ({close_val}) — skipping indicator calculation"
            )
            return {}
        indicators['close'] = close_val

        volume_val = float(df['volume'].iloc[-1])
        if math.isfinite(volume_val) and volume_val >= 0:
            indicators['volume'] = volume_val
        else:
            logger.warning(f"Invalid volume ({volume_val}) — omitting from indicators")

        # Volume SMA (for volume confirmation in rules)
        if 'volume' in df.columns and len(df) >= volume_sma_period:
            volume_sma = calculate_volume_sma(df['volume'], period=volume_sma_period)
            if not volume_sma.empty and not pd.isna(volume_sma.iloc[-1]):
                vsma = float(volume_sma.iloc[-1])
                if math.isfinite(vsma) and vsma >= 0:
                    indicators['volume_sma_20'] = vsma

        # =================================================================
        # TECHNICAL INDICATORS
        # Each indicator is independently validated before being published.
        # Rules that require a missing indicator will skip gracefully.
        # =================================================================

        # RSI — must be in [0, 100]
        rsi = calculate_rsi(df['close'], period=rsi_period)
        if not rsi.empty and not pd.isna(rsi.iloc[-1]):
            rsi_val = float(rsi.iloc[-1])
            if math.isfinite(rsi_val) and 0.0 <= rsi_val <= 100.0:
                indicators['RSI_14'] = rsi_val
            else:
                logger.warning(f"RSI out of bounds ({rsi_val}) — omitting")

        # MACD — all three components must be finite; partial sets are useless
        macd_data = calculate_macd(df['close'], fast=macd_fast, slow=macd_slow, signal=macd_signal)
        if not macd_data['macd'].empty and not pd.isna(macd_data['macd'].iloc[-1]):
            macd_val = float(macd_data['macd'].iloc[-1])
            signal_val = float(macd_data['signal'].iloc[-1])
            hist_val = float(macd_data['histogram'].iloc[-1])
            if (math.isfinite(macd_val)
                    and math.isfinite(signal_val)
                    and math.isfinite(hist_val)):
                indicators['MACD'] = macd_val
                indicators['MACD_SIGNAL'] = signal_val
                indicators['MACD_HISTOGRAM'] = hist_val
            else:
                logger.warning(
                    f"MACD components not all finite "
                    f"(macd={macd_val}, signal={signal_val}, hist={hist_val}) — omitting"
                )

        # SMAs
        for period in sma_periods:
            if len(df) >= period:
                sma = calculate_sma(df['close'], period=period)
                if not sma.empty and not pd.isna(sma.iloc[-1]):
                    sma_val = float(sma.iloc[-1])
                    if math.isfinite(sma_val) and sma_val > 0:
                        indicators[f'SMA_{period}'] = sma_val

        # Bollinger Bands — require all three bands to be coherent
        bb = calculate_bollinger_bands(df['close'], period=bb_period, std_dev=bb_std_dev)
        if not bb['upper'].empty and not pd.isna(bb['upper'].iloc[-1]):
            upper = float(bb['upper'].iloc[-1])
            middle = float(bb['middle'].iloc[-1])
            lower = float(bb['lower'].iloc[-1])
            if (math.isfinite(upper) and math.isfinite(middle) and math.isfinite(lower)
                    and upper >= middle >= lower):
                indicators['BB_UPPER'] = upper
                indicators['BB_MIDDLE'] = middle
                indicators['BB_LOWER'] = lower

        # ATR — must be strictly positive; zero/negative ATR is nonsensical
        atr = calculate_atr(df['high'], df['low'], df['close'], period=atr_period)
        if not atr.empty and not pd.isna(atr.iloc[-1]):
            atr_val = float(atr.iloc[-1])
            if math.isfinite(atr_val) and atr_val > 0:
                indicators['ATR_14'] = atr_val
            else:
                logger.warning(f"ATR non-positive ({atr_val}) — omitting")

        # EMAs — must be positive (same validation as SMA)
        for period in ema_periods:
            if len(df) >= period:
                ema = calculate_ema(df['close'], period=period)
                if not ema.empty and not pd.isna(ema.iloc[-1]):
                    ema_val = float(ema.iloc[-1])
                    if math.isfinite(ema_val) and ema_val > 0:
                        indicators[f'EMA_{period}'] = ema_val

        # Stochastic — both %K and %D must be in [0, 100]; omit both if either invalid
        if len(df) >= stoch_k:
            stoch = calculate_stochastic(
                df['high'], df['low'], df['close'],
                k=stoch_k, d=stoch_d, smooth_k=stoch_smooth_k,
            )
            if not stoch['k'].empty and not pd.isna(stoch['k'].iloc[-1]):
                k_val = float(stoch['k'].iloc[-1])
                d_val = float(stoch['d'].iloc[-1])
                if (math.isfinite(k_val) and math.isfinite(d_val)
                        and 0.0 <= k_val <= 100.0 and 0.0 <= d_val <= 100.0):
                    indicators['STOCH_K'] = k_val
                    indicators['STOCH_D'] = d_val
                else:
                    logger.warning(
                        f"Stochastic out of bounds (K={k_val}, D={d_val}) — omitting"
                    )

        # ADX — must be in [0, 100]; directional components also validated
        if len(df) >= adx_period:
            adx_data = calculate_adx(df['high'], df['low'], df['close'], length=adx_period)
            if not adx_data['adx'].empty and not pd.isna(adx_data['adx'].iloc[-1]):
                adx_val = float(adx_data['adx'].iloc[-1])
                dmp_val = float(adx_data['dmp'].iloc[-1])
                dmn_val = float(adx_data['dmn'].iloc[-1])
                if (math.isfinite(adx_val) and 0.0 <= adx_val <= 100.0
                        and math.isfinite(dmp_val) and math.isfinite(dmn_val)):
                    indicators['ADX_14'] = adx_val
                    indicators['DMP_14'] = dmp_val
                    indicators['DMN_14'] = dmn_val
                else:
                    logger.warning(
                        f"ADX out of bounds (ADX={adx_val}, DM+={dmp_val}, DM-={dmn_val}) — omitting"
                    )

    except Exception as e:
        logger.error(f"Error calculating indicators: {e}", exc_info=True)
        return {}

    return indicators
