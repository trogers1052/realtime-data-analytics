"""Extended indicator tests — Bollinger Bands, Stochastic, ADX, EMA validation
plus data length edge cases and individual function smoke tests."""

import math
import unittest
from unittest.mock import patch

try:
    import pandas as pd
    import numpy as np
    from analytics.indicators import (
        calculate_all_indicators,
        calculate_rsi,
        calculate_sma,
        calculate_ema,
        calculate_macd,
        calculate_bollinger_bands,
        calculate_atr,
        calculate_stochastic,
        calculate_adx,
        calculate_volume_sma,
    )
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


def _make_df(n=250, close_val=100.0, with_trend=False):
    """Return a minimal OHLCV DataFrame with `n` rows."""
    if with_trend:
        closes = [close_val + i * 0.1 for i in range(n)]
    else:
        closes = [close_val] * n
    return pd.DataFrame({
        "time": pd.date_range("2020-01-01", periods=n, freq="D"),
        "open": [c - 0.5 for c in closes],
        "high": [c + 1 for c in closes],
        "low": [c - 1 for c in closes],
        "close": closes,
        "volume": [1_000_000] * n,
    })


# ---------------------------------------------------------------------------
# Bollinger Bands validation
# ---------------------------------------------------------------------------

@unittest.skipUnless(PANDAS_AVAILABLE, "pandas/pandas-ta not installed")
class TestBollingerBandsValidation(unittest.TestCase):
    def _patch_bb(self, upper, middle, lower):
        fake = {
            "upper": pd.Series([upper] * 250),
            "middle": pd.Series([middle] * 250),
            "lower": pd.Series([lower] * 250),
        }
        return patch("analytics.indicators.calculate_bollinger_bands", return_value=fake)

    def test_valid_bb_included(self):
        with self._patch_bb(110.0, 100.0, 90.0):
            result = calculate_all_indicators(_make_df())
        self.assertIn("BB_UPPER", result)
        self.assertIn("BB_MIDDLE", result)
        self.assertIn("BB_LOWER", result)
        self.assertAlmostEqual(result["BB_UPPER"], 110.0)
        self.assertAlmostEqual(result["BB_MIDDLE"], 100.0)
        self.assertAlmostEqual(result["BB_LOWER"], 90.0)

    def test_upper_less_than_middle_omitted(self):
        with self._patch_bb(90.0, 100.0, 80.0):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("BB_UPPER", result)

    def test_middle_less_than_lower_omitted(self):
        with self._patch_bb(110.0, 80.0, 90.0):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("BB_UPPER", result)

    def test_inf_upper_omitted(self):
        with self._patch_bb(float("inf"), 100.0, 90.0):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("BB_UPPER", result)

    def test_nan_lower_omitted(self):
        with self._patch_bb(110.0, 100.0, float("nan")):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("BB_LOWER", result)

    def test_equal_bands_allowed(self):
        with self._patch_bb(100.0, 100.0, 100.0):
            result = calculate_all_indicators(_make_df())
        self.assertIn("BB_UPPER", result)


# ---------------------------------------------------------------------------
# Stochastic validation
# ---------------------------------------------------------------------------

@unittest.skipUnless(PANDAS_AVAILABLE, "pandas/pandas-ta not installed")
class TestStochasticValidation(unittest.TestCase):
    def _patch_stoch(self, k_val, d_val):
        fake = {
            "k": pd.Series([k_val] * 250),
            "d": pd.Series([d_val] * 250),
        }
        return patch("analytics.indicators.calculate_stochastic", return_value=fake)

    def test_valid_stochastic_included(self):
        with self._patch_stoch(75.0, 70.0):
            result = calculate_all_indicators(_make_df())
        self.assertIn("STOCH_K", result)
        self.assertIn("STOCH_D", result)

    def test_k_above_100_omits_both(self):
        with self._patch_stoch(101.0, 50.0):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("STOCH_K", result)
        self.assertNotIn("STOCH_D", result)

    def test_d_below_0_omits_both(self):
        with self._patch_stoch(50.0, -1.0):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("STOCH_K", result)
        self.assertNotIn("STOCH_D", result)

    def test_nan_k_omits_both(self):
        with self._patch_stoch(float("nan"), 50.0):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("STOCH_K", result)
        self.assertNotIn("STOCH_D", result)

    def test_boundary_values_0_and_100(self):
        with self._patch_stoch(0.0, 100.0):
            result = calculate_all_indicators(_make_df())
        self.assertIn("STOCH_K", result)
        self.assertIn("STOCH_D", result)


# ---------------------------------------------------------------------------
# ADX validation
# ---------------------------------------------------------------------------

@unittest.skipUnless(PANDAS_AVAILABLE, "pandas/pandas-ta not installed")
class TestADXValidation(unittest.TestCase):
    def _patch_adx(self, adx, dmp, dmn):
        fake = {
            "adx": pd.Series([adx] * 250),
            "dmp": pd.Series([dmp] * 250),
            "dmn": pd.Series([dmn] * 250),
        }
        return patch("analytics.indicators.calculate_adx", return_value=fake)

    def test_valid_adx_included(self):
        with self._patch_adx(25.0, 30.0, 15.0):
            result = calculate_all_indicators(_make_df())
        self.assertIn("ADX_14", result)
        self.assertIn("DMP_14", result)
        self.assertIn("DMN_14", result)

    def test_adx_above_100_omits_all(self):
        with self._patch_adx(101.0, 30.0, 15.0):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("ADX_14", result)

    def test_adx_below_0_omits_all(self):
        with self._patch_adx(-1.0, 30.0, 15.0):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("ADX_14", result)

    def test_inf_dmp_omits_all(self):
        with self._patch_adx(25.0, float("inf"), 15.0):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("ADX_14", result)

    def test_nan_dmn_omits_all(self):
        with self._patch_adx(25.0, 30.0, float("nan")):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("ADX_14", result)


# ---------------------------------------------------------------------------
# EMA validation
# ---------------------------------------------------------------------------

@unittest.skipUnless(PANDAS_AVAILABLE, "pandas/pandas-ta not installed")
class TestEMAValidation(unittest.TestCase):
    def _patch_ema(self, val):
        series = pd.Series([val] * 250)
        return patch("analytics.indicators.calculate_ema", return_value=series)

    def test_valid_ema_included(self):
        with self._patch_ema(100.0):
            result = calculate_all_indicators(_make_df())
        self.assertIn("EMA_9", result)
        self.assertIn("EMA_21", result)

    def test_zero_ema_omitted(self):
        with self._patch_ema(0.0):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("EMA_9", result)

    def test_negative_ema_omitted(self):
        with self._patch_ema(-5.0):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("EMA_9", result)

    def test_inf_ema_omitted(self):
        with self._patch_ema(float("inf")):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("EMA_9", result)


# ---------------------------------------------------------------------------
# Volume SMA validation
# ---------------------------------------------------------------------------

@unittest.skipUnless(PANDAS_AVAILABLE, "pandas/pandas-ta not installed")
class TestVolumeSMAValidation(unittest.TestCase):
    def test_volume_sma_present_on_normal_data(self):
        result = calculate_all_indicators(_make_df())
        self.assertIn("volume_sma_20", result)
        self.assertGreater(result["volume_sma_20"], 0)


# ---------------------------------------------------------------------------
# Data length edge cases
# ---------------------------------------------------------------------------

@unittest.skipUnless(PANDAS_AVAILABLE, "pandas/pandas-ta not installed")
class TestDataLengthEdgeCases(unittest.TestCase):
    def test_too_few_bars_returns_empty(self):
        result = calculate_all_indicators(_make_df(n=50), sma_periods=[200])
        self.assertEqual(result, {})

    def test_exactly_max_sma_period_bars(self):
        result = calculate_all_indicators(_make_df(n=200), sma_periods=[200])
        self.assertIn("close", result)
        # SMA_200 should be present with exactly 200 bars
        if "SMA_200" in result:
            self.assertGreater(result["SMA_200"], 0)

    def test_sma_only_for_periods_with_enough_data(self):
        # calculate_all_indicators short-circuits to {} when there are fewer
        # bars than the LARGEST requested SMA period (guard in indicators.py).
        # With enough bars for every requested period, the SMAs are present.
        result = calculate_all_indicators(_make_df(n=60), sma_periods=[20, 50])
        self.assertIn("SMA_20", result)
        self.assertIn("SMA_50", result)

        # Not enough bars for the largest period -> entire result is empty.
        empty = calculate_all_indicators(_make_df(n=60), sma_periods=[20, 50, 200])
        self.assertEqual(empty, {})

    def test_custom_periods(self):
        # Non-default periods must be reflected in the OUTPUT KEY NAMES so the
        # data is not mislabeled for downstream consumers. A 7-period RSI must
        # be published as "RSI_7", never as "RSI_14".
        result = calculate_all_indicators(
            _make_df(n=250, with_trend=True),
            sma_periods=[10, 50, 100],
            ema_periods=[5, 10],
            rsi_period=7,
            atr_period=10,
            adx_period=20,
        )
        self.assertIn("close", result)

        # RSI key reflects the configured period, and the default name is absent.
        self.assertIn("RSI_7", result)
        self.assertNotIn("RSI_14", result)

        # ATR key reflects the configured period.
        self.assertIn("ATR_10", result)
        self.assertNotIn("ATR_14", result)

        # ADX / DMP / DMN keys reflect the configured period.
        self.assertIn("ADX_20", result)
        self.assertIn("DMP_20", result)
        self.assertIn("DMN_20", result)
        self.assertNotIn("ADX_14", result)
        self.assertNotIn("DMP_14", result)
        self.assertNotIn("DMN_14", result)

    def test_default_periods_keep_canonical_key_names(self):
        # At default config the canonical key names MUST be byte-identical to
        # what downstream consumers (decision-engine) already expect.
        result = calculate_all_indicators(_make_df(n=250, with_trend=True))
        self.assertIn("RSI_14", result)
        self.assertIn("ATR_14", result)
        self.assertIn("ADX_14", result)
        self.assertIn("DMP_14", result)
        self.assertIn("DMN_14", result)
        self.assertIn("STOCH_K", result)
        self.assertIn("STOCH_D", result)
        # Period-suffixed variants of default keys must NOT appear.
        self.assertNotIn("RSI_7", result)
        self.assertNotIn("ATR_10", result)
        self.assertNotIn("ADX_20", result)


# ---------------------------------------------------------------------------
# Individual indicator function smoke tests
# ---------------------------------------------------------------------------

@unittest.skipUnless(PANDAS_AVAILABLE, "pandas/pandas-ta not installed")
class TestIndividualIndicatorFunctions(unittest.TestCase):
    """Verify individual functions return sensible shapes and values."""

    def setUp(self):
        self.df = _make_df(n=100, close_val=50.0, with_trend=True)
        self.close = self.df["close"]
        self.high = self.df["high"]
        self.low = self.df["low"]
        self.volume = self.df["volume"]

    def test_calculate_rsi_returns_series(self):
        result = calculate_rsi(self.close, period=14)
        self.assertIsInstance(result, pd.Series)
        self.assertEqual(len(result), 100)

    def test_calculate_sma_returns_series(self):
        result = calculate_sma(self.close, period=20)
        self.assertIsInstance(result, pd.Series)
        # Last value should be close to the recent close
        self.assertFalse(pd.isna(result.iloc[-1]))

    def test_calculate_ema_returns_series(self):
        result = calculate_ema(self.close, period=9)
        self.assertIsInstance(result, pd.Series)
        self.assertFalse(pd.isna(result.iloc[-1]))

    def test_calculate_macd_returns_dict(self):
        result = calculate_macd(self.close, fast=12, slow=26, signal=9)
        self.assertIn("macd", result)
        self.assertIn("signal", result)
        self.assertIn("histogram", result)
        for key in ("macd", "signal", "histogram"):
            self.assertIsInstance(result[key], pd.Series)

    def test_calculate_bollinger_bands_returns_dict(self):
        result = calculate_bollinger_bands(self.close, period=20, std_dev=2.0)
        self.assertIn("upper", result)
        self.assertIn("middle", result)
        self.assertIn("lower", result)
        # upper >= middle >= lower on the last value
        self.assertGreaterEqual(
            float(result["upper"].iloc[-1]),
            float(result["middle"].iloc[-1]),
        )
        self.assertGreaterEqual(
            float(result["middle"].iloc[-1]),
            float(result["lower"].iloc[-1]),
        )

    def test_calculate_atr_returns_series(self):
        result = calculate_atr(self.high, self.low, self.close, period=14)
        self.assertIsInstance(result, pd.Series)
        last = float(result.iloc[-1])
        self.assertTrue(math.isfinite(last))
        self.assertGreater(last, 0)

    def test_calculate_stochastic_returns_dict(self):
        result = calculate_stochastic(self.high, self.low, self.close)
        self.assertIn("k", result)
        self.assertIn("d", result)
        for key in ("k", "d"):
            self.assertIsInstance(result[key], pd.Series)

    def test_calculate_adx_returns_dict(self):
        result = calculate_adx(self.high, self.low, self.close, length=14)
        self.assertIn("adx", result)
        self.assertIn("dmp", result)
        self.assertIn("dmn", result)

    def test_calculate_volume_sma_returns_series(self):
        result = calculate_volume_sma(self.volume, period=20)
        self.assertIsInstance(result, pd.Series)
        last = float(result.iloc[-1])
        self.assertAlmostEqual(last, 1_000_000.0, places=0)


# ---------------------------------------------------------------------------
# Full pipeline integration — clean data produces all expected indicators
# ---------------------------------------------------------------------------

@unittest.skipUnless(PANDAS_AVAILABLE, "pandas/pandas-ta not installed")
class TestFullIndicatorSet(unittest.TestCase):
    def test_all_indicators_present_on_good_data(self):
        df = _make_df(n=250, close_val=100.0, with_trend=True)
        result = calculate_all_indicators(df)
        # Core indicators
        self.assertIn("close", result)
        self.assertIn("volume", result)
        self.assertIn("RSI_14", result)
        self.assertIn("MACD", result)
        self.assertIn("MACD_SIGNAL", result)
        self.assertIn("MACD_HISTOGRAM", result)
        self.assertIn("SMA_20", result)
        self.assertIn("SMA_50", result)
        self.assertIn("SMA_200", result)
        self.assertIn("BB_UPPER", result)
        self.assertIn("BB_MIDDLE", result)
        self.assertIn("BB_LOWER", result)
        self.assertIn("ATR_14", result)
        self.assertIn("EMA_9", result)
        self.assertIn("EMA_21", result)
        self.assertIn("STOCH_K", result)
        self.assertIn("STOCH_D", result)
        self.assertIn("ADX_14", result)
        self.assertIn("DMP_14", result)
        self.assertIn("DMN_14", result)
        self.assertIn("volume_sma_20", result)


if __name__ == "__main__":
    unittest.main()
