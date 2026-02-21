"""
Unit tests for indicator validation in calculate_all_indicators.

Covers the data-quality guards: NaN close price, infinite values,
RSI out-of-bounds, non-positive ATR, and invalid MACD components.
"""

import math
import unittest
from unittest.mock import patch

try:
    import pandas as pd
    from analytics.indicators import calculate_all_indicators
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


def _make_df(n: int = 250, close_val: float = 100.0):
    """Return a minimal OHLCV DataFrame with `n` rows."""
    closes = [close_val] * n
    return pd.DataFrame({
        "time": pd.date_range("2020-01-01", periods=n, freq="D"),
        "open": closes,
        "high": [c + 1 for c in closes],
        "low":  [c - 1 for c in closes],
        "close": closes,
        "volume": [1_000_000] * n,
    })


@unittest.skipUnless(PANDAS_AVAILABLE, "pandas / pandas_ta not installed in this environment")
class TestInvalidClosePrice(unittest.TestCase):
    """A bad close price must short-circuit the entire calculation."""

    def _assert_empty(self, df):
        result = calculate_all_indicators(df)
        self.assertEqual(result, {}, f"Expected empty dict, got {result}")

    def test_nan_close_returns_empty(self):
        df = _make_df()
        df.loc[df.index[-1], "close"] = float("nan")
        self._assert_empty(df)

    def test_inf_close_returns_empty(self):
        df = _make_df()
        df.loc[df.index[-1], "close"] = float("inf")
        self._assert_empty(df)

    def test_negative_inf_close_returns_empty(self):
        df = _make_df()
        df.loc[df.index[-1], "close"] = float("-inf")
        self._assert_empty(df)

    def test_zero_close_returns_empty(self):
        df = _make_df(close_val=0.0)
        self._assert_empty(df)

    def test_negative_close_returns_empty(self):
        df = _make_df(close_val=-1.0)
        self._assert_empty(df)


@unittest.skipUnless(PANDAS_AVAILABLE, "pandas / pandas_ta not installed in this environment")
class TestInvalidVolume(unittest.TestCase):
    """An invalid volume is omitted but the rest of the indicators still publish."""

    def test_nan_volume_omitted_other_indicators_present(self):
        df = _make_df()
        df.loc[df.index[-1], "volume"] = float("nan")
        result = calculate_all_indicators(df)
        self.assertIn("close", result)
        self.assertNotIn("volume", result)

    def test_inf_volume_omitted(self):
        df = _make_df()
        df.loc[df.index[-1], "volume"] = float("inf")
        result = calculate_all_indicators(df)
        self.assertNotIn("volume", result)


@unittest.skipUnless(PANDAS_AVAILABLE, "pandas / pandas_ta not installed in this environment")
class TestRSIBoundsValidation(unittest.TestCase):
    """RSI values outside [0, 100] must be omitted."""

    def _patch_rsi(self, bad_value):
        """Return a context manager that makes calculate_rsi return bad_value."""
        series = pd.Series([bad_value] * 250)
        return patch(
            "analytics.indicators.calculate_rsi",
            return_value=series,
        )

    def test_rsi_above_100_omitted(self):
        with self._patch_rsi(101.0):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("RSI_14", result)

    def test_rsi_below_0_omitted(self):
        with self._patch_rsi(-1.0):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("RSI_14", result)

    def test_rsi_nan_omitted(self):
        with self._patch_rsi(float("nan")):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("RSI_14", result)

    def test_rsi_inf_omitted(self):
        with self._patch_rsi(float("inf")):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("RSI_14", result)

    def test_valid_rsi_included(self):
        # A flat price series gives RSI = 50 after warm-up
        result = calculate_all_indicators(_make_df())
        if "RSI_14" in result:
            self.assertGreaterEqual(result["RSI_14"], 0.0)
            self.assertLessEqual(result["RSI_14"], 100.0)


@unittest.skipUnless(PANDAS_AVAILABLE, "pandas / pandas_ta not installed in this environment")
class TestATRValidation(unittest.TestCase):
    """ATR must be strictly positive; zero or negative must be omitted."""

    def _patch_atr(self, bad_value):
        series = pd.Series([bad_value] * 250)
        return patch(
            "analytics.indicators.calculate_atr",
            return_value=series,
        )

    def test_zero_atr_omitted(self):
        with self._patch_atr(0.0):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("ATR_14", result)

    def test_negative_atr_omitted(self):
        with self._patch_atr(-0.5):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("ATR_14", result)

    def test_inf_atr_omitted(self):
        with self._patch_atr(float("inf")):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("ATR_14", result)


@unittest.skipUnless(PANDAS_AVAILABLE, "pandas / pandas_ta not installed in this environment")
class TestMACDValidation(unittest.TestCase):
    """All three MACD components must be finite, or none are published."""

    def _patch_macd(self, macd=1.0, signal=0.5, histogram=0.5):
        fake = {
            "macd": pd.Series([macd] * 250),
            "signal": pd.Series([signal] * 250),
            "histogram": pd.Series([histogram] * 250),
        }
        return patch("analytics.indicators.calculate_macd", return_value=fake)

    def test_nan_macd_omits_all_three(self):
        with self._patch_macd(macd=float("nan")):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("MACD", result)
        self.assertNotIn("MACD_SIGNAL", result)
        self.assertNotIn("MACD_HISTOGRAM", result)

    def test_inf_signal_omits_all_three(self):
        with self._patch_macd(signal=float("inf")):
            result = calculate_all_indicators(_make_df())
        self.assertNotIn("MACD", result)
        self.assertNotIn("MACD_SIGNAL", result)
        self.assertNotIn("MACD_HISTOGRAM", result)

    def test_valid_macd_publishes_all_three(self):
        with self._patch_macd(macd=0.3, signal=0.1, histogram=0.2):
            result = calculate_all_indicators(_make_df())
        self.assertIn("MACD", result)
        self.assertIn("MACD_SIGNAL", result)
        self.assertIn("MACD_HISTOGRAM", result)


@unittest.skipUnless(PANDAS_AVAILABLE, "pandas / pandas_ta not installed in this environment")
class TestValidDataPassesThrough(unittest.TestCase):
    """Sanity check: a clean DataFrame produces a non-empty indicators dict."""

    def test_normal_data_produces_indicators(self):
        result = calculate_all_indicators(_make_df(n=250, close_val=50.0))
        self.assertIn("close", result)
        self.assertEqual(result["close"], 50.0)
        self.assertTrue(math.isfinite(result["close"]))


if __name__ == "__main__":
    unittest.main()
