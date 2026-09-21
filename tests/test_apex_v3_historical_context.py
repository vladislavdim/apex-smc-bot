import unittest
from unittest.mock import Mock

from apex.market.historical_context import (
    HistoricalContextProvider,
    format_historical_context,
)


def _candles(count=50):
    return [{
        "open": 100 + index, "high": 102 + index,
        "low": 99 + index, "close": 101 + index,
    } for index in range(count)]


class HistoricalContextTests(unittest.TestCase):
    def test_daily_history_preserves_trend_and_phase(self):
        reader = Mock(return_value=_candles())
        provider = HistoricalContextProvider(reader)
        result = provider.get_historical_context("BTCUSDT")
        self.assertEqual(result["trend_key"], "uptrend")
        self.assertEqual(result["phase_key"], "near_high")
        self.assertEqual(result["candles_count"], 50)
        reader.assert_called_once_with("BTCUSDT", "1d", 200)

    def test_short_daily_history_uses_four_hour_fallback(self):
        reader = Mock(side_effect=[_candles(10), _candles(20)])
        result = HistoricalContextProvider(reader).get_historical_context("ETHUSDT")
        self.assertEqual(result["candles_count"], 20)
        self.assertEqual(reader.call_args_list[-1].args, ("ETHUSDT", "4h", 200))

    def test_insufficient_history_remains_unknown(self):
        reader = Mock(side_effect=[[], []])
        self.assertIsNone(HistoricalContextProvider(reader).get_historical_context("BTCUSDT"))

    def test_formatter_does_not_change_context_geometry(self):
        historical = HistoricalContextProvider(Mock(return_value=_candles())).get_historical_context("BTCUSDT")
        rendered = format_historical_context("BTCUSDT", historical)
        self.assertIn("Исторический контекст (50 свечей)", rendered)
        self.assertIn("ВОСХОДЯЩИЙ", rendered)
        self.assertEqual(format_historical_context("BTCUSDT", None), "")


if __name__ == "__main__":
    unittest.main()
