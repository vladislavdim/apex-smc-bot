import unittest

from apex.market.entry_timing import check_entry_timing


def _candle(open_price, high, low, close, volume=1.0):
    return {
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


class EntryTimingTests(unittest.TestCase):
    def test_short_history_preserves_legacy_fail_open_shape(self):
        self.assertEqual(check_entry_timing([], "BULLISH", 100.0), {
            "valid": True,
            "score": 0,
            "reasons": [],
            "wait": "",
        })

    def test_bullish_sweep_impulse_and_zone_score_three(self):
        candles = [
            _candle(10, 11, 9, 10),
            _candle(10, 11, 9, 10),
            _candle(9, 11, 8, 9.5),
            _candle(9.5, 10.5, 9, 10),
            _candle(9, 11.2, 8.8, 11, volume=2),
        ]
        result = check_entry_timing(candles, "BULLISH", 10.5, "1h")
        self.assertTrue(result["valid"])
        self.assertEqual(result["score"], 3)
        self.assertTrue(result["swept"])
        self.assertEqual(result["wait"], "")
        self.assertEqual(len(result["reasons"]), 3)

    def test_missing_sweep_and_impulse_preserves_wait_reason(self):
        candles = [_candle(10, 11, 9, 10) for _ in range(5)]
        result = check_entry_timing(candles, "BULLISH", 10.0)
        self.assertFalse(result["valid"])
        self.assertEqual(result["score"], 1)
        self.assertEqual(result["wait"], "Ждать ложного пробоя уровня")


if __name__ == "__main__":
    unittest.main()
