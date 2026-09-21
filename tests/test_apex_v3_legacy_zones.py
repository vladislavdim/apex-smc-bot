import unittest

from apex.market.legacy_zones import find_fvg, find_ob


class LegacyZoneLookupTests(unittest.TestCase):
    def test_bullish_order_block_uses_latest_bearish_body(self):
        candles = [
            {"open": 10, "close": 11},
            {"open": 12, "close": 10},
            {"open": 11, "close": 12},
            {"open": 12, "close": 13},
        ]
        self.assertEqual(find_ob(candles, "BULLISH"), {
            "top": 12, "bottom": 10, "index": 1,
        })

    def test_bearish_order_block_uses_latest_bullish_body(self):
        candles = [
            {"open": 10, "close": 9},
            {"open": 9, "close": 11},
            {"open": 11, "close": 10},
            {"open": 10, "close": 9},
        ]
        self.assertEqual(find_ob(candles, "BEARISH"), {
            "top": 11, "bottom": 9, "index": 1,
        })

    def test_bullish_fvg_keeps_original_three_candle_boundary(self):
        candles = [
            {"high": 8, "low": 7}, {"high": 10, "low": 9},
            {"high": 12, "low": 11}, {"high": 14, "low": 13},
            {"high": 15, "low": 14},
        ]
        self.assertEqual(find_fvg(candles, "BULLISH"), {
            "top": 13, "bottom": 10, "index": 2,
        })

    def test_missing_zone_remains_none(self):
        candles = [{"open": 1, "close": 1, "high": 1, "low": 1}] * 5
        self.assertIsNone(find_ob(candles, "BULLISH"))
        self.assertIsNone(find_fvg(candles, "BULLISH"))


if __name__ == "__main__":
    unittest.main()
