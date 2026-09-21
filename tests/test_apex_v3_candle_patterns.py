import unittest

from apex.market.candle_patterns import detect_engulfing


class CandlePatternTests(unittest.TestCase):
    def test_bullish_engulfing_is_detected(self):
        candles = [
            {"open": 10.0, "close": 9.0},
            {"open": 8.5, "close": 10.5},
        ]
        self.assertTrue(detect_engulfing(candles, "BULLISH"))
        self.assertFalse(detect_engulfing(candles, "BEARISH"))

    def test_bearish_engulfing_is_detected(self):
        candles = [
            {"open": 9.0, "close": 10.0},
            {"open": 10.5, "close": 8.5},
        ]
        self.assertTrue(detect_engulfing(candles, "BEARISH"))

    def test_equal_body_or_doji_is_not_engulfing(self):
        equal_body = [
            {"open": 10.0, "close": 9.0},
            {"open": 9.0, "close": 10.0},
        ]
        doji_previous = [
            {"open": 10.0, "close": 10.0},
            {"open": 9.0, "close": 11.0},
        ]
        self.assertFalse(detect_engulfing(equal_body, "BULLISH"))
        self.assertFalse(detect_engulfing(doji_previous, "BULLISH"))

    def test_only_latest_three_candles_are_considered(self):
        old_pattern = [
            {"open": 10.0, "close": 9.0},
            {"open": 8.5, "close": 10.5},
            {"open": 10.0, "close": 10.2},
            {"open": 10.2, "close": 10.3},
            {"open": 10.3, "close": 10.4},
        ]
        self.assertFalse(detect_engulfing(old_pattern, "BULLISH"))


if __name__ == "__main__":
    unittest.main()
