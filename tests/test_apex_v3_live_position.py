import unittest

from apex.ui.live_position import LivePositionService


def _candles(count=30):
    return [
        {"open": 99.5, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 10.0}
        for _ in range(count)
    ]


class LivePositionServiceTests(unittest.TestCase):
    def service(self, candles):
        return LivePositionService(
            lambda *_args: candles,
            lambda _candles, lookback: ([(1, 102.0)], [(2, 98.0)]),
            lambda highs, lows: {"highs": highs, "lows": lows},
            lambda candles, classified: [{"direction": "BULLISH"}],
            lambda candles, direction: (
                {"bottom": 99.0, "top": 101.0}
                if direction == "BULLISH" else None
            ),
            lambda candles, direction: None,
            {"1h": "1 ЧАС"},
        )

    def test_bullish_zone_preserves_read_only_long_advice(self):
        result = self.service(_candles()).analyze("BTCUSDT", "1h")
        self.assertIn("BTCUSDT", result)
        self.assertIn("1 ЧАС", result)
        self.assertIn("✅ ВХОДИТЬ ЛОНГ", result)
        self.assertIn("Bull OB", result)
        self.assertIn("RR 1:2", result)

    def test_insufficient_candles_returns_no_analysis(self):
        self.assertIsNone(self.service(_candles(29)).analyze("BTCUSDT", "1h"))


if __name__ == "__main__":
    unittest.main()
