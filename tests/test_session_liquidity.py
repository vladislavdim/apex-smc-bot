import unittest

from apex.market.session_liquidity import SessionLiquidityProvider


def _candles(closed_volume=100.0, live_volume=1.0):
    rows = [{"volume": 100.0} for _ in range(20)]
    rows.append({"volume": closed_volume})
    rows.append({"volume": live_volume})
    return rows


class SessionLiquidityTests(unittest.TestCase):
    def test_ignores_partial_live_candle_volume(self):
        provider = SessionLiquidityProvider(lambda *_args, **_kwargs: _candles(
            closed_volume=100.0, live_volume=1.0,
        ))
        result = provider.check("BTCUSDT", "1h")

        self.assertTrue(result["ok"])
        self.assertEqual(result["ratio"], 1.0)

    def test_blocks_genuinely_weak_closed_candle(self):
        provider = SessionLiquidityProvider(lambda *_args, **_kwargs: _candles(
            closed_volume=50.0, live_volume=500.0,
        ))
        result = provider.check("BTCUSDT", "1h")

        self.assertFalse(result["ok"])
        self.assertEqual(result["ratio"], 0.5)

    def test_insufficient_history_does_not_false_block(self):
        provider = SessionLiquidityProvider(lambda *_args, **_kwargs: _candles()[:10])
        result = provider.check("BTCUSDT", "1h")

        self.assertTrue(result["ok"])
        self.assertEqual(result["ratio"], 1.0)


if __name__ == "__main__":
    unittest.main()
