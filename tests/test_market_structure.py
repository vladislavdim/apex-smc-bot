import random
import unittest

from core.market_structure import (
    analyze_market_structure,
    classify_swings,
)


def _candle(open_, high, low, close, timestamp):
    return {
        "open": float(open_),
        "high": float(high),
        "low": float(low),
        "close": float(close),
        "volume": 10.0,
        "timestamp": timestamp,
    }


def _mirror(candles, axis=220.0):
    return [
        _candle(
            axis - candle["open"],
            axis - candle["low"],
            axis - candle["high"],
            axis - candle["close"],
            candle["timestamp"],
        )
        for candle in candles
    ]


def _bullish_bos_candles():
    return [
        _candle(105, 106, 104, 105, 0),
        _candle(106, 108, 105, 107, 1),
        _candle(108, 110, 107, 109, 2),  # first high
        _candle(106, 107, 104, 105, 3),
        _candle(102, 103, 100, 101, 4),  # first low
        _candle(107, 109, 106, 108, 5),
        _candle(110, 112, 109, 111, 6),  # HH
        _candle(107, 108, 105, 106, 7),
        _candle(105, 106, 103, 104, 8),  # HL
        _candle(108, 111, 107, 110, 9),
        _candle(110, 114, 109, 113, 10),  # close through HH
    ]


def _bullish_choch_candles():
    return [
        _candle(107, 108, 106, 107, 0),
        _candle(109, 111, 108, 110, 1),
        _candle(111, 112, 110, 111, 2),  # first high
        _candle(105, 108, 103, 106, 3),
        _candle(101, 103, 100, 101, 4),  # first low
        _candle(106, 109, 105, 108, 5),
        _candle(109, 110, 108, 109, 6),  # LH
        _candle(103, 106, 101, 104, 7),
        _candle(99, 102, 98, 99, 8),     # LL
        _candle(104, 109, 103, 108, 9),
        _candle(108, 112, 107, 111, 10),  # close through LH
    ]


class MarketStructureTests(unittest.TestCase):
    def test_first_pivots_are_neutral_not_bullish_votes(self):
        classified = classify_swings([(2, 110.0)], [(4, 100.0)])
        self.assertEqual([s["kind"] for s in classified], ["H", "L"])

    def test_bullish_bos_has_a_mirrored_bearish_bos(self):
        bullish = analyze_market_structure(
            _bullish_bos_candles(), swing_lookback=1, max_break_age=1,
        )
        bearish = analyze_market_structure(
            _mirror(_bullish_bos_candles()), swing_lookback=1, max_break_age=1,
        )

        self.assertEqual(bullish["direction"], "BULLISH")
        self.assertEqual(bullish["event"]["type"], "BOS")
        self.assertEqual(bullish["event"]["direction"], "BULLISH")
        self.assertEqual(bearish["direction"], "BEARISH")
        self.assertEqual(bearish["event"]["type"], "BOS")
        self.assertEqual(bearish["event"]["direction"], "BEARISH")

    def test_bullish_choch_has_a_mirrored_bearish_choch(self):
        bullish = analyze_market_structure(
            _bullish_choch_candles(), swing_lookback=1, max_break_age=1,
        )
        bearish = analyze_market_structure(
            _mirror(_bullish_choch_candles()), swing_lookback=1, max_break_age=1,
        )

        self.assertEqual(bullish["event"]["type"], "CHoCH")
        self.assertEqual(bullish["event"]["prior_direction"], "BEARISH")
        self.assertEqual(bullish["direction"], "BULLISH")
        self.assertEqual(bearish["event"]["type"], "CHoCH")
        self.assertEqual(bearish["event"]["prior_direction"], "BULLISH")
        self.assertEqual(bearish["direction"], "BEARISH")

    def test_transitional_higher_low_does_not_erase_prior_bearish_structure(self):
        candles = _bullish_choch_candles()[:-2]
        candles.extend([
            _candle(103, 108, 102, 106, 9),
            _candle(104, 105, 100, 101, 10),  # internal HL, trend not reversed yet
            _candle(102, 109, 101, 108, 11),
            _candle(108, 112, 107, 111, 12),  # closes through protected LH
        ])
        result = analyze_market_structure(candles, swing_lookback=1, max_break_age=1)
        self.assertEqual(result["event"]["type"], "CHoCH")
        self.assertEqual(result["event"]["prior_direction"], "BEARISH")
        self.assertEqual(result["event"]["direction"], "BULLISH")

    def test_wick_through_level_does_not_confirm_structure(self):
        candles = _bullish_bos_candles()
        candles[-1] = _candle(110, 114, 109, 111.5, 10)
        result = analyze_market_structure(candles, swing_lookback=1, max_break_age=1)
        self.assertIsNone(result["event"])

    def test_mixed_structure_stays_unresolved(self):
        candles = _bullish_bos_candles()
        candles[6] = _candle(108, 109, 107, 108, 6)  # lower high
        candles[9] = _candle(107, 108, 106, 107, 9)
        candles[-1] = _candle(108, 109, 107, 108, 10)
        result = analyze_market_structure(candles, swing_lookback=1, max_break_age=1)
        self.assertIsNone(result["direction"])
        self.assertIsNone(result["event"])

    def test_event_contains_auditable_closed_candle_metadata(self):
        result = analyze_market_structure(
            _bullish_bos_candles(), swing_lookback=1, max_break_age=1,
        )
        event = result["event"]
        self.assertTrue(event["closed"])
        self.assertEqual(event["candle_time"], 10)
        self.assertEqual(event["candle_index"], 10)
        self.assertEqual(event["level"], 112.0)
        self.assertEqual(event["pivot_index"], 6)

    def test_stale_break_is_not_reused_as_a_fresh_trigger(self):
        candles = _bullish_bos_candles()
        candles.append(_candle(113, 115, 112, 114, 11))
        result = analyze_market_structure(candles, swing_lookback=1, max_break_age=1)
        self.assertIsNone(result["event"])

    def test_random_price_paths_remain_directionally_symmetric(self):
        rng = random.Random(49)
        for sample in range(100):
            price = 100.0
            candles = []
            for idx in range(40):
                open_ = price
                close = open_ + rng.uniform(-3.0, 3.0)
                high = max(open_, close) + rng.uniform(0.1, 1.5)
                low = min(open_, close) - rng.uniform(0.1, 1.5)
                candles.append(_candle(open_, high, low, close, idx))
                price = close

            original = analyze_market_structure(candles, swing_lookback=2, max_break_age=3)
            mirrored = analyze_market_structure(_mirror(candles), swing_lookback=2, max_break_age=3)
            opposite = {"BULLISH": "BEARISH", "BEARISH": "BULLISH", None: None}
            self.assertEqual(
                mirrored["direction"], opposite[original["direction"]], msg=f"sample={sample}",
            )
            if original["event"] is None:
                self.assertIsNone(mirrored["event"], msg=f"sample={sample}")
            else:
                self.assertIsNotNone(mirrored["event"], msg=f"sample={sample}")
                self.assertEqual(mirrored["event"]["type"], original["event"]["type"])
                self.assertEqual(
                    mirrored["event"]["direction"],
                    opposite[original["event"]["direction"]],
                )


if __name__ == "__main__":
    unittest.main()
