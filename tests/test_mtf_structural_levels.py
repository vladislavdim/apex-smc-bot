import ast
import logging
import os
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_level_functions():
    """Load only the pure level functions without importing Telegram/Groq."""
    path = os.path.join(ROOT, "market.py")
    with open(path, "r", encoding="utf-8") as source:
        tree = ast.parse(source.read(), filename=path)
    wanted = {
        "average_true_range",
        "select_structural_targets",
        "smart_round",
        "calc_smart_levels",
    }
    nodes = [
        node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in wanted
    ]
    namespace = {"logging": logging}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), path, "exec"), namespace)
    return namespace


def _candles(count=50):
    return [
        {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 10.0}
        for _ in range(count)
    ]


class MtfStructuralLevelTests(unittest.TestCase):
    def setUp(self):
        self.ns = _load_level_functions()
        self.ns["get_liquidity_heatmap"] = lambda candles: {
            "levels": [], "nearest_buy_stops": None, "nearest_sell_stops": None,
        }
        self.ns["find_fvg"] = lambda candles, direction: None

    def test_long_uses_correct_ote_and_two_market_anchored_targets(self):
        self.ns["find_swings"] = lambda candles, lookback=8: (
            [(10, 105.0), (30, 110.0)], [(5, 92.0), (20, 95.0)],
        )
        self.ns["find_ob"] = lambda candles, direction: (
            {"bottom": 99.0, "top": 100.0, "index": 48}
            if direction == "BULLISH" else None
        )

        levels = self.ns["calc_smart_levels"](_candles(), "BULLISH", 99.5, "1h")

        self.assertIsNotNone(levels)
        self.assertAlmostEqual(levels["entry"], 99.425, places=3)
        self.assertAlmostEqual(levels["sl"], 94.5, places=3)
        self.assertEqual(levels["tp1"], 110.0)
        self.assertGreater(levels["tp2"], levels["tp1"])
        self.assertTrue(2.0 <= levels["rr"] <= 4.0)
        self.assertIn("fib_ote", levels["source"])

    def test_short_uses_correct_ote_and_structural_stop(self):
        self.ns["find_swings"] = lambda candles, lookback=8: (
            [(10, 110.0), (20, 105.0)], [(15, 100.0), (30, 90.0)],
        )
        self.ns["find_ob"] = lambda candles, direction: (
            {"bottom": 100.0, "top": 101.0, "index": 48}
            if direction == "BEARISH" else None
        )

        levels = self.ns["calc_smart_levels"](_candles(), "BEARISH", 100.5, "1h")

        self.assertIsNotNone(levels)
        self.assertAlmostEqual(levels["entry"], 100.575, places=3)
        self.assertAlmostEqual(levels["sl"], 105.5, places=3)
        self.assertEqual(levels["tp1"], 90.0)
        self.assertLess(levels["tp2"], levels["tp1"])
        self.assertTrue(2.0 <= levels["rr"] <= 4.0)

    def test_missing_fresh_zone_rejects_instead_of_fabricating_levels(self):
        self.ns["find_swings"] = lambda candles, lookback=8: (
            [(10, 105.0), (30, 110.0)], [(5, 92.0), (20, 95.0)],
        )
        self.ns["find_ob"] = lambda candles, direction: None

        levels = self.ns["calc_smart_levels"](_candles(), "BULLISH", 99.5, "1h")

        self.assertIsNone(levels)

    def test_long_stop_uses_sell_side_not_buy_side_liquidity(self):
        self.ns["find_swings"] = lambda candles, lookback=8: (
            [(10, 105.0), (30, 110.0)], [(5, 92.0), (20, 95.0)],
        )
        self.ns["find_ob"] = lambda candles, direction: (
            {"bottom": 99.0, "top": 100.0, "index": 48}
            if direction == "BULLISH" else None
        )
        self.ns["get_liquidity_heatmap"] = lambda candles: {
            "levels": [],
            "nearest_buy_stops": {"price": 80.0},
            "nearest_sell_stops": {"price": 94.5},
        }

        levels = self.ns["calc_smart_levels"](_candles(), "BULLISH", 99.5, "1h")

        self.assertIsNotNone(levels)
        self.assertAlmostEqual(levels["sl"], 94.0, places=3)
        self.assertGreater(levels["sl"], 90.0)


if __name__ == "__main__":
    unittest.main()
