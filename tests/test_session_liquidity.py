import ast
import logging
import os
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_function():
    path = os.path.join(ROOT, "market.py")
    with open(path, "r", encoding="utf-8") as source:
        tree = ast.parse(source.read(), filename=path)
    node = next(
        item for item in tree.body
        if isinstance(item, ast.FunctionDef) and item.name == "check_session_liquidity"
    )
    namespace = {"logging": logging, "_liquidity_cache": {}}
    exec(compile(ast.Module(body=[node], type_ignores=[]), path, "exec"), namespace)
    return namespace


def _candles(closed_volume=100.0, live_volume=1.0):
    rows = [{"volume": 100.0} for _ in range(20)]
    rows.append({"volume": closed_volume})
    rows.append({"volume": live_volume})
    return rows


class SessionLiquidityTests(unittest.TestCase):
    def setUp(self):
        self.ns = _load_function()

    def test_ignores_partial_live_candle_volume(self):
        self.ns["get_candles"] = lambda *_args, **_kwargs: _candles(
            closed_volume=100.0,
            live_volume=1.0,
        )

        result = self.ns["check_session_liquidity"]("BTCUSDT", "1h")

        self.assertTrue(result["ok"])
        self.assertEqual(result["ratio"], 1.0)

    def test_blocks_genuinely_weak_closed_candle(self):
        self.ns["get_candles"] = lambda *_args, **_kwargs: _candles(
            closed_volume=50.0,
            live_volume=500.0,
        )

        result = self.ns["check_session_liquidity"]("BTCUSDT", "1h")

        self.assertFalse(result["ok"])
        self.assertEqual(result["ratio"], 0.5)

    def test_insufficient_history_does_not_false_block(self):
        self.ns["get_candles"] = lambda *_args, **_kwargs: _candles()[:10]

        result = self.ns["check_session_liquidity"]("BTCUSDT", "1h")

        self.assertTrue(result["ok"])
        self.assertEqual(result["ratio"], 1.0)


if __name__ == "__main__":
    unittest.main()
