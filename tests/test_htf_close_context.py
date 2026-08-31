import unittest

from core.htf_close_context import (
    build_htf_close_context,
    format_htf_close_context,
    strategy_uses_htf_close_context,
)


def _loader(symbol, timeframe, limit):
    rows = [
        {"open": 100, "high": 120, "low": 90, "close": 110},
        {"open": 110, "high": 130, "low": 100, "close": 125},
        {"open": 125, "high": 140, "low": 120, "close": 135},
        {"open": 135, "high": 150, "low": 130, "close": 145},  # forming; must be ignored
    ]
    return rows


class HTFCloseContextTests(unittest.TestCase):
    def test_only_slow_strategies_use_context(self):
        self.assertTrue(strategy_uses_htf_close_context("MTF"))
        self.assertTrue(strategy_uses_htf_close_context("SWING A"))
        self.assertTrue(strategy_uses_htf_close_context("WYCKOFF"))
        self.assertFalse(strategy_uses_htf_close_context("FAST"))
        self.assertFalse(strategy_uses_htf_close_context("ZONE"))

    def test_uses_last_closed_not_forming_bar(self):
        ctx = build_htf_close_context("BTCUSDT", "SWING", _loader)
        self.assertEqual(ctx["weekly"]["close"], 135.0)
        self.assertEqual(ctx["weekly"]["previous_high"], 130.0)
        self.assertEqual(ctx["weekly"]["close_vs_previous_range"], "ABOVE_PREVIOUS_HIGH")
        self.assertEqual(ctx["monthly"]["close"], 135.0)

    def test_fast_has_no_loader_calls(self):
        calls = []
        def loader(*args):
            calls.append(args)
            return []
        ctx = build_htf_close_context("BTCUSDT", "FAST", loader)
        self.assertFalse(ctx["used"])
        self.assertEqual(calls, [])

    def test_prompt_marks_context_as_non_blocking(self):
        block = format_htf_close_context(build_htf_close_context("BTCUSDT", "MTF", _loader))
        self.assertIn("Context only", block)
        self.assertIn("WEEKLY", block)
        self.assertIn("MONTHLY", block)


if __name__ == '__main__':
    unittest.main()
