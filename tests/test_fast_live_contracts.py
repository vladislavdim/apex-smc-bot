import unittest

from apex.compatibility.market_constants import FAST_PAIRS, SYMBOL_ALIASES, live_fast_pairs


class FastLiveContractsTests(unittest.TestCase):
    def test_delisted_symbol_is_skipped_while_other_reviewed_pairs_continue(self):
        self.assertIn("POLUSDT", FAST_PAIRS)
        self.assertNotIn("MATICUSDT", FAST_PAIRS)
        self.assertEqual(SYMBOL_ALIASES["polygon"], "POLUSDT")
        self.assertEqual(
            live_fast_pairs({"BTCUSDT": {"price": 1}, "POLUSDT": {"price": 1},
                             "MATICUSDT": {"price": 1}}),
            ["BTCUSDT", "POLUSDT"],
        )

    def test_missing_gate_market_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "Gate live FAST universe unavailable"):
            live_fast_pairs({})


if __name__ == "__main__":
    unittest.main()
