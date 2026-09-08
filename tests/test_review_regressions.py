import unittest

from core.replay_lab import replay_three_tracks
from core.shadow_evidence import evaluate_shadow_rule
from core.strategy_diagnostics import funnel_snapshot
from external_sources.budget import plan_daily_load
from external_sources.gate_microstructure import OrderBookReducer


class ReviewRegressions(unittest.TestCase):
    def test_protection_only_affects_future_candles(self):
        snap = dict(entry=100, initial_sl=90, tp1=110, tp2=140, direction="BULLISH")
        candles = [dict(id="1", open=100, high=115, low=95, close=112),
                   dict(id="2", open=112, high=116, low=104, close=110),
                   dict(id="3", open=110, high=200, low=100, close=150)]
        result = replay_three_tracks(snap, candles, actual_actions={"1": {"action": "PROTECT", "protect_level": 105}})["ACTUAL"]
        self.assertEqual(result["exit_candle_id"], "2")
        self.assertEqual(result["last_candle_id"], "2")
        self.assertNotIn("TP2", result["targets_reached"])
        self.assertLess(result["mfe_r"], 2)

    def test_partial_percentage_is_weighted(self):
        snap = dict(entry=100, initial_sl=90, tp1=110, tp2=140, direction="BULLISH")
        candles = [dict(id="1", open=100, high=112, low=99, close=110),
                   dict(id="2", open=110, high=141, low=109, close=140)]
        result = replay_three_tracks(snap, candles, actual_actions={"1": {"action": "PARTIAL_EXIT", "fraction": .5}})["ACTUAL"]
        self.assertEqual(result["gross_pct"], 25)

    def test_fail_is_not_pass(self):
        steps = funnel_snapshot([{"stages": {"RR": "FAIL"}}])["steps"]
        self.assertEqual(next(x for x in steps if x["stage"] == "RR")["passed"], 0)

    def test_book_features_use_snapshot_levels(self):
        book = OrderBookReducer("BTCUSDT")
        book.apply_snapshot([[99, 2]], [[101, 3]], 1)
        self.assertEqual(book.features()["mid_price"], 100)
        self.assertEqual(book.features()["bid_depth"], 2)
        self.assertFalse(OrderBookReducer("BTCUSDT").apply_delta([[99, 2]], [[101, 3]], 1, 1))

    def test_evidence_requires_unique_closed_trades(self):
        row = dict(signal_id=1, strategy="FAST", rule_id="x", status="CLOSED", closed_at="2026-09-08", old_r=0, new_r=1)
        result = evaluate_shadow_rule([row] * 30, min_eligible=1)
        self.assertEqual(result["eligible"], 1)
        self.assertEqual(result["minimum_required"], 30)
        self.assertFalse(result["promotion_proposed"])
        self.assertFalse(evaluate_shadow_rule([{"old_r": 0, "new_r": 1}] * 30)["promotion_proposed"])

    def test_daily_fit_does_not_hide_minute_burst(self):
        result = plan_daily_load({"hyperliquid": dict(symbols=10, endpoints=1, interval_seconds=900, units_per_symbol=20)})["hyperliquid"]
        self.assertTrue(result["within_day"])
        self.assertFalse(result["within_minute"])
        self.assertEqual(result["status"], "EXCEEDS_LOCAL_LIMITS")


if __name__ == "__main__":
    unittest.main()
