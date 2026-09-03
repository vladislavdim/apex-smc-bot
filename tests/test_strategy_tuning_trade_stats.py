import unittest
from pathlib import Path
from unittest.mock import patch

import stats_server


class StrategyTuningTradeStatsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.market = Path("market.py").read_text(encoding="utf-8")
        cls.stats = Path("stats_server.py").read_text(encoding="utf-8")

    def test_swing_retains_structure_and_final_rr_after_ltf_refinement(self):
        self.assertIn("find_swings(candles, lookback=7)", self.market)
        self.assertIn("get_bos_choch_event", self.market)
        self.assertIn("def _swing_build_ltf_entry", self.market)
        self.assertIn("LTF: fresh 1h BOS/CHoCH", self.market)
        self.assertIn("rr_check < 2.0 or rr_check > 4.0", self.market)

    def test_fast_retains_quality_stack(self):
        self.assertIn("_vol_threshold = 1.6", self.market)
        self.assertIn("curr_body / curr_range < 0.65", self.market)
        self.assertIn("not _acceptance", self.market)
        self.assertIn("not _fast_structure_event", self.market)
        self.assertIn("not 2.0 <= rr <= 4.0", self.market)

    def test_zone_retains_structure_quality_and_rr(self):
        self.assertIn("len(candles) < 40", self.market)
        self.assertIn("c_body / c_range >= 0.5", self.market)
        self.assertIn("q_score < _q_min", self.market)
        self.assertIn("not _zone_ltf_structure", self.market)
        self.assertIn("rr < 2.0", self.market)

    def test_trade_telemetry_is_passive_and_dashboard_only(self):
        self.assertIn("_emit_trade_stats_event", self.market)
        self.assertIn('"trade_event"', self.market)
        self.assertIn('"trade_stats":trade_stats', self.stats)
        self.assertIn("Статистика сделок", self.stats)
        self.assertIn("realized_r", self.stats)
        self.assertIn("pnl_pct", self.stats)

    def test_dashboard_aggregates_open_close_pnl_and_realized_r(self):
        events = [
            {
                "event_key": "trade:1:open:open",
                "kind": "trade_event",
                "strategy": "FAST",
                "symbol": "BTCUSDT",
                "occurred_at": "2026-09-03T10:00:00+00:00",
                "payload": {"action": "OPEN", "signal_id": 1},
            },
            {
                "event_key": "trade:1:close:tp1",
                "kind": "trade_event",
                "strategy": "FAST",
                "symbol": "BTCUSDT",
                "occurred_at": "2026-09-03T11:00:00+00:00",
                "payload": {
                    "action": "CLOSE",
                    "signal_id": 1,
                    "result": "tp1",
                    "pnl_pct": 2.5,
                    "realized_r": 2.0,
                },
            },
        ]
        with patch.object(stats_server, "_fetch", return_value=events):
            data = stats_server.build_dashboard(days=1)
        trade = data["trade_stats"]
        self.assertEqual(trade["opened"], 1)
        self.assertEqual(trade["closed"], 1)
        self.assertEqual(trade["wins"], 1)
        self.assertEqual(trade["losses"], 0)
        self.assertEqual(trade["win_rate"], 100.0)
        self.assertEqual(trade["pnl_pct"], 2.5)
        self.assertEqual(trade["avg_r"], 2.0)
        self.assertEqual(trade["by_strategy"][0]["strategy"], "FAST")


if __name__ == "__main__":
    unittest.main()
