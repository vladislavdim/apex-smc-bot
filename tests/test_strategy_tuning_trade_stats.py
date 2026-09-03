import unittest
from pathlib import Path


class StrategyTuningTradeStatsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.market = Path("market.py").read_text(encoding="utf-8")
        cls.stats = Path("stats_server.py").read_text(encoding="utf-8")

    def test_swing_is_relaxed_only_at_initial_structure_gate(self):
        self.assertIn("find_swings(candles, lookback=7)", self.market)
        self.assertIn("get_bos_choch_event", self.market)
        self.assertIn("Variant 2: минимум RR 2.0", self.market)

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


if __name__ == "__main__":
    unittest.main()
