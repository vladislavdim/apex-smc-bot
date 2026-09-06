import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BOT = (ROOT / "bot.py").read_text(encoding="utf-8")
MARKET = (ROOT / "market.py").read_text(encoding="utf-8")
STATS = (ROOT / "stats_server.py").read_text(encoding="utf-8")


class NumericDiagnosticsAndMtfThroughputTests(unittest.TestCase):
    def test_mtf_rotating_batch_is_one_third_and_timeout_is_not_relaxed(self):
        self.assertIn('_take_strategy_round_batch, "MTF", universe, (len(universe) + 2) // 3, DB_PATH', BOT)
        self.assertNotIn('_take_strategy_round_batch, "MTF", universe, (len(universe) + 1) // 2, DB_PATH', BOT)
        self.assertIn('_run_market_scan_exclusive("auto_scan_1h", _auto_scan_1h_impl, 210)', BOT)

    def test_mtf_rr_contract_has_floor_and_no_upper_cap_in_bot(self):
        self.assertIn("(_rr_val < 2.0)", BOT)
        self.assertNotIn("not 2.0 <= _rr_val <= 4.0", BOT)
        self.assertNotIn("RR {_rr_val:.2f} вне диапазона 2.0–4.0", BOT)

    def test_numeric_telemetry_does_not_replace_existing_thresholds(self):
        self.assertIn('candle_body / candle_range >= 0.50', MARKET)
        self.assertIn('last_vol >= avg_vol * 1.20', MARKET)
        self.assertIn('distance <= atr1h * 0.75', MARKET)
        self.assertIn('range_low + range_size * 0.30', MARKET)
        self.assertIn('range_high - range_size * 0.30', MARKET)
        self.assertIn('_test_count > 2', MARKET)
        self.assertIn('c_body / c_range >= 0.5', MARKET)
        self.assertIn('_q_min = 3', MARKET)
        self.assertIn('_dist_range_too_wide = dist_range_pct >= 25', MARKET)

    def test_expected_numeric_payloads_are_observability_only(self):
        self.assertIn('_audit_observe("mtf_numeric"', BOT)
        self.assertIn('_audit_observe("swing_numeric"', MARKET)
        self.assertIn('_audit_observe("zone_numeric"', MARKET)
        self.assertIn('"old_range_under_25"', MARKET)
        self.assertIn('"structural_box_under_25"', MARKET)

    def test_strategy_lab_aggregates_numeric_metrics_and_shadow_comparison(self):
        self.assertIn('"numeric_telemetry":numeric_telemetry', STATS)
        self.assertIn('"wyckoff_shadow":wy_shadow', STATS)
        self.assertIn('Numeric funnel diagnostics', STATS)
        self.assertIn('structural-only', STATS)


if __name__ == "__main__":
    unittest.main()
