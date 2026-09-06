import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BOT = (ROOT / "bot.py").read_text(encoding="utf-8")
MARKET = (ROOT / "market.py").read_text(encoding="utf-8")
STATS = (ROOT / "stats_server.py").read_text(encoding="utf-8")


class StrategyLabTerminalTruthTests(unittest.TestCase):
    def test_mtf_pending_ltf_is_instrumented_before_existing_return(self):
        marker = "'MTF_PASSIVE_LTF_BOS'"
        pending = 'if passive_watch and not _mtf_score_bos:'
        hard_gate = "(_mtf_score < 2 or not _mtf_score_bos)"
        self.assertIn(marker, BOT)
        self.assertLess(BOT.index(marker), BOT.index(pending, BOT.index(marker)))
        self.assertLess(BOT.index(pending, BOT.index(marker)), BOT.index(hard_gate))
        self.assertIn('"_pending_ltf": True', BOT)

    def test_mtf_trading_thresholds_are_unchanged(self):
        self.assertIn("(_rr_val < 2.0)", BOT)
        self.assertIn("abs(price - entry) > _atr_entry * 0.75", BOT)
        self.assertIn("(_mtf_score < 2 or not _mtf_score_bos)", BOT)
        self.assertIn('get_bos_choch_event(_c15m_m, direction, lookback=15, max_break_age=1)', BOT)

    def test_swing_raw_and_directional_displacement_are_separate(self):
        self.assertIn('"displacement_body_ratio"', MARKET)
        self.assertIn('"directional_displacement_ratio"', MARKET)
        self.assertIn('if candle_range > 0 and direction_ok else 0.0', MARKET)
        self.assertIn('"displacement_gate_pass": bool(out["displacement_ok"])', MARKET)
        # Existing trade gate is unchanged: body ratio AND correct candle direction.
        self.assertIn('candle_body / candle_range >= 0.50 and direction_ok', MARKET)

    def test_strategy_lab_surfaces_terminal_truth(self):
        self.assertIn('"steps": ordered[:30]', STATS)
        self.assertIn('"pending_ltf": sum(str(x.get("outcome")', STATS)
        self.assertIn('"pending_ltf":sum(r.get("outcome")=="PENDING_LTF"', STATS)
        self.assertIn("['Ждут LTF',s.pending_ltf||0]", STATS)
        self.assertIn('→ ждут LTF ${f.pending_ltf||0}', STATS)
        self.assertIn('directional_displacement_ratio', STATS)

    def test_wyckoff_is_not_tuned_in_this_pr(self):
        self.assertIn('_dist_range_too_wide = dist_range_pct >= 25', MARKET)
        self.assertIn("'WYCKOFF Distribution: 30d range < 25%'", MARKET)


if __name__ == "__main__":
    unittest.main()
