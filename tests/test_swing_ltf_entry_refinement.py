import unittest
from pathlib import Path


class SwingLtfEntryRefinementTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.s = Path("market.py").read_text(encoding="utf-8")

    def test_two_stage_entry_builder_exists(self):
        self.assertIn("def _swing_build_ltf_entry", self.s)
        self.assertIn('get_candles(symbol, "1h", 61)', self.s)
        self.assertIn('get_candles(symbol, "15m", 81)', self.s)
        self.assertIn('get_bos_choch_event(c1h, direction, lookback=8, max_break_age=2)', self.s)
        self.assertIn('find_ob(c1h, direction)', self.s)
        self.assertIn('find_fvg(c1h, direction)', self.s)

    def test_ltf_execution_keeps_quality_filters(self):
        self.assertIn('candle_body / candle_range >= 0.50', self.s)
        self.assertIn('last_vol >= avg_vol * 1.20', self.s)
        self.assertIn('distance <= atr1h * 0.75', self.s)
        self.assertIn('abs(entry - sl) / max(abs(entry), 1e-12) > _sl_max_pct', self.s)
        self.assertIn('rr_check < 2.0', self.s)

    def test_provisional_4h_levels_are_not_executable(self):
        self.assertIn('entry = sl = None  # do not count provisional 4h levels as a near/executable deal', self.s)
        self.assertIn("LTF: fresh 1h BOS/CHoCH", self.s)
        self.assertIn("LTF: recent 15m retest of 1h OB/FVG", self.s)
        self.assertNotIn("return _audit_fail('SWING_DETECT_SWING_SETUP_R7363'", self.s)
        self.assertNotIn("return _audit_fail('SWING_DETECT_SWING_SETUP_R7378'", self.s)
        self.assertNotIn("return _audit_fail('SWING_DETECT_SWING_SETUP_R7389'", self.s)

    def test_four_hour_structure_is_context_and_fresh_1h_stays_mandatory(self):
        self.assertIn("'SWING_4H_STRUCTURE_CONTEXT'", self.s)
        self.assertIn('4h BOS/CHoCH context after trigger (non-blocking)', self.s)
        self.assertNotIn("return _audit_fail('SWING_DETECT_SWING_SETUP_R7405'", self.s)
        self.assertIn('get_bos_choch_event(c1h, direction, lookback=8, max_break_age=2)', self.s)
        self.assertIn("LTF: fresh 1h BOS/CHoCH", self.s)

    def test_no_cooldown_added(self):
        helper = self.s[self.s.index('def _swing_build_ltf_entry'):self.s.index('@_audit_strategy("SWING")')]
        self.assertNotIn('cooldown', helper.lower())
        self.assertNotIn('sleep(', helper)


if __name__ == "__main__":
    unittest.main()
