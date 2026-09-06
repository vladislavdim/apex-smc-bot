import unittest
from pathlib import Path

from core.signal_integrity import validate_candidate
from core.setup_evidence import _geometry


class RRFloorFastBalancedTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.market = Path("market.py").read_text(encoding="utf-8")
        cls.bot = Path("bot.py").read_text(encoding="utf-8")
        cls.evidence = Path("core/setup_evidence.py").read_text(encoding="utf-8")

    def test_rr_above_four_is_valid_for_every_strategy_integrity(self):
        for strategy in ("MTF", "SWING", "ZONE", "FAST", "WYCKOFF"):
            candidate = {
                "symbol": "ETHUSDT", "direction": "BULLISH", "scan_type": strategy,
                "entry": 100.0, "sl": 99.0, "tp1": 105.0, "tp2": 106.0, "tp3": 107.0, "rr": 5.0,
            }
            with self.subTest(strategy=strategy):
                self.assertTrue(validate_candidate(candidate)["valid"])

    def test_rr_below_two_is_rejected(self):
        candidate = {"symbol":"ETHUSDT","direction":"BULLISH","scan_type":"MTF","entry":100,"sl":99,"tp1":101.9,"rr":1.9}
        self.assertFalse(validate_candidate(candidate)["valid"])
        self.assertFalse(_geometry(candidate, "BULLISH")[0])

    def test_setup_evidence_accepts_rr_above_four(self):
        candidate = {"entry":100,"sl":99,"tp1":105,"rr":5.0}
        self.assertTrue(_geometry(candidate, "BULLISH")[0])

    def test_all_active_strategy_rr_caps_removed(self):
        forbidden = [
            "rr_check < 2.0 or rr_check > 4.0",
            "not 2.0 <= rr <= 4.0",
            "not 2.5 <= rr <= 4.0",
            "not 2.0 <= reward / risk <= 4.0",
            "max_rr=4.0",
        ]
        for text in forbidden:
            self.assertNotIn(text, self.market)
            self.assertNotIn(text, self.bot)
        self.assertNotIn("not 2.0 <= _rr_val <= 4.0", self.bot)
        self.assertIn("(_rr_val < 2.0)", self.bot)
        self.assertIn("max_rr=None", self.market)

    def test_fast_is_ltf_primary_balanced_context(self):
        self.assertIn('FAST: one fresh 15m BOS/CHoCH direction', self.market)
        self.assertIn('FAST: 1h or 4h supports 15m direction', self.market)
        self.assertIn('FAST: recent 15m OB/FVG retest', self.market)
        self.assertIn('FAST: BTC 1h+4h both oppose 15m thesis', self.market)
        self.assertNotIn("return _audit_fail('FAST_DETECT_FAST_DEAL_R9225'", self.market)
        self.assertNotIn("return _audit_fail('FAST_DETECT_FAST_DEAL_R9238'", self.market)

    def test_fast_quality_guards_still_exist(self):
        self.assertIn('curr_body / curr_range < 0.65', self.market)
        self.assertIn('curr["volume"] < avg_vol_15m * _vol_threshold', self.market)
        self.assertIn('not _fast_structure_event', self.market)
        self.assertIn('(rr < 2.0)', self.market)


if __name__ == "__main__":
    unittest.main()
