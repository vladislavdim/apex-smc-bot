from pathlib import Path

p = Path('tests/test_strategy_tuning_trade_stats.py')
s = p.read_text(encoding='utf-8')
old = '''    def test_swing_is_relaxed_only_at_initial_structure_gate(self):\n        self.assertIn("find_swings(candles, lookback=7)", self.market)\n        self.assertIn("get_bos_choch_event", self.market)\n        self.assertIn("Variant 2: минимум RR 2.0", self.market)\n'''
new = '''    def test_swing_retains_structure_and_final_rr_after_ltf_refinement(self):\n        self.assertIn("find_swings(candles, lookback=7)", self.market)\n        self.assertIn("get_bos_choch_event", self.market)\n        self.assertIn("def _swing_build_ltf_entry", self.market)\n        self.assertIn("LTF: fresh 1h BOS/CHoCH", self.market)\n        self.assertIn("rr_check < 2.0 or rr_check > 4.0", self.market)\n'''
if old not in s:
    raise SystemExit('old SWING tuning regression test not found')
p.write_text(s.replace(old, new, 1), encoding='utf-8')
print('SWING tuning regression expectation updated')
