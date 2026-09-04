from pathlib import Path
import runpy

# Apply the already-reviewed RR floor + FAST Balanced transformation to real files.
runpy.run_path('.github/scripts/patch_rr_fast_balanced.py', run_name='__main__')

# --- market.py: make FAST evidence and legacy prompt match the LTF-primary design ---
p = Path('market.py')
s = p.read_text(encoding='utf-8')
s = s.replace('''            "ob":        ob_4h,\n            "fvg":       fvg_4h,''', '''            "ob":        _fast_ob_15m,\n            "fvg":       _fast_fvg_15m,\n            "htf_ob":    ob_4h,\n            "htf_fvg":   fvg_4h,\n            "htf_1h":    direction_1h,\n            "htf_4h":    direction_4h,''', 1)
s = s.replace('''                "2. 4h OB или FVG подтверждает зону — институционалы там входили\\n"''', '''                "2. 15m OB/FVG retest задаёт рабочую зону; 1h/4h только контекст\\n"''', 1)
s = s.replace('''                "3. Volume spike 2.0x — реальный интерес на engulfing свече\\n"''', '''                "3. Volume spike 1.6x+ — реальный интерес на trigger-свече\\n"''', 1)
s = s.replace('''                "4. Acceptance — цена закрылась за зоной OB/FVG\\n"''', '''                "4. Retest 15m OB/FVG + displacement/engulfing подтверждают реакцию\\n"''', 1)
s = s.replace('''                "6. BTC и 1d тренд совпадают — не иди против рынка\\n\\n"''', '''                "6. 1h/4h подтверждают контекст; BTC блокирует только при согласованном 1h+4h конфликте\\n\\n"''', 1)
s = s.replace('''                f"- RR={rr} < 1.5\\n"''', '''                f"- RR={rr} < 2.0\\n"''', 1)
s = s.replace('''                "- Нет OB и нет FVG на 4h — вход без подтверждения зоны\\n"''', '''                "- Нет свежего 15m OB/FVG retest — нет рабочей зоны входа\\n"''', 1)
s = s.replace('''                "- 1d тренд ПРОТИВ направления\\n"\n                "- BTC тренд ПРОТИВ направления\\n"''', '''                "- Ни 1h, ни 4h не поддерживают 15m thesis\\n"\n                "- BTC 1h и 4h оба согласованно ПРОТИВ направления\\n"''', 1)
s = s.replace('''                "- Engulfing чёткий с объёмом 2.0x+\\n"''', '''                "- Engulfing/displacement чёткий с объёмом 1.6x+\\n"''', 1)
s = s.replace('''                "- 4h OB или FVG подтверждает зону входа\\n"''', '''                "- Есть свежий retest 15m OB/FVG\\n"''', 1)
s = s.replace('''                "- 1d тренд и BTC в том же направлении\\n"''', '''                "- 1h или 4h поддерживает thesis, BTC не даёт hard conflict\\n"''', 1)
p.write_text(s, encoding='utf-8')

# --- signal_quality_gate.py: final reviewer unavailable => WAIT, never silent APPROVE ---
p = Path('core/signal_quality_gate.py')
s = p.read_text(encoding='utf-8')
s = s.replace('''            "decision": "APPROVE",\n            "confidence": 0.0,\n            "reasons": ["Groq review unavailable; existing APEX decision preserved"],''', '''            "decision": "WAIT",\n            "confidence": 0.0,\n            "reasons": ["Groq review unavailable; final confirmation required"],''', 1)
s = s.replace('''    decision = str(data.get("decision", "APPROVE")).upper()\n    if decision not in _VALID_DECISIONS:\n        decision = "APPROVE"''', '''    decision = str(data.get("decision", "WAIT")).upper()\n    if decision not in _VALID_DECISIONS:\n        decision = "WAIT"''', 1)
p.write_text(s, encoding='utf-8')

# --- outdated regression expectations ---
p = Path('tests/test_strategy_tuning_trade_stats.py')
s = p.read_text(encoding='utf-8')
s = s.replace('self.assertIn("rr_check < 2.0 or rr_check > 4.0", self.market)', 'self.assertIn("rr_check < 2.0", self.market)', 1)
s = s.replace('self.assertIn("not _acceptance", self.market)', 'self.assertIn("FAST: recent 15m OB/FVG retest", self.market)', 1)
s = s.replace('self.assertIn("not 2.0 <= rr <= 4.0", self.market)', 'self.assertIn("(rr < 2.0)", self.market)', 1)
p.write_text(s, encoding='utf-8')

p = Path('tests/test_signal_quality_gate.py')
s = p.read_text(encoding='utf-8')
s = s.replace('''    def test_invalid_response_preserves_existing_decision(self):\n        review = _normalize_review(None, "not json")\n        self.assertEqual(review["decision"], "APPROVE")''', '''    def test_invalid_response_waits_for_final_confirmation(self):\n        review = _normalize_review(None, "not json")\n        self.assertEqual(review["decision"], "WAIT")''', 1)
s = s.replace('''    def test_unknown_decision_preserves_existing_decision(self):\n        review = _normalize_review({"decision": "BLOCK", "confidence": 1}, "{}")\n        self.assertEqual(review["decision"], "APPROVE")''', '''    def test_unknown_decision_waits(self):\n        review = _normalize_review({"decision": "BLOCK", "confidence": 1}, "{}")\n        self.assertEqual(review["decision"], "WAIT")''', 1)
s = s.replace('''        self.assertEqual(review["decision"], "APPROVE")\n        self.assertTrue(review["degraded"])''', '''        self.assertEqual(review["decision"], "WAIT")\n        self.assertTrue(review["degraded"])''', 1)
p.write_text(s, encoding='utf-8')

print('strategy + Groq consistency patch applied')
