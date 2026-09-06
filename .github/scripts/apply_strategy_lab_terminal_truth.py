from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected exactly one match, found {count}: {old[:120]!r}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")


# 1) MTF: make the passive 15m BOS/CHoCH wait visible in the same audit funnel.
# This is observability-only: the existing PENDING_LTF return and the later hard
# score/BOS gate remain unchanged.
replace_once(
    "bot.py",
    '''        # Для MTF-сетапов нужны минимум 2 из 4 подтверждений,\n        # включая реальную структуру (BOS/CHoCH), а не только сессию.\n        if passive_watch and not _mtf_score_bos:\n            return {\n''',
    '''        # Для MTF-сетапов нужны минимум 2 из 4 подтверждений,\n        # включая реальную структуру (BOS/CHoCH), а не только сессию.\n        # Passive-watch may return PENDING_LTF before the hard score gate below.\n        # Record that terminal state explicitly so Strategy Lab cannot make RR\n        # look like the last reached gate when the candidate is actually waiting\n        # for a fresh closed 15m BOS/CHoCH.\n        if passive_watch:\n            _audit_test(\n                'MTF_PASSIVE_LTF_BOS',\n                (not _mtf_score_bos),\n                'MTF passive watch: closed 15m BOS/CHoCH before candidate',\n                'passive_watch and not _mtf_score_bos',\n                5050,\n            )\n        if passive_watch and not _mtf_score_bos:\n            return {\n''',
)

# 2) SWING: distinguish raw candle body/range from the actual directional gate.
# The trading predicate itself stays byte-for-byte unchanged.
replace_once(
    "market.py",
    '''        _audit_observe("swing_numeric", {\n            "displacement_body_ratio": round(candle_body / candle_range, 6) if candle_range > 0 else None,\n            "direction_ok": bool(direction_ok),\n            "volume_ratio": round(last_vol / avg_vol, 6) if avg_vol > 0 else None,\n            "retest_distance_atr": round(distance / atr1h, 6) if atr1h > 0 else None,\n        })\n''',
    '''        _audit_observe("swing_numeric", {\n            # Raw candle body/range is useful diagnostics but is not identical\n            # to the gate because the gate also requires the candle direction.\n            "displacement_body_ratio": round(candle_body / candle_range, 6) if candle_range > 0 else None,\n            "direction_ok": bool(direction_ok),\n            "directional_displacement_ratio": (\n                round(candle_body / candle_range, 6) if candle_range > 0 and direction_ok else 0.0\n            ),\n            "displacement_gate_pass": bool(out["displacement_ok"]),\n            "volume_ratio": round(last_vol / avg_vol, 6) if avg_vol > 0 else None,\n            "retest_distance_atr": round(distance / atr1h, 6) if atr1h > 0 else None,\n        })\n''',
)

# 3) Strategy Lab: never truncate away terminal gates and show PENDING_LTF as an
# explicit terminal outcome both per strategy and in the top summary.
replace_once(
    "stats_server.py",
    '''            "strategy": strategy, "attempts": len(items), "steps": ordered[:18],\n            "candidates": sum(str(x.get("outcome") or "").upper() == "CANDIDATE" for x in items),\n            "groq": sum(bool(x.get("groq_review")) for x in items),\n''',
    '''            "strategy": strategy, "attempts": len(items), "steps": ordered[:30],\n            "candidates": sum(str(x.get("outcome") or "").upper() == "CANDIDATE" for x in items),\n            "pending_ltf": sum(str(x.get("outcome") or "").upper() == "PENDING_LTF" for x in items),\n            "filtered": sum(str(x.get("outcome") or "").upper() == "FILTERED" for x in items),\n            "errors": sum(str(x.get("outcome") or "").upper() == "ERROR" for x in items),\n            "groq": sum(bool(x.get("groq_review")) for x in items),\n''',
)
replace_once(
    "stats_server.py",
    '''        "SWING": ("swing_numeric", ("displacement_body_ratio", "volume_ratio", "retest_distance_atr")),\n''',
    '''        "SWING": ("swing_numeric", ("displacement_body_ratio", "directional_displacement_ratio", "volume_ratio", "retest_distance_atr")),\n''',
)
replace_once(
    "stats_server.py",
    '''      "summary":{"attempts":total,"candidates":sum(r.get("outcome")=="CANDIDATE" for r in joined),"near_setups":sum(bool(r.get("near_setup")) for r in joined),"groq_total":reviews_n,''',
    '''      "summary":{"attempts":total,"candidates":sum(r.get("outcome")=="CANDIDATE" for r in joined),"pending_ltf":sum(r.get("outcome")=="PENDING_LTF" for r in joined),"near_setups":sum(bool(r.get("near_setup")) for r in joined),"groq_total":reviews_n,''',
)
replace_once(
    "stats_server.py",
    '''return [['Проверок',s.attempts],['Кандидатов',s.candidates],['Почти сделок',s.near_setups],['До Groq',s.groq_total]''',
    '''return [['Проверок',s.attempts],['Кандидатов',s.candidates],['Ждут LTF',s.pending_ltf||0],['Почти сделок',s.near_setups],['До Groq',s.groq_total]''',
)
replace_once(
    "stats_server.py",
    '''displacement_body_ratio:'SWING displacement body/range',volume_ratio:'SWING volume/avg' ''',
    '''displacement_body_ratio:'SWING body/range (raw; direction ignored)',directional_displacement_ratio:'SWING directional displacement (actual gate ratio)',volume_ratio:'SWING volume/avg' ''',
)

# The funnel header is a single JS template literal. Keep all existing fields and
# add the terminal outcome that caused the misleading 4/4 -> 0 observation.
p = Path("stats_server.py")
text = p.read_text(encoding="utf-8")
old_header = '''· старт ${f.attempts} → кандидаты ${f.candidates} → Groq ${f.groq} → отправлено ${f.delivered}'''
new_header = '''· старт ${f.attempts} → кандидаты ${f.candidates} → ждут LTF ${f.pending_ltf||0} → Groq ${f.groq} → отправлено ${f.delivered}'''
if text.count(old_header) != 1:
    raise SystemExit(f"stats_server.py: funnel header match count={text.count(old_header)}")
p.write_text(text.replace(old_header, new_header, 1), encoding="utf-8")

Path("tests/test_strategy_lab_terminal_truth.py").write_text(r'''import unittest
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
''', encoding="utf-8")

print("Strategy Lab terminal-truth observability patch applied")
