from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected exactly one match, found {count}: {old[:120]!r}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")


old_block = '''        dist_range_pct = (dist_high - dist_low) / dist_low * 100 if dist_low > 0 else 0\n        _audit_observe("wyckoff_distribution", {\n            "dist_range_pct": round(dist_range_pct, 6),\n            "old_range_under_25": bool(dist_range_pct < 25),\n        })\n        try:\n            _telemetry_phases = _find_wyckoff_phases_distribution(candles_1d, candles_4h)\n            _telemetry_points = []\n            for _telemetry_name in ("BC", "AR", "ST"):\n                _telemetry_phase = _telemetry_phases.get(_telemetry_name) if isinstance(_telemetry_phases, dict) else None\n                if isinstance(_telemetry_phase, dict) and _telemetry_phase.get("price") is not None:\n                    _telemetry_points.append((_telemetry_name, float(_telemetry_phase["price"])))\n            if len(_telemetry_points) >= 2:\n                _telemetry_prices = [p for _, p in _telemetry_points]\n                _telemetry_box_low = min(_telemetry_prices)\n                _telemetry_box_high = max(_telemetry_prices)\n                _telemetry_box_width_pct = (\n                    (_telemetry_box_high - _telemetry_box_low) / _telemetry_box_low * 100\n                    if _telemetry_box_low > 0 else None\n                )\n                _audit_observe("wyckoff_distribution", {\n                    "distribution_box_width_pct": round(_telemetry_box_width_pct, 6) if _telemetry_box_width_pct is not None else None,\n                    "structural_box_under_25": bool(_telemetry_box_width_pct < 25) if _telemetry_box_width_pct is not None else None,\n                    "structure_points": {name: price for name, price in _telemetry_points},\n                })\n        except Exception:\n            pass\n\n        _dist_range_too_wide = dist_range_pct >= 25\n        if _audit_test(\n            'WYCKOFF_DIST_RANGE',\n            _dist_range_too_wide,\n            'WYCKOFF Distribution: 30d range < 25%',\n            'dist_range_pct >= 25',\n            8720,\n        ):\n            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8725', 'WYCKOFF Distribution: 30d range < 25%', locals(), 'dist_range_pct >= 25', 8725)\n        if dist_range_pct < 15:\n            score += 20\n            signals.append(f"✅ Боковик {dist_range_pct:.1f}% у вершины")\n        else:\n            score += 10\n            signals.append(f"⚡️ Диапазон {dist_range_pct:.1f}% у вершины")\n'''

new_block = '''        # Keep the legacy 30d high-low metric for telemetry and for the existing\n        # downstream geometry anchors only.  It is no longer the Distribution\n        # width gate because it can include the pre-distribution pump itself.\n        dist_range_pct = (dist_high - dist_low) / dist_low * 100 if dist_low > 0 else 0\n\n        # The actual Distribution width is anchored to confirmed Wyckoff phases.\n        # _find_wyckoff_phases_distribution always supplies BC + AR when it\n        # returns a non-empty phase map; ST is included when confirmed.\n        _distribution_phases_preview = {}\n        _distribution_points = []\n        _distribution_box_width_pct = None\n        try:\n            _distribution_phases_preview = _find_wyckoff_phases_distribution(candles_1d, candles_4h)\n            for _distribution_name in ("BC", "AR", "ST"):\n                _distribution_phase = (\n                    _distribution_phases_preview.get(_distribution_name)\n                    if isinstance(_distribution_phases_preview, dict) else None\n                )\n                if isinstance(_distribution_phase, dict) and _distribution_phase.get("price") is not None:\n                    _distribution_points.append((_distribution_name, float(_distribution_phase["price"])))\n            if len(_distribution_points) >= 2:\n                _distribution_prices = [price for _, price in _distribution_points]\n                _distribution_box_low = min(_distribution_prices)\n                _distribution_box_high = max(_distribution_prices)\n                if _distribution_box_low > 0:\n                    _distribution_box_width_pct = (\n                        (_distribution_box_high - _distribution_box_low) / _distribution_box_low * 100\n                    )\n        except Exception:\n            _distribution_phases_preview = {}\n            _distribution_points = []\n            _distribution_box_width_pct = None\n\n        _audit_observe("wyckoff_distribution", {\n            "dist_range_pct": round(dist_range_pct, 6),\n            "old_range_under_25": bool(dist_range_pct < 25),\n            "distribution_box_width_pct": (\n                round(_distribution_box_width_pct, 6) if _distribution_box_width_pct is not None else None\n            ),\n            "structural_box_under_25": (\n                bool(_distribution_box_width_pct < 25) if _distribution_box_width_pct is not None else None\n            ),\n            "active_width_metric": "BC_AR_ST_STRUCTURAL_BOX",\n            "structure_points": {name: price for name, price in _distribution_points},\n        })\n\n        # Same numeric contract as before: <25% is mandatory.  Only the measured\n        # object changes from a rolling 30d high-low to the actual BC/AR/ST box.\n        # Missing structural phases fail closed here; they would also fail the\n        # mandatory phase check immediately below, so no synthetic fallback is used.\n        _distribution_box_too_wide = (_distribution_box_width_pct is None or _distribution_box_width_pct >= 25)\n        if _audit_test(\n            'WYCKOFF_DIST_STRUCTURAL_BOX',\n            _distribution_box_too_wide,\n            'WYCKOFF Distribution: BC/AR/ST structural box < 25%',\n            '_distribution_box_width_pct is None or _distribution_box_width_pct >= 25',\n            8720,\n        ):\n            return _audit_fail(\n                'WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8725',\n                'WYCKOFF Distribution: BC/AR/ST structural box < 25%',\n                locals(),\n                '_distribution_box_width_pct is None or _distribution_box_width_pct >= 25',\n                8725,\n            )\n        if _distribution_box_width_pct < 15:\n            score += 20\n            signals.append(f"✅ Structural BC/AR/ST box {_distribution_box_width_pct:.1f}% у вершины")\n        else:\n            score += 10\n            signals.append(f"⚡️ Structural BC/AR/ST box {_distribution_box_width_pct:.1f}% у вершины")\n'''

replace_once("market.py", old_block, new_block)

# Update old invariants that intentionally froze the legacy metric during the
# telemetry-only phase.  The 25% threshold remains frozen; only the metric changes.
replace_once(
    "tests/test_numeric_diagnostics_and_mtf_throughput.py",
    "        self.assertIn('_dist_range_too_wide = dist_range_pct >= 25', MARKET)\n",
    "        self.assertIn('_distribution_box_too_wide = (_distribution_box_width_pct is None or _distribution_box_width_pct >= 25)', MARKET)\n",
)

p = Path("tests/test_observability_telemetry_only.py")
text = p.read_text(encoding="utf-8")
text = text.replace(
    "        self.assertIn('_dist_range_too_wide = dist_range_pct >= 25', MARKET)\n",
    "        self.assertIn('_distribution_box_too_wide = (_distribution_box_width_pct is None or _distribution_box_width_pct >= 25)', MARKET)\n        self.assertIn('\\\"old_range_under_25\\\"', MARKET)\n",
    1,
)
old_test = '''    def test_wyckoff_telemetry_does_not_replace_trading_phases(self):\n        telemetry_call = '_telemetry_phases = _find_wyckoff_phases_distribution(candles_1d, candles_4h)'\n        trading_call = '\\n        phases = _find_wyckoff_phases_distribution(candles_1d, candles_4h)'\n        blocker = '_dist_range_too_wide = dist_range_pct >= 25'\n        self.assertIn(telemetry_call, MARKET)\n        self.assertIn(trading_call, MARKET)\n        telemetry_pos = MARKET.index(telemetry_call)\n        blocker_pos = MARKET.index(blocker, telemetry_pos)\n        trading_pos = MARKET.index(trading_call, blocker_pos)\n        self.assertLess(telemetry_pos, blocker_pos)\n        self.assertLess(blocker_pos, trading_pos)\n        self.assertNotIn('phases = _telemetry_phases', MARKET)\n\n'''
new_test = '''    def test_wyckoff_structural_box_is_active_but_legacy_range_remains_telemetry(self):\n        preview_call = '_distribution_phases_preview = _find_wyckoff_phases_distribution(candles_1d, candles_4h)'\n        trading_call = '\\n        phases = _find_wyckoff_phases_distribution(candles_1d, candles_4h)'\n        blocker = '_distribution_box_too_wide = (_distribution_box_width_pct is None or _distribution_box_width_pct >= 25)'\n        self.assertIn(preview_call, MARKET)\n        self.assertIn(trading_call, MARKET)\n        self.assertIn(blocker, MARKET)\n        self.assertIn('"dist_range_pct": round(dist_range_pct, 6)', MARKET)\n        self.assertIn('"old_range_under_25": bool(dist_range_pct < 25)', MARKET)\n        self.assertNotIn('_dist_range_too_wide = dist_range_pct >= 25', MARKET)\n        preview_pos = MARKET.index(preview_call)\n        blocker_pos = MARKET.index(blocker, preview_pos)\n        trading_pos = MARKET.index(trading_call, blocker_pos)\n        self.assertLess(preview_pos, blocker_pos)\n        self.assertLess(blocker_pos, trading_pos)\n        self.assertNotIn('phases = _distribution_phases_preview', MARKET)\n\n'''
if text.count(old_test) != 1:
    raise SystemExit(f"tests/test_observability_telemetry_only.py: old Wyckoff test count={text.count(old_test)}")
p.write_text(text.replace(old_test, new_test, 1), encoding="utf-8")

replace_once(
    "tests/test_strategy_lab_terminal_truth.py",
    '''    def test_wyckoff_is_not_tuned_in_this_pr(self):\n        self.assertIn('_dist_range_too_wide = dist_range_pct >= 25', MARKET)\n        self.assertIn("'WYCKOFF Distribution: 30d range < 25%'", MARKET)\n''',
    '''    def test_wyckoff_structural_box_keeps_same_numeric_threshold(self):\n        self.assertIn('_distribution_box_width_pct >= 25', MARKET)\n        self.assertIn("'WYCKOFF Distribution: BC/AR/ST structural box < 25%'", MARKET)\n        self.assertIn('"old_range_under_25": bool(dist_range_pct < 25)', MARKET)\n''',
)

Path("tests/test_wyckoff_structural_box_gate.py").write_text(r'''import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MARKET = (ROOT / "market.py").read_text(encoding="utf-8")


class WyckoffStructuralBoxGateTests(unittest.TestCase):
    def _distribution_gate_block(self):
        start = MARKET.index('# Keep the legacy 30d high-low metric for telemetry')
        end = MARKET.index('# ── 3. ФАЗЫ WYCKOFF DISTRIBUTION ──', start)
        return MARKET[start:end]

    def test_active_gate_uses_structural_box_at_same_25_percent_threshold(self):
        block = self._distribution_gate_block()
        self.assertIn('_distribution_box_width_pct >= 25', block)
        self.assertIn("'WYCKOFF_DIST_STRUCTURAL_BOX'", block)
        self.assertIn('BC/AR/ST structural box < 25%', block)
        self.assertNotIn('_dist_range_too_wide', block)

    def test_legacy_30d_metric_is_telemetry_only(self):
        block = self._distribution_gate_block()
        self.assertIn('dist_range_pct = (dist_high - dist_low) / dist_low * 100', block)
        self.assertIn('"old_range_under_25": bool(dist_range_pct < 25)', block)
        self.assertNotIn("'dist_range_pct >= 25'", block)

    def test_structural_box_is_real_bc_ar_st_phase_geometry(self):
        block = self._distribution_gate_block()
        self.assertIn('for _distribution_name in ("BC", "AR", "ST")', block)
        self.assertIn('min(_distribution_prices)', block)
        self.assertIn('max(_distribution_prices)', block)
        self.assertIn('(_distribution_box_high - _distribution_box_low) / _distribution_box_low * 100', block)

    def test_no_fallback_or_synthetic_width_is_used(self):
        block = self._distribution_gate_block()
        self.assertIn('_distribution_box_width_pct is None or _distribution_box_width_pct >= 25', block)
        self.assertNotIn('_distribution_box_width_pct = dist_range_pct', block)
        self.assertNotIn('or dist_range_pct', block)

    def test_score_bands_keep_15_and_25_contract_on_structural_metric(self):
        block = self._distribution_gate_block()
        self.assertIn('if _distribution_box_width_pct < 15:', block)
        self.assertIn('_distribution_box_width_pct >= 25', block)

    def test_downstream_trading_phase_detection_is_still_recomputed(self):
        self.assertIn('\n        phases = _find_wyckoff_phases_distribution(candles_1d, candles_4h)', MARKET)
        self.assertNotIn('phases = _distribution_phases_preview', MARKET)

    def test_unrelated_core_invariants_remain(self):
        self.assertIn('_vol_threshold = 1.6', MARKET)
        self.assertIn('(rr < 2.0)', MARKET)
        self.assertNotIn('rr > 4', MARKET)


if __name__ == "__main__":
    unittest.main()
''', encoding="utf-8")

print("WYCKOFF structural box gate switch applied")
