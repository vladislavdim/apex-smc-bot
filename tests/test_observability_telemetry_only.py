import unittest
from pathlib import Path
from unittest import mock

from core import setup_audit


ROOT = Path(__file__).resolve().parents[1]
MARKET = (ROOT / "market.py").read_text(encoding="utf-8")
STATS = (ROOT / "stats_server.py").read_text(encoding="utf-8")


class TelemetryOnlyInvariantTests(unittest.TestCase):
    def test_existing_trading_thresholds_are_unchanged(self):
        self.assertIn('get_bos_choch_event(c1h, direction, lookback=8, max_break_age=2)', MARKET)
        self.assertIn('get_bos_choch_event(_fast_context_15m, "BULLISH", lookback=15, max_break_age=4)', MARKET)
        self.assertIn('get_bos_choch_event(_fast_context_15m, "BEARISH", lookback=15, max_break_age=4)', MARKET)
        self.assertIn('_vol_threshold = 1.6', MARKET)
        self.assertIn('_dist_range_too_wide = dist_range_pct >= 25', MARKET)
        self.assertIn("(rr_check < 2.0)", MARKET)
        self.assertIn("(rr < 2.0)", MARKET)
        self.assertNotIn('rr > 4', MARKET)
        self.assertNotIn('rr_check > 4', MARKET)

    def test_wyckoff_telemetry_does_not_replace_trading_phases(self):
        telemetry_call = '_telemetry_phases = _find_wyckoff_phases_distribution(candles_1d, candles_4h)'
        trading_call = '\n        phases = _find_wyckoff_phases_distribution(candles_1d, candles_4h)'
        blocker = '_dist_range_too_wide = dist_range_pct >= 25'
        self.assertIn(telemetry_call, MARKET)
        self.assertIn(trading_call, MARKET)
        telemetry_pos = MARKET.index(telemetry_call)
        blocker_pos = MARKET.index(blocker, telemetry_pos)
        trading_pos = MARKET.index(trading_call, blocker_pos)
        self.assertLess(telemetry_pos, blocker_pos)
        self.assertLess(blocker_pos, trading_pos)
        self.assertNotIn('phases = _telemetry_phases', MARKET)

    def test_dashboard_exposes_only_observability_fields(self):
        self.assertIn('"bos_choch_age":bos_age_stats', STATS)
        self.assertIn('"wyckoff_box_width":wy_box_range', STATS)
        self.assertIn('BOS/CHoCH age telemetry', STATS)
        self.assertIn('WYCKOFF Distribution width telemetry', STATS)

    def test_observer_does_not_change_decorated_return_value(self):
        @setup_audit.audit_strategy("TEST")
        def baseline(value):
            return {"value": value}

        @setup_audit.audit_strategy("TEST")
        def observed(value):
            setup_audit.audit_observe("probe", {"value": value})
            return {"value": value}

        with mock.patch.object(setup_audit, "emit_event", return_value="x"):
            a = baseline(7)
            b = observed(7)
        a.pop("_audit_attempt_key", None)
        b.pop("_audit_attempt_key", None)
        self.assertEqual(a, b)

    def test_observer_merges_shallow_fields_inside_same_attempt(self):
        captured = []

        def capture(kind, strategy, symbol, payload, event_key=None):
            captured.append((kind, payload, event_key))
            return event_key or "x"

        @setup_audit.audit_strategy("TEST")
        def observed(_value):
            setup_audit.audit_observe("progress", {"retest": True})
            setup_audit.audit_observe("progress", {"volume": False})
            setup_audit.audit_observe("bos_event", {"age_bars": 1, "timeframe": "1h"})
            setup_audit.audit_observe("bos_execution_event", {"age_bars": 2, "timeframe": "15m"})
            return {"ok": True}

        with mock.patch.object(setup_audit, "emit_event", side_effect=capture):
            observed(1)
        payload = captured[-1][1]
        self.assertEqual(payload["telemetry"]["progress"], {"retest": True, "volume": False})
        self.assertEqual(payload["telemetry"]["bos_event"]["age_bars"], 1)
        self.assertEqual(payload["telemetry"]["bos_execution_event"]["age_bars"], 2)


if __name__ == "__main__":
    unittest.main()
