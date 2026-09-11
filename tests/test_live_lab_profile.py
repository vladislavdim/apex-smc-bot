import os
import unittest
from unittest.mock import patch
from core import live_lab_profile
import stats_server

class LiveLabProfileTests(unittest.TestCase):
    def setUp(self): live_lab_profile._LAST.clear()

    def test_disabled_never_queues(self):
        with patch.dict(os.environ, {"APEX_LAB_PROFILE_SHADOW": "0"}):
            self.assertFalse(live_lab_profile.schedule("FAST", "BTCUSDT"))

    def test_default_is_enabled_only_on_render(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(live_lab_profile._enabled())
        with patch.dict(os.environ, {"RENDER": "true"}, clear=True):
            self.assertTrue(live_lab_profile._enabled())

    def test_unknown_strategy_never_queues(self):
        self.assertFalse(live_lab_profile.schedule("OTHER", "BTCUSDT"))

    def test_default_scope_does_not_add_load_for_other_symbols(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(live_lab_profile.schedule("FAST", "ETHUSDT"))

    def test_cadence_deduplicates(self):
        with patch.dict(os.environ, {"APEX_LAB_PROFILE_SHADOW": "1",
                "APEX_LAB_PROFILE_SHADOW_SYMBOLS": "BTCUSDT"}), patch.object(live_lab_profile, "_STARTED", True), patch.object(live_lab_profile._QUEUE, "put_nowait") as put:
            self.assertTrue(live_lab_profile.schedule("FAST", "BTCUSDT"))
            self.assertFalse(live_lab_profile.schedule("FAST", "BTCUSDT")); put.assert_called_once()

    def test_normalisation_is_closed(self):
        row = live_lab_profile._normalise_closed([{"timestamp": 100, "open": 1, "high": 2,
            "low": .5, "close": 1.5, "volume": 3}], "15m")[0]
        self.assertEqual(row["close_time"], 1000); self.assertTrue(row["is_closed"])

    def test_dashboard_compacts_lab_profile_events(self):
        events = [{"kind": "lab_profile_shadow", "strategy": "FAST", "symbol": "BTCUSDT",
                   "occurred_at": "2026-09-11T00:00:00+00:00", "event_key": "lab-1",
                   "payload": {"lab_outcome": "CANDIDATE", "live_outcome": "FILTERED",
                       "hard_gate_match_pct": 75.0, "candidate": {"entry": 100, "sl": 99,
                           "tp1": 102, "terminal_tp": 104, "rr": 4},
                       "checks": [{"check_order": i, "check_code": f"GATE_{i}",
                           "label": f"Gate {i}", "role": "HARD_GATE", "status": "PASS",
                           "measured": {"blob": "x" * 1000}, "threshold": {"value": True}}
                           for i in range(80)]}}]
        with patch.object(stats_server, "_fetch", return_value=events):
            result = stats_server.build_dashboard()
        shadow = result["lab_profile_shadow"]
        self.assertEqual(shadow["total"], 1)
        self.assertEqual(shadow["by_strategy"][0]["lab_candidates"], 1)
        self.assertEqual(len(shadow["recent"][0]["ordered_hard_gates"]), 30)
        self.assertNotIn("technical_evidence", shadow["recent"][0]["candidate"])

if __name__ == "__main__": unittest.main()
