from __future__ import annotations

import sqlite3
import unittest

from core.groq_calibration import (
    analytical_calibration_summary,
    bind_analytical_prediction,
    calibration_summary,
    ensure_groq_calibration_schema,
    record_outcome,
    record_prediction,
    resolve_analytical_signal,
)


class GroqLiveCalibrationTests(unittest.TestCase):
    def setUp(self):
        self.db = ":memory:"

    def test_new_predictions_are_live_only_not_shadow(self):
        # A temporary file is required because each API owns its connection.
        import tempfile
        self.temp = tempfile.NamedTemporaryFile(suffix=".db")
        self.db = self.temp.name
        self.addCleanup(self.temp.close)
        self.assertTrue(record_prediction(
            "action-live", "APPROVE", .8, signal_id=1,
            prediction_target="TRADE_TERMINAL_OUTCOME", db_path=self.db,
        ))
        with sqlite3.connect(self.db) as conn:
            row = conn.execute(
                "SELECT shadow_only,evidence_scope FROM apex_v2_groq_calibration"
            ).fetchone()
        self.assertEqual(row, (0, "CONFIRMED_LIVE_ONLY"))
        self.assertTrue(record_outcome("action-live", outcome_label=True, db_path=self.db))
        summary = calibration_summary(self.db)
        self.assertEqual(summary["scope"], "CONFIRMED_LIVE_ONLY")
        self.assertEqual((summary["calls"], summary["resolved"]), (1, 1))

    def test_legacy_rows_are_excluded_and_cannot_be_resolved(self):
        import tempfile
        self.temp = tempfile.NamedTemporaryFile(suffix=".db")
        self.db = self.temp.name
        self.addCleanup(self.temp.close)
        ensure_groq_calibration_schema(self.db)
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """INSERT INTO apex_v2_groq_calibration
                   (action_id,action,shadow_only,evidence_scope)
                   VALUES('legacy','APPROVE',1,'LEGACY_UNKNOWN')"""
            )
        self.assertFalse(record_outcome("legacy", outcome_label=True, db_path=self.db))
        self.assertEqual(calibration_summary(self.db)["calls"], 0)

    def test_analytical_entry_review_does_not_claim_confirmed_live_evidence(self):
        import tempfile
        self.temp = tempfile.NamedTemporaryFile(suffix=".db")
        self.db = self.temp.name
        self.addCleanup(self.temp.close)
        self.assertTrue(record_prediction(
            "entry-groq:candidate", "APPROVE", .9,
            prediction_target="ORIGINAL_TP_BEFORE_ORIGINAL_SL",
            evidence_scope="ANALYTICAL_CANDIDATE", db_path=self.db,
        ))
        self.assertFalse(record_outcome("entry-groq:candidate", outcome_label=True, db_path=self.db))
        self.assertEqual(calibration_summary(self.db)["calls"], 0)
        self.assertTrue(bind_analytical_prediction("entry-groq:candidate", 47, db_path=self.db))
        self.assertFalse(bind_analytical_prediction("entry-groq:candidate", 48, db_path=self.db))
        self.assertEqual(resolve_analytical_signal(
            47, tp_before_original_sl=None, reason="ambiguous", db_path=self.db,
        ), 0)
        self.assertEqual(resolve_analytical_signal(
            47, tp_before_original_sl=True, reason="tp1_hit", db_path=self.db,
        ), 1)
        self.assertEqual(resolve_analytical_signal(
            47, tp_before_original_sl=False, reason="later_sl", db_path=self.db,
        ), 0)
        with sqlite3.connect(self.db) as conn:
            row = conn.execute(
                "SELECT signal_id,outcome_label,outcome_reason,reward_r FROM apex_v2_groq_calibration"
            ).fetchone()
        self.assertEqual(row, (47, 1.0, "tp1_hit", None))
        self.assertEqual(calibration_summary(self.db)["calls"], 0)
        analytical = analytical_calibration_summary(self.db)
        self.assertEqual((analytical["calls"], analytical["delivered_approvals"], analytical["resolved"]), (1, 1, 1))
        self.assertEqual(analytical["mean_review_confidence"], .9)
        self.assertEqual(analytical["observed_tp_before_sl_rate"], 1.0)


if __name__ == "__main__":
    unittest.main()
