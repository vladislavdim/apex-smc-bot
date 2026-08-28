import importlib
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch


class LearningStatsTests(unittest.TestCase):
    def test_only_activated_resolved_outcomes_affect_win_rate_and_rr(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as test_db:
            with patch.dict(
                os.environ, {"APEX_BRAIN_DB_PATH": test_db.name}
            ):
                learning = importlib.import_module("core.learning")
        conn = sqlite3.connect(":memory:")
        conn.execute("""CREATE TABLE signal_log (
            symbol TEXT, result TEXT, rr_achieved REAL
        )""")
        conn.execute("""CREATE TABLE signal_stats (
            symbol TEXT PRIMARY KEY, total INTEGER, wins INTEGER, losses INTEGER,
            tp1_hits INTEGER, tp2_hits INTEGER, tp3_hits INTEGER, sl_hits INTEGER,
            expired INTEGER, win_rate REAL, avg_rr REAL, last_updated TEXT
        )""")
        conn.executemany(
            "INSERT INTO signal_log VALUES ('BTCUSDT', ?, ?)",
            [
                ("tp1", 1.0), ("tp2", 2.0), ("sl", -1.0),
                ("expired", 99.0), ("cancelled", 99.0), ("PENDING", 99.0),
            ],
        )
        learning._update_stats(conn, "BTCUSDT")
        row = conn.execute(
            "SELECT total,wins,losses,expired,win_rate,avg_rr FROM signal_stats"
        ).fetchone()
        conn.close()
        self.assertEqual(row[:4], (3, 2, 1, 1))
        self.assertAlmostEqual(row[4], 200 / 3)
        self.assertAlmostEqual(row[5], 2 / 3)


if __name__ == "__main__":
    unittest.main()
