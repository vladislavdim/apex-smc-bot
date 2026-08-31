import json
import os
import sqlite3
import tempfile
import unittest

from core.telegram_dashboard import (
    fetch_system_health,
    fetch_strategy_stats,
    fetch_watchlist,
    format_strategy_stats,
    format_watchlist,
)


class TelegramDashboardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "brain.db")
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""CREATE TABLE timing_queue (
                symbol TEXT,direction TEXT,timeframe TEXT,entry REAL,sl REAL,tp1 REAL,tp2 REAL,
                grade TEXT,timing_score INTEGER,created_at TEXT,expires_at TEXT,status TEXT)""")
            conn.execute("""CREATE TABLE signals (
                id INTEGER PRIMARY KEY,symbol TEXT,direction TEXT,timeframe TEXT,entry REAL,sl REAL,
                tp1 REAL,tp2 REAL,grade TEXT,signal_type TEXT,result TEXT,created_at TEXT)""")
            conn.execute("""CREATE TABLE signal_execution_state (
                signal_id INTEGER PRIMARY KEY,status TEXT)""")
            conn.execute("""CREATE TABLE strategy_decisions (
                symbol TEXT,direction TEXT,timeframe TEXT,strategy TEXT,evidence_json TEXT,
                outcome TEXT,stage TEXT,created_at TEXT)""")
            conn.execute("""CREATE TABLE ai_signal_reviews (
                decision TEXT,created_at TEXT)""")

    def tearDown(self):
        self.tmp.cleanup()

    def test_watchlist_contains_only_real_persisted_candidates(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO timing_queue VALUES (?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,datetime('now','+1 hour'),'waiting')",
                ("BTCUSDT","BULLISH","1h",100,95,110,115,"MTF",2),
            )
            conn.execute(
                "INSERT INTO strategy_decisions VALUES (?,?,?,?,?,'WAIT','groq_quality_gate',CURRENT_TIMESTAMP)",
                ("ETHUSDT","BEARISH","4h","SWING",json.dumps({"candidate":{"entry":200,"sl":210,"tp1":180}})),
            )
        rows = fetch_watchlist(self.db_path)
        self.assertEqual({row["symbol"] for row in rows}, {"BTCUSDT", "ETHUSDT"})
        text = format_watchlist(rows)
        self.assertIn("Groq WAIT", text)
        self.assertIn("Наблюдение не является открытой сделкой", text)

    def test_strategy_stats_exclude_waiting_entry_and_separate_direction(self):
        with sqlite3.connect(self.db_path) as conn:
            signals = [
                (1,"BTCUSDT","BULLISH","1h",100,95,110,115,"MTF",None,"tp1","2026-01-01"),
                (2,"ETHUSDT","BEARISH","1h",100,105,90,85,"MTF",None,"sl","2026-01-01"),
                (3,"SOLUSDT","BULLISH","1h",100,95,110,115,"MTF",None,"tp2","2026-01-01"),
            ]
            conn.executemany("INSERT INTO signals VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", signals)
            conn.executemany("INSERT INTO signal_execution_state VALUES (?,?)", [(1,"closed"),(2,"closed"),(3,"waiting_entry")])
        mtf = next(row for row in fetch_strategy_stats(self.db_path) if row["strategy"] == "MTF")
        self.assertEqual(mtf["closed"], 2)
        self.assertEqual((mtf["tp1"], mtf["sl"], mtf["long"], mtf["short"]), (1, 1, 1, 1))
        self.assertEqual(mtf["win_rate"], 50.0)
        self.assertIn("нужно ещё 28", format_strategy_stats([mtf]))

    def test_system_health_counts_all_final_groq_reviews(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                "INSERT INTO ai_signal_reviews VALUES (?,CURRENT_TIMESTAMP)",
                [("APPROVE",), ("WAIT",), ("REJECT",)],
            )
            conn.execute(
                "INSERT INTO strategy_decisions VALUES (?,?,?,?,?,'REJECT','groq_quality_gate',CURRENT_TIMESTAMP)",
                ("BTCUSDT", "BULLISH", "1h", "MTF", "{}"),
            )

        health = fetch_system_health(self.db_path)
        self.assertEqual(health["groq_24h"], 3)
        self.assertIsNotNone(health["groq_last"])


if __name__ == "__main__":
    unittest.main()
