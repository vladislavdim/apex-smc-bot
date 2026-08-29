import os
import sqlite3
import tempfile
import unittest

from core.trade_views import fetch_trades, format_trade_view


class TradeViewsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "brain.db")
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""CREATE TABLE signals (
                id INTEGER PRIMARY KEY, symbol TEXT, direction TEXT, signal_type TEXT,
                entry REAL, sl REAL, tp1 REAL, tp2 REAL, tp3 REAL, timeframe TEXT,
                grade TEXT, result TEXT, created_at TEXT, closed_at TEXT,
                tp1_hit INTEGER DEFAULT 0, trailing_sl REAL DEFAULT 0
            )""")
            conn.execute("""CREATE TABLE signal_execution_state (
                signal_id INTEGER PRIMARY KEY, status TEXT
            )""")
            conn.execute("""CREATE TABLE trade_executions (
                signal_id INTEGER PRIMARY KEY, status TEXT
            )""")
            rows = [
                (1, "BTCUSDT", "BULLISH", "MTF", 100, 95, 110, 120, 120, "1h", "MTF", "pending", "2026-08-01 10:00", None, 0, 0),
                (2, "ETHUSDT", "BULLISH", "SWING", 200, 190, 220, 230, 230, "4h", "SWING", "tp2", "2026-08-01 10:00", "2026-08-02 10:00", 1, 205),
                (3, "SOLUSDT", "BEARISH", "ZONE", 150, 155, 140, 135, 135, "4h", "ZONE", "sl", "2026-08-01 10:00", "2026-08-01 18:00", 0, 0),
            ]
            conn.executemany(
                "INSERT INTO signals VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows,
            )
            conn.execute("INSERT INTO signal_execution_state VALUES (1,'waiting_entry')")
            conn.execute("INSERT INTO trade_executions VALUES (1,'PAPER_PENDING_ENTRY')")

    def tearDown(self):
        self.tmp.cleanup()

    def test_active_view_shows_entry_stop_targets_and_execution(self):
        rows = fetch_trades(self.db_path, "active")
        text = format_trade_view("active", rows)
        self.assertEqual(len(rows), 1)
        self.assertIn("BTCUSDT", text)
        self.assertIn("ждёт входа", text)
        self.assertIn("TP2", text)
        self.assertIn("PAPER_PENDING_ENTRY", text)

    def test_take_and_stop_are_separate(self):
        take = fetch_trades(self.db_path, "take")
        stop = fetch_trades(self.db_path, "stop")
        self.assertEqual([row["symbol"] for row in take], ["ETHUSDT"])
        self.assertEqual([row["symbol"] for row in stop], ["SOLUSDT"])
        self.assertIn("TP2", format_trade_view("take", take))
        self.assertIn("SL", format_trade_view("stop", stop))

    def test_empty_category_has_clear_message(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM signals")
        self.assertIn("нет сделок", format_trade_view("take", fetch_trades(self.db_path, "take")))


if __name__ == "__main__":
    unittest.main()
