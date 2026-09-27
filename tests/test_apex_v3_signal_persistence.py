import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor

from apex.db.legacy_signal_persistence import LegacySignalPersistence


class SignalPersistenceTests(unittest.TestCase):
    def test_signal_insert_registers_waiting_and_duplicate_is_rejected(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as handle:
            conn = sqlite3.connect(handle.name)
            conn.execute(
                """CREATE TABLE signals(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,symbol TEXT,direction TEXT,
                    signal_type TEXT,entry REAL,tp1 REAL,tp2 REAL,tp3 REAL,sl REAL,
                    timeframe TEXT,estimated_hours INTEGER,grade TEXT,result TEXT,
                    created_at TEXT,closed_at TEXT,learning_id INTEGER,
                    confluence INTEGER,regime TEXT)"""
            )
            conn.commit()
            conn.close()
            registered = []
            service = LegacySignalPersistence(
                sqlite3.connect,
                handle.name,
                lambda conn, signal_id: registered.append(signal_id),
            )
            arguments = (
                "BTCUSDT", "BULLISH", "MTF", 100, 110, 120, 130, 95,
                "1h", 72, "A", 8, "TREND",
            )

            first = service.save(*arguments)
            second = service.save(*arguments)

            self.assertEqual(first, (1, None))
            self.assertEqual(second, (None, None))
            self.assertEqual(registered, [1])
            conn = sqlite3.connect(handle.name)
            row = conn.execute(
                "SELECT direction,entry,sl,tp1,tp2,tp3,result FROM signals"
            ).fetchone()
            conn.close()
            self.assertEqual(row, ("BULLISH", 100, 95, 110, 120, 130, "pending"))

    def test_missing_lifecycle_fails_closed_before_database_open(self):
        service = LegacySignalPersistence(
            lambda *_args, **_kwargs: self.fail("database must not open"),
            "unused.db",
            lambda *_args: None,
            lifecycle_available=False,
        )
        self.assertEqual(
            service.save("BTCUSDT", "BULLISH", "MTF", 1, 2, 3, 4, .5,
                         "1h", 72, "A"),
            (None, None),
        )

    def test_concurrent_strategies_cannot_claim_same_pending_pair(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as handle:
            conn = sqlite3.connect(handle.name)
            conn.execute(
                """CREATE TABLE signals(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,symbol TEXT,direction TEXT,
                    signal_type TEXT,entry REAL,tp1 REAL,tp2 REAL,tp3 REAL,sl REAL,
                    timeframe TEXT,estimated_hours INTEGER,grade TEXT,result TEXT,
                    created_at TEXT,closed_at TEXT,learning_id INTEGER,
                    confluence INTEGER,regime TEXT)"""
            )
            conn.commit(); conn.close()
            service = LegacySignalPersistence(
                sqlite3.connect, handle.name, lambda conn, signal_id: None,
            )

            def deliver(index):
                return service.save(
                    "BTCUSDT", "BULLISH" if index % 2 else "BEARISH",
                    "FAST" if index % 2 else "ZONE", 100, 110, 120, 130, 95,
                    "5m" if index % 2 else "4h", 4, "A",
                )[0]

            with ThreadPoolExecutor(max_workers=8) as pool:
                signal_ids = list(pool.map(deliver, range(8)))
            self.assertEqual(sum(signal_id is not None for signal_id in signal_ids), 1)
            with sqlite3.connect(handle.name) as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
