import os
import sqlite3
import tempfile
import unittest

from apex.db.repositories.signal_lifecycle import SignalLifecycleRepository
from apex.db.signal_lifecycle_migration import (
    import_legacy_signal_lifecycle, signal_lifecycle_parity_report,
)
from apex.db.signal_monitor_cutover import (
    SignalMonitorProjectionError, sync_state_monitor_projection,
)
from apex.db.state_db import migrate_state
from apex.db.state_signal_persistence import StateSignalPersistence


class SignalMonitorCutoverTests(unittest.TestCase):
    def test_compatibility_failure_after_state_commit_recovers_without_second_id(self):
        with tempfile.TemporaryDirectory() as folder:
            legacy_path = os.path.join(folder, "legacy.db")
            state_path = os.path.join(folder, "state.db")
            legacy = sqlite3.connect(legacy_path)
            legacy.executescript("""CREATE TABLE signals(
                id INTEGER PRIMARY KEY AUTOINCREMENT,symbol TEXT,direction TEXT,
                signal_type TEXT,timeframe TEXT,entry REAL,sl REAL,tp1 REAL,
                tp2 REAL,tp3 REAL,estimated_hours REAL,grade TEXT,confluence REAL,
                regime TEXT,created_at TEXT,result TEXT,closed_at TEXT,
                tp1_hit INTEGER,trailing_sl REAL,best_price REAL);""")
            legacy.close()
            state = sqlite3.connect(state_path); migrate_state(state); state.close()
            lf = lambda: sqlite3.connect(legacy_path)
            sf = lambda: sqlite3.connect(state_path)
            service = StateSignalPersistence(lf, sf, "release-test")
            arguments = ("BTCUSDT", "BULLISH", "FAST", 100, 110, 120, 130,
                         95, "5m", 1, "A")
            with self.assertRaisesRegex(sqlite3.OperationalError, "signal_execution_state"):
                service.save(*arguments)
            self.assertEqual(SignalLifecycleRepository(sf).get(1)["source"], "state")
            with lf() as legacy:
                self.assertEqual(legacy.execute("SELECT COUNT(*) FROM signals").fetchone()[0], 0)
                legacy.execute("""CREATE TABLE signal_execution_state(
                    signal_id INTEGER PRIMARY KEY,status TEXT,activated_at TEXT,
                    last_checked_at TEXT,closed_at TEXT,cancel_reason TEXT)""")
            self.assertEqual(sync_state_monitor_projection(lf, sf), 1)
            self.assertIsNone(service.save(*arguments))
            with lf() as legacy:
                self.assertEqual(legacy.execute("SELECT COUNT(*) FROM signals").fetchone()[0], 1)

    def test_state_first_creation_pair_guard_and_restart_replay(self):
        with tempfile.TemporaryDirectory() as folder:
            legacy_path = os.path.join(folder, "legacy.db")
            state_path = os.path.join(folder, "state.db")
            legacy = sqlite3.connect(legacy_path)
            legacy.executescript("""CREATE TABLE signals(
                id INTEGER PRIMARY KEY AUTOINCREMENT,symbol TEXT,direction TEXT,
                signal_type TEXT,timeframe TEXT,entry REAL,sl REAL,tp1 REAL,
                tp2 REAL,tp3 REAL,estimated_hours REAL,grade TEXT,confluence REAL,
                regime TEXT,created_at TEXT,result TEXT,closed_at TEXT,
                tp1_hit INTEGER,trailing_sl REAL,best_price REAL);
                CREATE TABLE signal_execution_state(
                signal_id INTEGER PRIMARY KEY,status TEXT,activated_at TEXT,
                last_checked_at TEXT,closed_at TEXT,cancel_reason TEXT);
                INSERT INTO signals(id,symbol,result) VALUES(71,'OLDUSDT','closed');
            """)
            legacy.commit(); legacy.close()
            state = sqlite3.connect(state_path); migrate_state(state); state.close()
            lf = lambda: sqlite3.connect(legacy_path)
            sf = lambda: sqlite3.connect(state_path)
            import_legacy_signal_lifecycle(lf, sf)
            service = StateSignalPersistence(lf, sf, "release-test")
            arguments = ("BTCUSDT", "BULLISH", "MTF", 100, 110, 120, 130,
                         95, "1h", 72, "A")
            self.assertEqual(service.save(*arguments), 72)
            self.assertIsNone(service.save(*arguments))
            row = SignalLifecycleRepository(sf).get(72)
            self.assertEqual((row["source"], row["ownership"], row["status"]),
                             ("state", "state", "waiting_entry"))
            with lf() as legacy:
                self.assertEqual(legacy.execute(
                    "SELECT symbol,result FROM signals WHERE id=72"
                ).fetchone(), ("BTCUSDT", "pending"))
                legacy.execute("DELETE FROM signal_execution_state WHERE signal_id=72")
                legacy.execute("DELETE FROM signals WHERE id=72")
            self.assertEqual(sync_state_monitor_projection(lf, sf), 1)
            self.assertTrue(signal_lifecycle_parity_report(lf, sf)["ok"])
            with lf() as legacy:
                self.assertEqual(legacy.execute(
                    "SELECT status FROM signal_execution_state WHERE signal_id=72"
                ).fetchone(), ("waiting_entry",))

    def test_state_progress_survives_legacy_refresh_and_restart(self):
        with tempfile.TemporaryDirectory() as folder:
            legacy_path = os.path.join(folder, "legacy.db")
            state_path = os.path.join(folder, "state.db")
            legacy = sqlite3.connect(legacy_path)
            legacy.executescript("""CREATE TABLE signals(
                id INTEGER PRIMARY KEY,symbol TEXT,direction TEXT,signal_type TEXT,
                timeframe TEXT,entry REAL,sl REAL,tp1 REAL,tp2 REAL,tp3 REAL,
                estimated_hours REAL,grade TEXT,confluence REAL,regime TEXT,
                tp1_hit INTEGER,trailing_sl REAL,best_price REAL,
                created_at TEXT,result TEXT,closed_at TEXT);
                CREATE TABLE signal_execution_state(
                    signal_id INTEGER PRIMARY KEY,status TEXT,activated_at TEXT,
                    last_checked_at TEXT,closed_at TEXT,cancel_reason TEXT);
                INSERT INTO signals VALUES(
                    7,'BTCUSDT','BULLISH','MTF','1h',100,95,110,120,130,
                    72,'A',8,'TREND',0,NULL,NULL,'2026-09-01','pending',NULL);
                INSERT INTO signal_execution_state VALUES(
                    7,'waiting_entry',NULL,NULL,NULL,NULL);
            """)
            legacy.commit(); legacy.close()
            state = sqlite3.connect(state_path); migrate_state(state); state.close()
            legacy_factory = lambda: sqlite3.connect(legacy_path)
            state_factory = lambda: sqlite3.connect(state_path)
            import_legacy_signal_lifecycle(legacy_factory, state_factory)
            repository = SignalLifecycleRepository(state_factory)
            self.assertTrue(repository.advance_monitor(
                7, expected_status="waiting_entry", transition="activate",
            ))
            self.assertTrue(repository.advance_monitor(
                7, expected_status="active", transition="close", result="tp2",
            ))
            self.assertEqual(legacy_factory().execute(
                "SELECT result FROM signals WHERE id=7"
            ).fetchone(), ("pending",))
            self.assertEqual(sync_state_monitor_projection(legacy_factory, state_factory), 1)
            import_legacy_signal_lifecycle(legacy_factory, state_factory, refresh=True)
            self.assertTrue(signal_lifecycle_parity_report(legacy_factory, state_factory)["ok"])
            self.assertEqual(repository.get(7)["result"], "tp2")
            self.assertEqual(repository.get(7)["ownership"], "state")
            self.assertEqual(legacy_factory().execute(
                "SELECT result FROM signals WHERE id=7"
            ).fetchone(), ("tp2",))
            self.assertEqual(sync_state_monitor_projection(legacy_factory, state_factory), 1)

            with legacy_factory() as legacy:
                legacy.execute("DELETE FROM signals WHERE id=7")
            with self.assertRaisesRegex(SignalMonitorProjectionError, "identity_missing:7"):
                sync_state_monitor_projection(legacy_factory, state_factory)


if __name__ == "__main__":
    unittest.main()
