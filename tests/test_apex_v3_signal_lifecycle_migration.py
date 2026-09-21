import os
import sqlite3
import tempfile
import unittest

from apex.db.signal_lifecycle_migration import (
    import_legacy_signal_lifecycle,
    signal_lifecycle_parity_report,
)
from apex.db.state_db import migrate_state


class SignalLifecycleMigrationTests(unittest.TestCase):
    def test_import_refresh_and_field_parity_are_restart_safe(self):
        with tempfile.TemporaryDirectory() as folder:
            legacy_path = os.path.join(folder, "legacy.db")
            state_path = os.path.join(folder, "state.db")
            legacy = sqlite3.connect(legacy_path)
            legacy.executescript("""
                CREATE TABLE signals(
                    id INTEGER PRIMARY KEY,result TEXT,created_at TEXT,closed_at TEXT
                );
                CREATE TABLE signal_execution_state(
                    signal_id INTEGER PRIMARY KEY,status TEXT,activated_at TEXT,
                    last_checked_at TEXT,closed_at TEXT,cancel_reason TEXT
                );
                INSERT INTO signals VALUES(7,'pending','2026-09-01',NULL);
                INSERT INTO signal_execution_state VALUES(
                    7,'waiting_entry',NULL,'2026-09-01T01:00:00',NULL,NULL
                );
            """)
            legacy.commit(); legacy.close()
            state = sqlite3.connect(state_path); migrate_state(state); state.close()
            legacy_factory = lambda: sqlite3.connect(legacy_path)
            state_factory = lambda: sqlite3.connect(state_path)

            first = import_legacy_signal_lifecycle(legacy_factory, state_factory)
            second = import_legacy_signal_lifecycle(legacy_factory, state_factory)
            self.assertEqual(first, {"already_complete": False, "signals": 1})
            self.assertEqual(second, {"already_complete": True, "signals": 0})
            self.assertTrue(signal_lifecycle_parity_report(
                legacy_factory, state_factory,
            )["ok"])

            legacy = legacy_factory()
            legacy.execute(
                """UPDATE signal_execution_state SET status='closed',
                   closed_at='2026-09-02',cancel_reason='tp2' WHERE signal_id=7"""
            )
            legacy.execute(
                "UPDATE signals SET result='tp2',closed_at='2026-09-02' WHERE id=7"
            )
            legacy.commit(); legacy.close()
            import_legacy_signal_lifecycle(legacy_factory, state_factory, refresh=True)
            state = state_factory()
            row = state.execute(
                "SELECT status,result,closed_at,cancel_reason FROM signal_lifecycle"
            ).fetchone()
            self.assertEqual(row, ("closed", "tp2", "2026-09-02", "tp2"))
            state.execute("UPDATE signal_lifecycle SET result='sl' WHERE signal_id=7")
            state.commit(); state.close()
            parity = signal_lifecycle_parity_report(legacy_factory, state_factory)
            self.assertFalse(parity["ok"])
            self.assertIn("signal:7:result", parity["mismatches"])

    def test_minimal_legacy_signal_schema_has_safe_defaults(self):
        legacy = sqlite3.connect(":memory:")
        legacy.execute("CREATE TABLE signals(id INTEGER PRIMARY KEY,result TEXT)")
        legacy.execute("INSERT INTO signals VALUES(1,'pending')")
        state = sqlite3.connect(":memory:")
        migrate_state(state)

        class Proxy:
            def __init__(self, connection): self.connection = connection
            def __getattr__(self, name): return getattr(self.connection, name)
            def __setattr__(self, name, value):
                if name == "connection": object.__setattr__(self, name, value)
                else: setattr(self.connection, name, value)
            def close(self): return None

        result = import_legacy_signal_lifecycle(
            lambda: Proxy(legacy), lambda: Proxy(state),
        )
        self.assertEqual(result["signals"], 1)
        self.assertEqual(
            state.execute("SELECT status,result FROM signal_lifecycle").fetchone(),
            ("active", "pending"),
        )
        legacy.close(); state.close()


if __name__ == "__main__":
    unittest.main()
