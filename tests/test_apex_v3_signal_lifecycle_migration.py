import os
import sqlite3
import tempfile
import unittest

from apex.db.signal_lifecycle_migration import (
    import_legacy_signal_lifecycle,
    signal_lifecycle_parity_report,
)
from apex.db.state_db import migrate_state
from apex.db.repositories.signal_lifecycle import SignalLifecycleRepository, SignalLifecycleStateError
from apex.domain.ids import derived_id


class SignalLifecycleMigrationTests(unittest.TestCase):
    def test_state_monitor_transitions_are_restart_safe(self):
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "state.db")
            conn = sqlite3.connect(path); migrate_state(conn); conn.close()
            factory = lambda: sqlite3.connect(path)
            repository = SignalLifecycleRepository(factory)
            repository.import_row({
                "signal_id": 11, "status": "waiting_entry", "result": "pending",
                "symbol": "BTCUSDT", "direction": "BULLISH", "signal_type": "MTF",
                "timeframe": "1h", "entry": 100, "sl": 95, "tp1": 110,
                "tp2": 120, "tp3": 130,
            })
            self.assertTrue(repository.advance_monitor(11, expected_status="waiting_entry", transition="activate"))
            self.assertFalse(repository.advance_monitor(11, expected_status="waiting_entry", transition="activate"))
            self.assertTrue(repository.advance_monitor(
                11, expected_status="active", transition="progress",
                tp1_hit=True, trailing_sl=102, best_price=111,
            ))
            self.assertTrue(repository.advance_monitor(
                11, expected_status="active", transition="close", result="tp2",
            ))
            self.assertFalse(repository.advance_monitor(
                11, expected_status="active", transition="close", result="sl",
            ))
            self.assertEqual(repository.pending_for_monitor(), [])
            self.assertEqual(
                (repository.get(11)["result"], repository.get(11)["entry"], repository.get(11)["trailing_sl"]),
                ("tp2", 100.0, 102.0),
            )
            self.assertEqual(repository.get(11)["ownership"], "state")
            self.assertFalse(repository.import_row({
                "signal_id": 11, "status": "waiting_entry", "result": "pending",
                "symbol": "BTCUSDT", "direction": "BULLISH", "signal_type": "MTF",
                "timeframe": "1h", "entry": 100, "sl": 95, "tp1": 110,
                "tp2": 120, "tp3": 130,
            }))
            self.assertEqual(repository.get(11)["result"], "tp2")
            with self.assertRaisesRegex(SignalLifecycleStateError, "result_invalid"):
                repository.advance_monitor(11, expected_status="active", transition="close", result="pending")

    def test_import_refresh_and_field_parity_are_restart_safe(self):
        with tempfile.TemporaryDirectory() as folder:
            legacy_path = os.path.join(folder, "legacy.db")
            state_path = os.path.join(folder, "state.db")
            legacy = sqlite3.connect(legacy_path)
            legacy.executescript("""
                CREATE TABLE signals(
                    id INTEGER PRIMARY KEY,result TEXT,created_at TEXT,closed_at TEXT,
                    symbol TEXT,direction TEXT,signal_type TEXT,timeframe TEXT,
                    entry REAL,sl REAL,tp1 REAL,tp2 REAL,tp3 REAL,estimated_hours REAL,
                    grade TEXT,tp1_hit INTEGER,trailing_sl REAL,best_price REAL,
                    confluence REAL,regime TEXT
                );
                CREATE TABLE signal_execution_state(
                    signal_id INTEGER PRIMARY KEY,status TEXT,activated_at TEXT,
                    last_checked_at TEXT,closed_at TEXT,cancel_reason TEXT
                );
                INSERT INTO signals VALUES(
                    7,'pending','2026-09-01',NULL,'BTCUSDT','bullish','mtf','1h',
                    100,95,110,120,130,72,'A',0,NULL,NULL,8,'TREND'
                );
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
            pending = SignalLifecycleRepository(state_factory).pending_for_monitor()
            self.assertEqual(
                [(row["symbol"], row["entry"], row["sl"], row["tp1"]) for row in pending],
                [("BTCUSDT", 100, 95, 110)],
            )
            state = state_factory()
            self.assertEqual(
                state.execute(
                    "SELECT direction,entry,sl,tp1,tp2,tp3,signal_type FROM signal_lifecycle WHERE signal_id=7"
                ).fetchone(),
                ("BULLISH", 100, 95, 110, 120, 130, "MTF"),
            )
            state.close()
            state = state_factory()
            state.execute(
                """INSERT INTO signal_lifecycle(signal_entity_id,signal_id,status,result,symbol)
                   VALUES(?,?,?,?,?)""",
                (derived_id("signal", "state-only", 99), 99, "waiting_entry", "pending", "ETHUSDT"),
            )
            state.commit(); state.close()
            import_legacy_signal_lifecycle(legacy_factory, state_factory, refresh=True)
            self.assertTrue(signal_lifecycle_parity_report(
                legacy_factory, state_factory,
            )["ok"])
            state = state_factory()
            self.assertEqual(
                state.execute("SELECT symbol,status FROM signal_lifecycle WHERE signal_id=99").fetchone(),
                ("ETHUSDT", "waiting_entry"),
            )
            state.close()
            state = state_factory()
            self.assertEqual(
                state.execute("SELECT symbol FROM signal_lifecycle WHERE signal_id=7").fetchone(),
                ("BTCUSDT",),
            )
            state.execute(
                "UPDATE signal_lifecycle SET symbol='ETHUSDT' WHERE signal_id=7"
            )
            state.commit()
            state.close()
            self.assertIn(
                "signal:7:symbol",
                signal_lifecycle_parity_report(legacy_factory, state_factory)["mismatches"],
            )
            with self.assertRaisesRegex(SignalLifecycleStateError, "geometry_conflict:symbol"):
                import_legacy_signal_lifecycle(legacy_factory, state_factory, refresh=True)
            state = state_factory()
            state.execute("UPDATE signal_lifecycle SET symbol='BTCUSDT' WHERE signal_id=7")
            state.commit(); state.close()
            import_legacy_signal_lifecycle(legacy_factory, state_factory, refresh=True)
            self.assertEqual(
                SignalLifecycleRepository(state_factory).require_pending_for_execution(
                    7, "BTCUSDT"
                )["signal_id"], 7,
            )
            with self.assertRaisesRegex(SignalLifecycleStateError, "not_ready_for_execution"):
                SignalLifecycleRepository(state_factory).require_pending_for_execution(
                    7, "ETHUSDT"
                )

            legacy = legacy_factory()
            legacy.execute("UPDATE signals SET entry=101 WHERE id=7")
            legacy.commit(); legacy.close()
            with self.assertRaisesRegex(SignalLifecycleStateError, "geometry_conflict:entry"):
                import_legacy_signal_lifecycle(legacy_factory, state_factory, refresh=True)
            legacy = legacy_factory()
            legacy.execute("UPDATE signals SET entry=100 WHERE id=7")
            legacy.commit(); legacy.close()

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
            with self.assertRaisesRegex(SignalLifecycleStateError, "not_ready_for_execution"):
                SignalLifecycleRepository(state_factory).require_pending_for_execution(
                    7, "BTCUSDT"
                )
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
        with self.assertRaisesRegex(SignalLifecycleStateError, "signal_monitor_incomplete:1"):
            SignalLifecycleRepository(lambda: Proxy(state)).pending_for_monitor()
        legacy.close(); state.close()


if __name__ == "__main__":
    unittest.main()
