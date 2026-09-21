import os
import sqlite3
import tempfile
import unittest

from apex.db.manager_migration import import_legacy_manager, manager_parity_report
from apex.db.repositories.manager import ManagerRepository, ManagerStateError
from apex.db.state_db import migrate_state
from apex.domain.ids import is_id


class ManagerMigrationTests(unittest.TestCase):
    def test_manager_runtime_flags_are_state_owned(self):
        with tempfile.TemporaryDirectory() as folder:
            state_path = os.path.join(folder, "state.db")
            legacy_path = os.path.join(folder, "brain.db")

            def factory():
                return sqlite3.connect(state_path)

            conn = factory(); migrate_state(conn); conn.close()
            repository = ManagerRepository(factory)
            repository.set_runtime("opens_enabled", "0")
            repository.set_runtime("opens_enabled", "1")

            self.assertEqual(repository.runtime(), {"opens_enabled": "1"})
            self.assertFalse(os.path.exists(legacy_path))

    def test_import_is_complete_restart_safe_and_preserves_source(self):
        with tempfile.TemporaryDirectory() as folder:
            legacy_path = os.path.join(folder, "legacy.db")
            state_path = os.path.join(folder, "state.db")
            legacy = sqlite3.connect(legacy_path)
            legacy.executescript("""
                CREATE TABLE trade_manager_state(
                    signal_id INTEGER PRIMARY KEY,symbol TEXT,strategy TEXT,direction TEXT,
                    management_tf TEXT,initial_entry REAL,initial_sl REAL,initial_tp1 REAL,
                    initial_tp2 REAL,initial_tp3 REAL,initial_rr REAL,manager_version INTEGER,
                    thesis_json TEXT,status TEXT,manager_state TEXT,position_fraction REAL,
                    partial_exit_done INTEGER,last_price REAL,best_price REAL,current_r REAL,
                    tp1_seen INTEGER,tp2_seen INTEGER,tp3_seen INTEGER,manager_target REAL,
                    manager_protect_level REAL,proposed_protect_level REAL,last_event TEXT,
                    last_action TEXT,last_confidence REAL,last_reviewed_candle TEXT,
                    no_progress_bars INTEGER,progress_anchor_r REAL,last_progress_candle TEXT,
                    reconciliation_reason TEXT,created_at TEXT,updated_at TEXT,closed_at TEXT,
                    close_result TEXT,exit_price REAL,realized_pct REAL,realized_r REAL
                );
                CREATE TABLE trade_manager_events(
                    id INTEGER PRIMARY KEY,signal_id INTEGER,event_type TEXT,action TEXT,
                    confidence REAL,price REAL,r_multiple REAL,manager_target REAL,
                    manager_protect_level REAL,facts_json TEXT,reason TEXT
                );
                CREATE TABLE trade_manager_runtime(
                    key TEXT PRIMARY KEY,value TEXT,updated_at TEXT
                );
            """)
            legacy.execute(
                "INSERT INTO trade_manager_runtime(key,value) VALUES('opens_enabled','1')"
            )
            legacy.execute(
                """INSERT INTO trade_manager_state(
                    signal_id,symbol,strategy,direction,management_tf,initial_entry,initial_sl,
                    initial_tp1,initial_tp2,initial_tp3,initial_rr,manager_version,thesis_json,
                    status,manager_state,position_fraction,current_r,tp1_seen,created_at,updated_at
                ) VALUES(7,'BTCUSDT','MTF','BULLISH','15m',100,95,110,115,120,2,2,'{}',
                         'ACTIVE','PROFIT_PROTECTED',0.5,1.25,1,'2026-01-01','2026-01-02')"""
            )
            legacy.execute(
                """INSERT INTO trade_manager_events VALUES(
                    3,7,'TP1_HIT','PARTIAL_EXIT',0.9,110,2,115,100,'{}','confirmed')"""
            )
            legacy.commit(); legacy.close()

            state = sqlite3.connect(state_path); migrate_state(state); state.close()
            legacy_factory = lambda: sqlite3.connect(legacy_path)
            state_factory = lambda: sqlite3.connect(state_path)
            first = import_legacy_manager(legacy_factory, state_factory)
            second = import_legacy_manager(legacy_factory, state_factory)
            self.assertEqual(first, {"already_complete": False, "positions": 1, "events": 1})
            self.assertEqual(second, {"already_complete": True, "positions": 0, "events": 0})
            check = state_factory()
            position = check.execute(
                "SELECT initial_sl,manager_state,position_fraction,current_r FROM manager_positions"
            ).fetchone()
            self.assertEqual(position, (95.0, "PROFIT_PROTECTED", 0.5, 1.25))
            self.assertEqual(check.execute("SELECT COUNT(*) FROM manager_events").fetchone()[0], 1)
            self.assertEqual(
                check.execute(
                    "SELECT value_json FROM runtime_state WHERE key='manager_runtime:opens_enabled'"
                ).fetchone()[0],
                '"1"',
            )
            position_signal = check.execute(
                "SELECT signal_entity_id FROM manager_positions"
            ).fetchone()[0]
            event_signal = check.execute(
                "SELECT signal_entity_id FROM manager_events"
            ).fetchone()[0]
            self.assertTrue(is_id(position_signal, "signal"))
            self.assertEqual(event_signal, position_signal)
            check.close()
            source = legacy_factory()
            self.assertEqual(source.execute("SELECT COUNT(*) FROM trade_manager_state").fetchone()[0], 1)
            self.assertEqual(source.execute("SELECT COUNT(*) FROM trade_manager_events").fetchone()[0], 1)
            source.close()

            source = legacy_factory()
            source.execute(
                "UPDATE trade_manager_state SET current_r=2.5,manager_state='LET_RUN' WHERE signal_id=7"
            )
            source.execute(
                """INSERT INTO trade_manager_events VALUES(
                    4,7,'BOS','LET_RUN',0.8,112,2.4,120,100,'{}','continuation')"""
            )
            source.commit(); source.close()
            refreshed = import_legacy_manager(
                legacy_factory, state_factory, refresh=True,
            )
            self.assertEqual(refreshed["events"], 1)
            check = state_factory()
            self.assertEqual(
                check.execute("SELECT manager_state,current_r FROM manager_positions").fetchone(),
                ("PROFIT_PROTECTED", 1.25),
            )
            self.assertEqual(check.execute("SELECT COUNT(*) FROM manager_events").fetchone()[0], 2)
            check.close()
            self.assertTrue(manager_parity_report(legacy_factory, state_factory)["ok"])

            source = legacy_factory()
            source.execute("CREATE TABLE trade_executions(signal_id INTEGER,mode TEXT)")
            source.execute("INSERT INTO trade_executions VALUES(7,'live')")
            source.execute(
                """UPDATE trade_manager_state SET status='CLOSED',manager_state='CLOSED',
                   close_result='tp1',exit_price=110,realized_r=2,last_event='TRADE_CLOSED'
                   WHERE signal_id=7"""
            )
            source.commit(); source.close()
            import_legacy_manager(legacy_factory, state_factory, refresh=True)
            check = state_factory()
            projected = check.execute(
                "SELECT status,manager_state,realized_r,last_event FROM manager_positions"
            ).fetchone()
            check.close()
            self.assertEqual(projected, (
                "ACTIVE", "PROFIT_PROTECTED", None, None,
            ))
            self.assertTrue(manager_parity_report(legacy_factory, state_factory)["ok"])

            source = legacy_factory()
            source.execute(
                """UPDATE trade_manager_state SET realized_r=NULL,
                   last_event='CONFIRMED_BINANCE_FILLS_ACCOUNTING_PENDING' WHERE signal_id=7"""
            )
            source.commit(); source.close()
            import_legacy_manager(legacy_factory, state_factory, refresh=True)
            check = state_factory()
            self.assertEqual(check.execute(
                "SELECT status,last_event,realized_r FROM manager_positions"
            ).fetchone(), ("ACTIVE", None, None))
            check.close()
            self.assertTrue(manager_parity_report(legacy_factory, state_factory)["ok"])

            check = state_factory()
            check.execute("UPDATE manager_positions SET initial_sl=94 WHERE signal_id=7")
            check.commit(); check.close()
            parity = manager_parity_report(legacy_factory, state_factory)
            self.assertFalse(parity["ok"])
            self.assertIn("position:7:initial_sl", parity["mismatches"])

    def test_orphan_event_blocks_completion_marker(self):
        legacy = sqlite3.connect(":memory:")
        legacy.executescript("""
            CREATE TABLE trade_manager_state(signal_id INTEGER PRIMARY KEY);
            CREATE TABLE trade_manager_events(
                id INTEGER PRIMARY KEY,signal_id INTEGER,event_type TEXT,action TEXT,
                confidence REAL,price REAL,r_multiple REAL,manager_target REAL,
                manager_protect_level REAL,facts_json TEXT,reason TEXT
            );
            INSERT INTO trade_manager_events VALUES(1,404,'HOLD','HOLD',1,NULL,NULL,NULL,NULL,'{}','orphan');
        """)
        state = sqlite3.connect(":memory:")
        migrate_state(state)

        class Proxy:
            def __init__(self, connection): self.connection = connection
            def __getattr__(self, name): return getattr(self.connection, name)
            def __setattr__(self, name, value):
                if name == "connection": object.__setattr__(self, name, value)
                else: setattr(self.connection, name, value)
            def close(self): return None

        with self.assertRaisesRegex(ManagerStateError, "manager_event_orphan"):
            import_legacy_manager(lambda: Proxy(legacy), lambda: Proxy(state))
        self.assertIsNone(state.execute(
            "SELECT 1 FROM runtime_state WHERE key='manager_state_legacy_import_v1'"
        ).fetchone())
        legacy.close(); state.close()


if __name__ == "__main__":
    unittest.main()
