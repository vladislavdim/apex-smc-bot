import os
import sqlite3
import tempfile
import unittest

from apex.db.execution_migration import execution_parity_report, import_legacy_executions
from apex.db.state_db import migrate_state


class ExecutionMigrationTests(unittest.TestCase):
    def test_restart_safe_import_refresh_and_parity(self):
        with tempfile.TemporaryDirectory() as folder:
            legacy_path = os.path.join(folder, "brain.db")
            state_path = os.path.join(folder, "state.db")
            legacy = sqlite3.connect(legacy_path)
            legacy.executescript("""
                CREATE TABLE trade_executions(
                    id INTEGER PRIMARY KEY,signal_id INTEGER UNIQUE,mode TEXT,exchange TEXT,
                    symbol TEXT,direction TEXT,status TEXT,entry REAL,sl REAL,tp1 REAL,tp2 REAL,
                    quantity REAL,risk_usdt REAL,balance_usdt REAL,leverage INTEGER,
                    entry_order_id TEXT,stop_order_id TEXT,tp1_order_id TEXT,tp2_order_id TEXT,
                    active_stop_price REAL,pending_stop_order_id TEXT,previous_stop_order_id TEXT,
                    last_error TEXT,created_at TEXT,updated_at TEXT
                );
                CREATE TABLE manager_execution_actions(
                    action_key TEXT PRIMARY KEY,signal_id INTEGER,action TEXT,status TEXT,
                    requested_level REAL,exchange_order_id TEXT,error TEXT,
                    created_at TEXT,updated_at TEXT
                );
                INSERT INTO trade_executions VALUES(
                    4,21,'live','binance_futures','BTCUSDT','BULLISH','ENTRY_PENDING',
                    100,95,110,115,0.01,5,1000,5,'entry-1',NULL,NULL,NULL,NULL,NULL,NULL,'',
                    '2026-01-01','2026-01-01'
                );
                INSERT INTO manager_execution_actions VALUES(
                    'protect-21',21,'PROTECT','PROCESSING',98,NULL,NULL,'2026-01-01','2026-01-01'
                );
            """)
            legacy.commit(); legacy.close()
            state = sqlite3.connect(state_path); migrate_state(state); state.close()
            legacy_factory = lambda: sqlite3.connect(legacy_path)
            state_factory = lambda: sqlite3.connect(state_path)

            first = import_legacy_executions(legacy_factory, state_factory)
            second = import_legacy_executions(legacy_factory, state_factory)
            self.assertEqual(first, {"already_complete": False, "executions": 1, "actions": 1})
            self.assertEqual(second, {"already_complete": True, "executions": 0, "actions": 0})
            self.assertTrue(execution_parity_report(legacy_factory, state_factory)["ok"])

            source = legacy_factory()
            source.execute(
                """UPDATE trade_executions SET status='PROTECTED',stop_order_id='stop-1',
                   active_stop_price=95 WHERE signal_id=21"""
            )
            source.execute(
                """UPDATE manager_execution_actions SET status='EXECUTED',
                   exchange_order_id='stop-1' WHERE action_key='protect-21'"""
            )
            source.commit(); source.close()
            refreshed = import_legacy_executions(legacy_factory, state_factory, refresh=True)
            self.assertEqual(refreshed["executions"], 0)
            self.assertEqual(refreshed["actions"], 0)
            self.assertTrue(execution_parity_report(legacy_factory, state_factory)["ok"])
            target = state_factory()
            self.assertEqual(
                target.execute(
                    "SELECT status,stop_order_id,active_stop_price FROM executions WHERE signal_id=21"
                ).fetchone(),
                ("ENTRY_PENDING", None, None),
            )
            self.assertEqual(
                target.execute(
                    "SELECT status,exchange_order_id FROM execution_actions WHERE action_key='protect-21'"
                ).fetchone(),
                ("PROCESSING", None),
            )
            target.close()

            target = state_factory()
            target.execute("UPDATE executions SET sl=94 WHERE signal_id=21")
            target.commit(); target.close()
            report = execution_parity_report(legacy_factory, state_factory)
            self.assertFalse(report["ok"])
            self.assertIn("execution:21:sl", report["mismatches"])

    def test_orphan_action_blocks_import(self):
        legacy = sqlite3.connect(":memory:")
        legacy.executescript("""
            CREATE TABLE manager_execution_actions(
                action_key TEXT PRIMARY KEY,signal_id INTEGER,action TEXT,status TEXT,
                requested_level REAL,exchange_order_id TEXT,error TEXT,
                created_at TEXT,updated_at TEXT
            );
            INSERT INTO manager_execution_actions VALUES(
                'orphan',404,'PROTECT','ERROR',98,NULL,'missing','now','now'
            );
        """)
        state = sqlite3.connect(":memory:"); migrate_state(state)

        class Proxy:
            def __init__(self, connection): object.__setattr__(self, "connection", connection)
            def __getattr__(self, name): return getattr(self.connection, name)
            def __setattr__(self, name, value):
                if name == "connection": object.__setattr__(self, name, value)
                else: setattr(self.connection, name, value)
            def close(self): return None

        from apex.db.repositories.executions import ExecutionStateError
        with self.assertRaisesRegex(ExecutionStateError, "execution_action_orphan"):
            import_legacy_executions(lambda: Proxy(legacy), lambda: Proxy(state))
        self.assertIsNone(state.execute(
            "SELECT 1 FROM runtime_state WHERE key='execution_state_legacy_import_v1'"
        ).fetchone())
        legacy.close(); state.close()


if __name__ == "__main__":
    unittest.main()
