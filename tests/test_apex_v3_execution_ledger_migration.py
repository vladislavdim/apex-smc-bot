from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from apex.db.execution_ledger_migration import (
    execution_ledger_parity_report,
    import_legacy_execution_ledger,
)
from apex.db.repositories.execution_ledger import ExecutionLedgerStateError
from apex.db.repositories.executions import ExecutionRepository
from apex.db.state_db import migrate_state
from apex.domain.ids import is_id


class ExecutionLedgerMigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.legacy_path = os.path.join(self.temp.name, "brain.db")
        self.state_path = os.path.join(self.temp.name, "state.db")
        legacy = sqlite3.connect(self.legacy_path)
        legacy.executescript("""
            CREATE TABLE confirmed_execution_orders(
                signal_id INTEGER,symbol TEXT,kind TEXT,remote_id TEXT,is_algo INTEGER,
                expected_side TEXT,standard_id TEXT,complete INTEGER,checked_at REAL,error TEXT
            );
            CREATE TABLE confirmed_execution_fills(
                symbol TEXT,trade_id TEXT,signal_id INTEGER,order_id TEXT,kind TEXT,
                qty TEXT,price TEXT,commission TEXT,commission_asset TEXT,time_ms INTEGER,
                payload TEXT
            );
            CREATE TABLE confirmed_execution_funding(
                signal_id INTEGER,tran_id TEXT,symbol TEXT,income TEXT,asset TEXT,
                time_ms INTEGER,payload TEXT
            );
            CREATE TABLE confirmed_execution_funding_coverage(
                signal_id INTEGER,start_ms INTEGER,end_ms INTEGER,checked_at REAL,status TEXT
            );
            CREATE TABLE confirmed_execution_poll(id INTEGER,attempted_at REAL);
            CREATE TABLE confirmed_execution_funding_poll(id INTEGER,attempted_at REAL);
            INSERT INTO confirmed_execution_orders VALUES(
                7,'BTCUSDT','ENTRY','entry-7',0,'BUY','entry-7',1,123,NULL
            );
            INSERT INTO confirmed_execution_fills VALUES(
                'BTCUSDT','99',7,'entry-7','ENTRY','1','100','0.04','USDT',1000,'{}'
            );
            INSERT INTO confirmed_execution_funding VALUES(
                7,'fund-1','BTCUSDT','-0.1','USDT',2000,'{}'
            );
            INSERT INTO confirmed_execution_funding_coverage VALUES(
                7,1000,3000,5,'COMPLETE'
            );
            INSERT INTO confirmed_execution_poll VALUES(1,10);
            INSERT INTO confirmed_execution_funding_poll VALUES(1,20);
        """)
        legacy.commit(); legacy.close()
        state = sqlite3.connect(self.state_path)
        migrate_state(state); state.close()
        ExecutionRepository(self.state_factory).register({
            "signal_id": 7, "mode": "live", "symbol": "BTCUSDT",
            "direction": "LONG", "status": "PROTECTED", "entry": 100,
            "sl": 95, "tp1": 110, "tp2": 115, "quantity": 1,
        })

    def legacy_factory(self):
        return sqlite3.connect(self.legacy_path)

    def state_factory(self):
        return sqlite3.connect(self.state_path)

    def test_restart_safe_import_and_field_parity(self):
        first = import_legacy_execution_ledger(self.legacy_factory, self.state_factory)
        second = import_legacy_execution_ledger(self.legacy_factory, self.state_factory)
        self.assertEqual(first, {"already_complete": False, "rows": 6})
        self.assertEqual(second, {"already_complete": True, "rows": 0})
        self.assertTrue(
            execution_ledger_parity_report(self.legacy_factory, self.state_factory)["ok"]
        )
        state = self.state_factory()
        self.assertEqual(
            state.execute("SELECT attempted_at FROM execution_ledger_poll WHERE poll_kind='FILLS'").fetchone()[0],
            10,
        )
        typed_ids = {
            state.execute(f"SELECT signal_entity_id FROM {table}").fetchone()[0]
            for table in (
                "execution_orders", "execution_fills", "execution_funding",
                "execution_funding_coverage",
            )
        }
        self.assertEqual(len(typed_ids), 1)
        self.assertTrue(is_id(next(iter(typed_ids)), "signal"))
        state.close()

    def test_orphan_rolls_back_without_completion_marker(self):
        legacy = self.legacy_factory()
        legacy.execute("UPDATE confirmed_execution_orders SET signal_id=404")
        legacy.commit(); legacy.close()
        with self.assertRaisesRegex(ExecutionLedgerStateError, "ledger_import_orphan"):
            import_legacy_execution_ledger(self.legacy_factory, self.state_factory)
        state = self.state_factory()
        self.assertIsNone(state.execute(
            "SELECT 1 FROM runtime_state WHERE key='execution_ledger_legacy_import_v1'"
        ).fetchone())
        self.assertEqual(state.execute("SELECT COUNT(*) FROM execution_orders").fetchone()[0], 0)
        state.close()

    def test_refresh_never_overwrites_newer_state_progress(self):
        import_legacy_execution_ledger(self.legacy_factory, self.state_factory)
        state = self.state_factory()
        state.execute(
            """UPDATE execution_orders SET complete=0,checked_at=999,error='state-newer'
                 WHERE signal_id=7 AND kind='ENTRY' AND remote_id='entry-7'"""
        )
        state.execute(
            """UPDATE execution_funding_coverage
                  SET end_ms=9000,checked_at=999,status='PENDING' WHERE signal_id=7"""
        )
        state.execute(
            "UPDATE execution_ledger_poll SET attempted_at=999 WHERE poll_kind='FILLS'"
        )
        state.commit(); state.close()

        refreshed = import_legacy_execution_ledger(
            self.legacy_factory, self.state_factory, refresh=True,
        )
        self.assertEqual(refreshed["rows"], 0)
        self.assertTrue(
            execution_ledger_parity_report(self.legacy_factory, self.state_factory)["ok"]
        )
        state = self.state_factory()
        self.assertEqual(state.execute(
            "SELECT complete,checked_at,error FROM execution_orders WHERE signal_id=7"
        ).fetchone(), (0, 999.0, "state-newer"))
        self.assertEqual(state.execute(
            "SELECT end_ms,checked_at,status FROM execution_funding_coverage WHERE signal_id=7"
        ).fetchone(), (9000, 999.0, "PENDING"))
        self.assertEqual(state.execute(
            "SELECT attempted_at FROM execution_ledger_poll WHERE poll_kind='FILLS'"
        ).fetchone()[0], 999.0)
        state.close()

    def test_runtime_refreshes_ledger_before_live_outcome_sync(self):
        source = Path("bot.py").read_text(encoding="utf-8")
        startup = source.index("_execution_import = await _v3_refresh_execution_state_mirror()")
        startup_ledger = source.index("_ledger_import = await _v3_refresh_execution_ledger_mirror()")
        self.assertLess(startup, startup_ledger)
        reconcile = source.index("async def _run_auto_trade_reconcile_once():")
        ledger = source.index("await _v3_refresh_execution_ledger_mirror()", reconcile)
        outcomes = source.index("sync_confirmed_outcomes", ledger)
        self.assertLess(ledger, outcomes)


if __name__ == "__main__":
    unittest.main()
