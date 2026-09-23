from __future__ import annotations

import sqlite3
import os
import tempfile
import unittest

from apex.db.repositories.execution_ledger import (
    ExecutionLedgerRepository,
    ExecutionLedgerStateError,
)
from apex.db.repositories.executions import ExecutionRepository
from apex.db.state_db import migrate_state
from apex.domain.ids import is_id, new_id
from apex.execution.ledger import (
    ExecutionSnapshot,
    _save_funding_coverage,
    actual_result,
    configure_execution_ledger_state,
    connect,
    reconcile_funding_one,
    register_execution_orders,
    register_order,
    save_fills,
)


class ExecutionLedgerStateTests(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row
        migrate_state(self.connection)

        class SharedConnection:
            def __init__(shared, connection):
                object.__setattr__(shared, "connection", connection)

            def __getattr__(shared, name):
                return getattr(shared.connection, name)

            def __setattr__(shared, name, value):
                if name == "connection":
                    object.__setattr__(shared, name, value)
                else:
                    setattr(shared.connection, name, value)

            def close(shared):
                return None

        self.factory = lambda: SharedConnection(self.connection)
        ExecutionRepository(self.factory).register({
            "signal_id": 7, "mode": "live", "symbol": "BTCUSDT",
            "direction": "LONG", "status": "PROTECTED", "entry": 100,
            "sl": 95, "tp1": 110, "tp2": 115, "quantity": 1,
        })
        self.repository = ExecutionLedgerRepository(self.factory)

    def tearDown(self):
        configure_execution_ledger_state(None)
        self.connection.close()

    def test_order_and_fill_are_idempotent_and_owned(self):
        order = {
            "signal_id": 7, "symbol": "BTCUSDT", "kind": "ENTRY",
            "remote_id": "entry-1", "is_algo": False,
            "expected_side": "BUY", "standard_id": "entry-1",
        }
        self.assertTrue(self.repository.register_order(order))
        self.assertFalse(self.repository.register_order(order))
        fill = {
            "symbol": "BTCUSDT", "orderId": "entry-1", "id": 99,
            "side": "BUY", "qty": "1", "price": "100",
            "commission": "0.04", "commissionAsset": "USDT", "time": 1234,
        }
        self.assertEqual(self.repository.record_fills(7, "entry-1", "ENTRY", [fill]), 1)
        self.assertEqual(self.repository.record_fills(7, "entry-1", "ENTRY", [fill]), 0)
        stored = self.repository.fills(7)
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0]["trade_id"], "99")
        execution_signal = ExecutionRepository(self.factory).get(7)["signal_entity_id"]
        order_signal = self.connection.execute(
            "SELECT signal_entity_id FROM execution_orders WHERE signal_id=7"
        ).fetchone()[0]
        self.assertTrue(is_id(order_signal, "signal"))
        self.assertEqual(order_signal, execution_signal)
        self.assertEqual(stored[0]["signal_entity_id"], execution_signal)
        with self.assertRaisesRegex(
            ExecutionLedgerStateError, "ledger_signal_identity_mismatch"
        ):
            self.repository.register_order({
                **order, "remote_id": "entry-2", "standard_id": "entry-2",
                "signal_entity_id": new_id("signal"),
            })

    def test_order_poll_claim_is_state_owned_and_rate_limited(self):
        self.repository.register_order({
            "signal_id": 7, "symbol": "BTCUSDT", "kind": "ENTRY",
            "remote_id": "entry-1", "is_algo": False,
            "expected_side": "BUY", "standard_id": "entry-1",
        })
        status, order = self.repository.claim_order_poll(1000)
        self.assertEqual(status, "READY")
        self.assertEqual(order["remote_id"], "entry-1")
        self.assertEqual(order["checked_at"], 1000)
        self.assertEqual(self.repository.claim_order_poll(1001), ("DEFERRED", None))
        self.repository.update_order_state(7, "ENTRY", "entry-1", complete=1)
        self.assertEqual(self.repository.claim_order_poll(1061), ("IDLE", None))

    def test_owned_orders_are_discovered_from_state_without_legacy_db(self):
        executions = ExecutionRepository(self.factory)
        executions.update_exchange_state(
            7,
            entry_order_id="entry-7",
            stop_order_id="stop-7",
            tp1_order_id="tp1-7",
        )
        self.assertTrue(executions.claim_action("close-7", 7, "CLOSE", None))
        self.assertTrue(executions.finish_action(
            "close-7", "EXECUTED", order_id="close-order-7",
        ))
        configure_execution_ledger_state(self.factory)

        with tempfile.TemporaryDirectory() as folder:
            compatibility_path = os.path.join(folder, "brain.db")
            register_execution_orders(compatibility_path)
            self.assertFalse(os.path.exists(compatibility_path))

        rows = self.connection.execute(
            """SELECT kind,remote_id,expected_side,is_algo FROM execution_orders
                 WHERE signal_id=7 ORDER BY kind"""
        ).fetchall()
        self.assertEqual(
            [tuple(row) for row in rows],
            [
                ("CLOSE", "close-order-7", "SELL", 0),
                ("ENTRY", "entry-7", "BUY", 0),
                ("SL", "stop-7", "SELL", 1),
                ("TP1", "tp1-7", "SELL", 1),
            ],
        )

    def test_unregistered_or_mismatched_fill_is_rejected_atomically(self):
        self.repository.register_order({
            "signal_id": 7, "symbol": "BTCUSDT", "kind": "ENTRY",
            "remote_id": "entry-1", "is_algo": False,
            "expected_side": "BUY", "standard_id": "entry-1",
        })
        valid = {
            "symbol": "BTCUSDT", "orderId": "entry-1", "id": 1,
            "side": "BUY", "qty": "1", "price": "100",
            "commission": "0.04", "commissionAsset": "USDT", "time": 1234,
        }
        invalid = {**valid, "id": 2, "symbol": "ETHUSDT"}
        with self.assertRaisesRegex(ExecutionLedgerStateError, "symbol_mismatch"):
            self.repository.record_fills(7, "entry-1", "ENTRY", [valid, invalid])
        self.assertEqual(self.repository.fills(7), [])
        with self.assertRaisesRegex(ExecutionLedgerStateError, "unregistered_order"):
            self.repository.record_fills(7, "missing", "ENTRY", [valid])

    def test_authoritative_accounting_reads_and_completes_state_only(self):
        for kind, order_id, side in (
            ("ENTRY", "entry-1", "BUY"), ("CLOSE", "close-1", "SELL"),
        ):
            self.repository.register_order({
                "signal_id": 7, "symbol": "BTCUSDT", "kind": kind,
                "remote_id": order_id, "is_algo": False,
                "expected_side": side, "standard_id": order_id,
            })
        self.repository.record_fills(7, "entry-1", "ENTRY", [{
            "symbol": "BTCUSDT", "orderId": "entry-1", "id": 1,
            "side": "BUY", "qty": "1", "price": "100",
            "commission": "0.04", "commissionAsset": "USDT", "time": 1000,
        }])
        self.repository.record_fills(7, "close-1", "CLOSE", [{
            "symbol": "BTCUSDT", "orderId": "close-1", "id": 2,
            "side": "SELL", "qty": "1", "price": "110",
            "commission": "0.04", "commissionAsset": "USDT", "time": 2000,
        }])
        self.repository.record_funding_coverage(
            7, "BTCUSDT", 1000, 2000,
            [{
                "incomeType": "FUNDING_FEE", "symbol": "BTCUSDT",
                "asset": "USDT", "income": "-0.1", "time": 1500,
                "tranId": "fund-7",
            }],
            checked_at=1,
        )
        configure_execution_ledger_state(self.factory)
        with tempfile.TemporaryDirectory() as folder:
            compatibility_path = os.path.join(folder, "missing-brain.db")
            result = actual_result(
                compatibility_path,
                ExecutionSnapshot(7, "BTCUSDT", "FAST", "BULLISH", 100, 95, 110, 115, 115, 1),
                authoritative_snapshot=True,
            )
            self.assertFalse(os.path.exists(compatibility_path))
        self.assertEqual(result["status"], "CLOSED")
        self.assertEqual(result["funding_quote"], -0.1)
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) FROM execution_orders WHERE signal_id=7 AND complete=1"
            ).fetchone()[0],
            2,
        )

    def test_state_writer_does_not_create_compatibility_ledger(self):
        configure_execution_ledger_state(self.factory)
        with tempfile.TemporaryDirectory() as folder:
            compatibility_path = os.path.join(folder, "brain.db")
            register_order(
                compatibility_path, 7, "BTCUSDT", "ENTRY", "entry-7", "BUY",
            )
            self.assertFalse(os.path.exists(compatibility_path))
            order = {
                "signal_id": 7, "symbol": "BTCUSDT", "kind": "ENTRY",
                "remote_id": "entry-7", "standard_id": "entry-7",
                "expected_side": "BUY",
            }
            fill = {
                "symbol": "BTCUSDT", "orderId": "entry-7", "id": 9,
                "side": "BUY", "qty": "1", "price": "100",
                "commission": "0.04", "commissionAsset": "USDT", "time": 1000,
            }
            save_fills(compatibility_path, order, [fill])
            self.assertFalse(os.path.exists(compatibility_path))
        self.assertEqual(len(self.repository.fills(7)), 1)

    def test_state_funding_writer_does_not_create_compatibility_ledger(self):
        configure_execution_ledger_state(self.factory)
        funding = [{
            "symbol": "BTCUSDT", "incomeType": "FUNDING_FEE",
            "income": "-0.1", "asset": "USDT", "time": 1500, "tranId": 77,
        }]
        with tempfile.TemporaryDirectory() as folder:
            compatibility_path = os.path.join(folder, "brain.db")
            _save_funding_coverage(
                compatibility_path, 7, "BTCUSDT", 1000, 2000, funding,
            )
            self.assertFalse(os.path.exists(compatibility_path))
        evidence = self.repository.accounting_evidence(7)
        self.assertEqual(evidence["funding_status"], "COMPLETE")
        self.assertEqual(evidence["funding_income"], ["-0.1"])

    def test_funding_poll_uses_state_without_legacy_execution(self):
        for kind, order_id, side in (
            ("ENTRY", "entry-7", "BUY"), ("CLOSE", "close-7", "SELL"),
        ):
            self.repository.register_order({
                "signal_id": 7, "symbol": "BTCUSDT", "kind": kind,
                "remote_id": order_id, "is_algo": False,
                "expected_side": side, "standard_id": order_id,
            })
        for kind, order_id, side, trade_id, price, time_ms in (
            ("ENTRY", "entry-7", "BUY", 1, "100", 1000),
            ("CLOSE", "close-7", "SELL", 2, "110", 2000),
        ):
            self.repository.record_fills(7, order_id, kind, [{
                "symbol": "BTCUSDT", "orderId": order_id, "id": trade_id,
                "side": side, "qty": "1", "price": price,
                "commission": "0.04", "commissionAsset": "USDT", "time": time_ms,
            }])
        configure_execution_ledger_state(self.factory)

        class Client:
            def income_history(client_self, **kwargs):
                self.assertEqual(kwargs["start_time"], 1000)
                self.assertEqual(kwargs["end_time"], 2000)
                return [{
                    "symbol": "BTCUSDT", "incomeType": "FUNDING_FEE",
                    "income": "-0.1", "asset": "USDT", "time": 1500, "tranId": 88,
                }]

        with tempfile.TemporaryDirectory() as folder:
            compatibility_path = os.path.join(folder, "brain.db")
            self.assertEqual(
                reconcile_funding_one(compatibility_path, Client(), now=3000),
                "FUNDING_SAVED",
            )
            legacy = connect(compatibility_path)
            self.assertIsNone(legacy.execute(
                "SELECT 1 FROM sqlite_master WHERE name='trade_executions'"
            ).fetchone())
            legacy.close()
        self.assertEqual(self.repository.accounting_evidence(7)["funding_status"], "COMPLETE")
        self.assertFalse(self.repository.poll_due("FUNDING", 3001))


if __name__ == "__main__":
    unittest.main()
