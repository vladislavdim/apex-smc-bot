import os
import tempfile
import unittest
from core.execution_ledger import (
    ExecutionSnapshot, actual_result, connect, reconcile_funding_one,
    reconcile_one, register_order, save_fills,
)


class ExecutionLedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "brain.db")
        self.snap = ExecutionSnapshot(1, "AAVEUSDT", "MTF", "BULLISH", 100, 90, 110, 120, 120, 2)

    def test_snapshot_explicit_null_tp3_falls_back_to_tp2(self):
        snapshot = ExecutionSnapshot.from_mapping({
            "signal_id": 7,
            "symbol": "BTCUSDT",
            "strategy": "SWING",
            "direction": "LONG",
            "entry": 100,
            "initial_sl": 95,
            "tp1": 108,
            "tp2": 112,
            "tp3": None,
        })

        self.assertEqual(snapshot.terminal_tp, 112)

    def add(self, kind, order_id, trade_id, qty, price, fee=".1", asset="USDT"):
        side = "BUY" if kind == "ENTRY" else "SELL"
        register_order(self.path, 1, "AAVEUSDT", kind, order_id, side)
        conn = connect(self.path)
        order = dict(conn.execute("SELECT * FROM confirmed_execution_orders WHERE remote_id=?", (order_id,)).fetchone())
        conn.close()
        fill = dict(symbol="AAVEUSDT", orderId=order_id, id=trade_id, side=side, qty=qty,
                    price=price, commission=fee, commissionAsset=asset, time=trade_id*1000)
        save_fills(self.path, order, [fill])
        save_fills(self.path, order, [fill])

    def test_partial_exits_fees_and_restart_are_exact(self):
        self.add("ENTRY", "10", 1, "2", "100")
        self.add("TP1", "11", 2, "1", "110")
        self.assertIsNone(actual_result(self.path, self.snap)["net_r"])
        self.add("CLOSE", "12", 3, "1", "120")
        result = actual_result(self.path, self.snap)
        self.assertEqual(result["gross_r"], 1.5)
        self.assertAlmostEqual(result["net_r"], 1.485)
        self.assertAlmostEqual(result["fees_quote"], 0.3)
        self.assertEqual(result["gross_pct"], 15)
        self.assertEqual(result["exit_price"], 115)
        self.assertEqual(result, actual_result(self.path, self.snap))

    def test_unknown_fee_asset_never_invents_net(self):
        self.add("ENTRY", "10", 1, "2", "100", asset="BNB")
        self.add("CLOSE", "11", 2, "2", "110")
        self.assertEqual(actual_result(self.path, self.snap)["status"], "FEES_UNRESOLVED")
        self.assertIsNone(actual_result(self.path, self.snap)["net_r"])

    def test_authoritative_snapshot_quantity_ignores_stale_legacy_projection(self):
        self.add("ENTRY", "10", 1, "2", "100")
        self.add("CLOSE", "11", 2, "2", "110")
        conn = connect(self.path)
        conn.execute("""CREATE TABLE trade_executions (
            signal_id INTEGER, mode TEXT, quantity REAL
        )""")
        conn.execute("INSERT INTO trade_executions VALUES(1,'live',3)")
        conn.commit(); conn.close()
        self.assertEqual(actual_result(self.path, self.snap)["status"], "UNVERIFIED_EXECUTION")
        result = actual_result(self.path, self.snap, authoritative_snapshot=True)
        self.assertEqual(result["status"], "CLOSED")
        self.assertEqual(result["quantity"], 2)

    def test_manager_closes_on_fills_but_waits_for_funding_before_final_r(self):
        conn = connect(self.path)
        conn.execute("""CREATE TABLE trade_manager_state(
            signal_id INTEGER PRIMARY KEY,status TEXT,manager_state TEXT,close_result TEXT,
            exit_price REAL,realized_pct REAL,realized_r REAL,closed_at TEXT,last_price REAL,
            current_r REAL,last_event TEXT,updated_at TEXT
        )""")
        conn.execute(
            "INSERT INTO trade_manager_state(signal_id,status,manager_state) VALUES(1,'ACTIVE','PROTECTED')"
        )
        conn.commit(); conn.close()
        self.add("ENTRY", "10", 1, "2", "100")
        self.add("CLOSE", "11", 2, "2", "110")

        preliminary = actual_result(self.path, self.snap)
        self.assertEqual(preliminary["status"], "CLOSED")
        conn = connect(self.path)
        manager = conn.execute(
            "SELECT status,realized_r,last_event FROM trade_manager_state WHERE signal_id=1"
        ).fetchone()
        self.assertEqual(tuple(manager), (
            "CLOSED", None, "CONFIRMED_BINANCE_FILLS_ACCOUNTING_PENDING",
        ))
        conn.execute(
            """INSERT INTO confirmed_execution_funding_coverage
               (signal_id,start_ms,end_ms,checked_at,status) VALUES(1,1000,2000,1,'COMPLETE')"""
        )
        conn.commit(); conn.close()

        final = actual_result(self.path, self.snap)
        self.assertEqual(final["accounting_basis"], "confirmed_fills_after_commissions_and_funding")
        conn = connect(self.path)
        manager = conn.execute(
            "SELECT realized_r,last_event FROM trade_manager_state WHERE signal_id=1"
        ).fetchone()
        conn.close()
        self.assertAlmostEqual(manager[0], final["net_r"])
        self.assertEqual(manager[1], "CONFIRMED_BINANCE_FILLS")

    def test_no_client_when_idle_and_shared_poll_limit(self):
        calls = []
        def factory():
            calls.append(1)
            class Client:
                def account_order_trades(self, *args):
                    return []
            return Client()
        self.assertEqual(reconcile_one(self.path, factory, now=1000), "IDLE")
        self.assertEqual(calls, [])
        register_order(self.path, 1, "AAVEUSDT", "ENTRY", "10", "BUY")
        self.assertEqual(reconcile_one(self.path, factory, now=1000), "FILLS_SAVED")
        self.assertEqual(reconcile_one(self.path, factory, now=1001), "DEFERRED")
        self.assertEqual(len(calls), 1)

    def test_wrong_symbol_and_truncated_page_are_rejected(self):
        register_order(self.path, 1, "AAVEUSDT", "ENTRY", "10", "BUY")
        conn = connect(self.path)
        order = dict(conn.execute("SELECT * FROM confirmed_execution_orders").fetchone())
        conn.close()
        with self.assertRaises(ValueError):
            save_fills(self.path, order, [dict(symbol="BTCUSDT", orderId="10", side="BUY")])
        class Client:
            def account_order_trades(self, *args):
                return [{}]*1000
        self.assertEqual(reconcile_one(self.path, Client(), now=1000), "UNAVAILABLE")
        self.assertIsNone(actual_result(self.path, self.snap)["net_r"])

    def test_funding_is_resolved_from_exact_position_window(self):
        self.add("ENTRY", "10", 1, "2", "100")
        self.add("CLOSE", "11", 2, "2", "110")
        conn = connect(self.path)
        conn.execute("""CREATE TABLE trade_executions (
            id INTEGER PRIMARY KEY, signal_id INTEGER, mode TEXT, symbol TEXT,
            direction TEXT, entry REAL, sl REAL, tp1 REAL, tp2 REAL,
            quantity REAL, grade TEXT
        )""")
        conn.execute(
            "INSERT INTO trade_executions VALUES(1,1,'live','AAVEUSDT','BULLISH',100,90,110,120,2,'MTF')"
        )
        conn.commit(); conn.close()

        calls = []
        class Client:
            def income_history(self, **kwargs):
                calls.append(kwargs)
                return [{
                    "symbol": "AAVEUSDT", "incomeType": "FUNDING_FEE",
                    "income": "-0.2", "asset": "USDT", "time": 1500,
                    "tranId": 99,
                }]

        self.assertEqual(reconcile_funding_one(self.path, Client(), now=1000), "FUNDING_SAVED")
        self.assertEqual(calls[0]["start_time"], 1000)
        self.assertEqual(calls[0]["end_time"], 2000)
        result = actual_result(self.path, self.snap)
        self.assertEqual(result["funding_quote"], -0.2)
        self.assertAlmostEqual(result["net_r"], 0.98)
        self.assertEqual(result["accounting_basis"], "confirmed_fills_after_commissions_and_funding")
        self.assertEqual(reconcile_funding_one(self.path, Client(), now=1001), "DEFERRED")
