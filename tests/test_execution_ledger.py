import os
import tempfile
import unittest
from core.execution_ledger import register_order, connect, save_fills, actual_result, reconcile_one
from core.replay_lab import FrozenEntry


class ExecutionLedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "brain.db")
        self.snap = FrozenEntry(1, "AAVEUSDT", "MTF", "BULLISH", 100, 90, 110, 120, 120, 2)

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
        self.assertEqual(result["gross_pct"], 15)
        self.assertEqual(result["exit_price"], 115)
        self.assertEqual(result, actual_result(self.path, self.snap))

    def test_unknown_fee_asset_never_invents_net(self):
        self.add("ENTRY", "10", 1, "2", "100", asset="BNB")
        self.add("CLOSE", "11", 2, "2", "110")
        self.assertEqual(actual_result(self.path, self.snap)["status"], "FEES_UNRESOLVED")
        self.assertIsNone(actual_result(self.path, self.snap)["net_r"])

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
