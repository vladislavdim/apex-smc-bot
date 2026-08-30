import os
import sqlite3
import tempfile
import unittest
from decimal import Decimal

from core.trade_execution import (
    LIVE_CONFIRMATION,
    BinanceFuturesClient,
    ExecutionConfig,
    SymbolRules,
    build_order_plan,
    execute_approved_candidate,
    execution_status,
    reconcile_live_executions,
)


CANDIDATE = {
    "symbol": "BTCUSDT",
    "direction": "BULLISH",
    "entry": 100,
    "sl": 95,
    "tp1": 110,
    "tp2": 115,
    "rr": 2,
    "_external_quality_reviewed": True,
    "_external_quality_review": {
        "decision": "APPROVE", "confidence": 0.9, "degraded": False,
    },
}

RULES = SymbolRules(
    tick_size=Decimal("0.1"),
    step_size=Decimal("0.001"),
    min_qty=Decimal("0.001"),
    min_notional=Decimal("5"),
)


def live_config(**overrides):
    values = dict(
        enabled=True, mode="live", leverage=5, risk_pct=0.5,
        paper_balance_usdt=1000, fee_bps=10, tp1_fraction=0.5,
        api_key="key", api_secret="secret", base_url="https://example.invalid",
        live_confirmation=LIVE_CONFIRMATION, timeout_seconds=3, retries=1,
    )
    values.update(overrides)
    return ExecutionConfig(**values)


class FakeClient:
    def __init__(self, balance=1000, entry_status="NEW", mark_price=100):
        self.balance = balance
        self.entry_status = entry_status
        self.current_mark_price = mark_price
        self.calls = []

    def is_one_way_mode(self):
        self.calls.append("position_mode")
        return True

    def open_positions(self):
        self.calls.append("all_positions")
        return []

    def usdt_balance_details(self):
        self.calls.append("balance_details")
        return {"wallet_balance": self.balance, "available_balance": self.balance}

    def realized_pnl_since(self, start_time_ms):
        self.calls.append("daily_pnl")
        return 0.0

    def has_open_position(self, symbol):
        self.calls.append(("position", symbol))
        return False

    def has_open_orders(self, symbol):
        self.calls.append(("open_orders", symbol))
        return False

    def mark_price(self, symbol):
        self.calls.append(("mark_price", symbol))
        return self.current_mark_price

    def available_usdt(self):
        self.calls.append("balance")
        return self.balance

    def symbol_rules(self, symbol):
        self.calls.append(("rules", symbol))
        return RULES

    def set_leverage(self, symbol, leverage):
        self.calls.append(("leverage", symbol, leverage))
        return {"leverage": leverage}

    def set_isolated_margin(self, symbol):
        self.calls.append(("isolated", symbol))
        return {"code": 200}

    def place_limit_entry(self, plan, client_id):
        self.calls.append(("entry", plan.copy(), client_id))
        return {"orderId": "entry-1"}

    def query_order(self, symbol, order_id):
        self.calls.append(("query", symbol, order_id))
        return {
            "status": self.entry_status,
            "executedQty": "0.980" if self.entry_status == "FILLED" else "0",
        }

    def cancel_order(self, symbol, order_id):
        self.calls.append(("cancel", symbol, order_id))
        return {"status": "CANCELED"}

    def place_close_all_trigger(self, symbol, side, order_type, stop_price, client_id):
        self.calls.append(("close_trigger", side, order_type, stop_price, client_id))
        prefix = "stop" if order_type == "STOP_MARKET" else "tp2"
        return {"algoId": f"{prefix}-1"}

    def place_reduce_trigger(self, symbol, side, quantity, order_type, stop_price, client_id):
        self.calls.append(("reduce_trigger", side, quantity, order_type, stop_price, client_id))
        return {"algoId": "tp1-1"}

    def query_algo_order(self, algo_id="", client_algo_id=""):
        self.calls.append(("query_algo", algo_id or client_algo_id))
        return {"algoId": algo_id or "algo-1", "algoStatus": "NEW"}

    def cancel_algo_order(self, algo_id):
        self.calls.append(("cancel_algo", algo_id))
        return {"algoId": algo_id, "algoStatus": "CANCELED"}

    def emergency_close(self, symbol, direction, quantity, client_id):
        self.calls.append(("emergency", symbol, direction, quantity, client_id))
        return {"orderId": "exit-1"}


class RecordingResponse:
    status_code = 200
    headers = {}
    text = ""

    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload

    def raise_for_status(self):
        return None


class RecordingSession:
    def __init__(self):
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if url.endswith("/fapi/v3/balance"):
            return RecordingResponse([{
                "asset": "USDT", "balance": "11.25",
                "availableBalance": "10.75", "crossUnPnl": "0.50",
            }])
        if url.endswith("/fapi/v1/income"):
            return RecordingResponse([
                {"symbol": "BTCUSDT", "incomeType": "REALIZED_PNL", "income": "1.20", "asset": "USDT", "time": 2000, "tranId": 2},
                {"symbol": "ETHUSDT", "incomeType": "REALIZED_PNL", "income": "-0.30", "asset": "USDT", "time": 1000, "tranId": 1},
                {"symbol": "BTCUSDT", "incomeType": "COMMISSION", "income": "-0.05", "asset": "USDT", "time": 2000, "tranId": 3},
                {"symbol": "", "incomeType": "TRANSFER", "income": "10", "asset": "USDT", "time": 500, "tranId": 4},
            ])
        if url.endswith("/fapi/v1/algoOrder") and method == "POST":
            return RecordingResponse({"algoId": 42, "algoStatus": "NEW"})
        return RecordingResponse({})


class TradeExecutionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "brain.db")

    def tearDown(self):
        self.tmp.cleanup()

    def test_default_configuration_is_disabled_paper(self):
        config = ExecutionConfig.from_env({})
        self.assertFalse(config.enabled)
        self.assertEqual(config.mode, "paper")
        self.assertFalse(config.live_armed)

    def test_environment_caps_leverage_and_risk(self):
        config = ExecutionConfig.from_env({
            "AUTO_TRADING_ENABLED": "true",
            "AUTO_TRADING_LEVERAGE": "50",
            "AUTO_TRADING_RISK_PCT": "9",
        })
        self.assertEqual(config.leverage, 5)
        self.assertEqual(config.risk_pct, 1.0)

    def test_live_kill_switch_blocks_before_exchange(self):
        client = FakeClient()
        result = execute_approved_candidate(
            CANDIDATE, 99, db_path=self.db_path,
            config=live_config(kill_switch=True), client=client,
        )
        self.assertEqual(result["status"], "BLOCKED_KILL_SWITCH")
        self.assertEqual(client.calls, [])

    def test_live_daily_loss_limit_blocks_new_order(self):
        client = FakeClient(balance=1000)
        client.realized_pnl_since = lambda _: -25.0
        result = execute_approved_candidate(
            CANDIDATE, 100, db_path=self.db_path,
            config=live_config(max_daily_loss_pct=2.0), client=client,
        )
        self.assertEqual(result["status"], "BLOCKED_DAILY_LOSS")
        self.assertFalse(any(call[0] == "entry" for call in client.calls if isinstance(call, tuple)))

    def test_live_open_position_limit_blocks_new_order(self):
        client = FakeClient()
        client.open_positions = lambda: [{"symbol": "ETHUSDT", "positionAmt": "1"}]
        result = execute_approved_candidate(
            CANDIDATE, 102, db_path=self.db_path,
            config=live_config(max_open_positions=1), client=client,
        )
        self.assertEqual(result["status"], "BLOCKED_MAX_POSITIONS")
        self.assertFalse(any(call[0] == "entry" for call in client.calls if isinstance(call, tuple)))

    def test_position_size_uses_stop_risk_and_balance(self):
        plan = build_order_plan(CANDIDATE, 1000, live_config(), RULES)
        self.assertTrue(plan["ok"])
        self.assertEqual(plan["leverage"], 5)
        self.assertEqual(plan["quantity"], "0.98")
        self.assertEqual(plan["risk_budget"], 5.0)
        self.assertEqual((plan["entry"], plan["sl"], plan["tp1"], plan["tp2"]), ("100", "95", "110", "115"))

    def test_exchange_rounding_never_moves_stop_inside_structure(self):
        candidate = {
            **CANDIDATE,
            "entry": 100.04, "sl": 95.06, "tp1": 110.09, "tp2": 115.09,
        }
        plan = build_order_plan(candidate, 1000, live_config(), RULES)
        self.assertTrue(plan["ok"])
        self.assertEqual(plan["entry"], "100")
        self.assertEqual(plan["sl"], "95")
        self.assertEqual(plan["tp1"], "110")

        short = {
            **candidate,
            "direction": "BEARISH", "sl": 105.04, "tp1": 90.01, "tp2": 85.01,
        }
        short_plan = build_order_plan(short, 1000, live_config(), RULES)
        self.assertTrue(short_plan["ok"])
        self.assertEqual(short_plan["sl"], "105.1")
        self.assertEqual(short_plan["tp1"], "90.1")

    def test_zero_balance_is_a_safe_skip(self):
        plan = build_order_plan(CANDIDATE, 0, live_config(), RULES)
        self.assertFalse(plan["ok"])
        self.assertEqual(plan["status"], "SKIPPED_NO_BALANCE")

    def test_live_mode_needs_exact_second_confirmation(self):
        config = live_config(live_confirmation="yes")
        result = execute_approved_candidate(CANDIDATE, 1, db_path=self.db_path, config=config)
        self.assertEqual(result["status"], "LIVE_NOT_ARMED")

    def test_live_mode_rejects_candidate_without_groq_review_before_exchange(self):
        candidate = {
            key: value for key, value in CANDIDATE.items()
            if not key.startswith("_external_quality_review")
        }
        client = FakeClient()
        result = execute_approved_candidate(
            candidate, 101, db_path=self.db_path, config=live_config(), client=client,
        )
        self.assertEqual(result["status"], "BLOCKED_GROQ_GUARD")
        self.assertEqual(client.calls, [])

    def test_live_mode_rejects_degraded_groq_approval_before_exchange(self):
        candidate = {
            **CANDIDATE,
            "_external_quality_review": {
                "decision": "APPROVE", "confidence": 0.99, "degraded": True,
            },
        }
        client = FakeClient()
        result = execute_approved_candidate(
            candidate, 102, db_path=self.db_path, config=live_config(), client=client,
        )
        self.assertEqual(result["status"], "BLOCKED_GROQ_GUARD")
        self.assertEqual(client.calls, [])

    def test_paper_mode_records_without_exchange(self):
        config = ExecutionConfig(enabled=True, mode="paper", paper_balance_usdt=1000)
        result = execute_approved_candidate(CANDIDATE, 2, db_path=self.db_path, config=config)
        self.assertEqual(result["status"], "PAPER_PENDING_ENTRY")
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT mode,status,leverage FROM trade_executions WHERE signal_id=2"
            ).fetchone()
        self.assertEqual(row, ("paper", "PAPER_PENDING_ENTRY", 5))

    def test_live_zero_balance_does_not_submit_an_order(self):
        client = FakeClient(balance=0)
        result = execute_approved_candidate(
            CANDIDATE, 3, db_path=self.db_path, config=live_config(), client=client,
        )
        self.assertEqual(result["status"], "SKIPPED_NO_BALANCE")
        self.assertFalse(any(call == "entry" or (isinstance(call, tuple) and call[0] == "entry") for call in client.calls))

    def test_live_setup_already_at_target_does_not_submit_an_order(self):
        client = FakeClient(balance=1000, mark_price=111)
        result = execute_approved_candidate(
            CANDIDATE, 4, db_path=self.db_path, config=live_config(), client=client,
        )
        self.assertEqual(result["status"], "SKIPPED_STALE_MARKET")
        self.assertFalse(any(isinstance(call, tuple) and call[0] == "entry" for call in client.calls))

    def test_filled_live_entry_gets_stop_and_two_take_orders(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE signals (id INTEGER PRIMARY KEY, result TEXT)")
            conn.execute("INSERT INTO signals (id,result) VALUES (7,'pending')")
        client = FakeClient(balance=1000, entry_status="FILLED")
        submitted = execute_approved_candidate(
            CANDIDATE, 7, db_path=self.db_path, config=live_config(), client=client,
        )
        self.assertEqual(submitted["status"], "PROTECTED")
        call_types = [call[0] for call in client.calls if isinstance(call, tuple)]
        self.assertIn("close_trigger", call_types)
        self.assertIn("reduce_trigger", call_types)
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT status,stop_order_id,tp1_order_id,tp2_order_id FROM trade_executions WHERE signal_id=7"
            ).fetchone()
        self.assertEqual(row, ("PROTECTED", "stop-1", "tp1-1", "tp2-1"))

    def test_expired_signal_filled_at_exchange_is_closed_not_protected(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE signals (id INTEGER PRIMARY KEY, result TEXT)")
            conn.execute("INSERT INTO signals (id,result) VALUES (8,'sl')")
        client = FakeClient(balance=1000, entry_status="FILLED")
        submitted = execute_approved_candidate(
            CANDIDATE, 8, db_path=self.db_path, config=live_config(), client=client,
        )
        self.assertEqual(submitted["status"], "EMERGENCY_CLOSED")
        call_types = [call[0] for call in client.calls if isinstance(call, tuple)]
        self.assertIn("emergency", call_types)
        self.assertNotIn("close_trigger", call_types)
        with sqlite3.connect(self.db_path) as conn:
            status = conn.execute(
                "SELECT status FROM trade_executions WHERE signal_id=8"
            ).fetchone()[0]
        self.assertEqual(status, "EMERGENCY_CLOSED")

    def test_closed_signal_cancels_only_its_remaining_algo_orders(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE signals (id INTEGER PRIMARY KEY, result TEXT)")
            conn.execute("INSERT INTO signals (id,result) VALUES (9,'pending')")
        client = FakeClient(balance=1000, entry_status="FILLED")
        submitted = execute_approved_candidate(
            CANDIDATE, 9, db_path=self.db_path, config=live_config(), client=client,
        )
        self.assertEqual(submitted["status"], "PROTECTED")
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE signals SET result='tp2' WHERE id=9")

        outcomes = reconcile_live_executions(
            db_path=self.db_path, config=live_config(), client=client,
        )

        self.assertEqual(outcomes[0]["status"], "CLOSED_TP2")
        cancelled = [call[1] for call in client.calls if isinstance(call, tuple) and call[0] == "cancel_algo"]
        self.assertEqual(set(cancelled), {"stop-1", "tp1-1", "tp2-1"})

    def test_conditional_orders_use_current_algo_endpoint_and_fields(self):
        session = RecordingSession()
        client = BinanceFuturesClient(live_config(), session=session)

        result = client.place_close_all_trigger(
            "BTCUSDT", "SELL", "STOP_MARKET", "95", "apex_s_10",
        )

        self.assertEqual(result["algoId"], 42)
        method, url, kwargs = session.calls[0]
        self.assertEqual(method, "POST")
        self.assertTrue(url.endswith("/fapi/v1/algoOrder"))
        params = kwargs["params"]
        self.assertEqual(params["algoType"], "CONDITIONAL")
        self.assertEqual(params["triggerPrice"], "95")
        self.assertEqual(params["clientAlgoId"], "apex_s_10")
        self.assertNotIn("stopPrice", params)

    def test_live_status_shows_actual_wallet_and_net_pnl_without_transfers(self):
        session = RecordingSession()
        client = BinanceFuturesClient(live_config(), session=session)

        status = execution_status(
            self.db_path, config=live_config(), client=client,
        )

        account = status["account"]
        self.assertTrue(account["available"])
        self.assertEqual(account["wallet_balance"], 11.25)
        self.assertEqual(account["available_balance"], 10.75)
        self.assertEqual(account["pnl"]["gross_profit"], 1.2)
        self.assertEqual(account["pnl"]["gross_loss"], -0.3)
        self.assertEqual(account["pnl"]["commission"], -0.05)
        self.assertEqual(account["pnl"]["net_trading_pnl"], 0.85)
        self.assertEqual(account["pnl"]["positive_count"], 1)
        self.assertEqual(account["pnl"]["negative_count"], 1)
        requested_paths = [call[1] for call in session.calls]
        self.assertTrue(any(path.endswith("/fapi/v3/balance") for path in requested_paths))
        self.assertTrue(any(path.endswith("/fapi/v1/income") for path in requested_paths))


if __name__ == "__main__":
    unittest.main()
