import os
import sqlite3
import tempfile
import unittest
from decimal import Decimal
from unittest.mock import patch

from core.trade_execution import (
    LIVE_CONFIRMATION,
    BinanceFuturesClient,
    ExecutionConfig,
    SymbolRules,
    build_order_plan,
    execute_approved_candidate,
    execute_manager_review,
    execution_status,
    reconcile_live_executions,
)
from core import trade_execution
from apex.db.state_db import migrate_state
from apex.db.execution_recovery import recovery_path, replay_recovery
from apex.db.repositories.signal_lifecycle import SignalLifecycleRepository
from apex.db.repositories.manager import ManagerRepository
from apex.db.repositories.executions import ExecutionRepository
from apex.domain.ids import derived_id
from apex.execution.plan import client_order_ids


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

    def query_order_by_client_id(self, symbol, client_id):
        self.calls.append(("query_client", symbol, client_id))
        return {
            "orderId": "entry-recovered", "status": self.entry_status,
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
        return {"orderId": "exit-1", "status": "FILLED", "executedQty": quantity}


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
        if url.endswith("/fapi/v1/order") and method == "POST":
            quantity = kwargs.get("params", {}).get("quantity", "0")
            return RecordingResponse({
                "orderId": 43, "status": "FILLED", "executedQty": quantity,
            })
        return RecordingResponse({})


class TradeExecutionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "brain.db")
        trade_execution._binance_blocked_until = 0.0
        trade_execution._shared_symbol_rules_cache.clear()
        trade_execution.configure_execution_state(None)

    def tearDown(self):
        trade_execution.configure_execution_state(None)
        self.tmp.cleanup()

    def test_balance_cache_can_be_owned_by_state_db_without_legacy_write(self):
        state_path = os.path.join(self.tmp.name, "apex_state.db")

        def factory():
            return sqlite3.connect(state_path)

        conn = factory()
        migrate_state(conn)
        conn.close()
        trade_execution.configure_execution_state(factory)
        trade_execution._store_balance_attempt(
            self.db_path,
            attempted_at=123.0,
            balance={
                "wallet_balance": 50,
                "available_balance": 45,
                "cross_unrealized_pnl": -1,
            },
        )
        cached = trade_execution._read_balance_cache(self.db_path, now=130.0)
        self.assertEqual(cached["wallet_balance"], 50.0)
        self.assertEqual(cached["cache_age_seconds"], 7)
        self.assertFalse(os.path.exists(self.db_path))

    def test_execution_status_reads_state_without_creating_legacy_db(self):
        state_path = os.path.join(self.tmp.name, "apex_state.db")

        def factory():
            return sqlite3.connect(state_path)

        conn = factory(); migrate_state(conn); conn.close()
        trade_execution.configure_execution_state(factory)
        ExecutionRepository(factory).register({
            "signal_id": 502, "mode": "live", "symbol": "ETHUSDT",
            "direction": "LONG", "status": "PROTECTED", "entry": 100,
            "sl": 95, "tp1": 110, "tp2": 115, "quantity": 0.5,
            "entry_order_id": "entry-502", "stop_order_id": "stop-502",
        })

        status = execution_status(self.db_path, config=live_config())

        self.assertEqual(status["counts"], {"PROTECTED": 1})
        self.assertEqual(status["live_active_count"], 1)
        self.assertFalse(os.path.exists(self.db_path))

    def test_execution_recovery_and_manager_action_converge_in_state(self):
        state_path = os.path.join(self.tmp.name, "apex_state.db")

        def factory():
            return sqlite3.connect(state_path)

        conn = factory()
        migrate_state(conn)
        conn.close()
        trade_execution.configure_execution_state(factory)

        trade_execution._store_execution(
            self.db_path, 501, live_config(), CANDIDATE, "ENTRY_PENDING",
            {
                "entry": "100", "sl": "95", "tp1": "110", "tp2": "115",
                "quantity": "0.25", "risk_budget": 5.0,
                "available_balance": 1000.0, "leverage": 5,
            },
            entry_order_id="entry-501",
        )
        trade_execution._update_execution(
            self.db_path, 501, "PROTECTED",
            stop_order_id="stop-501", active_stop_price=95.0,
        )
        self.assertTrue(trade_execution._claim_manager_action(
            self.db_path, "action-501", 501, "PROTECT", 98.0,
        ))
        trade_execution._finish_manager_action(
            self.db_path, "action-501", "EXECUTED", order_id="stop-502",
        )

        state = factory()
        execution = state.execute(
            "SELECT status,entry_order_id,stop_order_id,active_stop_price FROM executions WHERE signal_id=501"
        ).fetchone()
        action = state.execute(
            "SELECT status,exchange_order_id FROM execution_actions WHERE action_key='action-501'"
        ).fetchone()
        state.close()
        self.assertEqual(execution, ("PROTECTED", "entry-501", "stop-501", 95.0))
        self.assertEqual(action, ("EXECUTED", "stop-502"))
        self.assertFalse(os.path.exists(self.db_path))
        cached = trade_execution.cached_execution_snapshot(501, self.db_path)
        self.assertEqual(cached["status"], "PROTECTED")
        self.assertEqual(cached["stop_order_id"], "stop-501")
        status = execution_status(self.db_path, config=live_config())
        self.assertEqual(status["counts"].get("PROTECTED"), 1)
        self.assertNotIn("ERROR", status["counts"])
        self.assertEqual(status["live_active_count"], 1)

    def test_live_entry_persists_state_intent_before_binance_submission(self):
        state_path = os.path.join(self.tmp.name, "apex_state.db")
        candidate_id = derived_id("candidate", "test", 508)
        execution_id = derived_id("execution", candidate_id)

        def factory():
            return sqlite3.connect(state_path)

        conn = factory(); migrate_state(conn); conn.close()
        trade_execution.configure_execution_state(factory)

        class InspectingClient(FakeClient):
            def place_limit_entry(client_self, plan, client_id):
                intent = ExecutionRepository(factory).get(508)
                self.assertIsNotNone(intent)
                self.assertEqual(intent["status"], "SUBMITTING")
                self.assertTrue(intent["plan_hash"])
                self.assertEqual(intent["candidate_id"], candidate_id)
                self.assertEqual(intent["execution_id"], execution_id)
                self.assertEqual(client_id, client_order_ids(execution_id)["entry"])
                return super().place_limit_entry(plan, client_id)

        result = execute_approved_candidate(
            {**CANDIDATE, "_v3_candidate_id": candidate_id}, 508,
            db_path=self.db_path,
            config=live_config(), client=InspectingClient(entry_status="NEW"),
        )
        self.assertEqual(result["status"], "ENTRY_PENDING")
        persisted = ExecutionRepository(factory).get(508)
        self.assertEqual(persisted["status"], "ENTRY_PENDING")
        self.assertEqual(persisted["execution_id"], execution_id)

    def test_submitting_intent_recovers_by_typed_execution_client_order_id(self):
        state_path = os.path.join(self.tmp.name, "apex_state.db")

        def factory():
            return sqlite3.connect(state_path)

        conn = factory(); migrate_state(conn); conn.close()
        trade_execution.configure_execution_state(factory)
        candidate_id = derived_id("candidate", "test", 511)
        execution_id = derived_id("execution", candidate_id)
        repository = ExecutionRepository(factory)
        repository.register({
            "signal_id": 511, "execution_id": execution_id,
            "candidate_id": candidate_id, "mode": "live", "symbol": "BTCUSDT",
            "direction": "BULLISH", "status": "SUBMITTING",
            "entry": 100, "sl": 95, "tp1": 110, "tp2": 115,
            "quantity": 0.98,
        })
        SignalLifecycleRepository(factory).import_row({
            "signal_id": 511, "status": "active", "result": "pending",
        })
        client = FakeClient(entry_status="FILLED")
        outcomes = trade_execution._reconcile_live_executions_unlocked(
            db_path=self.db_path, config=live_config(), client=client,
        )
        self.assertEqual(outcomes, [{"status": "PROTECTED", "signal_id": 511}])
        expected_client_id = client_order_ids(execution_id)["entry"]
        self.assertIn(("query_client", "BTCUSDT", expected_client_id), client.calls)
        execution = repository.get(511)
        self.assertEqual(execution["entry_order_id"], "entry-recovered")
        self.assertEqual(execution["status"], "PROTECTED")

    def test_submitting_intent_recovers_by_deterministic_client_order_id(self):
        state_path = os.path.join(self.tmp.name, "apex_state.db")

        def factory():
            return sqlite3.connect(state_path)

        conn = factory(); migrate_state(conn); conn.close()
        trade_execution.configure_execution_state(factory)
        repository = ExecutionRepository(factory)
        repository.register({
            "signal_id": 509, "mode": "live", "symbol": "BTCUSDT",
            "direction": "BULLISH", "status": "SUBMITTING",
            "entry": 100, "sl": 95, "tp1": 110, "tp2": 115,
            "quantity": 0.98,
        })
        SignalLifecycleRepository(factory).import_row({
            "signal_id": 509, "status": "active", "result": "pending",
        })
        client = FakeClient(entry_status="FILLED")
        outcomes = trade_execution._reconcile_live_executions_unlocked(
            db_path=self.db_path, config=live_config(), client=client,
        )
        self.assertEqual(outcomes, [{"status": "PROTECTED", "signal_id": 509}])
        self.assertIn(("query_client", "BTCUSDT", "apex_e_509"), client.calls)
        execution = repository.get(509)
        self.assertEqual(execution["entry_order_id"], "entry-recovered")
        self.assertEqual(execution["status"], "PROTECTED")
        self.assertEqual(execution["stop_order_id"], "stop-1")

    def test_live_entry_is_not_submitted_when_state_intent_cannot_persist(self):
        def unavailable():
            raise sqlite3.OperationalError("state unavailable")

        trade_execution.configure_execution_state(unavailable)
        client = FakeClient(entry_status="NEW")
        result = execute_approved_candidate(
            CANDIDATE, 510, db_path=self.db_path,
            config=live_config(), client=client,
        )
        self.assertEqual(result["status"], "BLOCKED_STATE_PERSISTENCE")
        self.assertFalse(any(
            isinstance(call, tuple) and call[0] == "entry" for call in client.calls
        ))

    def test_pre_exchange_state_does_not_create_legacy_execution(self):
        state_path = os.path.join(self.tmp.name, "state-pre-entry.db")
        legacy_path = os.path.join(self.tmp.name, "absent-pre-entry.db")

        def factory():
            return sqlite3.connect(state_path)

        conn = factory(); migrate_state(conn); conn.close()
        trade_execution.configure_execution_state(factory)
        result = trade_execution._store_execution(
            legacy_path, 511, live_config(), CANDIDATE, "BLOCKED_KILL_SWITCH",
            error="test fence",
        )

        self.assertTrue(result["state_persisted"])
        self.assertFalse(os.path.exists(legacy_path))
        state = ExecutionRepository(factory).get(511)
        self.assertEqual(state["status"], "BLOCKED_KILL_SWITCH")

    def test_accepted_entry_persists_only_in_state_when_state_is_available(self):
        state_path = os.path.join(self.tmp.name, "state-accepted-entry.db")
        legacy_path = os.path.join(self.tmp.name, "accepted-entry-recovery.db")

        def factory():
            return sqlite3.connect(state_path)

        conn = factory(); migrate_state(conn); conn.close()
        trade_execution.configure_execution_state(factory)
        trade_execution._store_execution(
            legacy_path, 512, live_config(), CANDIDATE, "ENTRY_PENDING",
            {
                "entry": "100", "sl": "95", "tp1": "110", "tp2": "115",
                "quantity": "0.25", "risk_budget": 5.0,
                "available_balance": 1000.0, "leverage": 5,
            },
            entry_order_id="entry-512",
        )

        self.assertFalse(os.path.exists(legacy_path))
        self.assertFalse(recovery_path(legacy_path).exists())
        state = ExecutionRepository(factory).get(512)
        self.assertEqual((state["status"], state["entry_order_id"]), ("ENTRY_PENDING", "entry-512"))

    def test_manager_action_claim_is_state_owned_and_fails_closed(self):
        state_path = os.path.join(self.tmp.name, "apex_state.db")

        def factory():
            return sqlite3.connect(state_path)

        conn = factory(); migrate_state(conn); conn.close()
        trade_execution.configure_execution_state(factory)
        ExecutionRepository(factory).register({
            "signal_id": 77, "mode": "live", "symbol": "BTCUSDT",
            "direction": "LONG", "status": "PROTECTED", "entry": 100,
            "sl": 95, "tp1": 110, "tp2": 115, "quantity": 1,
        })
        self.assertTrue(trade_execution._claim_manager_action(
            self.db_path, "state-action", 77, "PROTECT", 98.0,
        ))
        with sqlite3.connect(self.db_path) as legacy:
            table = legacy.execute(
                """SELECT 1 FROM sqlite_master
                     WHERE type='table' AND name='manager_execution_actions'"""
            ).fetchone()
            self.assertIsNone(table)
        # The canonical claim alone prevents a duplicate Binance mutation.
        self.assertFalse(trade_execution._claim_manager_action(
            self.db_path, "state-action", 77, "PROTECT", 98.0,
        ))

        def unavailable():
            raise sqlite3.OperationalError("state unavailable")

        trade_execution.configure_execution_state(unavailable)
        self.assertFalse(trade_execution._claim_manager_action(
            self.db_path, "blocked-action", 78, "CLOSE", None,
        ))
        with sqlite3.connect(self.db_path) as legacy:
            table = legacy.execute(
                """SELECT 1 FROM sqlite_master
                     WHERE type='table' AND name='manager_execution_actions'"""
            ).fetchone()
        self.assertIsNone(table)

    def test_manager_action_finish_uses_durable_journal_when_state_fails(self):
        state_path = os.path.join(self.tmp.name, "manager-action-state.db")
        recovery_db_path = os.path.join(self.tmp.name, "manager-action-recovery.db")

        def factory():
            return sqlite3.connect(state_path)

        conn = factory(); migrate_state(conn); conn.close()
        trade_execution.configure_execution_state(factory)
        ExecutionRepository(factory).register({
            "signal_id": 79, "mode": "live", "symbol": "BTCUSDT",
            "direction": "LONG", "status": "PROTECTED", "entry": 100,
            "sl": 95, "tp1": 110, "tp2": 115, "quantity": 1,
        })
        self.assertTrue(trade_execution._claim_manager_action(
            recovery_db_path, "protect-recovery", 79, "PROTECT", 98.0,
        ))
        self.assertFalse(os.path.exists(recovery_db_path))

        def unavailable():
            raise sqlite3.OperationalError("state unavailable after exchange action")

        trade_execution.configure_execution_state(unavailable)
        trade_execution._finish_manager_action(
            recovery_db_path, "protect-recovery", "EXECUTED", order_id="stop-79",
        )
        journal = recovery_path_fn = recovery_path(recovery_db_path)
        self.assertTrue(journal.exists())
        self.assertFalse(os.path.exists(recovery_db_path))
        self.assertEqual(replay_recovery(recovery_db_path, factory), 1)
        action = ExecutionRepository(factory).latest_action(79, ("PROTECT",))
        self.assertEqual((action["status"], action["exchange_order_id"]), ("EXECUTED", "stop-79"))
        self.assertFalse(recovery_path_fn.exists())

    def test_reconcile_uses_state_signal_lifecycle_and_missing_is_pending(self):
        state_path = os.path.join(self.tmp.name, "apex_state.db")

        def factory():
            return sqlite3.connect(state_path)

        conn = factory()
        migrate_state(conn)
        conn.close()
        trade_execution.configure_execution_state(factory)

        conn = sqlite3.connect(self.db_path)
        conn.execute("CREATE TABLE signals (id INTEGER PRIMARY KEY, result TEXT)")
        conn.execute("INSERT INTO signals (id,result) VALUES (901,'sl')")
        conn.commit()
        conn.close()
        trade_execution._store_execution(
            self.db_path, 901, live_config(), CANDIDATE, "ENTRY_PENDING",
            {
                "entry": "100", "sl": "95", "tp1": "110", "tp2": "115",
                "quantity": "0.25", "risk_budget": 5.0,
                "available_balance": 1000.0, "leverage": 5,
            },
            entry_order_id="entry-901",
        )
        trade_execution._update_execution(
            self.db_path, 901, "PROTECTED", stop_order_id="stop-901",
        )

        # Legacy says closed, but a missing State projection must not make a
        # protected execution actionable or cancel exchange protection.
        self.assertEqual(trade_execution._live_reconcile_rows(self.db_path), [])

        SignalLifecycleRepository(factory).import_row({
            "signal_id": 901,
            "status": "closed",
            "result": "sl",
        })
        rows = trade_execution._live_reconcile_rows(self.db_path)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["signal_id"], 901)
        self.assertEqual(rows[0]["signal_result"], "sl")

    def test_reconcile_selects_execution_status_from_state_not_legacy(self):
        state_path = os.path.join(self.tmp.name, "apex_state.db")

        def factory():
            return sqlite3.connect(state_path)

        conn = factory(); migrate_state(conn); conn.close()
        trade_execution.configure_execution_state(factory)
        trade_execution._store_execution(
            self.db_path, 905, live_config(), CANDIDATE, "ENTRY_PENDING",
            {
                "entry": "100", "sl": "95", "tp1": "110", "tp2": "115",
                "quantity": "0.25", "risk_budget": 5.0,
                "available_balance": 1000.0, "leverage": 5,
            }, entry_order_id="entry-905",
        )
        trade_execution._update_execution(
            self.db_path, 905, "PROTECTED", stop_order_id="stop-905",
        )
        SignalLifecycleRepository(factory).import_row({
            "signal_id": 905, "status": "active", "result": "pending",
        })
        trade_execution.ensure_execution_schema(self.db_path)
        with sqlite3.connect(self.db_path) as legacy:
            legacy.execute(
                "UPDATE trade_executions SET status='ENTRY_PENDING' WHERE signal_id=905"
            )

        # A stale compatibility status cannot make State-owned protection look
        # like an entry that needs another Binance reconciliation pass.
        self.assertEqual(trade_execution._live_reconcile_rows(self.db_path), [])

        SignalLifecycleRepository(factory).import_row({
            "signal_id": 905, "status": "closed", "result": "sl",
        })
        rows = trade_execution._live_reconcile_rows(self.db_path)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "PROTECTED")
        self.assertEqual(rows[0]["signal_result"], "sl")

    def test_state_reconcile_selection_does_not_create_legacy_db(self):
        state_path = os.path.join(self.tmp.name, "state-only.db")
        legacy_path = os.path.join(self.tmp.name, "absent-brain.db")

        def factory():
            return sqlite3.connect(state_path)

        conn = factory(); migrate_state(conn); conn.close()
        trade_execution.configure_execution_state(factory)
        ExecutionRepository(factory).register({
            "signal_id": 990, "mode": "live", "symbol": "BTCUSDT",
            "direction": "LONG", "status": "ENTRY_PENDING", "entry": 100,
            "sl": 95, "tp1": 110, "tp2": 115, "quantity": 0.25,
            "entry_order_id": "entry-990",
        })

        rows = trade_execution._live_reconcile_rows(legacy_path)

        self.assertEqual([row["signal_id"] for row in rows], [990])
        self.assertFalse(os.path.exists(legacy_path))

    def test_reconcile_updates_state_without_legacy_execution_row(self):
        state_path = os.path.join(self.tmp.name, "apex_state.db")

        def factory():
            return sqlite3.connect(state_path)

        conn = factory(); migrate_state(conn); conn.close()
        trade_execution.configure_execution_state(factory)
        ExecutionRepository(factory).register({
            "signal_id": 906, "mode": "live", "symbol": "BTCUSDT",
            "direction": "BULLISH", "status": "PROTECTED",
            "entry": 100, "sl": 95, "tp1": 110, "tp2": 115,
            "quantity": 0.25, "entry_order_id": "entry-906",
            "stop_order_id": "stop-906", "active_stop_price": 95,
        })
        SignalLifecycleRepository(factory).import_row({
            "signal_id": 906, "status": "closed", "result": "sl",
        })

        outcomes = trade_execution._reconcile_live_executions_unlocked(
            db_path=self.db_path, config=live_config(), client=FakeClient(),
        )
        self.assertEqual(outcomes, [{"status": "CLOSED_SL", "signal_id": 906}])
        state = ExecutionRepository(factory).get(906)
        self.assertEqual(state["status"], "CLOSED_SL")
        with sqlite3.connect(self.db_path) as legacy:
            table = legacy.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trade_executions'"
            ).fetchone()
        self.assertIsNone(table)

    def test_stop_replacement_recovery_uses_state_action_without_legacy_rows(self):
        state_path = os.path.join(self.tmp.name, "apex_state.db")

        def factory():
            return sqlite3.connect(state_path)

        conn = factory(); migrate_state(conn); conn.close()
        trade_execution.configure_execution_state(factory)
        repository = ExecutionRepository(factory)
        repository.register({
            "signal_id": 907, "mode": "live", "symbol": "BTCUSDT",
            "direction": "BULLISH", "status": "STOP_REPLACEMENT_PENDING",
            "entry": 100, "sl": 95, "tp1": 110, "tp2": 115,
            "quantity": 0.25, "entry_order_id": "entry-907",
            "stop_order_id": "stop-new", "active_stop_price": 98,
            "pending_stop_order_id": "stop-new",
            "previous_stop_order_id": "stop-old",
            "tp1_order_id": "tp1-907", "tp2_order_id": "tp2-907",
        })
        self.assertTrue(repository.claim_action(
            "protect-907", 907, "PROTECT", 98,
        ))
        row = repository.get(907)
        row["signal_result"] = "pending"
        client = FakeClient()

        result = trade_execution._reconcile_stop_replacement(
            row, client, self.db_path,
        )
        self.assertEqual(result["status"], "STOP_REPLACEMENT_RECOVERED")
        self.assertIn(("cancel_algo", "stop-old"), client.calls)
        execution = repository.get(907)
        action = repository.latest_action(907, ("PROTECT",))
        self.assertEqual(execution["status"], "PROTECTED")
        self.assertIsNone(execution["pending_stop_order_id"])
        self.assertEqual(action["status"], "EXECUTED")
        self.assertEqual(action["exchange_order_id"], "stop-new")

    def test_unavailable_state_lifecycle_keeps_legacy_close_non_actionable(self):
        state_path = os.path.join(self.tmp.name, "apex_state.db")

        def factory():
            return sqlite3.connect(state_path)

        conn = factory(); migrate_state(conn); conn.close()
        trade_execution.configure_execution_state(factory)
        conn = sqlite3.connect(self.db_path)
        conn.execute("CREATE TABLE signals (id INTEGER PRIMARY KEY, result TEXT)")
        conn.execute("INSERT INTO signals (id,result) VALUES (902,'sl')")
        conn.commit(); conn.close()
        trade_execution._store_execution(
            self.db_path, 902, live_config(), CANDIDATE, "ENTRY_PENDING",
            {
                "entry": "100", "sl": "95", "tp1": "110", "tp2": "115",
                "quantity": "0.25", "risk_budget": 5.0,
                "available_balance": 1000.0, "leverage": 5,
            }, entry_order_id="entry-902",
        )
        trade_execution._update_execution(
            self.db_path, 902, "PROTECTED", stop_order_id="stop-902",
        )

        def unavailable():
            raise sqlite3.OperationalError("state unavailable")

        trade_execution.configure_execution_state(unavailable)
        self.assertEqual(trade_execution._live_reconcile_rows(self.db_path), [])

    def test_manager_cutover_and_action_validation_read_state(self):
        state_path = os.path.join(self.tmp.name, "apex_state.db")

        def factory():
            return sqlite3.connect(state_path)

        conn = factory(); migrate_state(conn); conn.close()
        trade_execution.configure_execution_state(factory)
        repository = ManagerRepository(factory)
        repository.register({
            "signal_id": 903, "symbol": "BTCUSDT", "strategy": "ZONE",
            "direction": "BULLISH", "management_tf": "15m",
            "initial_entry": 100, "initial_sl": 95, "initial_tp1": 110,
            "initial_tp2": 115, "initial_tp3": 120, "initial_rr": 2,
            "manager_version": 2,
        })
        conn = factory()
        conn.execute(
            """UPDATE manager_positions
               SET status='CLOSING',manager_state='RECONCILIATION_REQUIRED'
               WHERE signal_id=903"""
        )
        conn.commit(); conn.close()
        self.assertEqual(
            trade_execution._reconciliation_required_signal_ids(self.db_path), {903},
        )

        result = execute_manager_review(
            {
                "signal_id": 904,
                "review": {"action": "PROTECT", "confidence": 0.9},
                "facts": {},
            },
            db_path=self.db_path, config=live_config(), client=FakeClient(),
        )
        self.assertEqual(result["status"], "MANAGER_STATE_UNAVAILABLE")

    def test_strategy_pause_blocks_before_exchange_calls(self):
        candidate = {**CANDIDATE, "_strategy_risk_state": {
            "mode": "PAUSED", "reason": "5 consecutive activated SL",
            "live_risk_multiplier": 0.0,
        }}
        client = FakeClient()
        result = execute_approved_candidate(
            candidate, 101, db_path=self.db_path, config=live_config(), client=client,
        )
        self.assertEqual(result["status"], "BLOCKED_STRATEGY_PAUSE")
        self.assertEqual(client.calls, [])

    def test_strategy_caution_halves_paper_risk_budget(self):
        candidate = {**CANDIDATE, "_strategy_risk_state": {
            "mode": "CAUTION", "live_risk_multiplier": 0.5,
        }}
        config = ExecutionConfig(enabled=True, mode="paper", risk_pct=0.5, paper_balance_usdt=1000)
        result = execute_approved_candidate(candidate, 102, db_path=self.db_path, config=config)
        self.assertEqual(result["status"], "PAPER_PENDING_ENTRY")
        self.assertAlmostEqual(result["plan"]["risk_budget"], 2.5)

    def test_default_configuration_is_disabled_paper(self):
        config = ExecutionConfig.from_env({})
        self.assertFalse(config.enabled)
        self.assertEqual(config.mode, "paper")
        self.assertFalse(config.live_armed)

    def test_binance_rate_limit_opens_process_wide_circuit(self):
        class RateLimitedResponse:
            status_code = 418
            headers = {"Retry-After": "120"}
            text = "banned"

            @staticmethod
            def json():
                return {"code": -1003, "msg": "IP banned until 4102444800000"}

        class RateLimitedSession:
            def __init__(self):
                self.calls = 0

            def request(self, *args, **kwargs):
                self.calls += 1
                return RateLimitedResponse()

        first_session = RateLimitedSession()
        first = BinanceFuturesClient(live_config(), session=first_session)
        with self.assertRaises(trade_execution.BinanceAPIError):
            first._request("GET", "/fapi/v1/exchangeInfo")

        second_session = RateLimitedSession()
        second = BinanceFuturesClient(live_config(), session=second_session)
        with self.assertRaisesRegex(RuntimeError, "Binance circuit open"):
            second._request("GET", "/fapi/v1/exchangeInfo")
        self.assertEqual(first_session.calls, 1)
        self.assertEqual(second_session.calls, 0)

    def test_reconciliation_single_flight_skips_overlapping_call(self):
        self.assertTrue(trade_execution._reconcile_process_lock.acquire(blocking=False))
        try:
            result = reconcile_live_executions(
                db_path=self.db_path, config=live_config(), client=FakeClient(),
            )
        finally:
            trade_execution._reconcile_process_lock.release()
        self.assertEqual(result, [])

    def test_idle_reconciliation_never_constructs_binance_client(self):
        with patch.object(
            trade_execution,
            "BinanceFuturesClient",
            side_effect=AssertionError("idle reconciliation touched Binance"),
        ) as client_class:
            result = reconcile_live_executions(
                db_path=self.db_path,
                config=live_config(),
            )
        self.assertEqual(result, [])
        client_class.assert_not_called()

    def test_telegram_status_defers_binance_account_probe(self):
        with patch.object(
            trade_execution,
            "BinanceFuturesClient",
            side_effect=AssertionError("status view touched Binance"),
        ) as client_class:
            status = execution_status(self.db_path, config=live_config())
        self.assertTrue(status["account"]["deferred"])
        self.assertEqual(status["live_reconcile_pending"], 0)
        client_class.assert_not_called()

    def test_explicit_balance_refresh_is_persisted_and_throttled(self):
        first_client = FakeClient(balance=321.25)
        first = execution_status(
            self.db_path,
            config=live_config(),
            client=first_client,
            refresh_balance=True,
        )
        self.assertTrue(first["account"]["available"])
        self.assertEqual(first["account"]["wallet_balance"], 321.25)
        self.assertEqual(first_client.calls, ["balance_details"])

        second_client = FakeClient(balance=999)
        second = execution_status(
            self.db_path,
            config=live_config(),
            client=second_client,
            refresh_balance=True,
        )
        self.assertEqual(second["account"]["wallet_balance"], 321.25)
        self.assertEqual(second_client.calls, [])

        persisted = execution_status(self.db_path, config=live_config())
        self.assertTrue(persisted["account"]["available"])
        self.assertTrue(persisted["account"]["deferred"])
        self.assertEqual(persisted["account"]["wallet_balance"], 321.25)

    def test_failed_balance_refresh_preserves_stale_snapshot_and_throttles_retry(self):
        trade_execution._store_balance_attempt(
            self.db_path,
            attempted_at=1,
            balance={
                "wallet_balance": 42.5,
                "available_balance": 40,
                "cross_unrealized_pnl": -0.5,
            },
        )

        class FailingBalanceClient:
            def __init__(self):
                self.calls = 0

            def usdt_balance_details(self):
                self.calls += 1
                raise RuntimeError("temporary balance failure")

        failing = FailingBalanceClient()
        failed = execution_status(
            self.db_path,
            config=live_config(),
            client=failing,
            refresh_balance=True,
        )
        self.assertEqual(failing.calls, 1)
        self.assertTrue(failed["account"]["available"])
        self.assertTrue(failed["account"]["stale"])
        self.assertEqual(failed["account"]["wallet_balance"], 42.5)

        retry_client = FakeClient(balance=1000)
        throttled = execution_status(
            self.db_path,
            config=live_config(),
            client=retry_client,
            refresh_balance=True,
        )
        self.assertEqual(retry_client.calls, [])
        self.assertEqual(throttled["account"]["wallet_balance"], 42.5)

    def test_protected_order_is_visible_but_not_polled_while_signal_is_open(self):
        trade_execution.ensure_execution_schema(self.db_path)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT INTO trade_executions
                   (signal_id, mode, symbol, direction, status)
                   VALUES (501, 'live', 'BTCUSDT', 'BULLISH', 'PROTECTED')"""
            )

        with patch.object(
            trade_execution,
            "BinanceFuturesClient",
            side_effect=AssertionError("protected order was polled without a state change"),
        ) as client_class:
            outcomes = reconcile_live_executions(
                db_path=self.db_path,
                config=live_config(),
            )
            status = execution_status(self.db_path, config=live_config())

        self.assertEqual(outcomes, [])
        self.assertEqual(status["live_active_count"], 1)
        self.assertEqual(status["live_reconcile_pending"], 0)
        client_class.assert_not_called()

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

    def test_gate_close_never_removes_protection_while_binance_position_is_open(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE signals (id INTEGER PRIMARY KEY, result TEXT)")
            conn.execute("INSERT INTO signals (id,result) VALUES (74,'pending')")
        client = FakeClient(balance=1000, entry_status="FILLED")
        execute_approved_candidate(
            CANDIDATE, 74, db_path=self.db_path, config=live_config(), client=client,
        )
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE signals SET result='sl' WHERE id=74")
        client.calls.clear()
        client.open_positions = lambda: [{"symbol": "BTCUSDT", "positionAmt": "0.25"}]
        outcomes = reconcile_live_executions(
            db_path=self.db_path, config=live_config(), client=client,
        )
        self.assertEqual(outcomes, [{"status": "AWAITING_BINANCE_CLOSE", "signal_id": 74}])
        self.assertFalse(any(
            isinstance(call, tuple) and call[0] == "cancel_algo" for call in client.calls
        ))
        with sqlite3.connect(self.db_path) as conn:
            self.assertEqual(conn.execute(
                "SELECT status FROM trade_executions WHERE signal_id=74"
            ).fetchone()[0], "PROTECTED")

    def test_position_snapshot_failure_keeps_protective_orders(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE signals (id INTEGER PRIMARY KEY, result TEXT)")
            conn.execute("INSERT INTO signals (id,result) VALUES (75,'pending')")
        client = FakeClient(balance=1000, entry_status="FILLED")
        execute_approved_candidate(
            CANDIDATE, 75, db_path=self.db_path, config=live_config(), client=client,
        )
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE signals SET result='tp2' WHERE id=75")
        client.calls.clear()
        client.open_positions = lambda: (_ for _ in ()).throw(RuntimeError("temporary"))
        outcomes = reconcile_live_executions(
            db_path=self.db_path, config=live_config(), client=client,
        )
        self.assertEqual(outcomes[0]["reason"], "BINANCE_POSITION_SNAPSHOT_UNAVAILABLE")
        self.assertFalse(any(
            isinstance(call, tuple) and call[0] == "cancel_algo" for call in client.calls
        ))

    def test_manager_protect_is_validated_and_idempotent(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE signals (id INTEGER PRIMARY KEY, result TEXT)")
            conn.execute("INSERT INTO signals (id,result) VALUES (70,'pending')")
        client = FakeClient(balance=1000, entry_status="FILLED")
        self.assertEqual(
            execute_approved_candidate(
                CANDIDATE, 70, db_path=self.db_path, config=live_config(), client=client,
            )["status"],
            "PROTECTED",
        )
        update = {
            "signal_id": 70,
            "review": {"action": "PROTECT", "confidence": 0.9, "protect_level": 100},
            "facts": {"management_candle_id": "2026-09-07T12:00:00Z"},
        }
        first = execute_manager_review(
            update, db_path=self.db_path, config=live_config(), client=client,
        )
        second = execute_manager_review(
            update, db_path=self.db_path, config=live_config(), client=client,
        )
        self.assertEqual(first["status"], "EXECUTED")
        self.assertEqual(second["status"], "DUPLICATE_SKIPPED")
        manager_stops = [
            call for call in client.calls
            if isinstance(call, tuple) and call[0] == "close_trigger" and "apex_mp_" in call[-1]
        ]
        self.assertEqual(len(manager_stops), 1)

    def test_manager_hold_never_touches_binance(self):
        client = FakeClient()
        result = execute_manager_review(
            {"signal_id": 1, "review": {"action": "HOLD", "confidence": 1}},
            db_path=self.db_path, config=live_config(), client=client,
        )
        self.assertEqual(result["status"], "NO_EXECUTION")
        self.assertEqual(client.calls, [])

    def test_manager_partial_does_not_duplicate_exchange_tp1(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE signals (id INTEGER PRIMARY KEY, result TEXT)")
            conn.execute("INSERT INTO signals (id,result) VALUES (71,'pending')")
        client = FakeClient(balance=1000, entry_status="FILLED")
        execute_approved_candidate(
            CANDIDATE, 71, db_path=self.db_path, config=live_config(), client=client,
        )
        before = len(client.calls)
        result = execute_manager_review(
            {"signal_id": 71, "review": {"action": "PARTIAL_EXIT", "confidence": .9}},
            db_path=self.db_path, config=live_config(), client=client,
        )
        self.assertEqual(result["status"], "BRACKET_MANAGED")
        self.assertEqual(len(client.calls), before)

    def test_manager_exit_uses_actual_position_and_closes_signal(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE signals (id INTEGER PRIMARY KEY, result TEXT, closed_at TEXT)")
            conn.execute("INSERT INTO signals (id,result) VALUES (72,'pending')")
        client = FakeClient(balance=1000, entry_status="FILLED")
        execute_approved_candidate(
            CANDIDATE, 72, db_path=self.db_path, config=live_config(), client=client,
        )
        client.open_positions = lambda: [{"symbol": "BTCUSDT", "positionAmt": "0.49"}]
        result = execute_manager_review(
            {
                "signal_id": 72,
                "review": {"action": "EXIT", "confidence": .9},
                "facts": {"management_candle_id": "2026-09-07T12:05:00Z"},
            },
            db_path=self.db_path, config=live_config(), client=client,
        )
        self.assertEqual(result["status"], "EXECUTED")
        with sqlite3.connect(self.db_path) as conn:
            self.assertEqual(conn.execute("SELECT result FROM signals WHERE id=72").fetchone()[0], "manager_exit")

    def test_manager_ack_without_fill_does_not_advance_trade_state(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE signals (id INTEGER PRIMARY KEY, result TEXT, closed_at TEXT)")
            conn.execute("INSERT INTO signals (id,result) VALUES (73,'pending')")
        client = FakeClient(balance=1000, entry_status="FILLED")
        execute_approved_candidate(
            CANDIDATE, 73, db_path=self.db_path, config=live_config(), client=client,
        )
        client.open_positions = lambda: [{"symbol": "BTCUSDT", "positionAmt": "0.49"}]
        client.emergency_close = lambda *_args, **_kwargs: {
            "orderId": "accepted-only", "status": "NEW", "executedQty": "0",
        }
        result = execute_manager_review(
            {"signal_id": 73, "review": {"action": "EXIT", "confidence": .9}},
            db_path=self.db_path, config=live_config(), client=client,
        )
        self.assertEqual(result["status"], "ERROR")
        with sqlite3.connect(self.db_path) as conn:
            self.assertEqual(conn.execute("SELECT result FROM signals WHERE id=73").fetchone()[0], "pending")

    def test_manager_market_order_requests_final_fill_response(self):
        session = RecordingSession()
        client = BinanceFuturesClient(live_config(), session=session)

        result = client.emergency_close("BTCUSDT", "BULLISH", "0.1", "apex-close-1")

        self.assertEqual(result["status"], "FILLED")
        params = session.calls[0][2]["params"]
        self.assertEqual(params["newOrderRespType"], "RESULT")
        self.assertEqual(params["reduceOnly"], "true")

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
