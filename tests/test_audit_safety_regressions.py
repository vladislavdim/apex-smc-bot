"""Exchange outage and access-control regressions from the September audit."""
from __future__ import annotations

import asyncio
import sqlite3
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from apex.app.bootstrap import build_webhook_application
from apex.config.settings import ApexConfig
from apex.db.repositories.executions import ExecutionRepository
from apex.db.repositories.manager import ManagerRepository
from apex.db.repositories.signal_lifecycle import SignalLifecycleRepository
from apex.db.state_db import migrate_state
from apex.execution import orders
from apex.strategies.state_signal_monitor import StateSignalMonitor
from apex.ui.telegram.router import TelegramHandlers, register_telegram_handlers
from tests.test_trade_execution import CANDIDATE, FakeClient, live_config
from tests.test_apex_v3_bootstrap import FakeApplication, FakeRuntime, FakeWeb


class SafetyRegressionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = str(Path(self.tmp.name) / "brain.db")
        state_path = str(Path(self.tmp.name) / "state.db")
        self.factory = lambda: sqlite3.connect(state_path)
        conn = self.factory(); migrate_state(conn); conn.close()
        orders.configure_execution_state(self.factory)
        self.addCleanup(orders.configure_execution_state, None)
        self.addCleanup(orders.configure_manager_confirmation, None)
        self.ready = patch("apex.app.runtime.entry_admission", return_value=(True, ""))
        self.ready.start(); self.addCleanup(self.ready.stop)
        self.quote = patch("apex.execution.orders._fresh_gate_quote", side_effect=lambda _:
                           {"price": 100, "source": "gate", "observed_at": time.time()})
        self.quote.start(); self.addCleanup(self.quote.stop)
        self.repo = ExecutionRepository(self.factory)

    def execute(self, signal_id, client=None, candidate=None, config=None):
        return orders.execute_approved_candidate(
            dict(candidate or CANDIDATE), signal_id, db_path=self.db_path,
            client=client or FakeClient(), config=config or live_config(),
        )

    def test_ambiguous_entry_keeps_plan_and_reconciles_by_client_id(self):
        class Ambiguous(FakeClient):
            def place_limit_entry(self, plan, client_id):
                self.order_was_accepted = client_id
                raise TimeoutError("response lost")
        client = Ambiguous()
        result = self.execute(1, client)
        self.assertEqual(result["status"], "SUBMITTING")
        self.assertGreater(self.repo.get(1)["quantity"], 0)
        self.assertEqual(len(orders._live_reconcile_rows(self.db_path)), 1)
        recovered = orders.reconcile_live_executions(db_path=self.db_path,
                                                     config=live_config(), client=client)
        self.assertEqual(recovered, [])  # Unfilled order remains pending.
        self.assertEqual(self.repo.get(1)["status"], "ENTRY_PENDING")
        self.assertEqual(self.repo.get(1)["entry_order_id"], "entry-recovered")

    def test_unprotected_position_stays_actionable_until_protected(self):
        class Broken(FakeClient):
            def place_close_all_trigger(self, *a):
                raise TimeoutError("stop failed")
            def emergency_close(self, *a):
                raise TimeoutError("close failed")
        result = self.execute(2, Broken(entry_status="FILLED"))
        self.assertEqual(result["status"], "UNPROTECTED_POSITION")
        self.assertEqual(self.execute(3)["status"], "BLOCKED_UNPROTECTED_POSITION")
        recovered = FakeClient()
        recovered.open_positions = lambda: [{"symbol": "BTCUSDT", "positionAmt": "0.980"}]
        outcomes = orders.reconcile_live_executions(db_path=self.db_path,
                                                    config=live_config(), client=recovered)
        self.assertEqual(outcomes[0]["status"], "PROTECTED")

    def test_terminal_partial_fill_gets_stop(self):
        class PartiallyCancelled(FakeClient):
            def query_order(self, *a):
                return {"status": "CANCELED", "executedQty": "0.400"}
        client = PartiallyCancelled()
        self.assertEqual(self.execute(4, client)["status"], "PROTECTED")
        self.assertTrue(any(call[0] == "close_trigger" for call in client.calls if isinstance(call, tuple)))

    def test_duplicate_invocation_cannot_clear_live_order(self):
        first = self.execute(5)
        self.assertEqual(first["status"], "ENTRY_PENDING")
        class Existing(FakeClient):
            def has_open_orders(self, symbol): return True
        second = self.execute(5, Existing())
        self.assertTrue(second["duplicate"])
        self.assertEqual(self.repo.get(5)["status"], "ENTRY_PENDING")
        self.assertEqual(self.repo.get(5)["entry_order_id"], "entry-1")

    def test_state_manager_breakeven_reads_named_initial_entry(self):
        self.repo.register({"signal_id": 6, "mode": "live", "symbol": "BTCUSDT",
                            "direction": "BULLISH", "status": "PROTECTED", "entry": 100,
                            "sl": 95, "tp1": 110, "tp2": 115, "quantity": 1,
                            "stop_order_id": "stop-6"})
        ManagerRepository(self.factory).register({
            "signal_id": 6, "symbol": "BTCUSDT", "strategy": "MTF",
            "direction": "BULLISH", "management_tf": "1h", "initial_entry": 100,
            "initial_sl": 95, "initial_tp1": 110, "manager_version": 2,
        })
        orders.configure_manager_confirmation(transition_validator=lambda *a: (True, ""))
        result = orders.execute_manager_review(
            {"signal_id": 6, "review": {"action": "MOVE_STOP_TO_BREAKEVEN", "confidence": .9}},
            db_path=self.db_path, config=live_config(), client=FakeClient(),
        )
        self.assertEqual(result["status"], "EXECUTED")
        self.assertEqual(self.repo.get(6)["active_stop_price"], 100)

    def test_pending_intents_reserve_slots(self):
        for i in (7, 8, 9):
            self.repo.register({"signal_id": i, "mode": "live", "symbol": f"PAIR{i}USDT",
                                "direction": "BULLISH", "status": "ENTRY_PENDING", "risk_usdt": 1})
        self.assertEqual(self.execute(10, config=live_config(max_open_positions=3))["status"],
                         "BLOCKED_MAX_POSITIONS")

    def test_exposure_read_failure_and_zero_multiplier_block(self):
        with patch.object(ExecutionRepository, "risk_exposure",
                          side_effect=sqlite3.OperationalError("state unavailable")):
            self.assertEqual(self.execute(11)["status"], "BLOCKED_RISK_ENGINE")
        self.assertEqual(self.execute(
            12, candidate={**CANDIDATE, "_strategy_risk_state": {"live_risk_multiplier": 0}}
        )["status"], "BLOCKED_ZERO_RISK")

    def test_stale_gate_quote_blocks_and_fresh_invalidation_blocks(self):
        with patch("apex.execution.orders._fresh_gate_quote", return_value={
                "price": 100, "source": "gate", "observed_at": time.time() - 100}):
            self.assertEqual(self.execute(13)["status"], "SKIPPED_STALE_GATE_DATA")
        with patch("apex.execution.orders._fresh_gate_quote", return_value={
                "price": 94, "source": "gate", "observed_at": time.time()}):
            self.assertEqual(self.execute(14)["status"], "SKIPPED_STALE_MARKET")

    def test_monitor_replays_intervening_closed_candle_once(self):
        now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        activated = now - timedelta(minutes=25)
        lifecycle = SignalLifecycleRepository(self.factory)
        lifecycle.import_row({
            "signal_id": 15, "symbol": "BTCUSDT", "direction": "BULLISH",
            "signal_type": "MTF", "timeframe": "1h", "entry": 100,
            "sl": 95, "tp1": 110, "tp2": 120, "tp3": 130, "grade": "A",
            "status": "active", "result": "pending", "created_at": activated.isoformat(),
            "activated_at": activated.isoformat(), "estimated_hours": 72,
        })
        times = [now - timedelta(minutes=n) for n in (20, 15, 10)]
        candles = [{"open_time": t.timestamp(), "low": low, "high": 101, "close": 100}
                   for t, low in zip(times, (94, 99, 99))]
        monitor = StateSignalMonitor(lifecycle, lambda: {"BTCUSDT": 100},
                                     lambda *a: candles, lambda *a, **k: None)
        self.assertEqual(monitor.check(now=now)[0]["result"], "sl")
        self.assertEqual(monitor.check(now=now), [])


class TransportRegressionTests(unittest.IsolatedAsyncioTestCase):
    def test_private_backup_check_rejects_public_repo(self):
        from apex.db.backup import BrainPersistence
        session = SimpleNamespace(get=lambda *a, **k: SimpleNamespace(
            status_code=200, json=lambda: {"private": False}))
        persistence = BrainPersistence("/tmp/apex-test.db", "owner/public", "token", session=session)
        with self.assertRaisesRegex(RuntimeError, "BACKUP_REPOSITORY_NOT_PRIVATE"):
            persistence._require_private_repository()

    async def test_webhook_rejects_spoofing_and_retries_failures(self):
        class Web(FakeWeb):
            @staticmethod
            def Response(*, text, status=200):
                return SimpleNamespace(text=text, status=status)
        deps = SimpleNamespace(
            config=ApexConfig.from_env({"TELEGRAM_TOKEN": "token", "WEBHOOK_URL": "https://example.test"}),
            runtime=FakeRuntime(), telegram_bot=object(), dispatcher=SimpleNamespace(feed_update=AsyncMock()),
            update_type=lambda **kw: kw, web=Web, initialize=AsyncMock(),
            shutdown=AsyncMock(), token_snapshot=lambda: {},
        )
        app = build_webhook_application(deps)
        handler = next(h for method, path, h, _ in app.router.routes if method == "POST")
        request = SimpleNamespace(headers={}, read=AsyncMock(return_value=b'{"update_id":1}'))
        self.assertEqual((await handler(request)).status, 403)
        deps.dispatcher.feed_update.assert_not_awaited()
        request.headers = {"X-Telegram-Bot-Api-Secret-Token":
                           deps.config.integrations.telegram_webhook_secret}
        deps.dispatcher.feed_update.side_effect = RuntimeError("state down")
        self.assertEqual((await handler(request)).status, 503)
        deps.dispatcher.feed_update.side_effect = None
        self.assertEqual((await handler(request)).status, 200)

    async def test_all_telegram_routes_reject_other_users(self):
        class Route:
            def __init__(self): self.handlers = []
            def register(self, callback, *a): self.handlers.append(callback)
        dispatcher = SimpleNamespace(message=Route(), callback_query=Route(), chat_member=Route())
        called = AsyncMock()
        handlers = TelegramHandlers(**{key: called for key in TelegramHandlers.__dataclass_fields__})
        register_telegram_handlers(dispatcher, handlers, lambda command: command,
                                   admin_ids=frozenset({123}))
        unknown = SimpleNamespace(from_user=SimpleNamespace(id=999))
        for route in (dispatcher.message, dispatcher.callback_query, dispatcher.chat_member):
            for callback in route.handlers:
                await callback(unknown)
        called.assert_not_awaited()
        await dispatcher.callback_query.handlers[0](SimpleNamespace(from_user=SimpleNamespace(id=123)))
        called.assert_awaited_once()
