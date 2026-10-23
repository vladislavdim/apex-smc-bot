"""Reproduce October audit failures at persistence and production boundaries."""

import asyncio
import json
import sqlite3
import time
from collections import defaultdict, deque
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock, MagicMock, patch

import pytest
from apex.db.state_db import migrate_state
from apex.db.repositories.deliveries import DeliveryClaimRepository
from apex.db.repositories.delivery_outbox import DeliveryOutbox
from apex.execution import orders
from apex.market.candle_router import GateCandleRouter
from tests.test_trade_execution import RULES, live_config


@pytest.fixture
def state(tmp_path):
    path = str(tmp_path / "state.db")
    conn = sqlite3.connect(path)
    migrate_state(conn)
    conn.close()
    return path


def test_remote_outbox_restores_full_signal_and_blocks_expired_claim(state, tmp_path):
    box = DeliveryOutbox(state)
    claims = DeliveryClaimRepository(lambda: sqlite3.connect(state))
    assert claims.claim("BTC:MTF", 100, 3600)
    assert claims.claim("pair:BTC", 100, 3600)
    identity = box.prepare(
        {"symbol": "BTC", "entry": 100, "sl": 95, "tp1": 110},
        "BTC:MTF",
        "pair:BTC",
        100,
        [{"chat_id": 123, "text": "exact payload"}],
    )
    # SQLite backup represents the acknowledged remote checkpoint before send.
    restored = str(tmp_path / "restored.db")
    source = sqlite3.connect(state)
    target = sqlite3.connect(restored)
    source.backup(target)
    source.close()
    target.close()
    recovered = DeliveryOutbox(restored)
    assert json.loads(recovered.get(identity)["candidate_json"])["sl"] == 95
    restored_claims = DeliveryClaimRepository(lambda: sqlite3.connect(restored))
    assert not restored_claims.claim("pair:BTC", 100 + 86400 * 365, 3600)
    recovered.resolve(identity, "release", "admin", "Telegram history verified: absent")
    assert restored_claims.claim("pair:BTC", 100 + 86400 * 365, 3600)
    assert json.loads(recovered.get(identity)["resolution_json"])["actor"] == "admin"


def test_outbox_unknown_preserves_receipt_and_requires_evidence(state):
    box = DeliveryOutbox(state)
    identity = box.prepare({"entry": 100}, "key", "pair:BTC", 100, [123])
    box.receipt(identity, "123:0", "SENT", 42)
    box.finish(identity, "UNKNOWN")
    assert json.loads(box.pending()[0]["receipts_json"])["123:0"]["message_id"] == 42
    with pytest.raises(ValueError):
        box.resolve(identity, "release", "admin", "")
    box.resolve(identity, "confirm", "admin", "message 42 verified")
    assert box.pending() == []


def test_legacy_unknown_release_is_audited(state):
    claims = DeliveryClaimRepository(lambda: sqlite3.connect(state))
    claims.claim("old", 100, 3600)
    claims.mark_uncertain("old", 100)
    box = DeliveryOutbox(state)
    assert len(box.uncertain_claims()) == 1
    box.release_legacy_claim("old", 100, "admin", "verified absent")
    with sqlite3.connect(state) as conn:
        assert conn.execute(
            "SELECT actor,evidence FROM delivery_resolutions"
        ).fetchone() == ("admin", "verified absent")


def bars(now=None):
    boundary = int((now or time.time()) // 3600) * 3600
    return [
        {
            "timestamp": boundary - (5 - i) * 3600,
            "open": 100,
            "high": 101,
            "low": 99,
            "close": 100,
            "volume": 1,
        }
        for i in range(5)
    ]


@pytest.mark.parametrize("source", ["gate", "cache", "shared"])
@pytest.mark.parametrize("defect", ["stale", "gap", "reversed", "ohlc", "unknown_time"])
def test_live_router_rejects_each_invalid_source(source, defect):
    rows = bars()
    if defect == "stale":
        rows = bars(time.time() - 86400)
    if defect == "gap":
        rows[2]["timestamp"] -= 1800
    if defect == "reversed":
        rows.reverse()
    if defect == "ohlc":
        rows[-1]["high"] = 90
    if defect == "unknown_time":
        rows[-1].pop("timestamp")
    health = Mock()
    router = GateCandleRouter(
        cache={"BTCUSDT_1h": (rows, time.time())} if source == "cache" else {},
        get_shared=lambda *_: rows if source == "shared" else [],
        update_shared=Mock(),
        fetch_gate=lambda *_: {"candles": rows} if source == "gate" else {},
        gate_available=lambda: True,
        record_health=health,
        last_closed_at=lambda _: None,
    )
    assert router.get_candles("BTCUSDT", "1h", 5) == []
    assert health.call_args.args[2] is False


def test_cancelled_stop_repaired_without_recreating_take_profits():
    row = {
        "signal_id": 1,
        "symbol": "BTCUSDT",
        "direction": "BULLISH",
        "status": "PROTECTED",
        "stop_order_id": "old",
        "quantity": 1,
        "sl": 95,
    }
    client = Mock()
    client.query_algo_order.return_value = {"algoStatus": "CANCELED"}
    client.symbol_rules.return_value = RULES
    client.place_close_all_trigger.return_value = {"algoId": "repaired"}
    with (
        patch.object(orders, "_checkpoint_intent") as checkpoint,
        patch.object(orders, "_update_execution") as update,
    ):
        orders._verify_protective_stop(row, client, "unused")
    client.query_algo_order.assert_called_once_with(algo_id="old")
    assert client.place_close_all_trigger.call_args.args[2] == "STOP_MARKET"
    assert checkpoint.called and update.call_args.kwargs["stop_order_id"] == "repaired"
    client.place_reduce_trigger.assert_not_called()


def test_unknown_stop_does_not_blindly_install_duplicate():
    client = Mock()
    client.query_algo_order.side_effect = TimeoutError()
    with pytest.raises(TimeoutError):
        orders._verify_protective_stop({"stop_order_id": "old"}, client, "unused")
    client.place_close_all_trigger.assert_not_called()


def test_empty_state_discovers_remote_exposure():
    client = Mock()
    client.open_owned_orders.return_value = [{"clientOrderId": "apex_e_unknown"}]
    client.open_positions.return_value = []
    with (
        patch.object(orders, "_live_reconcile_rows", return_value=[]),
        patch.object(orders, "_execution_state_repository", return_value=None),
    ):
        result = orders._reconcile_live_executions_unlocked(
            config=live_config(), client=client
        )
    assert result[0]["reason"] == "REMOTE_EXPOSURE_WITHOUT_LOCAL_OWNER"
    client.emergency_close.assert_not_called()


def test_missing_durable_intent_fails_closed():
    with (
        patch.object(orders, "_ACCOUNT_STATE_FACTORY", Mock()),
        patch.object(orders, "_DURABLE_CHECKPOINT", None),
    ):
        with pytest.raises(RuntimeError, match="unconfigured"):
            orders._checkpoint_intent("execution_intent:test")


def test_gate_missing_multiplier_and_liquidations_remain_unknown():
    from external_sources import live_tape as tape

    with (
        patch.object(tape, "_trades", defaultdict(lambda: deque())),
        patch.object(tape, "_liquidations", defaultdict(lambda: deque())),
        patch.object(tape, "_latest", {}),
        patch.object(tape, "_apex_from_provider", return_value="BTCUSDT"),
        patch.object(tape, "get_pair", return_value={}),
    ):
        tape.ingest_gate(
            {
                "event": "update",
                "channel": "futures.trades",
                "result": [{"contract": "BTC_USDT", "size": 100, "price": 100}],
            }
        )
        tape.ingest_gate(
            {
                "event": "update",
                "channel": "futures.tickers",
                "result": [
                    {"contract": "BTC_USDT", "total_size": 100, "mark_price": 100}
                ],
            }
        )
        result = tape.snapshot("BTCUSDT")
        assert result["cvd_real_delta_usd_60s"] is None
        assert result["buy_usd_60s"] is None
        assert result["long_liq_usd_300s"] is None
        assert result["sources"]["gate"]["oi"] is None


def test_depth_history_retention_has_hard_row_cap(tmp_path):
    from external_sources.storage import persist_gate_microstructure

    path = str(tmp_path / "depth.db")
    persist_gate_microstructure("BTC", {"update_id": "seed"}, path)
    with sqlite3.connect(path) as conn:
        conn.executemany(
            "INSERT INTO gate_microstructure_live_context(event_key,symbol,payload_json) VALUES(?,?,'{}')",
            [(str(i), "BTC") for i in range(10010)],
        )
        conn.execute(
            "INSERT INTO gate_microstructure_live_context(event_key,symbol,created_at,payload_json) VALUES('ancient','BTC','2000-01-01','{}')"
        )
    persist_gate_microstructure("BTC", {"update_id": "latest"}, path)
    with sqlite3.connect(path) as conn:
        assert (
            conn.execute(
                "SELECT count(*) FROM gate_microstructure_live_context"
            ).fetchone()[0]
            == 10000
        )
        assert (
            conn.execute(
                "SELECT 1 FROM gate_microstructure_live_context WHERE event_key='ancient'"
            ).fetchone()
            is None
        )


def test_timeout_keeps_one_job_owner_and_late_success_recovers():
    from apex.app import scheduler

    async def verify():
        release = asyncio.Event()
        callback = Mock()
        runtime = Mock()
        recorder = MagicMock()

        async def slow():
            callback()
            await release.wait()
            return True

        definition = replace(scheduler.JOB_BY_ID["state_backup"], timeout_seconds=0.01)
        with (
            patch.dict(scheduler.JOB_BY_ID, {"state_backup": definition}),
            patch.object(scheduler, "runtime_supervisor", runtime),
            patch.object(scheduler, "recorder_for", return_value=recorder),
            patch.object(scheduler, "recover_incident"),
            patch.object(scheduler, "report_incident"),
        ):
            run = scheduler._guarded("state_backup", slow)
            with pytest.raises(asyncio.TimeoutError):
                await run()
            assert await run() is False
            assert callback.call_count == 1
            release.set()
            await scheduler.drain_production_jobs()
            await asyncio.sleep(0)
            recorder.finish_late.assert_called_once_with("LATE_OK", error_code="")
            assert runtime.mark_component.call_args.args[1].value == "READY"

    asyncio.run(verify())


@pytest.mark.parametrize("final_price", [None, 111, 100])
def test_send_rechecks_gate_after_review_and_checkpoints_payload_before_call(
    final_price,
):
    from tests.test_runtime_recovery import _worker_case

    _worker_case(
        """
import asyncio, tempfile, sqlite3, time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from apex.app import worker
from apex.db.state_db import migrate_state
from apex.db.repositories.delivery_outbox import DeliveryOutbox
from apex.quality.integrity import validate_legacy_candidate
async def verify():
    with tempfile.TemporaryDirectory() as directory:
        path = directory+'/state.db'
        conn=sqlite3.connect(path); migrate_state(conn); conn.close()
        candidate={'symbol':'BTCUSDT','direction':'BULLISH','grade':'MTF','entry':100,'sl':95,'tp1':110,'timeframe':'1h','text':'signal'}
        review={'decision':'APPROVE','confidence':0.95,'degraded':False,'context':{},'reasons':[]}
        async def checkpoint(reason):
            assert DeliveryOutbox(path).pending(), 'full outbox must exist at checkpoint'
            return {'status':'saved'}
        async def send(*args,**kwargs):
            row=DeliveryOutbox(path).pending()[0]
            import json
            assert json.loads(row['candidate_json'])['sl']==95
            raise worker._SignalDeliveryUncertain('lost reply')
        quote = Mock(side_effect=TimeoutError()) if FINAL_PRICE is None else Mock(return_value={'source':'gate','price':FINAL_PRICE,'observed_at':time.time()})
        with patch.object(worker,'_V3_CONFIG',SimpleNamespace(database=SimpleNamespace(state_db_path=path),execution=SimpleNamespace(enabled=False),integrations=SimpleNamespace(groq_min_approval_confidence=.65))), \\
             patch.object(worker,'_V3_RUNTIME',Mock()),patch.object(worker,'ADMIN_IDS',[123]), \\
             patch.object(worker,'_strategy_risk_state',return_value={'mode':'NORMAL'}), \\
             patch.object(worker,'get_live_prices',return_value={'BTCUSDT':{'price':100}}), \\
             patch.object(worker,'_validate_signal_candidate',side_effect=validate_legacy_candidate), \\
             patch.object(worker,'_has_pending_signal_for_symbol',return_value=False), \\
             patch.object(worker,'_assess_setup_candidate',return_value={'blocking':False}), \\
             patch.object(worker,'_persist_setup_assessment'),patch.object(worker,'_record_strategy_decision'), \\
             patch.object(worker,'_review_signal_candidate',new=AsyncMock(return_value=review)), \\
             patch.object(worker,'_SIGNAL_QUALITY_GATE_OK',True),patch.object(worker,'_SIGNAL_INTEGRITY_OK',True), \\
             patch.object(worker,'backup_state_db_to_github',side_effect=checkpoint), \\
             patch.object(worker,'_send_signal_message',new=AsyncMock(side_effect=send)) as delivery, \\
             patch.object(worker,'_v3_report_incident'),patch.object(worker,'_v3_recover_incident'), \\
             patch('apex.market.gate_tickers.get_execution_quote',quote):
            assert await worker._send_signal(candidate) is False
            assert delivery.await_count == (1 if FINAL_PRICE==100 else 0)
            if FINAL_PRICE==100:
                assert DeliveryOutbox(path).pending()[0]['status']=='UNKNOWN'
asyncio.run(verify())
""".replace("FINAL_PRICE", repr(final_price))
    )


def test_startup_inventory_detects_orphan_beside_known_position():
    client = Mock()
    client.open_owned_orders.return_value = [
        {"orderId": "foreign", "clientOrderId": "apex_e_missing"}
    ]
    client.open_positions.return_value = [{"symbol": "BTCUSDT", "positionAmt": "1"}]
    row = {"signal_id": 1, "symbol": "BTCUSDT", "entry_order_id": "known"}
    with (
        patch.object(orders, "_live_reconcile_rows", return_value=[row]),
        patch.object(orders, "_ACCOUNT_STATE_FACTORY", None),
    ):
        with pytest.raises(RuntimeError, match="WITHOUT_STATE_OWNER"):
            orders.verify_remote_ownership("unused", client)
    client.emergency_close.assert_not_called()


def test_price_observation_does_not_fabricate_management_decision(state):
    from apex.db.repositories.manager import ManagerRepository

    repo = ManagerRepository(lambda: sqlite3.connect(state))
    repo.register(
        {
            "signal_id": 1,
            "symbol": "BTCUSDT",
            "strategy": "MTF",
            "direction": "BULLISH",
            "management_tf": "1h",
            "initial_entry": 100,
            "initial_sl": 95,
            "initial_tp1": 110,
            "manager_version": 2,
        }
    )
    before = repo.get(1)
    repo.observe_price(1, 102, 0.4, 102)
    after = repo.get(1)
    assert after["last_price"] == 102 and after["current_r"] == 0.4
    assert after["price_observed_at"]
    assert (
        after["last_event"] == before["last_event"]
        and after["last_action"] == before["last_action"]
    )


def test_critical_checkpoint_waits_for_shared_slot_instead_of_dropping():
    import threading
    from apex.db import backup

    persistence = backup.BrainPersistence(
        "unused", "repo", "token", remote_name="apex_state.db"
    )
    started = threading.Event()
    result = []

    def run():
        started.set()
        result.append(persistence.backup("signal_delivery_claim"))

    backup._BACKUP_RESOURCE_LOCK.acquire()
    thread = threading.Thread(target=run)
    try:
        with patch.object(
            persistence, "_backup_locked", return_value={"status": "saved"}
        ):
            thread.start()
            assert started.wait(1)
            # Release the maintenance owner; the same critical request succeeds.
            backup._BACKUP_RESOURCE_LOCK.release()
            thread.join(2)
    finally:
        if thread.is_alive():
            thread.join(2)
    assert result and result[0]["status"] == "saved"


def test_unqueried_tape_pair_retains_only_consumed_window():
    from external_sources import live_tape as tape

    trades = defaultdict(lambda: deque(maxlen=1500))
    liqs = defaultdict(lambda: deque(maxlen=1200))
    with (
        patch.object(tape, "_trades", trades),
        patch.object(tape, "_liquidations", liqs),
        patch.object(tape, "_latest", {}),
    ):
        for timestamp in range(3600):
            tape._trade("gate", "NEVER_SCANNED", timestamp, 100, "buy")
            tape._liquidation("bybit", "NEVER_SCANNED", timestamp, 100, "long")
        assert len(trades[("gate", "NEVER_SCANNED")]) <= 61
        assert len(liqs[("bybit", "NEVER_SCANNED")]) <= 301
        assert not hasattr(trades[("gate", "NEVER_SCANNED")][0], "__dict__")


def test_busy_tape_aggregates_alternating_sides_without_losing_notional():
    from external_sources import live_tape as tape

    trades = defaultdict(lambda: deque(maxlen=1500))
    with patch.object(tape, "_trades", trades), patch.object(tape, "_latest", {}):
        for index in range(5000):
            tape._trade(
                "gate",
                "BUSY",
                1000 + index / 10000,
                100,
                "buy" if index % 2 else "sell",
            )
        events = trades[("gate", "BUSY")]
        assert len(events) == 2
        assert sum(event.value_usd for event in events) == 500000
        assert sum(event.count for event in events) == 5000
