import asyncio
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock, Mock, patch

from external_sources import live_tape
from external_sources.budget import BudgetDenied, SourceBudget


def test_live_tape_retries_after_transient_budget_lock():
    stop = asyncio.Event()
    attempts = 0

    def reserve(*_args):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise sqlite3.OperationalError("database is locked")
        stop.set()
        raise BudgetDenied("test stop")

    with patch.object(live_tape, "_stop_event", stop), patch.object(live_tape, "budget") as budget, \
         patch.object(live_tape.asyncio, "sleep", new_callable=AsyncMock):
        budget.reserve.side_effect = reserve
        asyncio.run(live_tape._consume("gate", "wss://fx-ws.gateio.ws/v4/ws/usdt", [], Mock()))
    assert attempts == 2


def test_budget_initializes_schema_once_per_instance(tmp_path):
    ledger = SourceBudget(str(tmp_path / "budget.db"))
    ledger.reserve("gate")
    lock = sqlite3.connect(ledger.db_path)
    lock.execute("BEGIN IMMEDIATE")
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(ledger.reserve, "gate")
            time.sleep(0.05)
            lock.commit()
            future.result(timeout=5)
    finally:
        lock.close()
    assert ledger._schema_ready


def _worker_case(script):
    import os
    env = dict(os.environ, TELEGRAM_TOKEN="123456:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
    subprocess.run([sys.executable, "-c", script], env=env, check=True, timeout=30)


def test_ambiguous_telegram_signal_is_never_resent():
    _worker_case("""
import asyncio
from unittest.mock import AsyncMock
from apex.app import worker
send = AsyncMock(side_effect=TimeoutError('reply lost'))
worker.bot.send_message = send
try:
    asyncio.run(worker._send_signal_message(123, 'signal'))
except worker._SignalDeliveryUncertain:
    pass
else:
    raise AssertionError('outcome should be uncertain')
assert send.await_count == 1
""")


def test_gate_probe_requires_recent_closed_candle_and_recovers():
    _worker_case("""
import asyncio
import time
from types import SimpleNamespace
from unittest.mock import Mock
from apex.app import worker
now = time.time()
client = SimpleNamespace(candles=Mock(return_value=SimpleNamespace(payload=[
    [int(now) - 3600, '1', '100', '101', '99', '100'],
])))
runtime = Mock()
worker._V3_RUNTIME = runtime
worker._v3_gate_probe_symbol = 'BTCUSDT'
worker._get_v3_strategy_snapshot_provider = lambda: SimpleNamespace(client=client)
worker._v3_report_incident = Mock()
worker._v3_recover_incident = Mock()
asyncio.run(worker._v3_probe_gate_freshness(force=True))
runtime.inhibit_entries.assert_called_with('GATE_CANDLE_UNAVAILABLE')
assert runtime.mark_component.call_args_list[-2].args[1] == worker._V3_COMPONENT_STATE.UNAVAILABLE
client.candles.return_value.payload = [[int(now) - 120, '1', '100', '101', '99', '100']]
asyncio.run(worker._v3_probe_gate_freshness(force=True))
runtime.clear_inhibit.assert_called_with('GATE_CANDLE_UNAVAILABLE')
assert runtime.mark_component.call_args_list[-2].args[1] == worker._V3_COMPONENT_STATE.FRESH
""")
