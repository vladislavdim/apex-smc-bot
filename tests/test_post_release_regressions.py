"""Failure-injection checks from the 23 October production audit."""
import ast
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import threading
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from apex.app.scan_budget import scan_deadline, scan_has_budget
from apex.db.repositories.scan_candidates import ScanCandidates
from apex.market.candles import confirmed_candles
from apex.market.freshness import assess_candles
from apex.market.session_liquidity import SessionLiquidityProvider
from scripts.verify_production_acceptance import verify_acceptance, AcceptanceIncomplete
from tests.test_production_acceptance import rows, SHA


def test_degraded_cannot_pass_even_with_48h_of_ready_observations():
    data = rows()
    for row in data:
        row['unhealthy_components'] = {'scanner_zone': 'DEGRADED'}
    with pytest.raises(AcceptanceIncomplete, match='degraded_components'):
        verify_acceptance(data, SHA)


def test_invalid_candle_and_internal_gap_cannot_be_fresh():
    now = datetime.now(timezone.utc)
    bars = [dict(open_time=now.timestamp()-(5-i)*3600,open=100,high=101,low=99,close=100,volume=10) for i in range(5)]
    bars[-1].update(high=50,low=150,volume=-1)
    clean = confirmed_candles(bars,'1h',as_of=now)
    assert len(clean)==4
    bars[-1].update(high=101,low=99,volume=1)
    del bars[2]
    clean = confirmed_candles(bars,'1h',as_of=now)
    assert assess_candles('1h',clean,as_of=now).reason_code=='GATE_HISTORY_GAP'


def test_unknown_liquidity_and_fully_closed_low_volume_tail():
    assert SessionLiquidityProvider(lambda *_: []).check('XUSDT')['ok'] is False
    candles = [dict(volume=100,is_closed=True) for _ in range(25)]
    candles[-1]['volume']=1
    result=SessionLiquidityProvider(lambda *_: candles).check('XUSDT')
    assert result['ratio']==.01 and not result['ok']


def test_candidate_survives_restart_and_is_retired_only_after_processing(tmp_path):
    factory=lambda:sqlite3.connect(tmp_path/'state.db')
    first=ScanCandidates(factory,'ZONE')
    candidate=dict(symbol='ETHUSDT',entry=100,sl=95,tp=110,snapshot_id='one')
    first.append(candidate)
    first.append(candidate)
    second=ScanCandidates(factory,'ZONE')
    assert list(second)==[candidate]
    second.retire(candidate)
    assert not ScanCandidates(factory,'ZONE')


def test_inner_timeout_keeps_lock_until_actual_thread_completes():
    source=Path('apex/app/worker.py').read_text()
    function=next(n for n in ast.parse(source).body if isinstance(n,ast.AsyncFunctionDef) and n.name=='_run_market_scan_exclusive')
    from apex.app.scan_budget import ScanDeadlineExceeded
    entered=threading.Event();finish=threading.Event()
    def native():
        entered.set();finish.wait(2)
    async def callback():
        await asyncio.to_thread(native)
    async def scenario():
        lock=asyncio.Lock()
        async def noop(*args): return None
        ns=dict(asyncio=asyncio,time=time,logging=__import__('logging'),_market_scan_lock=lock,
                _active_market_scan=None,_active_market_scan_started=0,_active_scan_run_id=None,
                _begin_scan_control=lambda *a:1,_finish_scan_control=lambda *a:None,
                _finish_latest_running=lambda *a:None,_mark_scan_skipped=lambda *a:None,
                _scanner_strategy=lambda name:name,DB_PATH='unused',scan_deadline=scan_deadline,
                ScanDeadlineExceeded=ScanDeadlineExceeded,ScanBudget=__import__("apex.app.scan_budget",fromlist=["ScanBudget"]).ScanBudget)
        exec(compile(ast.Module(body=[function],type_ignores=[]),'worker','exec'),ns)
        task=asyncio.create_task(ns['_run_market_scan_exclusive']('ZONE',callback,.03))
        await asyncio.sleep(.1)
        try:
            assert entered.is_set() and lock.locked() and not task.done()
        finally: finish.set()
        with pytest.raises(ScanDeadlineExceeded): await task
        assert not lock.locked() and ns['_active_scan_run_id'] is None
    asyncio.run(scenario())


def test_snapshot_collection_parallel_but_bounded_and_joins_every_call():
    from apex.market.provider import _CollectedClient
    active=0;maximum=0;lock=threading.Lock()
    def read(value):
        nonlocal active,maximum
        with lock:active+=1;maximum=max(maximum,active)
        time.sleep(.02)
        with lock:active-=1
        return value
    collected=_CollectedClient(SimpleNamespace(read=read),[('read',(i,),{}) for i in range(12)])
    assert 1<maximum<=4 and active==0
    assert [collected.read(i) for i in range(12)]==list(range(12))


def test_partial_activation_bar_never_creates_certain_win(tmp_path):
    from apex.db.state_db import migrate_state
    from apex.db.repositories.signal_lifecycle import SignalLifecycleRepository
    from apex.strategies.state_signal_monitor import StateSignalMonitor
    path=tmp_path/'monitor.db';c=sqlite3.connect(path);migrate_state(c);c.close()
    repo=SignalLifecycleRepository(lambda:sqlite3.connect(path))
    start=datetime(2026,10,23,10,tzinfo=timezone.utc)
    repo.import_row(dict(signal_id=1,status='active',result='pending',symbol='ETHUSDT',direction='BULLISH',signal_type='ZONE',timeframe='4h',entry=100,sl=95,tp1=110,tp2=120,tp3=120,grade='ZONE',created_at=start.isoformat(),activated_at=(start+timedelta(seconds=150)).isoformat()))
    bars=[dict(open_time=start.timestamp(),low=94,high=102,is_closed=True),dict(open_time=(start+timedelta(minutes=5)).timestamp(),low=100,high=121,is_closed=True)]
    monitor=StateSignalMonitor(repo,lambda:{'ETHUSDT':{'price':120}},lambda *_:bars,lambda *a,**k:None)
    result=monitor.check(now=start+timedelta(minutes=15))[0]
    assert result['result']=='ambiguous' and result['original_tp1_before_sl'] is None


def test_polling_respects_retry_after():
    from apex.app.telegram_retry import PollingRetryAfter
    from aiogram.exceptions import TelegramRetryAfter
    from aiogram.methods import GetUpdates
    method=GetUpdates()
    make_request=AsyncMock(side_effect=TelegramRetryAfter(method=method,message='limited',retry_after=5))
    async def run():
        with patch('apex.app.telegram_retry.asyncio.sleep',new_callable=AsyncMock) as sleep:
            with pytest.raises(TelegramRetryAfter):await PollingRetryAfter()(make_request,object(),method)
            sleep.assert_awaited_once_with(5.0)
    asyncio.run(run())


def test_zone_formatter_keeps_original_snapshot_and_trace():
    source=ast.parse(Path('apex/app/worker.py').read_text())
    function=next(n for n in source.body if isinstance(n,ast.FunctionDef) and n.name=='_zone_candidate_from_setup')
    ns={'smart_price_fmt':str}
    exec(compile(ast.Module(body=[function],type_ignores=[]),'worker','exec'),ns)
    setup=dict(symbol='ETHUSDT',direction='BULLISH',entry=100,sl=95,tp=110,tp2=120,zone='Discount',zone_type='OB',logic='zone reaction',q_score=3,rr=2,
               snapshot_id='snapshot_original',snapshot_as_of='2026-10-23T06:00:00Z',_v3_strategy_trace={'checks':[{'outcome':'PASS'}]})
    candidate=ns['_zone_candidate_from_setup'](setup)
    assert candidate['snapshot_id']=='snapshot_original'
    assert candidate['_v3_strategy_trace']==setup['_v3_strategy_trace']


def test_repeated_timeout_escalates_once_and_never_downgrades(tmp_path):
    from apex.db.state_db import migrate_state
    from apex.telemetry.incidents import open_incident
    conn=sqlite3.connect(tmp_path/'incidents.db');migrate_state(conn)
    results=[open_incident(conn,'JOB_TIMEOUT','scanner_zone','WARNING') for _ in range(5)]
    assert [r['severity'] for r in results]==['WARNING','WARNING','ERROR','ERROR','ERROR']
    assert conn.execute('select count(*) from incident_notifications').fetchone()[0]==2
    conn.close()


def test_acceptance_requires_completed_universe_rounds():
    data=rows()
    data[-1]['scanner_coverage']['ZONE']=None
    with pytest.raises(AcceptanceIncomplete,match='scanner_coverage_missing:ZONE'):
        verify_acceptance(data,SHA)


def test_collection_cannot_turn_an_earlier_forming_bar_into_a_closed_bar():
    from apex.market.gate_client import GateMarketClient
    from apex.market.provider import GateSnapshotProvider
    from tests.test_apex_v3_gate_client import FakeGate
    client=GateMarketClient(get_json=FakeGate(),clock=lambda:1799)
    build=GateSnapshotProvider(client,candle_limit=100).build('FAST','BTCUSDT',as_of=datetime.fromtimestamp(1800,timezone.utc))
    # The 900–1800 bar was still forming when retrieved at 1799.
    assert len(build.snapshot.candles['15m'])==1


def test_cold_context_is_usable_at_post_collection_decision_time():
    from apex.market.gate_client import GateMarketClient
    from apex.market.provider import GateSnapshotProvider
    from tests.test_apex_v3_gate_client import FakeGate
    class Clock:
        @classmethod
        def now(cls, tz=None):return datetime.fromtimestamp(1101,timezone.utc)
    with patch('apex.market.provider.datetime',Clock):
        build=GateSnapshotProvider(GateMarketClient(get_json=FakeGate(),clock=lambda:1100)).build('FAST','BTCUSDT')
    assert build.snapshot.derivatives_context['funding']['available']
    assert build.snapshot.microstructure_context['visible_orderbook']['available']
