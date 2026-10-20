from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
import threading

from apex.market.gate_client import GateMarketClient
from apex.ops.resource_guard import memory_snapshot
from scripts.render_client import RenderReleaseClient, ReleaseError


def test_memory_relief_retries_during_sustained_pressure_and_bounds_flapping():
    from apex.ops.resource_guard import MemoryReliefPolicy, MemorySnapshot
    high = MemorySnapshot(450, 512, 450 / 512, "NEW_ENTRIES_OFF")
    normal = MemorySnapshot(250, 512, 250 / 512, "NORMAL")
    policy = MemoryReliefPolicy()
    assert policy.claim(high, 0)
    assert not policy.claim(high, 15)
    assert policy.claim(high, 300)  # Pressure never returned to NORMAL.
    assert not policy.claim(normal, 400)
    assert not policy.claim(high, 450)  # A brief recovery must not defeat cooldown.
    assert policy.claim(high, 600)


def test_memory_health_uses_measured_result_and_collects_off_event_loop():
    import ast, asyncio, logging
    from pathlib import Path
    from types import SimpleNamespace
    from unittest.mock import Mock
    from apex.domain.enums import ComponentState
    from apex.ops.resource_guard import MemoryReliefPolicy, MemorySnapshot

    node = next(n for n in ast.parse(Path('apex/app/worker.py').read_text()).body
                if isinstance(n, ast.AsyncFunctionDef) and n.name == '_v3_update_memory_health')
    high = MemorySnapshot(470, 512, 470 / 512, 'NEW_ENTRIES_OFF', 490, 470)
    normal = MemorySnapshot(300, 512, 300 / 512, 'NORMAL', 320, 300)
    degraded = MemorySnapshot(410, 512, 410 / 512, 'DEGRADED', 430, 410)
    main_thread = threading.get_ident()
    def collect():
        assert threading.get_ident() != main_thread
        return True
    runtime = Mock()
    samples = Mock(side_effect=[high, normal])
    namespace = {
        'asyncio': asyncio, 'time': SimpleNamespace(monotonic=lambda: 1000),
        'logging': logging, '_V3_MEMORY_RELIEF': MemoryReliefPolicy(),
        '_v3_sample_memory': samples, '_v3_memory_diagnostics': lambda: {},
        'clear_market_runtime_caches': Mock(return_value={'candles': 5}),
        '_get_v3_strategy_snapshot_provider': lambda: SimpleNamespace(client=Mock()),
        '_v3_release_unused_memory': collect, '_V3_RUNTIME': runtime,
        '_V3_COMPONENT_STATE': ComponentState,
        '_v3_report_incident': Mock(), '_v3_recover_incident': Mock(),
    }
    exec(compile(ast.Module(body=[node], type_ignores=[]), 'worker-memory', 'exec'), namespace)
    asyncio.run(namespace['_v3_update_memory_health']())
    assert runtime.mark_component.call_args.args[1] == ComponentState.READY
    runtime.inhibit_entries.assert_not_called()
    namespace['_v3_recover_incident'].assert_called_once_with('MEMORY_PRESSURE', 'memory')

    # A successful malloc_trim is not proof that pressure is gone.
    runtime.reset_mock()
    namespace['_V3_MEMORY_RELIEF'] = MemoryReliefPolicy()
    samples.side_effect = [high, degraded]
    asyncio.run(namespace['_v3_update_memory_health']())
    runtime.inhibit_entries.assert_called_once_with('RESOURCE_MEMORY_DEGRADED')
    runtime.clear_inhibit.assert_called_once_with('RESOURCE_MEMORY_NEW_ENTRIES_OFF')
    assert runtime.mark_component.call_args.args[1] == ComponentState.DEGRADED

    # The next watchdog tick still inhibits entries but does not collect again.
    samples.side_effect = [high]
    namespace['clear_market_runtime_caches'].reset_mock()
    runtime.reset_mock()
    asyncio.run(namespace['_v3_update_memory_health']())
    namespace['clear_market_runtime_caches'].assert_not_called()
    runtime.inhibit_entries.assert_called_once_with('RESOURCE_MEMORY_NEW_ENTRIES_OFF')


def test_historical_candle_keys_cannot_grow_cache_or_lock_registry():
    clock=[100]
    calls=[]
    client=GateMarketClient(clock=lambda:clock[0],max_cache_entries=8,
        get_json=lambda url,params,timeout:calls.append(params) or [[1,2,3,4,5,6]])
    for start in range(300):
        client.candles('BTCUSDT','1m',from_time=start,to_time=start+10)
    assert len(client._cache)==8
    assert len(client._key_locks)==64
    clock[0]=1000
    client.candles('ETHUSDT','1m')
    assert len(client._cache)==1


def test_bounded_cache_preserves_same_key_singleflight():
    entered=threading.Event(); release=threading.Event(); calls=[]
    def get(*args):
        calls.append(1); entered.set(); assert release.wait(5); return [1]
    client=GateMarketClient(get_json=get)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first=pool.submit(client.candles,'BTCUSDT','1m')
        assert entered.wait(5)
        second=pool.submit(client.candles,'BTCUSDT','1m')
        release.set()
        assert first.result() is second.result()
    assert len(calls)==1


def test_guard_includes_git_child_working_set_without_reclaimable_file_pages():
    with patch('apex.ops.resource_guard._rss_bytes',return_value=100), \
         patch('apex.ops.resource_guard._cgroup_memory',return_value=(490,420)):
        result=memory_snapshot(limit_bytes=512)
    assert result.rss_bytes==100 and result.service_bytes==490
    assert result.working_set_bytes==420 and result.state=='DEGRADED'


def test_missing_deploy_id_resolves_only_new_exact_commit_without_repeat_post():
    client=RenderReleaseClient('fake')
    old={'deploy':{'id':'old','commit':{'id':'a'*40}}}
    new={'deploy':{'id':'new','commit':{'id':'a'*40}}}
    with patch.object(client,'_request',side_effect=[[old],{},[old,new]]) as request:
        assert client.trigger('worker','a'*40)['id']=='new'
    assert sum(call.args[0]=='POST' for call in request.call_args_list)==1


def test_ambiguous_deploy_acknowledgement_fails_closed():
    client=RenderReleaseClient('fake')
    rows=[{'deploy':{'id':name,'commit':{'id':'a'*40}}} for name in ('one','two')]
    with patch.object(client,'_request',side_effect=[[],{},rows]):
        try:client.trigger('worker','a'*40)
        except ReleaseError as exc: assert 'ambiguous' in str(exc)
        else:raise AssertionError('ambiguous deployments must not be selected')


def test_web_runtime_is_removed_and_telegram_is_independent():
    from pathlib import Path
    assert not Path('stats_server.py').exists()
    assert not Path('apex/ui/dashboard').exists()
    source=Path('apex/app/worker.py').read_text()
    assert '_emit_apex_v2_dashboard_snapshot' not in source
    from apex.ui.telegram.statistics import general_statistics
    assert callable(general_statistics)


def test_candle_router_evicts_old_pairs_without_truncating_strategy_history():
    from apex.market.candle_router import GateCandleRouter
    rows=[{'open_time':i,'close':i} for i in range(10)]
    router=GateCandleRouter(cache={},get_shared=lambda *_:[],update_shared=lambda *_:None,
        fetch_gate=lambda *_:{'candles':rows},gate_available=lambda:True,
        record_health=lambda *_,**__:None,last_closed_at=lambda _:0)
    for symbol in range(100):
        assert router.get_candles(f'PAIR{symbol}USDT','1h',10)==rows
    assert len(router.cache)==64
    assert router.get_candles('PAIR0USDT','1h',10)==rows


def test_optional_diagnostic_queue_has_bounded_detached_payloads():
    import queue
    from apex.telemetry import event_log
    events=queue.Queue(maxsize=128)
    payload={'score':7,'context':[{'value':'x'*1000} for _ in range(200)]}
    with patch.object(event_log,'_EVENT_QUEUE',events),patch.object(event_log,'_ensure_worker'), \
            patch.object(event_log,'_persist_event',return_value=True) as overflow_write:
        for number in range(200):
            event_log.emit_event('strategy_attempt','FAST','BTCUSDT',payload,event_key=str(number))
    payload['score']=99
    assert events.qsize()==128
    saved=events.get_nowait()['payload']
    assert saved['score']==7 and saved.get('payload_truncated') is True
    assert 'context' not in saved
    assert overflow_write.call_count==72


def test_diagnostic_queue_overflow_persists_locally_without_waiting_for_worker():
    import queue, sqlite3, tempfile
    from pathlib import Path
    from apex.telemetry import event_log
    with tempfile.TemporaryDirectory() as directory:
        path=str(Path(directory)/'audit.db')
        events=queue.Queue(maxsize=1)
        with patch.object(event_log,'DB_PATH',path),patch.object(event_log,'_EVENT_QUEUE',events), \
                patch.object(event_log,'_ensure_worker'):
            event_log.emit_event('strategy_attempt','FAST','BTCUSDT',{'score':1},event_key='first')
            event_log.emit_event('strategy_attempt','FAST','BTCUSDT',{'score':2},event_key='overflow')
        with sqlite3.connect(path) as conn:
            saved=conn.execute("SELECT payload_json FROM setup_audit_events WHERE event_key='overflow'").fetchone()
        assert saved is not None and '"score":2' in saved[0]
        assert events.qsize()==1


def test_diagnostic_queue_reports_failed_overflow_without_spam():
    import queue
    from apex.telemetry import event_log
    events=queue.Queue(maxsize=1)
    events.put({'event_key':'first'})
    with patch.object(event_log,'_EVENT_QUEUE',events), patch.object(event_log,'_ensure_worker'), \
            patch.object(event_log,'_persist_event',return_value=False), \
            patch.object(event_log,'_LAST_DROP_WARNING',float('-inf')), \
            patch.object(event_log,'_DROPPED_EVENTS',0), \
            patch.object(event_log.logging,'warning') as warning:
        event_log.emit_event('strategy_attempt','FAST','BTCUSDT',{},event_key='second')
        event_log.emit_event('strategy_attempt','FAST','BTCUSDT',{},event_key='third')
        assert warning.call_count==1
        assert event_log._DROPPED_EVENTS==2


def test_technical_audit_starts_locally_without_restore_or_archiving():
    import ast, asyncio, logging, sqlite3, tempfile
    from pathlib import Path
    from contextlib import closing
    from types import SimpleNamespace
    from unittest.mock import Mock
    from apex.db.audit_db import migrate_audit
    from apex.domain.enums import ComponentState
    source=Path('apex/app/worker.py').read_text()
    node=next(n for n in ast.parse(source).body if isinstance(n,ast.AsyncFunctionDef) and n.name=='_v3_prepare_audit_store')
    with tempfile.TemporaryDirectory() as directory:
        target=str(Path(directory)/'audit.db')
        events=SimpleNamespace(DB_PATH='state.db')
        namespace={'asyncio':asyncio,'logging':logging,'_audit_store_ready':False,
            '_AUDIT_DB_PATH':target,'_audit_event_log':events,'_closing':closing,
            '_audit_connect':sqlite3.connect,'_migrate_audit':migrate_audit,
            '_V3_RUNTIME':Mock(),'_V3_COMPONENT_STATE':ComponentState}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'worker-audit','exec'),namespace)
        assert asyncio.run(namespace['_v3_prepare_audit_store']())
        assert events.DB_PATH==target
        with sqlite3.connect(target) as conn:
            assert conn.execute("SELECT name FROM sqlite_master WHERE name='setup_audit_events'").fetchone()
    assert '_AUDIT_PERSISTENCE' not in source
    assert 'audit_backup=' not in source


def test_shutdown_with_active_scan_name_drains_jobs_before_final_backup():
    import ast, asyncio, logging
    from pathlib import Path
    from types import SimpleNamespace
    from unittest.mock import Mock, AsyncMock
    node = next(n for n in ast.parse(Path('apex/app/worker.py').read_text()).body
                if isinstance(n, ast.AsyncFunctionDef) and n.name == '_shutdown_production_runtime')
    order = []
    checkpoint_gate = asyncio.Event()
    async def drain():
        order.append('drain')
        checkpoint_gate.set()
    async def checkpoint():
        await checkpoint_gate.wait()
        order.append('checkpoint')
    async def finish(*args):
        order.append('backup')
    namespace = {'asyncio': asyncio, 'logging': logging, '_V3_RUNTIME': Mock(),
        '_production_scheduler': Mock(), '_auto_trade_reconcile_task': None,
        '_trade_manager_task': None, '_signal_checkpoint_task': None,
        '_active_market_scan': 'MTF_1H',
        '_v3_get_lease_client': lambda: SimpleNamespace(configured=False),
        '_V3_CONFIG': SimpleNamespace(database=SimpleNamespace(state_db_path='state.db'),
                                    runtime=SimpleNamespace(instance_id='test')),
        'ShutdownDependencies': lambda **values: values, 'shutdown_production': finish,
        '_v3_release_runtime_lease': AsyncMock(), '_v3_record_shutdown': Mock(),
        '_v3_maintenance_and_backup': AsyncMock(), '_MARKET_INTELLIGENCE_OK': False}
    exec(compile(ast.Module(body=[node], type_ignores=[]), 'worker-shutdown', 'exec'), namespace)
    async def exercise():
        namespace['_signal_checkpoint_task'] = asyncio.create_task(checkpoint())
        await namespace['_shutdown_production_runtime']('deploy')
    with patch('apex.app.scheduler.drain_production_jobs', side_effect=drain):
        asyncio.run(exercise())
    assert order == ['drain', 'checkpoint', 'backup']
    namespace['_production_scheduler'].shutdown.assert_called_once_with(wait=True)
