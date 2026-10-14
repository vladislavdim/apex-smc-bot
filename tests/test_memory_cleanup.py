from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
import threading

from apex.market.gate_client import GateMarketClient
from apex.ops.resource_guard import memory_snapshot
from scripts.render_client import RenderReleaseClient, ReleaseError


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
