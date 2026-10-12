import builtins
import importlib.util
import sqlite3
from pathlib import Path
from unittest.mock import Mock, patch
import pytest
from apex.execution import orders
from apex.execution.ledger import configure_execution_ledger_state, register_execution_orders
from apex.db.repositories.execution_ledger import ExecutionLedgerRepository
from apex.db.repositories.executions import ExecutionRepository
from apex.db.state_db import migrate_state
from apex.manager.engine import _parse_review, build_structure_facts
from apex.telemetry import event_log
from test_trade_execution import live_config, RULES

@pytest.fixture
def state_factory(tmp_path):
    path=str(tmp_path/'state.db')
    with sqlite3.connect(path) as conn: migrate_state(conn)
    factory=lambda: sqlite3.connect(path)
    ExecutionRepository(factory).register({'signal_id':1,'mode':'live','symbol':'BTCUSDT',
        'direction':'BULLISH','status':'ENTRY_PENDING','entry':100,'sl':95,
        'tp1':110,'tp2':115,'quantity':1,'entry_order_id':'entry-1'})
    orders.configure_execution_state(factory)
    configure_execution_ledger_state(factory)
    yield factory
    orders.configure_execution_state(None)
    configure_execution_ledger_state(None)

def expired_entry(factory, response):
    row=ExecutionRepository(factory).get(1)
    row['signal_result']='sl'
    client=Mock()
    client.query_order.return_value={'status':'FILLED','executedQty':'1','orderId':'entry-1'}
    client.symbol_rules.return_value=RULES
    client.emergency_close.return_value=response
    with patch.object(orders,'_live_reconcile_rows',return_value=[row]), patch.object(orders,'_reconciliation_required_signal_ids',return_value=set()):
        result=orders._reconcile_live_executions_unlocked(config=live_config(),client=client,db_path='unused')
    return result

def test_expired_entry_requires_confirmed_emergency_exit(state_factory):
    result=expired_entry(state_factory,{'orderId':'exit-1','status':'NEW','executedQty':'0'})
    assert result[0]['status'] != 'EMERGENCY_CLOSED'

def test_emergency_close_order_enters_owned_fill_ledger(state_factory):
    expired_entry(state_factory,{'orderId':'exit-1','status':'FILLED','executedQty':'1'})
    register_execution_orders('unused')
    with state_factory() as conn:
        ids=[r[0] for r in conn.execute('SELECT remote_id FROM execution_orders')]
    assert 'exit-1' in ids

def test_manager_database_read_failure_blocks_exchange_mutation():
    repository=Mock()
    repository.get.side_effect=sqlite3.OperationalError('transient read failure')
    client=Mock()
    client.symbol_rules.return_value=RULES
    client.open_positions.return_value=[{'symbol':'BTCUSDT','positionAmt':'1'}]
    client.emergency_close.return_value={'orderId':'exit-1','status':'FILLED','executedQty':'.5'}
    snapshot={'mode':'live','status':'PROTECTED','symbol':'BTCUSDT','direction':'BULLISH','quantity':1}
    with patch.object(orders,'_manager_state_repository',return_value=repository), patch.object(orders,'_MANAGER_TRANSITION_VALIDATOR',Mock(return_value=(False,'FORBIDDEN'))), patch.object(orders,'cached_execution_snapshot',return_value=snapshot), patch.object(orders,'_claim_manager_action',return_value=True), patch.object(orders,'_finish_manager_action'), patch.object(orders,'_MANAGER_CONFIRM_CALLBACK',None):
        result=orders.execute_manager_review({'signal_id':1,'review':{'action':'PARTIAL_EXIT','confidence':.9},'facts':{'management_candle_id':'1'}},config=live_config(),client=client)
    assert result['status'] != 'EXECUTED'

def test_nan_manager_confidence_is_rejected():
    result=_parse_review('{"action":"PARTIAL_EXIT","confidence":"NaN"}',{}, {'direction':'BULLISH','initial_sl':95})
    assert result['confidence'] < .7 or result['action']=='HOLD'

def test_manager_rejects_stale_candle_structure():
    rows=[{'timestamp':1000+i*900,'open':100,'high':102,'low':99,'close':101,'volume':10,'is_closed':True} for i in range(20)]
    assert build_structure_facts(rows,'BULLISH',timeframe='15m')['closed_candle'] is False

def test_changed_synced_event_is_returned_to_outbox(tmp_path):
    path=str(tmp_path/'audit.db')
    with patch.object(event_log,'DB_PATH',path):
        original={'event_key':'runtime-status:x','kind':'runtime_status','strategy':'SYSTEM','symbol':'','occurred_at':'2026-10-12T09:00:00+00:00','payload':{'ready':True}}
        event_log._persist_event(original)
        with sqlite3.connect(path) as conn: conn.execute('UPDATE setup_audit_events SET synced=1')
        event_log._persist_event({**original,'occurred_at':'2026-10-12T10:00:00+00:00','payload':{'ready':False}})
    with sqlite3.connect(path) as conn: synced=conn.execute('SELECT synced FROM setup_audit_events').fetchone()[0]
    assert synced==0

def test_daily_loss_uses_net_commissions_and_funding():
    client=orders.BinanceFuturesClient(live_config(),session=Mock())
    client.income_history=Mock(return_value=[{'incomeType':'REALIZED_PNL','income':'-9','asset':'USDT'}, {'incomeType':'COMMISSION','income':'-2','asset':'USDT'}, {'incomeType':'FUNDING_FEE','income':'-1','asset':'USDT'}])
    assert client.realized_pnl_since(1)==-12

def test_release_workflow_installs_database_driver():
    import yaml
    workflow=yaml.safe_load(Path('.github/workflows/controlled-production-release.yml').read_text())
    install='\n'.join(s.get('run','') for s in workflow['jobs']['deploy']['steps'])
    assert 'psycopg2' in install or '-r requirements.txt' in install

def test_nonfinite_daily_loss_config_is_invalid():
    from apex.config.settings import ApexConfig
    from apex.config.validation import validate_config
    from apex.config.settings import ConfigParseError
    with pytest.raises(ConfigParseError):
        ApexConfig.from_env({'AUTO_TRADING_MAX_DAILY_LOSS_PCT':'NaN'})

def test_old_ticker_cache_does_not_claim_live_prices():
    from apex.market import gate_tickers
    with patch.object(gate_tickers,'_price_cache',{'BTCUSDT':{'price':100}}), patch.object(gate_tickers,'_last_price_update',1), patch.object(gate_tickers,'_request_get',side_effect=TimeoutError):
        result=gate_tickers.get_live_prices()
    assert not result or result['BTCUSDT'].get('stale') is True

def test_tp1_state_transition_is_persisted_in_state_mode(tmp_path):
    from test_trade_manager_runtime import _db
    from apex.db.manager_migration import import_legacy_manager
    from apex.manager import engine
    db=_db(tmp_path)
    engine.register_pending_signals(db)
    state=str(tmp_path/'manager-state.db')
    with sqlite3.connect(state) as conn: migrate_state(conn)
    factory=lambda: sqlite3.connect(state)
    import_legacy_manager(lambda:sqlite3.connect(db),factory,refresh=True)
    engine.configure_manager_state(factory)
    try:
        row=engine.load_state(1,db)
        engine.persist_review(row,111,['TP1_HIT'],{'management_candle_id':'1','new_management_candle':True},
            {'action':'MOVE_STOP_TO_BREAKEVEN','confidence':.9},db)
        updated=engine.load_state(1,db)
        assert updated['manager_state']=='TP1_REACHED'
    finally:
        engine.configure_manager_state(None)

def test_database_driver_failure_is_classified_as_local_dependency_error():
    spec=importlib.util.spec_from_file_location('apex_audit_release', 'scripts/render_controlled_release.py')
    release=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(release)
    client=Mock()
    client._request.return_value=[{'envVar':{'key':'DATABASE_URL','value':'postgresql://example.invalid/db'}}]
    real_import=builtins.__import__
    def imported(name,*args,**kwargs):
        if name=='psycopg2': raise ModuleNotFoundError('psycopg2 missing')
        return real_import(name,*args,**kwargs)
    with patch('builtins.__import__',side_effect=imported):
        result=release.print_database_diagnostics(client)
    assert result['status']!='unavailable'

def test_same_side_exposure_normalizes_direction_aliases(state_factory):
    repository=ExecutionRepository(state_factory)
    repository.register({'signal_id':2,'mode':'live','symbol':'ETHUSDT',
        'direction':'LONG','status':'PROTECTED','entry':100,'sl':95,'tp1':110,
        'quantity':1,'risk_usdt':5})
    exposure=repository.risk_exposure('BULLISH',('PROTECTED',))
    assert exposure['same_side_risk_usdt']==5

def test_manager_actions_require_single_instance_ownership():
    repository=Mock()
    repository.get.return_value={'manager_state':'TP1_REACHED','partial_exit_done':0,'initial_entry':100}
    client=Mock()
    client.symbol_rules.return_value=RULES
    client.open_positions.return_value=[{'symbol':'BTCUSDT','positionAmt':'1'}]
    client.emergency_close.return_value={'orderId':'exit-1','status':'FILLED','executedQty':'.5'}
    snapshot={'mode':'live','status':'PROTECTED','symbol':'BTCUSDT','direction':'BULLISH','quantity':1}
    with patch('apex.app.runtime.runtime_supervisor',Mock(owns_exchange_management=False)), patch.object(orders,'_manager_state_repository',return_value=repository), patch.object(orders,'_MANAGER_TRANSITION_VALIDATOR',Mock(return_value=(True,'TP1_REACHED'))), patch.object(orders,'cached_execution_snapshot',return_value=snapshot), patch.object(orders,'_claim_manager_action',return_value=True), patch.object(orders,'_finish_manager_action'), patch.object(orders,'_MANAGER_CONFIRM_CALLBACK',None):
        result=orders.execute_manager_review({'signal_id':1,'review':{'action':'PARTIAL_EXIT','confidence':.9},'facts':{'management_candle_id':'1'}},config=live_config(),client=client)
    assert result['status']!='EXECUTED'

def test_gate_overlapping_update_is_valid():
    from apex.market.microstructure import GateOrderBook
    book=GateOrderBook('BTCUSDT')
    assert book.snapshot([[100,1]],[[101,1]],100)
    assert book.delta([[100,2]],[],99,102)

def test_closed_binance_position_releases_execution_slot_while_signal_pending(state_factory):
    from apex.execution.ledger import actual_result, ExecutionSnapshot
    executions=ExecutionRepository(state_factory)
    executions.update_exchange_state(1,status='PROTECTED',stop_order_id='stop-1')
    ledger=ExecutionLedgerRepository(state_factory)
    for kind,oid,side,price,tid in [('ENTRY','entry-1','BUY','100',1),('SL','stop-1','SELL','98',2)]:
        ledger.register_order({'signal_id':1,'symbol':'BTCUSDT','kind':kind,'remote_id':oid,'is_algo':False,'expected_side':side,'standard_id':oid})
        ledger.record_fills(1,oid,kind,[{'symbol':'BTCUSDT','orderId':oid,'id':tid,'side':side,'qty':'1','price':price,'commission':'.04','commissionAsset':'USDT','time':tid*1000}])
    assert actual_result('unused',ExecutionSnapshot.from_mapping(executions.get(1)),authoritative_snapshot=True)['status']=='CLOSED'
    row=executions.get(1)
    row['signal_result']='pending'
    client=Mock()
    client.open_positions.return_value=[]
    with patch.object(orders,'_live_reconcile_rows',return_value=[row]), patch.object(orders,'_reconciliation_required_signal_ids',return_value=set()):
        orders._reconcile_live_executions_unlocked(config=live_config(),client=client,db_path='unused')
    assert executions.get(1)['status']!='PROTECTED'

def test_definitive_binance_rejection_does_not_reserve_submitting_forever(state_factory,tmp_path):
    import time
    from test_trade_execution import FakeClient,CANDIDATE
    client=FakeClient()
    client.place_limit_entry=Mock(side_effect=orders.BinanceAPIError(400,-1111,'Precision is over maximum'))
    result=orders.execute_approved_candidate(dict(CANDIDATE),2,db_path=str(tmp_path/'brain.db'),config=live_config(),client=client,
        quote_provider=lambda _:{'source':'gate','price':100,'observed_at':time.time()})
    assert result['status']!='SUBMITTING'


def test_emergency_close_lost_ack_is_recovered_without_resubmission(state_factory):
    row = ExecutionRepository(state_factory).get(1)
    client = Mock()
    client.emergency_close.side_effect = TimeoutError('acknowledgement lost')
    with pytest.raises(TimeoutError):
        orders._owned_emergency_close(row, client, 'unused', orders.Decimal('1'))
    client.query_order_by_client_id.return_value = {'orderId': 'exit-recovered', 'status': 'FILLED', 'executedQty': '1'}
    recovered = orders._owned_emergency_close(row, client, 'unused', orders.Decimal('1'))
    assert recovered['orderId'] == 'exit-recovered'
    client.emergency_close.assert_called_once()
    client.query_order_by_client_id.assert_called_once_with('BTCUSDT', 'apex_x_1')
    with state_factory() as conn:
        assert conn.execute("SELECT remote_id FROM execution_orders WHERE kind='CLOSE'").fetchone()[0] == 'exit-recovered'


def test_cleanup_retry_preserves_protection_while_exchange_position_exists(state_factory):
    row = ExecutionRepository(state_factory).get(1)
    row.update(status='CLEANUP_PENDING', signal_result='sl', stop_order_id='stop-1')
    client = Mock()
    client.open_positions.return_value = [{'symbol': 'BTCUSDT', 'positionAmt': '1'}]
    assert orders._cleanup_protective_orders(row, client, 'unused')['status'] == 'AWAITING_BINANCE_CLOSE'
    client.cancel_algo_order.assert_not_called()


def test_daily_income_pagination_does_not_ignore_second_page():
    client = orders.BinanceFuturesClient(live_config(), session=Mock())
    client.income_history = Mock(side_effect=[
        [{'incomeType':'REALIZED_PNL','income':'0','asset':'USDT'}]*1000,
        [{'incomeType':'COMMISSION','income':'-2','asset':'USDT'}]])
    assert client.realized_pnl_since(1) == -2
    assert client.income_history.call_args.kwargs['page'] == 2


def test_daily_income_truncation_fails_closed():
    client = orders.BinanceFuturesClient(live_config(), session=Mock())
    client.income_history = Mock(return_value=[{'incomeType':'REALIZED_PNL','income':'0','asset':'USDT'}]*1000)
    with pytest.raises(RuntimeError, match='pagination limit'):
        client.realized_pnl_since(1)


def test_audit_merge_unions_events_without_overwriting_trading_state(tmp_path):
    from apex.db.audit_db import migrate_audit, merge_audit_snapshot
    local, remote = str(tmp_path/'local.db'), str(tmp_path/'remote.db')
    for path, key in [(local,'local'),(remote,'remote')]:
        with sqlite3.connect(path) as conn:
            migrate_audit(conn)
            conn.execute("INSERT INTO setup_audit_events(event_key,kind,occurred_at,payload_json) VALUES(?,'scan','2026-10-12T01:00:00+00:00','{}')",(key,))
    assert merge_audit_snapshot(local, remote) == 1
    assert merge_audit_snapshot(local, remote) == 0
    with sqlite3.connect(local) as conn:
        assert conn.execute('SELECT COUNT(*) FROM setup_audit_events').fetchone()[0] == 2
    with sqlite3.connect(remote) as conn:
        conn.execute("UPDATE setup_audit_events SET event_key='local',payload_json='[1]'")
    with pytest.raises(RuntimeError, match='immutable event conflict'):
        merge_audit_snapshot(local, remote)
    with sqlite3.connect(local) as conn:
        assert conn.execute("SELECT payload_json FROM setup_audit_events WHERE event_key='local'").fetchone()[0] == '{}'
    with sqlite3.connect(remote) as conn:
        conn.execute('CREATE TABLE executions(id INTEGER)')
    with pytest.raises(RuntimeError, match='non-audit tables'):
        merge_audit_snapshot(local, remote)


def test_large_backup_uses_native_git_before_json_blob_upload(tmp_path):
    from apex.db.backup import BrainPersistence
    path = tmp_path/'large.db.gz'
    with path.open('wb') as stream:
        stream.truncate(8*1024*1024)
    manager = BrainPersistence(str(tmp_path/'brain.db'), 'owner/repository', 'fake', session=Mock())
    with patch('apex.db.git_snapshot.push_snapshot', return_value='native') as upload:
        assert manager._upload_via_git_database(snapshot_path=str(path), current_blob_sha='base', message='backup') == 'native'
    assert upload.call_args.kwargs['expected_blob_sha'] == 'base'
    manager.session.post.assert_not_called()


def test_nonunit_gate_contract_notional_is_scaled():
    from apex.market.gate_client import GateMarketClient
    from apex.market.live_context import fetch_live_context
    from test_apex_v3_gate_client import FakeGate
    fake = FakeGate()
    def transport(url, params, timeout):
        if url.endswith('/contracts/BTC_USDT'):
            return {'quanto_multiplier': '0.0001'}
        return fake(url, params, timeout)
    context = fetch_live_context(GateMarketClient(get_json=transport, clock=lambda:1100), 'BTCUSDT', as_of=1100)
    assert context.microstructure['cvd_real']['delta_notional'] == pytest.approx(0.01)


def test_worker_background_health_startup_and_final_backup_regressions():
    import subprocess
    import sys
    result = subprocess.run([sys.executable, 'tests/worker_repair_probe.py'],
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, (result.stdout+result.stderr)[-6000:]


def test_scheduler_timeout_retains_write_owner_until_shutdown_drain():
    import asyncio
    from dataclasses import replace
    from apex.app import scheduler
    async def verify():
        complete = asyncio.Event()
        entered = asyncio.Event()
        async def job():
            entered.set()
            await complete.wait()
        definition = replace(scheduler.JOB_BY_ID['execution_reconcile'], timeout_seconds=.01)
        with patch.dict(scheduler.JOB_BY_ID, {'execution_reconcile': definition}), \
             patch.object(scheduler,'runtime_supervisor',Mock()), \
             patch.object(scheduler,'recorder_for',return_value=None), \
             patch.object(scheduler,'report_incident'):
            with pytest.raises(asyncio.TimeoutError):
                await scheduler._guarded('execution_reconcile',job)()
            assert entered.is_set() and scheduler._RUNNING_JOBS
            drain = asyncio.create_task(scheduler.drain_production_jobs())
            await asyncio.sleep(0)
            assert not drain.done()
            complete.set()
            await drain
            assert not scheduler._RUNNING_JOBS
    asyncio.run(verify())


def test_funding_candidates_exclude_unfilled_and_retry_fairly(state_factory):
    executions = ExecutionRepository(state_factory)
    for signal_id in range(2,62):
        executions.register({'signal_id':signal_id,'mode':'live','symbol':'BTCUSDT',
            'direction':'LONG','status':'ENTRY_PENDING','entry':100,'sl':95,'tp1':110,
            'quantity':1,'entry_order_id':f'entry-{signal_id}'})
    ledger = ExecutionLedgerRepository(state_factory)
    for signal_id in (60,61):
        for kind,side,price in [('ENTRY','BUY','100'),('CLOSE','SELL','105')]:
            order_id = f'{kind}-{signal_id}'
            ledger.register_order({'signal_id':signal_id,'symbol':'BTCUSDT','kind':kind,
                'remote_id':order_id,'is_algo':False,'expected_side':side,'standard_id':order_id})
            ledger.record_fills(signal_id,order_id,kind,[{'symbol':'BTCUSDT','id':order_id,
                'orderId':order_id,'side':side,'qty':'1','price':price,'commission':'.04',
                'commissionAsset':'USDT','time':1000 if kind=='ENTRY' else 2000}])
    assert [row['signal_id'] for row in ledger.funding_candidates()] == [60,61]
    ledger.record_funding_attempt(60,100)
    assert ledger.funding_candidates(limit=1)[0]['signal_id'] == 61


def test_stale_audit_backup_adopts_only_verified_event_union(tmp_path):
    import hashlib
    from apex.db.audit_db import migrate_audit
    from apex.db.backup import BrainPersistence
    from test_brain_persistence import _GitHubSession
    local, remote = tmp_path/'apex_audit.db', tmp_path/'remote.db'
    with sqlite3.connect(remote) as conn:
        migrate_audit(conn)
        conn.execute("INSERT INTO setup_audit_events(event_key,kind,occurred_at,payload_json) VALUES('base','scan','2026-10-12T01:00:00+00:00','{}')")
    def sha(data):
        return hashlib.sha1(f'blob {len(data)}\0'.encode()+data).hexdigest()
    content = remote.read_bytes()
    session = _GitHubSession(content, sha=sha(content))
    manager = BrainPersistence(str(local),'owner/repository','fake',remote_name='apex_audit.db',session=session)
    assert manager.restore()['ready']
    with sqlite3.connect(local) as conn:
        conn.execute("INSERT INTO setup_audit_events(event_key,kind,occurred_at,payload_json) VALUES('local','scan','2026-10-12T02:00:00+00:00','{}')")
    with sqlite3.connect(remote) as conn:
        conn.execute("INSERT INTO setup_audit_events(event_key,kind,occurred_at,payload_json) VALUES('remote','scan','2026-10-12T03:00:00+00:00','{}')")
    session.content = remote.read_bytes()
    session.sha = sha(session.content)
    assert manager.backup()['saved']
    with sqlite3.connect(local) as conn:
        assert conn.execute('SELECT COUNT(*) FROM setup_audit_events').fetchone()[0] == 3
    assert session.puts[-1]['sha'] == sha(remote.read_bytes())


def test_raw_order_post_is_not_replayed_after_transport_timeout():
    import requests
    from dataclasses import replace
    session = Mock()
    session.request.side_effect = requests.Timeout('lost acknowledgment')
    client = orders.BinanceFuturesClient(replace(live_config(),retries=3),session=session)
    with pytest.raises(RuntimeError,match='failed safely'):
        client._request('POST','/fapi/v1/order',{'symbol':'BTCUSDT'},signed=True)
    session.request.assert_called_once()


def test_owned_order_is_not_resubmitted_when_recovery_lookup_also_fails():
    from dataclasses import replace
    client = orders.BinanceFuturesClient(replace(live_config(),retries=3),session=Mock())
    client._request = Mock(side_effect=RuntimeError('uncertain submission'))
    client.query_order_by_client_id = Mock(side_effect=TimeoutError('lookup offline'))
    with pytest.raises(RuntimeError,match='ambiguous Binance order submission'):
        client._submit_standard_order({'symbol':'BTCUSDT','newClientOrderId':'owned-1'})
    client._request.assert_called_once()


def test_uncertain_manager_close_recovers_owned_order_and_confirms_only_when_flat(state_factory):
    repository = ExecutionRepository(state_factory)
    repository.claim_action('a'*32,1,'CLOSE',None)
    repository.finish_action('a'*32,'SUBMISSION_UNCONFIRMED')
    client = Mock()
    client.query_order_by_client_id.return_value = {'orderId':'manager-recovered','status':'FILLED','executedQty':'1','origQty':'1'}
    client.open_positions.return_value = [{'symbol':'BTCUSDT','positionAmt':'0.1'}]
    with patch.object(orders,'_MANAGER_CONFIRM_CALLBACK',None):
        assert orders._recover_manager_market_action('unused',client)[0]['status'] == 'SUBMISSION_UNCONFIRMED'
        client.open_positions.return_value = []
        assert orders._recover_manager_market_action('unused',client)[0]['status'] == 'MANAGER_ACTION_RECOVERED'
    assert repository.get(1)['status'] == 'CLEANUP_PENDING'
    assert repository.latest_action(1,('CLOSE',))['status'] == 'EXECUTED'
    client.emergency_close.assert_not_called()
