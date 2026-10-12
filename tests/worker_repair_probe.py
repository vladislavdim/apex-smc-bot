import os
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import asyncio
import tempfile
from unittest.mock import Mock, AsyncMock, patch
with tempfile.TemporaryDirectory() as directory:
    os.environ.update(TELEGRAM_TOKEN='123456:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',GROQ_API_KEY='test',
        APEX_COMPAT_DB_PATH=directory+'/brain.db',APEX_STATE_DB_PATH=directory+'/state.db',APEX_MEMORY_DB_PATH=directory+'/memory.db')
    from apex.app import worker, scheduler
    from apex.execution.orders import ExecutionConfig
    async def verify():
        worker._auto_trade_reconcile_task=None
        runtime=Mock()
        with patch.object(worker,'_TRADE_EXECUTION_OK',True), patch.object(worker,'_reconcile_live_executions',side_effect=RuntimeError('forced reconciliation failure')), patch.object(scheduler,'runtime_supervisor',runtime), patch.object(scheduler,'recorder_for',return_value=None), patch.object(scheduler,'recover_incident'):
            try:
                await scheduler._guarded('execution_reconcile',worker.auto_trade_reconcile_job)()
            except RuntimeError:
                pass
            else:
                raise AssertionError('background failure was swallowed')
            assert runtime.mark_component.call_args.args[1].value == 'DEGRADED'
        runtime=Mock()
        live=ExecutionConfig(enabled=True,mode='live',api_key='key',api_secret='test',live_confirmation='ENABLE_LIVE_BINANCE_FUTURES')
        client=Mock()
        client.is_one_way_mode.return_value=True
        with patch.object(worker,'_V3_RUNTIME',runtime), patch.object(worker,'_TRADE_EXECUTION_OK',True), patch.object(worker,'_BinanceFuturesClient',return_value=client), patch.object(worker,'_execution_status',return_value={'account':{'available':True,'wallet_balance':1000}}), patch.object(worker,'_reconcile_live_executions',return_value=[{'signal_id':1,'status':'RECONCILE_ERROR'}]), patch.object(worker,'_v3_sync_live_learning',return_value={}), patch.object(worker._ExecutionConfig,'from_env',return_value=live), patch.object(worker,'_reconcile_manager_states_from_signals',return_value=0), patch.object(worker,'_register_pending_manager_signals',return_value=0), patch.object(worker,'_load_active_manager_states',return_value=[]), patch.object(worker,'_v3_probe_gate_freshness',AsyncMock()):
            result=await worker._v3_startup_reconcile_and_market_check()
            calls = [c for c in runtime.mark_component.call_args_list if c.args[0]=='binance_reconciliation']
            assert calls and calls[-1].args[1].value != 'READY'
        stores=[]
        original=worker._v3_maintenance_and_backup
        async def store_backup(reason,*,store='all'):
            stores.append(store)
            if store=='state': raise RuntimeError('forced state upload failure')
            return {'saved':True}
        with patch.object(worker,'_v3_maintenance_and_backup',store_backup):
            result = await original('render_sigterm',store='all')
        assert stores == ['state','memory','brain','audit']
        assert result['state']['status'] == 'backup_failed'
        assert result['audit']['saved'] is True
    asyncio.run(verify())
