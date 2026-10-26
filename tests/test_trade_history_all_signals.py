"""All delivered signals belong in history; distinguish analytical/live outcomes."""
import sqlite3
import pytest
from apex.db.state_db import migrate_state
from apex.domain.ids import derived_id
from apex.ui.telegram.trades import fetch_live_trades,format_trade_view

@pytest.fixture
def store(tmp_path):
 p=tmp_path/'state.db'
 with sqlite3.connect(p) as c:migrate_state(c)
 return p,lambda:sqlite3.connect(f'file:{p}?mode=ro',uri=True)

def signal(p,i=1,status='closed',result='sl',execution=None,manager_result=None,confirmed=False):
 entity=derived_id('signal','history',i)
 with sqlite3.connect(p) as c:
  c.execute("""INSERT INTO signal_lifecycle(signal_entity_id,signal_id,status,result,symbol,direction,signal_type,timeframe,entry,sl,tp1,tp2,tp3,ownership,source,created_at,closed_at) VALUES(?,?,?,?,?,'BULLISH','ZONE','4h',100,95,110,120,130,'state','state','2026-10-26 08:00:00',?)""",(entity,i,status,result,f'PAIR{i}USDT','2026-10-26 09:00:00' if status in ('closed','cancelled') else None))
  if execution:
   c.execute("""INSERT INTO executions(signal_entity_id,signal_id,mode,symbol,direction,status,position_id,plan_json,plan_hash) VALUES(?,?,'live',?,'BULLISH',?,?,'{}','test')""",(entity,i,f'PAIR{i}USDT',execution,derived_id('position','history',i) if confirmed else None))
  if manager_result:
   c.execute("""INSERT INTO manager_positions(signal_entity_id,signal_id,symbol,strategy,direction,management_tf,initial_entry,initial_sl,initial_tp1,manager_version,snapshot_json,snapshot_hash,status,close_result,closed_at) VALUES(?,?,?,'ZONE','BULLISH','4h',100,95,110,2,'{}','test','CLOSED',?,'2026-10-26 09:00:00')""",(entity,i,f'PAIR{i}USDT',manager_result))

@pytest.mark.parametrize('result',['sl','tp1','tp2','tp3'])
@pytest.mark.parametrize('execution',[None,'DISABLED'])
def test_analytical_close_is_in_same_outcome_tab_with_explicit_label(store,result,execution):
 p,f=store;signal(p,result=result,execution=execution)
 category='stop' if result=='sl' else 'take'
 rows=fetch_live_trades(category,12,f)
 assert [r['id'] for r in rows]==[1]
 assert rows[0]['_view_kind']=='analytical' and rows[0]['result']==result
 assert [r['id'] for r in fetch_live_trades('closed',12,f)]==[1]
 text=format_trade_view(category,rows)
 assert 'Аналитический сигнал' in text and result.upper() in text
 assert 'не означает исполненную сделку' in text

@pytest.mark.parametrize('status',['waiting_entry','active','cancelled'])
def test_unfilled_or_cancelled_signal_is_not_invented_as_stop_or_take(store,status):
 p,f=store;signal(p,status=status,result='cancelled' if status=='cancelled' else 'pending')
 cat='closed' if status=='cancelled' else 'active'
 rows=fetch_live_trades(cat,12,f);assert [r['id'] for r in rows]==[1]
 assert fetch_live_trades('take',12,f)==[] and fetch_live_trades('stop',12,f)==[]
 text=format_trade_view(cat,rows)
 if status=='waiting_entry':assert 'ждёт входа' in text
 elif status=='active':assert 'аналитически активен' in text
 else:assert 'отменена' in text
 assert 'в позиции' not in text

def test_real_manager_result_wins_over_conflicting_analytical_result_and_is_not_duplicated(store):
 p,f=store;signal(p,result='tp2',execution='CLOSED',manager_result='sl',confirmed=True)
 rows=fetch_live_trades('stop',12,f)
 assert len(rows)==1 and rows[0]['result']=='sl' and rows[0]['_view_kind']=='live'
 assert fetch_live_trades('take',12,f)==[]
 assert len(fetch_live_trades('closed',12,f))==1 and fetch_live_trades('signals',12,f)==[]
 assert 'Binance' in format_trade_view('stop',rows)

def test_filter_is_applied_before_limit_and_mixed_history_has_global_limit(store):
 p,f=store;signal(p)
 for i in range(2,18):signal(p,i,status='waiting_entry',result='pending')
 assert [r['id'] for r in fetch_live_trades('stop',1,f)]==[1]
 assert len(fetch_live_trades('active',2,f))==2
 with sqlite3.connect(p) as c:before=c.execute('SELECT status,result FROM signal_lifecycle ORDER BY signal_id').fetchall()
 fetch_live_trades('closed',30,f)
 with sqlite3.connect(p) as c:assert before==c.execute('SELECT status,result FROM signal_lifecycle ORDER BY signal_id').fetchall()

def test_missing_manager_row_cannot_hide_delivered_signal_with_position_reference(store):
 p,f=store;signal(p,execution='PROTECTED',confirmed=True)
 rows=fetch_live_trades('closed',12,f)
 assert len(rows)==1 and rows[0]['_view_kind']=='analytical'
 text=format_trade_view('closed',rows)
 assert 'Позиция Binance подтверждена' in text and 'Аналитический сигнал' in text
