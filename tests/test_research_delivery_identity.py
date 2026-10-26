"""Idempotent canonical State identity across delivery recovery."""
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest

from apex.db.migrations import MigrationRunner
from apex.db.state_db import migrate_state, STATE_MIGRATIONS
from apex.db.state_signal_persistence import StateSignalPersistence
from apex.db.signal_monitor_cutover import sync_state_monitor_projection
from apex.db.repositories.delivery_outbox import DeliveryOutbox
from apex.db.repositories.signal_lifecycle import SignalLifecycleRepository


@pytest.fixture
def storage(tmp_path):
    legacy_path=str(tmp_path/'legacy.db'); state_path=str(tmp_path/'state.db')
    with sqlite3.connect(legacy_path) as c:
        c.executescript('''CREATE TABLE signals(
            id INTEGER PRIMARY KEY AUTOINCREMENT,symbol TEXT,direction TEXT,
            signal_type TEXT,timeframe TEXT,entry REAL,sl REAL,tp1 REAL,
            tp2 REAL,tp3 REAL,estimated_hours REAL,grade TEXT,confluence REAL,
            regime TEXT,created_at TEXT,result TEXT,closed_at TEXT,
            tp1_hit INTEGER,trailing_sl REAL,best_price REAL);
            CREATE TABLE signal_execution_state(signal_id INTEGER PRIMARY KEY,
            status TEXT,activated_at TEXT,last_checked_at TEXT,closed_at TEXT,cancel_reason TEXT);''')
    with sqlite3.connect(state_path) as c:migrate_state(c)
    lf=lambda:sqlite3.connect(legacy_path,timeout=20)
    sf=lambda:sqlite3.connect(state_path,timeout=20)
    service=StateSignalPersistence(lf,sf,'research-edf')
    box=DeliveryOutbox(state_path)
    candidate=dict(symbol='BTCUSDT',direction='BULLISH',scan_type='FAST',entry=100,sl=95,
                   tp1=110,tp2=120,tp3=130,timeframe='5m',estimated_hours=1,grade='A')
    identity=box.prepare(candidate,'cache','pair:BTCUSDT',1,[])
    arguments=('BTCUSDT','BULLISH','FAST',100,110,120,130,95,'5m',1,'A')
    return lf,sf,service,box,identity,arguments,candidate


def test_same_delivery_recovers_exact_id_after_concurrent_requests(storage):
    lf,sf,service,box,identity,args,candidate=storage
    with ThreadPoolExecutor(max_workers=2) as pool:
        ids=list(pool.map(lambda _:service.save(*args,delivery_id=identity),range(2)))
    assert ids==[1,1]
    with sf() as c:assert c.execute('SELECT COUNT(*) FROM signal_lifecycle').fetchone()[0]==1


def test_projection_failure_commits_link_with_signal(storage):
    lf,sf,service,box,identity,args,candidate=storage
    with patch.object(service,'_project_one',side_effect=sqlite3.OperationalError('injected')):
        with pytest.raises(sqlite3.OperationalError):service.save(*args,delivery_id=identity)
    with sf() as c:
        assert c.execute('SELECT signal_id,delivery_id FROM signal_lifecycle').fetchone()==(1,identity)
    sync_state_monitor_projection(lf,sf)
    restarted=StateSignalPersistence(lf,sf,'research-new-release')
    assert restarted.save(*args,delivery_id=identity)==1


@pytest.mark.parametrize('terminal',['closed','cancelled'])
def test_terminal_signal_never_recreated(storage,terminal):
    lf,sf,service,box,identity,args,candidate=storage
    service.save(*args,delivery_id=identity)
    repo=SignalLifecycleRepository(sf)
    if terminal=='closed':
        assert repo.advance_monitor(1,expected_status='waiting_entry',transition='activate')
        assert repo.advance_monitor(1,expected_status='active',transition='close',result='win')
    else:
        assert repo.advance_monitor(1,expected_status='waiting_entry',transition='cancel',result='cancelled')
    assert service.save(*args,delivery_id=identity)==1
    with sf() as c:
        assert c.execute('SELECT signal_id,status FROM signal_lifecycle').fetchall()==[(1,terminal)]


@pytest.mark.parametrize('index,value',[(0,'ETHUSDT'),(1,'BEARISH'),(3,101),(4,111),(5,121),(6,131),(7,96),(8,'1h')])
def test_intent_geometry_cannot_be_replaced(storage,index,value):
    lf,sf,service,box,identity,args,candidate=storage
    changed=list(args);changed[index]=value
    with pytest.raises(ValueError,match='delivery_candidate_identity_conflict'):
        service.save(*changed,delivery_id=identity)
    with sf() as c:assert c.execute('SELECT COUNT(*) FROM signal_lifecycle').fetchone()[0]==0


@pytest.mark.parametrize('status',['ABORTED','legacy','missing'])
def test_unverifiable_intent_fails_closed(storage,status):
    lf,sf,service,box,identity,args,candidate=storage
    if status=='ABORTED':box.finish(identity,'ABORTED')
    elif status=='legacy':
        with sf() as c:c.execute('UPDATE signal_delivery_outbox SET identity_version=0')
    else:identity='missing'
    with pytest.raises(ValueError,match='delivery_identity_requires_reconciliation'):
        service.save(*args,delivery_id=identity)
    with sf() as c:assert c.execute('SELECT COUNT(*) FROM signal_lifecycle').fetchone()[0]==0


def test_foreign_pending_is_not_assigned_to_delivery(storage):
    lf,sf,service,box,identity,args,candidate=storage
    other=list(args);other[1]='BEARISH';other[3]=200
    assert service.save(*other)==1
    assert service.save(*args,delivery_id=identity) is None
    with sf() as c:assert c.execute('SELECT delivery_id FROM signal_lifecycle').fetchone()==(None,)


def test_same_candidate_with_other_delivery_is_not_duplicate(storage):
    lf,sf,service,box,identity,args,candidate=storage
    assert service.save(*args,delivery_id=identity)==1
    other=box.prepare(candidate,'cache','pair:BTCUSDT',2,[])
    assert service.save(*args,delivery_id=other) is None


def test_existing_canonical_identity_conflict_is_not_reused(storage):
    lf,sf,service,box,identity,args,candidate=storage
    service.save(*args,delivery_id=identity)
    changed=list(args);changed[-1]='changed grade'
    with pytest.raises(ValueError,match='delivery_signal_identity_conflict'):
        service.save(*changed,delivery_id=identity)


def test_migration_preserves_old_outbox_as_unlinked(tmp_path):
    path=str(tmp_path/'state.db')
    with sqlite3.connect(path) as c:
        MigrationRunner(STATE_MIGRATIONS[:-1]).run(c)
        c.execute("INSERT INTO signal_delivery_outbox(delivery_id,cache_key,pair_key,claimed_at,candidate_json,destinations_json) VALUES('old','cache','pair',1,'{}','[]')")
    with sqlite3.connect(path) as c:migrate_state(c)
    assert DeliveryOutbox(path).get('old')['identity_version']==0
