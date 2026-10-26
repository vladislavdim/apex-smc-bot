import sqlite3
from pathlib import Path
import pytest
from apex.db.backup import BrainPersistence
from apex.db.state_db import migrate_state
from tests.state_restore_missing_probe import run,Remote

@pytest.mark.parametrize('mode',['deleted_cold','first_use_cold','deleted_warm'])
def test_startup_never_treats_404_as_authorization_to_create_critical_state(mode):
    r=run(mode)
    assert r['ready'] is False and r['startup_allowed'] is False
    assert r['reason']=='STATE_REMOTE_MISSING'
    assert r['remote_puts']==0 and r['remote_was_recreated'] is False
    assert r['claim_admitted'] is None
    if mode=='deleted_warm':assert r['claim_keys']==['BTCUSDT:BULLISH:signal','pair:BTCUSDT']

@pytest.mark.parametrize('mode',['metadata_403','metadata_500'])
def test_nonmissing_errors_do_not_create_state(mode):
    r=run(mode)
    assert r['ready'] is False and r['remote_puts']==0
    assert r['startup_allowed'] is False

def test_valid_current_state_still_starts_with_dedup_claim():
    r=run('current_valid')
    assert r['ready'] is True and r['startup_allowed'] is True
    assert r['claim_admitted'] is False and r['remote_puts']==0

def test_explicit_first_provisioning_api_is_not_removed(tmp_path):
    p=str(tmp_path/'fresh.db')
    with sqlite3.connect(p) as c:migrate_state(c)
    remote=Remote('first_use_cold',b'')
    manager=BrainPersistence(p,'research/offline','dummy-offline-token','backups',remote_name='apex_state.db',session=remote)
    result=manager.initialize('explicit_research_fixture_initial_provisioning')
    assert result['ready'] is True and result['status']=='initialized'
    assert remote.puts==1

def test_old_valid_head_remains_a_recency_evidence_gap():
    # Diagnostic control, not a claimed fix: no trusted external high-water mark.
    r=run('old_valid_head')
    assert r['ready'] is True and r['generation']==40
    assert r['claim_admitted'] is True
