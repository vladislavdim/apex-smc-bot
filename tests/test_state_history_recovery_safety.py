import pytest
from tests.state_restore_history_probe import run

@pytest.mark.parametrize('mode',['state_history','state_wrapper','state_recheckpoint'])
def test_older_critical_state_cannot_reopen_delivery_admission(mode):
    r=run(mode)
    assert r['integrity']=='ok'
    assert r['recovered_from']=='older-commit'
    assert r['ready'] is False
    assert r['startup_allowed'] is False
    assert r['claim_admitted'] is None
    assert r['reason']=='STATE_HISTORY_RECOVERY'
    if mode=='state_recheckpoint':
        assert r['backup_status']=='blocked_unrestored'
        assert r['remote_puts']==0

def test_current_state_preserves_existing_claims():
    r=run('state_latest')
    assert r['ready'] is True and r['startup_allowed'] is True
    assert r['claim_admitted'] is False
    assert r['claim_keys']==['BTCUSDT:BULLISH:signal','pair:BTCUSDT']

@pytest.mark.parametrize('mode',['brain_history','memory_history'])
def test_noncritical_history_restore_policy_is_preserved(mode):
    r=run(mode)
    assert r['ready'] is True and r['recovered_from']=='older-commit'
