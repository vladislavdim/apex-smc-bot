"""R0014 independent, preserved-window contracts for actual ZONE method."""
import copy
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
import pytest
from apex.strategies.native import SnapshotContext
from apex.strategies.parity_corpus import load_corpus_directory
from tests.zone_freshness_probe import run, fixture, END

@pytest.mark.parametrize('direction',['BULLISH','BEARISH'])
@pytest.mark.parametrize('mode',['clean','preformation_touches','old_zone_three_retests','late_three_retests','spanning_three_retests'])
def test_ob_freshness_preserves_window_and_rejects_only_later_tests(direction,mode):
    row=run(mode,direction)
    # The existing detector excludes the latest three setup bars. Keep that
    # policy; the independent R0013 all-prior-bars oracle remains archived.
    expected_block=mode in ('old_zone_three_retests','spanning_three_retests')
    assert (row['native']['_test_count']>2)==expected_block,row
    assert (row['result'] is None)==expected_block,row
    if expected_block:
        assert row['native']['stop_code']=='ZONE_DETECT_ZONE_SETUP_R7868'
    else:
        assert row['result']['_pending_ltf']

def fvg_fixture(mode,direction):
    rows=fixture('clean')
    for i in (20,24,28):rows[i]['low']=102.5 if mode=='preformation_touches' else 104.
    for i in range(40,51):rows[i].update(open=103.2,close=104.,low=102.5,high=104.2)
    rows[45].update(open=101.2,close=101.4,low=101.,high=102.)
    rows[46].update(open=101.4,close=104.,low=101.,high=105.)
    rows[47].update(open=103.2,close=104.,low=103.,high=104.2)
    rows[50].update(open=102.2,close=103.,low=101.9,high=103.2)
    if mode in ('old_three_retests','old_spanning_retests'):
        for i in range(37,50):rows[i].update(open=103.2,close=104.,low=103.1,high=104.2)
        rows[37].update(open=101.2,close=101.4,low=101.,high=102.)
        rows[38].update(open=101.4,close=104.,low=101.,high=105.)
        rows[39].update(open=103.2,close=104.,low=103.,high=104.2)
        for i in (43,44,45):
            rows[i].update(low=101. if mode=='old_spanning_retests' else 102.8)
    if direction=='BEARISH':
        for r in rows:r.update(open=220-r['open'],close=220-r['close'],high=220-r['low'],low=220-r['high'])
    return rows

def evaluate_fvg(mode,direction):
    rows=fvg_fixture(mode,direction);original=copy.deepcopy(rows)
    base=load_corpus_directory(Path('tests/fixtures/apex_v3_strategy_parity'))[0].snapshot
    snapshot=replace(base,symbol='BTCUSDT',as_of=datetime.fromtimestamp(END,timezone.utc),candles={'4h':rows},market_context={},derivatives_context={})
    observed={}
    def trace(frame,event,arg):
        if frame.f_code.co_name=='detect_zone_setup' and event=='return':
            observed.update({k:copy.deepcopy(frame.f_locals[k]) for k in ('ob','fvg','zone_type','_test_count') if k in frame.f_locals})
        return trace
    with patch('socket.socket.connect',side_effect=AssertionError('network forbidden')) as network,patch('sqlite3.connect',side_effect=AssertionError('DB forbidden')) as db:
        sys.settrace(trace)
        try:result=SnapshotContext(snapshot).detect_zone_setup('BTCUSDT',passive_watch=True)
        finally:sys.settrace(None)
        network.assert_not_called();db.assert_not_called()
    assert rows==original
    return observed,result

@pytest.mark.parametrize('direction',['BULLISH','BEARISH'])
@pytest.mark.parametrize('mode',['clean','preformation_touches'])
def test_fvg_excludes_history_before_right_candle_close(direction,mode):
    observed,result=evaluate_fvg(mode,direction)
    assert observed['ob'] is None
    assert observed['zone_type']=='FVG'
    assert observed['fvg']['index']==46
    # Right candle47 is formation, no eligible test in [48,48).
    assert observed['_test_count']==0,observed
    assert result and result['_pending_ltf'],observed

@pytest.mark.parametrize('direction',['BULLISH','BEARISH'])
@pytest.mark.parametrize('mode',['old_three_retests','old_spanning_retests'])
def test_old_fvg_rejects_three_post_birth_intersections(direction,mode):
    observed,result=evaluate_fvg(mode,direction)
    assert observed['ob'] is None and observed['zone_type']=='FVG'
    assert observed['fvg']['index']==38
    assert observed['_test_count']>=3,observed
    assert result is None
