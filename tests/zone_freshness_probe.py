"""R0013 actual ZONE detector, independent post-formation touch oracle."""
import copy
import hashlib
import json
import math
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from apex.strategies.parity_corpus import load_corpus_directory
from apex.strategies.native import SnapshotContext
from apex.market.legacy_zones import find_ob

ROOT = Path(__file__).resolve().parent
END = 1792886400

def fixture(mode, direction='BULLISH'):
    rows=[dict(open=110.,close=110.2,high=112.,low=109.,volume=100.) for _ in range(51)]
    rows[10].update(high=120.)
    for i in (20,24,28):
        rows[i].update(low=101. if mode=='preformation_touches' else 103.)
    rows[35].update(open=110.,close=118.,low=109.8,high=118.2)
    rows[47].update(open=102.,close=100.,low=99.8,high=102.2)
    rows[48].update(open=100.,close=106.,low=99.9,high=106.2)
    rows[49].update(open=106.,close=107.,low=105.9,high=107.2)
    rows[50].update(open=100.,close=101.,low=99.5,high=101.2,volume=200.)
    if mode in ('old_zone_three_retests','late_three_retests','spanning_three_retests'):
        rows[47]=dict(open=110.,close=110.2,high=112.,low=109.,volume=100.)
        rows[40].update(open=102.,close=100.,low=99.8,high=102.2)
        rows[41].update(open=100.,close=106.,low=99.9,high=106.2)
        for i in (48,49):
            rows[i].update(open=110.,close=110.2,high=112.,low=109.)
        touched=(47,48,49) if mode=='late_three_retests' else (43,44,45)
        for i in touched:
            rows[i].update(open=100.2,close=101.,low=99. if mode=='spanning_three_retests' else 100.1,
                           high=103. if mode=='spanning_three_retests' else 101.5)
    for i,r in enumerate(rows):
        r.update(open_time=END-(51-i)*14400,close_time=END-(50-i)*14400,is_closed=True)
    if direction=='BEARISH':
        for r in rows:
            r.update(open=220-r['open'],close=220-r['close'],high=220-r['low'],low=220-r['high'])
    return rows

def run(mode,direction='BULLISH'):
    rows=fixture(mode,direction)
    for r in rows:
        assert all(math.isfinite(r[k]) for k in ('open','close','high','low','volume'))
        assert 0<r['low']<=min(r['open'],r['close'])<=max(r['open'],r['close'])<=r['high']
        assert r['is_closed'] and r['close_time']<=END
    original=copy.deepcopy(rows)
    base=load_corpus_directory(Path('tests/fixtures/apex_v3_strategy_parity'))[0].snapshot
    snapshot=replace(base,symbol='BTCUSDT',as_of=datetime.fromtimestamp(END,timezone.utc),
        candles={'4h':rows},market_context={},derivatives_context={})
    observed={}
    def trace(frame,event,arg):
        if frame.f_code.co_name=='audit_fail' and event=='call':
            observed['stop_code']=frame.f_locals.get('code')
        if frame.f_code.co_name=='detect_zone_setup' and event=='return':
            names=('ob','fvg','zone_level','zone_type','_test_count','atr','_vf_zone','_strong_move','direction')
            observed.update({k:copy.deepcopy(frame.f_locals[k]) for k in names if k in frame.f_locals})
        return trace
    with patch('socket.socket.connect',side_effect=AssertionError('network forbidden')) as network,patch('sqlite3.connect',side_effect=AssertionError('database forbidden')) as database:
        sys.settrace(trace)
        try: result=SnapshotContext(snapshot).detect_zone_setup('BTCUSDT',passive_watch=True)
        finally: sys.settrace(None)
        network.assert_not_called()
        database.assert_not_called()
    assert rows==original
    ob=find_ob(rows,direction)
    assert ob and ob['index']==(40 if mode in ('old_zone_three_retests','late_three_retests','spanning_three_retests') else 47)
    # Origin and next bar are formation, not later revisits. Count later
    # overlaps before the current decision/rejection candle separately.
    post=[i for i in range(ob['index']+2,len(rows)-1) if rows[i]['low']<=ob['top'] and rows[i]['high']>=ob['bottom']]
    pre=[i for i in range(0,ob['index']) if rows[i]['low']<=ob['top'] and rows[i]['high']>=ob['bottom']]
    return dict(mode=mode,direction=direction,native=observed,prior_to_origin=pre,post_formation_prior_tests=post,
                result=result,expected_fresh=len(post)<=2)
