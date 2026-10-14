"""Exercise the complete accepted SWING path in an isolated real runtime."""
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
with tempfile.TemporaryDirectory() as directory:
    os.environ.update(TELEGRAM_TOKEN='123456:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', GROQ_API_KEY='test',
        APEX_COMPAT_DB_PATH=directory+'/brain.db', APEX_STATE_DB_PATH=directory+'/state.db',
        APEX_MEMORY_DB_PATH=directory+'/memory.db', APEX_TELEGRAM_ONLY='true')
    from apex.compatibility import legacy_market_runtime as market
    candles = [{'open':108.,'high':109.,'low':105.,'close':107.,'volume':100.} for _ in range(30)]
    candles[-1] = {'open':100.,'high':103.,'low':99.,'close':102.,'volume':200.}
    ltf = {key:True for key in ('data_ok','structure_ok','zone_ok','retest_ok','displacement_ok',
        'volume_ok','chase_ok','target_ok','ready')}
    ltf.update(entry=102.,sl=100., structure_event={'type':'BOS'},zone_type='OB')
    settings = dict(get_candles=Mock(return_value=candles),get_confirmed_candles=lambda x:x,
        average_true_range=lambda *a:2., get_adaptive_params=lambda *a:{'volatility_factor':1},
        find_swings=lambda *a,**k:([(5,120.),(10,130.)],[(5,100.),(10,101.)]),
        smart_round=lambda x:x, find_equal_highs_lows=lambda *a,**k:(None,None),
        get_bos_choch_event=lambda *a,**k:{'type':'BOS'}, _SMC_ENGINE_OK=False,
        calculate_cvd=lambda *a:{'signal':'BULLISH'},find_fvg=lambda *a:None,
        _swing_build_ltf_entry=lambda *a:dict(ltf),check_opposing_ob=lambda c,d,e,tp:tp,
        smc_on_tf=lambda *a:'BULLISH',get_funding_rate=lambda *a:0.,get_fear_greed=lambda:None,
        get_market_regime=lambda *a:{'mode':'trend'}, legacy_strategy_groq_enabled=lambda:False)
    for ob_result in ({'bottom':99.,'top':100.}, RuntimeError('optional provider offline')):
        ob = Mock(side_effect=[None,ob_result])
        with patch.multiple(market,**settings), patch.object(market,'find_ob',ob):
            result=market.detect_swing_setup.__wrapped__('BTCUSDT')
        assert result is not None, 'valid SWING disappeared before the final return'
        assert result['entry']==102. and result['sl']==100. and result['rr']==9.
        assert result['ob']==(None if isinstance(ob_result,Exception) else ob_result)
    ltf['sl']=90.
    with patch.multiple(market,**settings),patch.object(market,'find_ob',return_value=None):
        assert market.detect_swing_setup.__wrapped__('BTCUSDT') is None, 'wide structural stop must still reject'
