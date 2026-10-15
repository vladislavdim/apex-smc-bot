"""Run the actual Spring body with deterministic phase/provider fixtures."""
import os
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
with tempfile.TemporaryDirectory() as directory:
    os.environ.update(
        TELEGRAM_TOKEN='123456:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', GROQ_API_KEY='test',
        APEX_COMPAT_DB_PATH=directory+'/brain.db', APEX_STATE_DB_PATH=directory+'/state.db',
        APEX_MEMORY_DB_PATH=directory+'/memory.db', APEX_TELEGRAM_ONLY='true',
    )
    from apex.compatibility import legacy_market_runtime as market
    candles = [dict(open=100., high=110., low=99., close=100., volume=100.) for _ in range(60)]
    candles[-35]['high'] = 150.
    for row in candles[-7:]:
        row['volume'] = 50.
    phases = {'SC': {'price': 99., 'vol': 600.}, 'AR': {'price': 110.},
              'ST': {'price': 99.5}, 'Spring': {'found': True, 'price': 98.},
              'SOS': {'found': True, 'price': 110.}}
    settings = dict(
        get_candles=lambda *a: candles, get_confirmed_candles=lambda x: x,
        average_true_range=lambda *a: 1.,
        _find_wyckoff_phases_accumulation=lambda *a: phases,
        check_entry_timing=lambda *a: {'valid': True}, find_ob=lambda *a: None,
        find_fvg=lambda *a: None, get_fear_greed=lambda: None,
        get_funding_rate=lambda *a: 0., smc_on_tf=lambda *a: 'BULLISH',
        legacy_strategy_groq_enabled=lambda: False, smart_round=lambda x: x,
    )
    with patch.multiple(market, **settings):
        result = market.detect_wyckoff_spring.__wrapped__('BTCUSDT')
        assert result is not None
        assert result['entry'] == 100. and result['sl'] == 97.75 and result['tp'] == 110.
        assert result['rr'] == 4.44 and result['spring'] and result['sos']
        phases['SOS']['found'] = False
        captured = {}
        def fail(code, label, values, *args):
            captured.update(code=code, signals=values.get('signals', []))
            return None
        with patch.object(market, '_audit_fail', side_effect=fail):
            assert market.detect_wyckoff_spring.__wrapped__('BTCUSDT') is None
        assert captured['code'] == 'WYCKOFF_DETECT_WYCKOFF_SPRING_R8470'
        # 600 / average(60 candles) = 6.37, not 600 / sum / 60 = 0.00177.
        assert any('SC (Selling Climax)' in text and 'x6.4' in text for text in captured['signals'])
