"""Display nominal signal returns without inventing cash PnL or net fills."""
import sqlite3

import pytest

from apex.db.state_db import migrate_state
from apex.domain.ids import derived_id
from apex.ui.telegram.trades import fetch_live_trades, format_trade_view


def row(**overrides):
    return dict(id=479, symbol='GRAMUSDT', direction='BULLISH', grade='ZONE',
                timeframe='4h', entry=1.3231, sl=1.3061, tp1=1.3660,
                tp2=1.3955, tp3=None, lifecycle_status='closed', result='sl',
                _view_kind='analytical', **overrides)


@pytest.mark.parametrize('side,entry,stop,target,outcome,expected', [
    ('BULLISH', 1.3231, 1.3061, 1.3660, 'sl', '-1.28% · -1.00R'),
    ('BULLISH', 100, 95, 110, 'tp1', '+10.00% · +2.00R'),
    ('BEARISH', 100, 105, 90, 'sl', '-5.00% · -1.00R'),
    ('BEARISH', 100, 105, 90, 'tp1', '+10.00% · +2.00R'),
    ('BEARISH', 100, 105, 80, 'tp2', '+20.00% · +4.00R'),
    ('BULLISH', 100, 95, 120, 'tp3', '+20.00% · +4.00R'),
])
def test_nominal_barrier_returns_are_directional(side, entry, stop, target, outcome, expected):
    r = row(); r.update(direction=side, entry=entry, sl=stop, result=outcome)
    if outcome != 'sl': r[outcome] = target
    text = format_trade_view('closed', [r])
    assert f'Расчёт до уровня {outcome.upper()}' in text
    assert expected in text
    assert 'без плеча, комиссий, funding и проскальзывания' in text
    assert 'Денежный PnL счёта не рассчитан' in text


@pytest.mark.parametrize('patch', [
    {'entry': 0}, {'entry': float('nan')}, {'sl': 1.3231}, {'sl': float('inf')},
    {'direction': 'UNKNOWN'}, {'result': 'ambiguous'}, {'result': 'tp3'},
    {'sl': 1.4}, {'result': 'tp1', 'tp1': 1.0},
])
def test_incomplete_or_invalid_evidence_does_not_invent_pnl(patch):
    r = row(); r.update(patch)
    text = format_trade_view('closed', [r])
    assert 'Расчёт результата: недостаточно данных' in text
    assert '\n   Расчёт до уровня' not in text


@pytest.mark.parametrize('status', ['waiting_entry', 'active', 'cancelled'])
def test_unfilled_pending_and_cancelled_signals_have_no_realized_return(status):
    r = row(); r.update(lifecycle_status=status, result='cancelled' if status == 'cancelled' else 'pending')
    text = format_trade_view('signals', [r])
    assert '\n   Расчёт до уровня' not in text
    assert 'Результат Manager:' not in text


def test_trailing_tp1_label_is_never_claimed_as_actual_exit():
    r = row(); r.update(result='tp1', tp1_hit=1, trailing_sl=1.34)
    text = format_trade_view('closed', [r])
    assert 'Расчёт до уровня TP1' in text
    assert 'при трейлинге это не цена выхода и не итоговый PnL' in text


def test_real_position_uses_recorded_manager_values_not_target_geometry(tmp_path):
    path = tmp_path/'state.db'
    entity = derived_id('signal', 'pnl-test', 1)
    with sqlite3.connect(path) as c:
        migrate_state(c)
        c.execute("""INSERT INTO executions(signal_entity_id, signal_id, mode, symbol,
            direction,status,position_id,plan_json,plan_hash)
            VALUES(?,1,'live','BTCUSDT','BEARISH','CLOSED',?,'{}','test')""",
            (entity, derived_id('position', 'pnl-test', 1)))
        c.execute("""INSERT INTO manager_positions(signal_entity_id,signal_id,symbol,strategy,
            direction,management_tf,initial_entry,initial_sl,initial_tp1,manager_version,
            snapshot_json,snapshot_hash,status,close_result,realized_pct,realized_r)
            VALUES(?,1,'BTCUSDT','MTF','BEARISH','1h',100,105,90,2,'{}','test',
            'CLOSED','manual',3.25,0.65)""", (entity,))
    factory = lambda: sqlite3.connect(f'file:{path}?mode=ro', uri=True)
    rows = fetch_live_trades('closed', 12, factory)
    assert rows[0]['realized_pct'] == 3.25
    assert rows[0]['realized_r'] == .65
    text = format_trade_view('closed', rows)
    assert 'Результат Manager: <b>+3.25% · +0.65R</b>' in text
    assert '\n   Расчёт до уровня' not in text
    assert 'не подтверждает чистый PnL' in text


def test_live_result_with_no_recorded_metrics_does_not_fall_back_to_analytical_stop():
    r = row(); r.update(_view_kind='live', realized_pct=None, realized_r=None)
    text = format_trade_view('stop', [r])
    assert 'нет подтверждённого расчёта' in text
    assert '-1.00R' not in text
    assert '\n   Расчёт до уровня' not in text


def test_telegram_message_with_many_results_fits_limit():
    text = format_trade_view('closed', [dict(row(), id=i) for i in range(30)])
    assert len(text) <= 4096
    assert text.count('<b>') == text.count('</b>')
    assert text.count('<i>') == text.count('</i>')
