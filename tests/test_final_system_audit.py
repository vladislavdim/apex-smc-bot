"""Adversarial boundaries exercised independently of strategy parity fixtures."""
import ast
import asyncio
import logging
import hashlib
import json
from pathlib import Path
import sqlite3
import time
from types import SimpleNamespace as NS
from unittest.mock import Mock, AsyncMock

import pytest

from apex.db.repositories.scan_candidates import ScanCandidates
from apex.market.candles import confirmed_candles
from apex.market.freshness import assess_candles


def test_identical_candidates_from_two_strategies_survive_independently(tmp_path):
    factory = lambda: sqlite3.connect(tmp_path / 'state.db')
    item = dict(symbol='BTCUSDT', direction='BULLISH', entry=100, sl=95, tp=110)
    zone, swing = ScanCandidates(factory, 'ZONE'), ScanCandidates(factory, 'SWING')
    zone.append(item)
    swing.append(item)
    assert len(ScanCandidates(factory, 'ZONE')) == 1
    assert len(ScanCandidates(factory, 'SWING')) == 1
    zone.retire(item)
    assert len(ScanCandidates(factory, 'SWING')) == 1


def test_invalid_explicit_close_cannot_admit_a_future_bar():
    row = dict(open_time=3600, close_time=0, open=100, high=101, low=99, close=100, volume=1)
    assert not confirmed_candles([row], '1h', as_of=1800)


def test_historical_epoch_zero_is_not_replaced_by_wall_clock():
    row = dict(open_time=0, open=100, high=101, low=99, close=100, volume=1)
    assert not confirmed_candles([row], '1h', as_of=0)


def test_future_history_is_never_fresh():
    row = dict(open_time=3600, open=100, high=101, low=99, close=100, volume=1)
    assert assess_candles('1h', [row], as_of=3600).status != 'FRESH'


def test_legacy_queue_identity_is_migrated_without_duplicate_or_ttl_extension(tmp_path):
    factory = lambda: sqlite3.connect(tmp_path / 'state.db')
    ScanCandidates(factory, 'ZONE')
    item = dict(symbol='BTCUSDT', direction='BULLISH', entry=100, sl=95, tp=110)
    facts = {key: item.get(key) for key in ('symbol','direction','entry','sl','tp','tp1','tp2','snapshot_id')}
    old = hashlib.sha256(json.dumps(facts, sort_keys=True, default=str).encode()).hexdigest()
    created = time.time() - 50
    with factory() as conn:
        conn.execute('INSERT INTO scan_candidate_queue VALUES(?,?,?,?)', (old, 'ZONE', json.dumps(item), created))
    queue = ScanCandidates(factory, 'ZONE')
    queue.append(item)
    assert list(queue) == [item]
    with factory() as conn:
        assert conn.execute('SELECT created FROM scan_candidate_queue').fetchall() == [(created,)]
    queue.retire(item)
    assert not ScanCandidates(factory, 'ZONE')


@pytest.mark.parametrize('bad', [None, 'not-a-number', float('nan'), float('inf')])
def test_strict_candles_reject_malformed_required_values(bad):
    row = dict(open_time=0, open=100, high=101, low=99, close=bad, volume=1)
    with pytest.raises(ValueError, match='invalid_candle'):
        confirmed_candles([row], '1h', as_of=3600, strict=True)


@pytest.mark.parametrize('review', [
    dict(decision='APPROVE', confidence=.7, degraded=False),
    dict(decision='APPROVE', confidence=.99, degraded=True),
    *[dict(decision='APPROVE', confidence=value, degraded=False)
      for value in [float('nan'), float('inf'), 2, None]],
])
def test_cached_review_must_pass_current_quality_gate_before_delivery(review):
    fn = next(n for n in ast.parse(Path('apex/app/worker.py').read_text()).body
              if isinstance(n, ast.AsyncFunctionDef) and n.name == '_send_signal')
    claim = Mock(side_effect=RuntimeError('unexpected delivery boundary'))
    ns = dict(asyncio=asyncio, logging=logging, time=time, _active_scan_run_id=None,
              _signal_type_from_candidate=lambda _: 'ZONE',
              _strategy_risk_state=lambda *a: dict(mode='NORMAL', groq_min_confidence=.8),
              _SIGNAL_INTEGRITY_OK=True, get_live_prices=lambda: {},
              _validate_signal_candidate=lambda *a: dict(valid=True),
              _has_pending_signal_for_symbol=lambda _: False,
              _assess_setup_candidate=lambda _: dict(blocking=False, state='VALID'),
              _persist_setup_assessment=lambda *a: None,
              _record_strategy_decision=lambda *a, **kw: None, DB_PATH='unused',
              _SIGNAL_QUALITY_GATE_OK=True, _V3_RUNTIME=Mock(),
              _V3_COMPONENT_STATE=NS(DEGRADED='DEGRADED'),
              _V3_CONFIG=NS(execution=NS(enabled=False), database=NS(state_db_path='unused'),
                            integrations=NS(groq_min_approval_confidence=.65)),
              ADMIN_IDS=[1], _signal_delivery_key=lambda *a: 'signal',
              _pair_delivery_key=lambda _: 'pair', _claim_signal_delivery=claim,
              _SIGNAL_COOLDOWN_HOURS=4, _v3_report_incident=Mock())
    exec(compile(ast.Module(body=[fn], type_ignores=[]), 'worker', 'exec'), ns)
    candidate = dict(symbol='BTCUSDT', direction='BULLISH', grade='ZONE', text='test',
                     _external_quality_reviewed=True, _external_quality_review=review)
    assert asyncio.run(ns['_send_signal'](candidate)) is False
    claim.assert_not_called()


def test_real_fast_wrapper_budget_allows_the_pair_detector_to_run():
    from apex.app.scan_budget import ScanBudget, scan_deadline, scan_has_budget
    names = {'auto_fast_deal_scan', '_auto_fast_deal_scan_impl'}
    functions = [n for n in ast.parse(Path('apex/app/worker.py').read_text()).body
                 if isinstance(n, ast.AsyncFunctionDef) and n.name in names]
    detected = Mock(return_value=None)
    async def exclusive(name, factory, timeout):
        token = scan_deadline.set(ScanBudget(timeout))
        try:
            return await factory()
        finally:
            scan_deadline.reset(token)
    ns = dict(asyncio=asyncio, logging=logging, fast_session=lambda _: 'NY',
              _run_market_scan_exclusive=exclusive, scan_has_budget=scan_has_budget,
              get_live_prices=lambda: {}, live_fast_pairs=lambda _: ['BTCUSDT'],
              FAST_PAIRS=['BTCUSDT'], get_candles=lambda *a: [],
              ScanCandidates=lambda *a: [],
              _take_strategy_round_batch=lambda *a: dict(pairs=['BTCUSDT'], round_id=1, target=1),
              DB_PATH='unused', _control_scan_round=AsyncMock(), _control_scan_scope=AsyncMock(),
              _control_scan_pair=AsyncMock(), _control_scan_outcome=AsyncMock(),
              _V3_CONFIG=NS(operational=NS(fast_concurrency=1)),
              _get_v3_strategy_activation=lambda: NS(active=True),
              _v3_strategy_candidate=detected)
    exec(compile(ast.Module(body=functions, type_ignores=[]), 'worker', 'exec'), ns)
    asyncio.run(ns['auto_fast_deal_scan']())
    detected.assert_called_once_with('FAST', 'BTCUSDT')


def test_provider_does_not_hide_a_malformed_recent_venue_bar():
    from datetime import datetime, timezone
    from apex.market.gate_client import GateMarketClient
    from apex.market.provider import GateSnapshotProvider
    from tests.test_apex_v3_gate_client import FakeGate
    class CorruptGate(FakeGate):
        def __call__(self, url, params, timeout):
            rows = super().__call__(url, params, timeout)
            if url.endswith('/candlesticks') and params['interval'] == '15m':
                rows[-1][2] = 'not-a-price'
            return rows
    build = GateSnapshotProvider(GateMarketClient(get_json=CorruptGate(), clock=lambda: 2000)).build(
        'FAST', 'BTCUSDT', as_of=datetime.fromtimestamp(2000, timezone.utc))
    assert build.snapshot.candles['15m'] == ()
    assert build.wait_reason_codes


def test_partial_scan_returns_actual_persisted_status(tmp_path):
    from apex.telemetry.scanner_metrics import begin_scan, finish_scan
    path = str(tmp_path / 'control.db')
    run = begin_scan('FAST', 'fast', 20, 20, path)
    assert finish_scan(run, 'COMPLETED', db_path=path) == 'PARTIAL'
    with sqlite3.connect(path) as conn:
        assert conn.execute('SELECT status,pairs_attempted FROM scan_runs').fetchone() == ('PARTIAL', 0)


@pytest.mark.parametrize('method', ['GET', 'POST'])
def test_successful_binance_response_recovers_restored_rate_limit_incident(method):
    from unittest.mock import patch
    from apex.execution import orders
    from tests.test_trade_execution import live_config
    session = Mock()
    response = session.request.return_value
    response.status_code = 200
    response.json.return_value = {'serverTime': 1}
    with patch.object(orders, '_binance_blocked_until', 0), \
            patch('apex.telemetry.incidents.recover_incident') as recovered:
        client = orders.BinanceFuturesClient(live_config(), session=session)
        assert client._request(method, '/fapi/v1/time', signed=False) == {'serverTime': 1}
        recovered.assert_not_called()
        orders.recover_binance_rate_limit()
        recovered.assert_called_once_with('BINANCE_RATE_LIMIT', 'binance')


@pytest.mark.parametrize('case', ['blocked', 'stale', 'failed', 'unparsed'])
def test_binance_recovery_requires_recent_parsed_success_and_closed_circuit(case):
    from unittest.mock import patch
    from apex.execution import orders
    now = time.time()
    metrics = dict(last_status=200, last_success_epoch=now, last_request_epoch=now - 1)
    if case == 'stale':
        metrics.update(last_success_epoch=now - 700, last_request_epoch=now - 701)
    elif case == 'failed':
        metrics['last_status'] = 429
    elif case == 'unparsed':
        metrics['last_success_epoch'] = now - 2
    with patch.object(orders, '_binance_request_metrics', metrics), \
            patch.object(orders, '_binance_blocked_until', now + 60 if case == 'blocked' else 0), \
            patch('apex.telemetry.incidents.recover_incident') as recovered:
        assert orders.recover_binance_rate_limit() is False
        recovered.assert_not_called()


def test_release_input_is_not_interpolated_into_shell_source():
    import yaml
    workflow = yaml.safe_load(Path('.github/workflows/controlled-production-release.yml').read_text())
    step = next(s for s in workflow['jobs']['verify']['steps'] if s.get('id') == 'commit')
    assert '${{ inputs.release_sha }}' not in step['run']
    assert step['env']['REQUESTED_RELEASE_SHA'] == '${{ inputs.release_sha }}'
    assert '^[0-9a-f]{40}$' in step['run']


@pytest.mark.parametrize('age', [None, -1, -.1, float('nan'), float('inf'), 'invalid'])
def test_unknown_or_future_context_age_is_not_eligible_for_groq(age):
    from external_sources.aggregator import _usable, _status
    from external_sources.models import empty_context
    result = dict(source='public_futures', status='fresh', age_seconds=age)
    assert not _usable(result)
    context = empty_context('BTCUSDT')
    _status(context, result)
    assert context['data_quality']['source_status']['public_futures'] == 'age_unknown'


def test_groq_boolean_confidence_is_not_a_numeric_approval():
    from apex.quality.groq_schema import parse_review, approval_error
    from apex.domain.enums import Decision
    payload = dict(decision='APPROVE', confidence=True, reason_codes=['VALID'], short_summary='ok')
    assert parse_review(payload).decision is Decision.WAIT
    assert approval_error(payload, .65)


def test_news_rejects_entity_declarations_and_oversized_feed():
    from news_context.sources import parse_calendar_payload, parse_rss_xml, MAX_NEWS_BYTES
    document = '<!DOCTYPE rss [<!ENTITY x "expanded">]><rss><item><title>&x;</title></item></rss>'
    with pytest.raises(ValueError, match='declarations_forbidden'):
        parse_rss_xml(document, 'untrusted')
    with pytest.raises(ValueError, match='too_large'):
        parse_calendar_payload(' ' * (MAX_NEWS_BYTES + 1))


def test_news_http_reader_stops_at_size_limit():
    from unittest.mock import patch
    from news_context.sources import _fetch_text_sync, MAX_NEWS_BYTES
    response = Mock()
    response.read.return_value = b'x' * (MAX_NEWS_BYTES + 1)
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    with patch('news_context.sources.urlopen', return_value=response):
        with pytest.raises(ValueError, match='too_large'):
            _fetch_text_sync('https://example.com/rss')
    response.read.assert_called_once_with(MAX_NEWS_BYTES + 1)


@pytest.mark.parametrize('value', [float('nan'), float('inf'), '-Infinity', True, 10**1000])
def test_nonfinite_external_values_remain_unknown(value):
    from external_sources.models import number
    assert number(value) is None


@pytest.mark.parametrize('venue', ['gate', 'external'])
def test_public_json_reader_rejects_oversized_response(venue):
    from unittest.mock import patch
    from apex.market.gate_client import _stdlib_get, GateTransportError
    from external_sources.http_client import ExternalHTTPClient, ExternalHTTPError
    limit = 4 * 1024 * 1024
    response = Mock()
    response.read.return_value = b'x' * (limit + 1)
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    module = 'apex.market.gate_client' if venue == 'gate' else 'external_sources.http_client'
    with patch(module + '.urlopen', return_value=response):
        with pytest.raises((GateTransportError, ExternalHTTPError)):
            if venue == 'gate':
                _stdlib_get('https://example.com/candles', {}, 1)
            else:
                ExternalHTTPClient()._fetch_sync('https://example.com/context', None, None)
    response.read.assert_called_once_with(limit + 1)


def test_cold_restart_accepts_state_after_runtime_candidate_persistence(tmp_path):
    from apex.db.state_db import STATE_MIGRATIONS, migrate_state
    from apex.db.memory_db import migrate_memory
    from apex.db.migrations import MigrationRunner
    from apex.db.ownership import assert_schema_ownership
    path = tmp_path / 'restored-state.db'
    factory = lambda: sqlite3.connect(path)
    with factory() as conn:
        MigrationRunner(tuple(m for m in STATE_MIGRATIONS if m.version <= 26)).run(conn)
    queue = ScanCandidates(factory, 'ZONE')
    item = dict(symbol='ETHUSDT', direction='BULLISH', entry=100, sl=95, tp=110)
    queue.append(item)
    with factory() as conn:
        before = conn.execute('SELECT identity,payload,created FROM scan_candidate_queue').fetchall()
    # Same cold-start order as the actual worker: migrate, then ownership gate.
    with factory() as state, sqlite3.connect(':memory:') as memory:
        migrate_state(state)
        migrate_memory(memory)
        assert assert_schema_ownership(state, memory).ok
        assert state.execute('SELECT identity,payload,created FROM scan_candidate_queue').fetchall() == before
        assert migrate_state(state) == ()
    assert list(ScanCandidates(factory, 'ZONE')) == [item]


def test_bootstrap_logs_original_failure_before_shutdown_can_exit(caplog):
    from apex.app.bootstrap import _run_polling
    deps = NS(initialize=AsyncMock(side_effect=RuntimeError('original_boot_failure')),
              shutdown=AsyncMock(side_effect=SystemExit(0)))
    with pytest.raises(SystemExit):
        asyncio.run(_run_polling(deps))
    assert 'original_boot_failure' in caplog.text
