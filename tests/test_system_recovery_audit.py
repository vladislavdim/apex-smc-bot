"""Regressions reproduced during the 2026-09-30 production audit."""
import asyncio
import math
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from apex.app.cutover import CutoverSpec, refresh_cutover
from apex.app.runtime import RuntimeSupervisor, runtime_supervisor
from apex.app.scheduler import _guarded
from apex.db.connection import connect_compatibility
from apex.db.state_db import migrate_state
from apex.domain.enums import ComponentState
from apex.market.adaptive_indicators import LegacyAdaptiveIndicators
from apex.market.indicators import ema_value
from apex.market.runtime_cache import get_confirmed_candles
from apex.telemetry.incidents import (
    configure_incidents, mark_notifications_delivered, open_incident,
    pending_notifications, resolve_incident,
)
from apex.ui.telegram.incidents import format_notification, notification_batches
from core import historical_zones


def ready_runtime(runtime):
    runtime.activate()
    for component in runtime.REQUIRED_COMPONENTS:
        runtime.mark_component(component, ComponentState.READY)
    runtime.set_instance_lease(1, (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat())
    assert runtime.evaluate_readiness()


class RecoveryAuditTests(unittest.TestCase):
    def tearDown(self):
        runtime_supervisor.deactivate()
        configure_incidents(None)

    def test_cutover_recovery_releases_stale_failed_health_and_records_sqlite_code(self):
        runtime = RuntimeSupervisor()
        ready_runtime(runtime)
        spec = CutoverSpec('Signals', 'STATE_DB_SIGNALS_FAILED', 'parity', Mock(), Mock())
        incident = Mock()
        recovered = Mock()
        error = sqlite3.OperationalError('sensitive SQL omitted')
        error.sqlite_errorcode = sqlite3.SQLITE_BUSY
        error.sqlite_errorname = 'SQLITE_BUSY'
        kwargs = dict(runtime=runtime, failed_state=ComponentState.FAILED,
                      report_incident=incident, recover_incident=recovered)
        with self.assertRaises(sqlite3.OperationalError):
            asyncio.run(refresh_cutover(spec, Mock(side_effect=error), **kwargs))
        self.assertFalse(runtime.evaluate_readiness())
        self.assertEqual(runtime.snapshot()['components']['state_db']['state'], 'FAILED')
        self.assertEqual(incident.call_args.args[3], {
            'error_type': 'OperationalError', 'sqlite_errorcode': sqlite3.SQLITE_BUSY,
            'sqlite_errorname': 'SQLITE_BUSY',
        })
        recovered.assert_not_called()
        asyncio.run(refresh_cutover(spec, lambda: {'parity_ok': True}, **kwargs))
        self.assertTrue(runtime.evaluate_readiness())
        self.assertTrue(runtime.allows_new_entries)
        self.assertEqual(runtime.snapshot()['components']['state_db']['state'], 'READY')

    def test_recovery_does_not_hide_other_mirrors_or_a_base_db_failure(self):
        runtime = RuntimeSupervisor()
        ready_runtime(runtime)
        runtime.fail_component('state_db', 'mirror-a')
        runtime.fail_component('state_db', 'mirror-b')
        runtime.recover_component('state_db', 'mirror-a')
        self.assertFalse(runtime.evaluate_readiness())
        self.assertIn('mirror-b', runtime.snapshot()['reason_codes'])
        runtime.mark_component('state_db', 'FAILED', 'schema check failed')
        runtime.recover_component('state_db', 'mirror-b')
        self.assertFalse(runtime.evaluate_readiness())
        self.assertEqual(runtime.snapshot()['components']['state_db']['detail'], 'schema check failed')

    def test_mirror_success_does_not_make_uninitialized_database_ready(self):
        runtime = RuntimeSupervisor()
        runtime.activate()
        runtime.fail_component('state_db', 'mirror')
        runtime.recover_component('state_db', 'mirror')
        self.assertEqual(runtime.snapshot()['components']['state_db']['state'], 'UNKNOWN')
        self.assertFalse(runtime.evaluate_readiness())

    def test_optional_job_timeouts_do_not_block_healthy_execution(self):
        for job in ('state_backup', 'market_intelligence_primary'):
            with self.subTest(job=job):
                ready_runtime(runtime_supervisor)
                async def timeout():
                    raise asyncio.TimeoutError()
                with self.assertRaises(asyncio.TimeoutError):
                    asyncio.run(_guarded(job, timeout)())
                self.assertTrue(runtime_supervisor.evaluate_readiness())
                self.assertTrue(runtime_supervisor.allows_new_entries)
                snapshot = runtime_supervisor.snapshot()
                self.assertEqual(snapshot['components']['gate']['state'], 'READY')
                self.assertEqual(snapshot['components']['market_data']['state'], 'READY')
                self.assertEqual(snapshot['health'], 'DEGRADED')
                runtime_supervisor.inhibit_entries('STATE_BACKUP_DEFERRED')
                self.assertFalse(runtime_supervisor.evaluate_readiness())

    def test_skipping_failed_critical_job_is_not_a_recovery(self):
        ready_runtime(runtime_supervisor)
        async def broken():
            raise RuntimeError('failure')
        with self.assertRaises(RuntimeError):
            asyncio.run(_guarded('execution_reconcile', broken)())
        async def skipped():
            return False
        asyncio.run(_guarded('execution_reconcile', skipped)())
        self.assertFalse(runtime_supervisor.evaluate_readiness())
        self.assertIn('JOB_FAILED:execution_reconcile', runtime_supervisor.snapshot()['reason_codes'])
        self.assertEqual(runtime_supervisor.snapshot()['components']['binance_reconciliation']['state'], 'DEGRADED')

    def test_explicit_required_components_participate_in_readiness(self):
        runtime = RuntimeSupervisor()
        ready_runtime(runtime)
        runtime.mark_component('custom_protection', 'FAILED', required=True)
        self.assertFalse(runtime.evaluate_readiness())


class ZoneStorageAuditTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.path = str(Path(self.folder.name) / 'brain.db')
        self.candles = [dict(open=100, high=102 + math.sin(i / 3),
                             low=98 + math.sin(i / 3), close=100,
                             timestamp=1000 + i * 3600) for i in range(80)]

    def tearDown(self):
        self.folder.cleanup()

    def test_event_deduplication_uses_zone_index(self):
        conn = historical_zones._connect(self.path)
        try:
            plan = conn.execute('EXPLAIN QUERY PLAN SELECT 1 FROM historical_zone_events '
                                'WHERE zone_id=? AND event_key LIKE ? LIMIT 1', (99, 'prefix%')).fetchall()
            self.assertIn('SEARCH', plan[0][3])
            self.assertIn('idx_historical_zone_events_zone_event', plan[0][3])
        finally:
            conn.close()

    def test_zone_exception_rolls_back_and_closes_writer(self):
        conn = historical_zones._connect(self.path)
        with patch.object(historical_zones, '_connect', return_value=conn), \
             patch.object(historical_zones, 'advance_level', side_effect=RuntimeError('injected')):
            with self.assertRaisesRegex(RuntimeError, 'injected'):
                historical_zones.refresh_zones('BTCUSDT', '1h', self.candles, self.path)
        with self.assertRaises(sqlite3.ProgrammingError):
            conn.execute('SELECT 1')
        check = sqlite3.connect(self.path, timeout=0.1)
        try:
            check.execute('BEGIN IMMEDIATE')
            self.assertEqual(check.execute('SELECT COUNT(*) FROM historical_zones').fetchone()[0], 0)
            check.rollback()
        finally:
            check.close()
        self.assertGreater(historical_zones.refresh_zones('BTCUSDT', '1h', self.candles, self.path)['zones'], 0)

    def test_connection_respects_callers_lock_timeout(self):
        conn = connect_compatibility(self.path, timeout=0.25)
        try:
            self.assertEqual(conn.execute('PRAGMA busy_timeout').fetchone()[0], 250)
        finally:
            conn.close()


class IndicatorInputAuditTests(unittest.TestCase):
    def test_closed_snapshot_does_not_lose_its_latest_candle_twice(self):
        rows = [{'close': 1, 'is_closed': True}, {'close': 2, 'is_closed': True}]
        self.assertEqual(get_confirmed_candles(rows), rows)
        self.assertEqual(get_confirmed_candles(rows[-1:]), rows[-1:])
        self.assertEqual(get_confirmed_candles(rows + [{'close': 99, 'is_closed': False}]), rows)
        self.assertEqual(get_confirmed_candles([{'close': 1}, {'close': 2}]), [{'close': 1}])

    def test_ema200_has_sufficient_history_and_open_candle_cannot_change_indicators(self):
        rows = [dict(open=100+i, high=102+i, low=99+i, close=101+i, volume=10+i) for i in range(201)]
        fetch = Mock(return_value=rows)
        baseline = LegacyAdaptiveIndicators(fetch, ema_value).get_precomputed_indicators('BTCUSDT')
        self.assertEqual(baseline['ema200'], sum(range(101, 301)) / 200)
        self.assertEqual(baseline['price'], 300)
        rows[-1].update(high=1e9, low=0.01, close=1e8, volume=1e9)
        changed = LegacyAdaptiveIndicators(fetch, ema_value).get_precomputed_indicators('BTCUSDT')
        self.assertEqual(changed, baseline)
        self.assertEqual(fetch.call_args.args[2], 201)


class IncidentNotificationAuditTests(unittest.TestCase):
    def tearDown(self):
        configure_incidents(None)

    def test_brief_episode_crossing_batch_limit_is_one_recovered_message(self):
        with tempfile.TemporaryDirectory() as folder:
            path = str(Path(folder) / 'state.db')
            conn = sqlite3.connect(path)
            migrate_state(conn)
            open_incident(conn, 'MIRROR_FAILED', 'state_db', 'CRITICAL', {'error_type': 'OperationalError'})
            resolve_incident(conn, 'MIRROR_FAILED', 'state_db')
            open_incident(conn, 'GATE_STALE', 'gate', 'ERROR')
            conn.close()
            configure_incidents(lambda: sqlite3.connect(path))
            rows = pending_notifications(limit=1)
            self.assertEqual([r['event_type'] for r in rows], ['OPENED', 'RESOLVED'])
            batches = notification_batches(rows)
            self.assertEqual(len(batches), 1)
            self.assertEqual(batches[0]['event_type'], 'RECOVERED')
            text = format_notification(batches[0])
            self.assertIn('Сбой уже устранён', text)
            self.assertIn('CRITICAL', text)
            self.assertIn('Начало:', text)
            self.assertIn('Восстановление компонента:', text)
            self.assertIn('OperationalError', text)
            # No ACK on a failed delivery: the entire episode remains retryable.
            self.assertEqual(pending_notifications(limit=1), rows)
            self.assertTrue(mark_notifications_delivered(batches[0]['notification_ids']))
            remaining = notification_batches(pending_notifications())
            self.assertEqual(len(remaining), 1)
            self.assertEqual(remaining[0]['event_type'], 'OPENED')
            self.assertIn('GATE_STALE', format_notification(remaining[0]))
