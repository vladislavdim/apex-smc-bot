"""Behaviour regressions for the October full-system audit repairs."""
import asyncio
import gzip
import io
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from apex.app import bootstrap
from apex.db.audit_db import audit_path, migrate_audit
from apex.db.snapshot_io import copy_stream, sqlite_snapshot
from apex.telemetry import event_log
from apex.ui import context_store, user_memory
from apex.ui.telegram.chat import ChatDependencies, TelegramChatHandlers
from external_sources.cache import TTLCache
from scripts import render_telegram_release as release


class CacheRegressions(unittest.IsolatedAsyncioTestCase):
    async def test_unique_successes_and_failures_are_bounded_after_expiry(self):
        cache = TTLCache(max_entries=32)
        clock = [100.0]
        async def fetch(): return {'value': 1}
        async def fail(): raise RuntimeError('offline')
        with patch('external_sources.cache.time.time', side_effect=lambda: clock[0]):
            for index in range(1000):
                await cache.get_or_fetch(str(index), 2, 10, fetch)
            self.assertLessEqual(len(cache._items), 32)
            for index in range(1000):
                with self.assertRaises(RuntimeError):
                    await cache.get_or_fetch('fail'+str(index), 2, 10, fail)
            self.assertLessEqual(len(cache._failed_until), 32)
            self.assertEqual(len(cache._locks), 64)
            clock[0] = 200
            await cache.get_or_fetch('new', 2, 10, fetch)
            self.assertEqual(len(cache._items), 1)
            self.assertFalse(cache._failed_until)

    async def test_clear_during_fetch_keeps_single_flight_and_stale_fallback(self):
        cache = TTLCache()
        started, finish = asyncio.Event(), asyncio.Event()
        calls = []
        async def fetch():
            calls.append(1); started.set(); await finish.wait(); return 42
        first = asyncio.create_task(cache.get_or_fetch('key', 10, 100, fetch))
        await started.wait()
        cache.clear()
        second = asyncio.create_task(cache.get_or_fetch('key', 10, 100, fetch))
        finish.set()
        results = await asyncio.gather(first, second)
        self.assertEqual(calls, [1])
        self.assertEqual([r[0] for r in results], [42, 42])
        async def fail(): raise RuntimeError('offline')
        with patch('external_sources.cache.time.time', return_value=cache._items['key'].saved_at+20):
            value, status, _ = await cache.get_or_fetch('key', 10, 100, fail)
        self.assertEqual((value, status), (42, 'stale_fallback'))


class ChatScanRegressions(unittest.IsolatedAsyncioTestCase):
    def handlers(self, scanner):
        self.ai = Mock(side_effect=AssertionError('scan must not fall back to AI'))
        return TelegramChatHandlers(ChatDependencies({}, {}, Mock(), Mock(), Mock(),
            self.ai, Mock(), Mock(), AsyncMock(), deep_scan=scanner,
            format_deep_scan=lambda signals, accumulations, limit: 'Найдено: '+str(len(signals))))
    def message(self):
        return SimpleNamespace(text='найди сделки', from_user=SimpleNamespace(id=1, first_name='V'),
                               answer=AsyncMock(return_value=SimpleNamespace(delete=AsyncMock())))
    async def test_scan_runs_in_current_event_loop_and_returns_actual_result(self):
        scanner = AsyncMock(return_value=([{'symbol': 'BTCUSDT'}], []))
        handler = self.handlers(scanner); message = self.message()
        await handler.text(message)
        scanner.assert_awaited_once_with(limit=80)
        self.assertEqual(message.answer.await_args.args[0], 'Найдено: 1')
        self.ai.assert_not_called()
    async def test_overlap_is_rejected_and_failure_is_explicit(self):
        started, finish = asyncio.Event(), asyncio.Event()
        async def fail(**kwargs):
            started.set(); await finish.wait(); raise RuntimeError('offline')
        handler = self.handlers(fail)
        first_message, other = self.message(), self.message()
        task = asyncio.create_task(handler.text(first_message)); await started.wait()
        await handler.text(other)
        self.assertIn('уже выполняется', other.answer.await_args.args[0])
        finish.set(); await task
        self.assertIn('не завершилось', first_message.answer.await_args.args[0])
        self.ai.assert_not_called()
        self.assertFalse(handler._deep_scan_lock.locked())


class StorageRegressions(unittest.TestCase):
    def test_audit_retention_prunes_unsynced_diagnostics_without_touching_state(self):
        with tempfile.TemporaryDirectory() as directory:
            state = os.path.join(directory, 'state.db'); audit = audit_path(state)
            config = SimpleNamespace(integrations=SimpleNamespace(telegram_only=True),
                                     database=SimpleNamespace(state_db_path=state))
            for path in (state, audit):
                with closing(sqlite3.connect(path)) as conn:
                    migrate_audit(conn)
                    conn.executemany("INSERT INTO setup_audit_events(event_key,kind,occurred_at,payload_json) VALUES (?, 'test', datetime('now'), '{}')",
                                     [(str(i),) for i in range(1100)])
                    conn.execute("INSERT INTO setup_audit_events(event_key,kind,occurred_at,payload_json) VALUES ('old','test',datetime('now','-4 days'),'{}')")
                    conn.commit()
            with patch.object(event_log, '_CONFIG', config), patch.object(event_log, 'DB_PATH', state):
                event_log._prune_local_audit()
            with patch.object(event_log, '_CONFIG', config), patch.object(event_log, 'DB_PATH', audit):
                event_log._prune_local_audit()
            with closing(sqlite3.connect(state)) as conn:
                self.assertEqual(conn.execute('SELECT COUNT(*) FROM setup_audit_events').fetchone()[0],1101)
            with closing(sqlite3.connect(audit)) as conn:
                self.assertEqual(conn.execute('SELECT COUNT(*) FROM setup_audit_events').fetchone()[0],1000)
                self.assertEqual(conn.execute('SELECT COUNT(*) FROM setup_audit_events WHERE synced=0').fetchone()[0],1000)
                self.assertIsNone(conn.execute("SELECT event_key FROM setup_audit_events WHERE event_key='old'").fetchone())
    def test_ui_connection_closes_after_sql_error(self):
        for module, name, args in ((context_store,'save_news',('q','text')), (context_store,'get_recent_news',()),
                                   (user_memory,'save_chat_log',(1,'user','text'))):
            conn = Mock(); conn.execute.side_effect = sqlite3.OperationalError('locked')
            with patch.object(module,'get_db_conn',return_value=conn):
                getattr(module,name)(*args)
            conn.close.assert_called_once()
    def test_snapshot_and_streaming_compression_preserve_large_sqlite_contents(self):
        with tempfile.TemporaryDirectory() as directory:
            source_path, target_path = [os.path.join(directory, x) for x in ('source.db','copy.db')]
            with closing(sqlite3.connect(source_path)) as source, closing(sqlite3.connect(target_path)) as target:
                source.execute('CREATE TABLE sample(id INTEGER PRIMARY KEY, payload BLOB)')
                source.executemany('INSERT INTO sample(payload) VALUES (?)', [(os.urandom(1024*1024),) for _ in range(12)])
                source.commit(); sqlite_snapshot(source,target,source_path,target_path)
                self.assertEqual(target.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
                self.assertEqual(target.execute('SELECT COUNT(*), SUM(length(payload)) FROM sample').fetchone(), (12,12*1024*1024))
            compressed = io.BytesIO()
            with open(target_path,'rb') as source, gzip.GzipFile(fileobj=compressed,mode='wb') as target:
                copy_stream(source,target)
            self.assertEqual(gzip.decompress(compressed.getvalue()),Path(target_path).read_bytes())
    def test_full_swing_positive_path_and_optional_failure(self):
        import subprocess, sys
        result = subprocess.run([sys.executable, 'tests/swing_positive_probe.py'], capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr[-4000:])
    def test_fatal_polling_exits_nonzero_after_shutdown(self):
        shutdown = AsyncMock()
        deps = SimpleNamespace(config=SimpleNamespace(integrations=SimpleNamespace(telegram_only=True)),
            initialize=AsyncMock(), shutdown=shutdown,
            dispatcher=SimpleNamespace(start_polling=AsyncMock(side_effect=RuntimeError('failed')),
                                       resolve_used_update_types=lambda: []), telegram_bot=Mock())
        with patch.object(bootstrap,'_acquire_process_lock',return_value=Mock()), patch.object(bootstrap.time,'sleep'):
            with self.assertRaises(SystemExit) as caught:
                bootstrap.run_production(deps,max_polling_restarts=2)
        self.assertEqual(caught.exception.code,1)
        self.assertEqual(shutdown.await_count,2)


if __name__ == '__main__': unittest.main()
