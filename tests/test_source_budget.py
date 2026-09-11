import asyncio
import os
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from concurrent.futures import ThreadPoolExecutor

from external_sources.budget import SourceBudget, Policy, BudgetDenied, request_scope, projected_load, plan_daily_load
from external_sources.http_client import ExternalHTTPClient, ExternalHTTPError
from core.control_loop import due_ltf_watches, ensure_control_schema, upsert_ltf_watch


class BudgetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, 'brain.db')
        self.now = 100000
        self.budget = SourceBudget(self.path, clock=lambda: self.now, policies={'test': Policy(10, 20, 30)})

    def tearDown(self):
        self.tmp.cleanup()

    def test_restart_and_rolling_windows(self):
        self.budget.reserve('test', 10)
        restarted = SourceBudget(self.path, clock=lambda: self.now, policies=self.budget.policies)
        with self.assertRaises(BudgetDenied): restarted.reserve('test')
        self.now += 60
        restarted.reserve('test', 10)
        self.now += 60
        with self.assertRaises(BudgetDenied): restarted.reserve('test')
        self.now += 3600
        restarted.reserve('test', 10)
        self.now += 3600
        with self.assertRaises(BudgetDenied): restarted.reserve('test')
        self.now += 86400
        restarted.reserve('test')

    def test_concurrent_admission_is_atomic(self):
        self.budget.snapshot()
        def reserve(_):
            try:
                self.budget.reserve('test')
                return 1
            except BudgetDenied:
                return 0
        with ThreadPoolExecutor(max_workers=8) as pool:
            self.assertEqual(sum(pool.map(reserve, range(30))), 10)

    def test_ban_survives_restart_and_concurrent_success(self):
        self.budget.outcome('test', failed=True, rate_limited=True, retry_after=300)
        self.budget.outcome('test')
        with self.assertRaises(BudgetDenied): self.budget.reserve('test')
        self.now += 301
        self.budget.reserve('test')

    def test_batch_weights_and_binance_denial(self):
        self.assertEqual(request_scope('https://api.coinalyze.net/v1/open-interest', {'symbols': 'BTC,ETH'}), ('coinalyze', 2))
        self.assertEqual(request_scope('https://api.hyperliquid.xyz/info')[1], 20)
        with self.assertRaises(BudgetDenied): request_scope('https://fapi.binance.com/fapi/v1/klines')
        self.assertEqual(request_scope('https://api.bls.gov/publicAPI/v1/timeseries/data/')[0], 'bls')
        self.assertEqual(request_scope('https://cointelegraph.com/rss')[0], 'news')

    def test_full_day_projection(self):
        # 10 symbols * 3 endpoints every 15 min, plus 20% retry reserve.
        self.assertEqual(projected_load(10, 3, 900)['with_retry_reserve'], 3456)

    def test_daily_plan_reports_headroom_without_reserving(self):
        plan = plan_daily_load({'test': {'symbols': 1, 'endpoints': 1, 'interval_seconds': 900}}, policies={'test': Policy(10, 20, 200)})
        self.assertEqual(plan['test']['cycles_day'], 96)
        self.assertTrue(plan['test']['within_day'])

    def test_ltf_reobservation_keeps_identity_and_expiry(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'brain.db')
            ensure_control_schema(path)
            upsert_ltf_watch('ZONE', 'APTUSDT', 'BULLISH', '1h', 'first', 8, path)
            conn = __import__('sqlite3').connect(path)
            before = conn.execute('SELECT setup_id,expires_at,attempts FROM ltf_watchlist').fetchone()
            conn.close()
            upsert_ltf_watch('ZONE', 'APTUSDT', 'BULLISH', '1h', 'reobserved', 1, path)
            conn = __import__('sqlite3').connect(path)
            after = conn.execute('SELECT setup_id,expires_at,attempts,reason FROM ltf_watchlist').fetchone()
            conn.close()
            self.assertEqual(after[0], before[0])
            self.assertEqual(after[1], before[1])
            self.assertEqual(after[2], before[2])
            self.assertEqual(after[3], 'reobserved')

    def test_ltf_watch_is_suppressed_for_existing_active_signal(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'brain.db')
            ensure_control_schema(path)
            conn = __import__('sqlite3').connect(path)
            conn.execute('CREATE TABLE signals(id INTEGER PRIMARY KEY,symbol TEXT,direction TEXT,signal_type TEXT,result TEXT)')
            conn.execute("INSERT INTO signals VALUES(1,'AAVEUSDT','BULLISH','ZONE','pending')")
            conn.commit(); conn.close()
            upsert_ltf_watch('ZONE', 'AAVEUSDT', 'BULLISH', '1h', 'waiting structure', 8, path)
            self.assertEqual(due_ltf_watches(db_path=path), [])


class ClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_rate_limit_does_not_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = SourceBudget(os.path.join(tmp, 'brain.db'))
            client = ExternalHTTPClient(retries=3)
            error = HTTPError('https://api.coinalyze.net/x', 429, 'limited', {'Retry-After': '180'}, None)
            with patch('external_sources.http_client.budget', ledger), patch.object(client, '_fetch_sync', side_effect=error) as fetch:
                with self.assertRaises(ExternalHTTPError): await client.get_json('https://api.coinalyze.net/x')
                with self.assertRaises(ExternalHTTPError): await client.get_json('https://api.coinalyze.net/x')
                self.assertEqual(fetch.call_count, 1)
            row = next(x for x in ledger.snapshot() if x['source']=='coinalyze')
            self.assertEqual(row['used']['day'], 1)
            self.assertEqual(row['health']['rate_limits'], 1)

    async def test_each_retry_is_metered(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = SourceBudget(os.path.join(tmp, 'brain.db'))
            client = ExternalHTTPClient(retries=1)
            with patch('external_sources.http_client.budget', ledger), patch.object(client, '_fetch_sync', side_effect=[TimeoutError(), {'ok': True}]):
                self.assertEqual(await client.get_json('https://api.coinalyze.net/x'), {'ok': True})
            row = next(x for x in ledger.snapshot() if x['source']=='coinalyze')
            self.assertEqual(row['used']['day'], 2)
