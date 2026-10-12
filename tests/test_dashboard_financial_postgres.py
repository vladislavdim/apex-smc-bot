"""Real PostgreSQL regression; CI provides an isolated local test database."""
import json
import os
import unittest
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from apex.ui.dashboard.server import _financial_totals


class FinancialPostgresTests(unittest.TestCase):
    @unittest.skipUnless(os.environ.get('APEX_TEST_DATABASE_URL'), 'isolated PostgreSQL supplied by CI')
    def test_complete_totals_above_diagnostic_window_with_duplicates_and_unknown_fees(self):
        url = os.environ['APEX_TEST_DATABASE_URL']
        self.assertIn(urlparse(url).hostname, ('127.0.0.1', 'localhost'))
        conn = psycopg2.connect(url)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""CREATE TEMP TABLE apex_stats_events(
                    event_key TEXT,kind TEXT,strategy TEXT,symbol TEXT,
                    occurred_at TIMESTAMPTZ,received_at TIMESTAMPTZ,payload JSONB)""")
                rows = [(str(i), 'execution_accounting', 'FAST','BTCUSDT',
                    '2026-10-12T00:00:00Z','2026-10-12T00:00:00Z', json.dumps({
                        'signal_id':i, 'action':'CLOSE', 'realized_r':1,
                        'pnl_pct':0.5, 'accounting_basis':'confirmed_fills_after_commissions_and_funding'}))
                    for i in range(6001)]
                # Duplicate evidence must not count twice; unvalued fees cannot be treated as zero.
                rows += [('dup', *rows[0][1:]), ('unknown', 'execution_accounting','FAST',
                    'BTCUSDT','2026-10-12T00:00:00Z','2026-10-12T00:00:00Z',json.dumps({
                        'signal_id':9000,'action':'CLOSE','realized_r':999,'accounting_basis':'FEES_UNRESOLVED'}))]
                execute_values(cur,'INSERT INTO apex_stats_events VALUES %s',rows)
                totals = _financial_totals(cur, ['symbol=%s'], ['BTCUSDT'])
                self.assertEqual(totals['closed'],6001)
                self.assertEqual(totals['wins'],6001)
                self.assertEqual(totals['pnl_pct'],3000.5)
                self.assertEqual(totals['avg_r'],1)
                self.assertEqual(totals['by_strategy'][0]['closed'],6001)
        finally:
            conn.rollback()
            conn.close()
