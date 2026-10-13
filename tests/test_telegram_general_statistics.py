import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from apex.config.settings import ApexConfig
from apex.db.memory_db import migrate_memory
from apex.telemetry.scanner_metrics import ensure_control_schema, begin_scan, record_scan_event, scan_heartbeat
from apex.ui.telegram.statistics import general_statistics, format_general_statistics


class GeneralStatisticsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.brain = str(Path(self.tmp.name)/'brain.db')
        self.memory = str(Path(self.tmp.name)/'memory.db')
        self.config = ApexConfig.from_env({'APEX_COMPAT_DB_PATH':self.brain,
                                          'APEX_MEMORY_DB_PATH':self.memory})
        with sqlite3.connect(self.memory) as conn:
            migrate_memory(conn)
        ensure_control_schema(self.brain)

    def test_checked_found_reviewed_delivered_and_all_strategies_are_distinct(self):
        run = begin_scan('FAST','fast',80,2,self.brain)
        for symbol in ('BTCUSDT','ETHUSDT'):
            scan_heartbeat(run,symbol,self.brain)
            record_scan_event(run,'FAST',symbol,'data','DATA_OK',db_path=self.brain)
        for outcome in ('CANDIDATE','GROQ_APPROVE','DELIVERED'):
            record_scan_event(run,'FAST','BTCUSDT','gate',outcome,db_path=self.brain)
        text = format_general_statistics(general_statistics(self.config))
        self.assertIn('Проверок пар: <b>2</b>',text)
        self.assertIn('Найдено кандидатов: <b>1</b>',text)
        self.assertIn('Проверок Groq: <b>1</b>',text)
        self.assertIn('Отправлено сигналов: <b>1</b>',text)
        for strategy in ('FAST','MTF','SWING','ZONE','WYCKOFF'):
            self.assertIn(f'<b>{strategy}</b>',text)
        self.assertLess(len(text),4000)

    def test_totals_survive_detail_deletion_and_schema_is_idempotent(self):
        run = begin_scan('FAST','fast',80,1,self.brain)
        scan_heartbeat(run,'BTCUSDT',self.brain)
        ensure_control_schema(self.brain)
        ensure_control_schema(self.brain)
        with sqlite3.connect(self.brain) as conn:
            conn.execute('DELETE FROM scan_runs')
        second = begin_scan('FAST','fast',80,1,self.brain)
        scan_heartbeat(second,'ETHUSDT',self.brain)
        result = general_statistics(self.config)
        self.assertEqual(result['scans'][0]['pairs_attempted'],2)

    def test_period_filters_apply_to_scans_and_confirmed_trade_outcomes(self):
        with sqlite3.connect(self.brain) as conn:
            conn.execute("INSERT INTO scan_statistics_daily(day,strategy,pairs_attempted,candidates) VALUES('2026-10-13','FAST',2,1),('2026-10-01','MTF',100,50)")
        with sqlite3.connect(self.memory) as conn:
            conn.execute("""INSERT INTO live_trade_outcomes(outcome_id,position_id,strategy,symbol,direction,net_r,closed_at)
              VALUES('one','pos-one','FAST','BTCUSDT','LONG',1.5,'2026-10-13T05:00:00+00:00'),
                    ('two','pos-two','MTF','ETHUSDT','SHORT',-3,'2026-10-01T05:00:00+00:00')""")
        now = datetime(2026,10,13,12,tzinfo=timezone.utc)
        result = general_statistics(self.config,'7d','ALL',now=now)
        self.assertEqual(result['scans'][0]['pairs_attempted'],2)
        self.assertEqual(len(result['trades']),1)
        text = format_general_statistics(result)
        self.assertIn('+1.50R',text)
        self.assertNotIn('-3.00R',text)
        self.assertEqual(general_statistics(self.config,'all','MTF',now=now)['scans'][0]['pairs_attempted'],100)

    def test_missing_sources_are_unavailable_not_zero(self):
        Path(self.brain).unlink()
        result = general_statistics(self.config)
        text = format_general_statistics(result)
        self.assertIn('История проверок временно недоступна',text)
        self.assertNotIn('Проверок пар: <b>0</b>',text)

    def test_invalid_filter_cannot_reach_sql(self):
        with self.assertRaises(ValueError):
            general_statistics(self.config,'all',"FAST' OR 1=1")

