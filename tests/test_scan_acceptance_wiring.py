"""Exercise the real release-health function against distinct control/audit DBs."""
import ast
from contextlib import closing
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import sqlite3
import tempfile
import time
from types import SimpleNamespace as NS
import unittest


class ReleaseCoverageWiringTests(unittest.TestCase):
    def test_release_health_reads_the_same_database_as_scanner_control(self):
        with tempfile.TemporaryDirectory() as root:
            brain = str(Path(root)/'brain.db')
            audit = str(Path(root)/'apex_audit.db')
            strategies = ('FAST','MTF','ZONE','SWING','WYCKOFF')
            with sqlite3.connect(brain) as conn:
                conn.execute('CREATE TABLE scan_coverage_rounds(strategy TEXT,status TEXT,completed_at TEXT)')
                conn.executemany('INSERT INTO scan_coverage_rounds VALUES(?,?,?)',
                                 [(s,'COMPLETED','2026-10-23 15:00:00') for s in strategies])
            with sqlite3.connect(audit) as conn:
                conn.execute('CREATE TABLE setup_audit_events(event_key TEXT)')
            calls=[];messages=[]
            def connect(path,**kwargs):
                calls.append(path)
                return sqlite3.connect(path)
            class Capture(logging.Handler):
                def emit(self, record):messages.append(record.getMessage())
            source=ast.parse(Path('apex/app/worker.py').read_text())
            function=next(n for n in source.body if isinstance(n,ast.FunctionDef) and n.name=='_v3_log_release_health')
            ns=dict(time=time,json=json,logging=logging,datetime=datetime,timezone=timezone,
                    _closing=closing,_audit_connect=connect,DB_PATH=brain,_AUDIT_DB_PATH=audit,
                    _v3_last_release_health=None,_v3_last_release_log_at=0,
                    _V3_CONFIG=NS(runtime=NS(release_sha='a'*40,instance_id='one',deploy_id='deploy'),
                                  integrations=NS(telegram_only=True,runtime_lease_backend='github'),
                                  execution=NS(enabled=False),operational=NS(memory_limit_bytes=512)),
                    _V3_RUNTIME=NS(public_snapshot=lambda:dict(ready=True,status='READY',reason_codes=[],components={},fencing_generation=48)),
                    _get_v3_strategy_activation=lambda:NS(active=True,verdict_sha256='b'*64),
                    _v3_memory_snapshot=lambda **kw:NS(rss_bytes=250,service_bytes=280,working_set_bytes=250,limit_bytes=512))
            exec(compile(ast.Module(body=[function],type_ignores=[]),'worker','exec'),ns)
            handler=Capture();logger=logging.getLogger('apex.release');logger.addHandler(handler)
            try:ns['_v3_log_release_health']()
            finally:logger.removeHandler(handler)
            self.assertEqual(calls,[brain])
            value=json.loads(next(m.split('[APEX Release Ready] ',1)[1] for m in messages if '[APEX Release Ready] ' in m))
            self.assertEqual(value['scanner_coverage'],{s:'2026-10-23 15:00:00' for s in strategies})
