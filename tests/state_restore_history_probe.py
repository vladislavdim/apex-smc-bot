"""Actual EDF restore, startup restore wrapper/guard and real State claims.
Only the GitHub HTTP session is fake. No real signals or remote writes.
"""
import ast,asyncio,base64,hashlib,json,sqlite3,tempfile
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import patch
from apex.db.backup import BrainPersistence
from apex.db.state_db import migrate_state
from apex.db.repositories.deliveries import DeliveryClaimRepository
ROOT=Path(__file__).resolve().parent
NOW=1793000000.

def blob(data):return hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()
class Response:
    def __init__(self,payload=None,content=b'',status=200):self.status_code=status;self.content=content;self.payload=payload or {};self.headers={}
    def json(self):return self.payload
class Remote:
    def __init__(self,head,older):self.head=head;self.older=older;self.sha=blob(head);self.puts=0
    def get(self,url,*,params=None,headers=None,timeout=None):
        if url.endswith('/commits'):return Response([{'sha':'head-commit'},{'sha':'older-commit'}])
        if '/contents/' not in url:return Response({'private':True})
        data=self.older if (params or {}).get('ref')=='older-commit' else self.head
        return Response(content=data) if 'raw' in headers.get('Accept','') else Response({'sha':blob(data),'size':len(data)})
    def put(self,url,*,headers,json,timeout):
        self.puts+=1
        if json.get('sha')!=self.sha:return Response(status=409)
        self.head=base64.b64decode(json['content']);self.sha=blob(self.head)
        return Response({'content':{'sha':self.sha}})

def seed(directory):
    p=Path(directory)/'seed.db'
    with sqlite3.connect(p) as c:
        migrate_state(c);BrainPersistence._ensure_meta(c)
        c.execute("UPDATE brain_persistence_meta SET generation=40,backed_up_at='2026-10-26T07:00:00+00:00',reason='synthetic_preclaim'")
    h=BrainPersistence._logical_hash(str(p))
    with sqlite3.connect(p) as c:c.execute('UPDATE brain_persistence_meta SET content_hash=?',(h,))
    older=p.read_bytes()
    claims=DeliveryClaimRepository(lambda:sqlite3.connect(p))
    assert claims.claim('BTCUSDT:BULLISH:signal',NOW,14400)
    assert claims.claim('pair:BTCUSDT',NOW,14400)
    h=BrainPersistence._logical_hash(str(p))
    with sqlite3.connect(p) as c:c.execute("UPDATE brain_persistence_meta SET generation=41,parent_blob_sha=?,content_hash=?,backed_up_at='2026-10-26T07:01:00+00:00',reason='synthetic_claim_checkpoint'",(blob(older),h))
    return older,p.read_bytes()

def restore_wrapper(manager):
    tree=ast.parse(Path('apex/app/worker.py').read_text())
    fn=next(n for n in tree.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='restore_state_db_from_github')
    ns={'asyncio':asyncio,'_STATE_PERSISTENCE':manager}
    exec(compile(ast.Module(body=[fn],type_ignores=[]),'actual_worker_restore','exec'),ns)
    return asyncio.run(ns['restore_state_db_from_github']())

def startup_gate(result):
    tree=ast.parse(Path('apex/app/worker.py').read_text())
    # Execute the unchanged guard used after await restore_state_db_from_github.
    guard=next(n for n in ast.walk(tree) if isinstance(n,ast.If) and ast.unparse(n.test)=="_STATE_PERSISTENCE.configured and (not _state_restore.get('ready'))")
    ns={'_STATE_PERSISTENCE':NS(configured=True),'_state_restore':result}
    try:exec(compile(ast.Module(body=[guard],type_ignores=[]),'actual_startup_guard','exec'),ns)
    except RuntimeError:return False
    return True

def run(mode):
    with tempfile.TemporaryDirectory(prefix='apex-r0019-') as td:
        older,claimed=seed(td)
        remote_name='brain.db' if mode=='brain_history' else 'apex_memory.db' if mode=='memory_history' else 'apex_state.db'
        head=claimed if mode=='state_latest' else b'x'*16+claimed[16:]
        remote=Remote(head,older)
        p=str(Path(td)/'restore.db')
        manager=BrainPersistence(p,'research/offline','dummy-offline-token','backups',remote_name=remote_name,session=remote,compression='gzip')
        with patch('socket.socket.connect',side_effect=AssertionError('network forbidden')):
            result=restore_wrapper(manager) if mode=='state_wrapper' else manager.restore()
            startup_allowed=startup_gate(result) if remote_name=='apex_state.db' else None
            if startup_allowed:
                repo=DeliveryClaimRepository(lambda:sqlite3.connect(p))
                admitted=repo.claim('BTCUSDT:BULLISH:signal',NOW+60,14400)
            else:admitted=None
            backup=manager.backup('signal_delivery_claim') if mode=='state_recheckpoint' else None
        with sqlite3.connect(p) as c:
            keys=[x[0] for x in c.execute('SELECT cache_key FROM delivery_claims ORDER BY cache_key')]
            integrity=c.execute('PRAGMA integrity_check').fetchone()[0]
        return dict(mode=mode,restore_status=result.get('status'),ready=result.get('ready'),reason=result.get('reason'),generation=result.get('generation'),recovered_from=result.get('recovered_from'),startup_allowed=startup_allowed,claim_admitted=admitted,claim_keys=keys,backup_status=backup.get('status') if backup else None,remote_puts=remote.puts,integrity=integrity)
MODES=['state_latest','state_history','state_wrapper','state_recheckpoint','brain_history','memory_history']
