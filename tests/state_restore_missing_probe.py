"""Actual critical State wrapper/initialize/startup guard, synthetic HTTP404."""
import ast,asyncio,base64,hashlib,json,sqlite3,tempfile
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import patch
from apex.db.backup import BrainPersistence
from apex.db.state_db import migrate_state
from apex.db.repositories.deliveries import DeliveryClaimRepository
from tests.state_restore_history_probe import seed
NOW=1793000000.
MODES=['deleted_cold','first_use_cold','deleted_warm','metadata_403','metadata_500','current_valid','old_valid_head']
def blob(b):return hashlib.sha1(b'blob '+str(len(b)).encode()+b'\0'+b).hexdigest()
class Response:
    def __init__(self,payload=None,content=b'',status=200):self.status_code=status;self.content=content;self.payload=payload or {};self.headers={}
    def json(self):return self.payload
class Remote:
    def __init__(self,mode,data):
        self.mode=mode;self.data=data;self.missing=mode not in ('current_valid','old_valid_head');self.puts=0;self.history_reads=0;self.created_payload=None
    def get(self,url,*,params=None,headers=None,timeout=None):
        if url.endswith('/commits'):
            self.history_reads+=1
            return Response([] if self.mode=='first_use_cold' else [{'sha':'delete'},{'sha':'retained-generation41'}])
        if '/contents/' not in url:return Response({'private':True})
        if self.mode=='metadata_403':return Response(status=403)
        if self.mode=='metadata_500':return Response(status=500)
        if (params or {}).get('ref')=='retained-generation41':
            data=self.data
        elif self.missing:return Response(status=404)
        else:data=self.data
        return Response(content=data) if 'raw' in (headers or {}).get('Accept','') else Response({'sha':blob(data),'size':len(data)})
    def put(self,url,*,headers,json,timeout):
        self.puts+=1
        if not self.missing:return Response(status=409)
        if 'sha' in json:return Response(status=409)
        self.data=base64.b64decode(json['content']);self.missing=False;self.created_payload=self.data
        return Response({'content':{'sha':blob(self.data)}},status=201)

def wrapper(manager,path):
    tree=ast.parse(Path('apex/app/worker.py').read_text())
    fn=next(n for n in tree.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='restore_state_db_from_github')
    ns={'asyncio':asyncio,'_STATE_PERSISTENCE':manager,'_V3_CONFIG':NS(),
        '_v3_connect_state':lambda *a:sqlite3.connect(path),'_v3_migrate_state':migrate_state}
    exec(compile(ast.Module(body=[fn],type_ignores=[]),'actual_restore_wrapper','exec'),ns)
    result=asyncio.run(ns['restore_state_db_from_github']())
    guard=next(n for n in ast.walk(tree) if isinstance(n,ast.If) and ast.unparse(n.test)=="_STATE_PERSISTENCE.configured and (not _state_restore.get('ready'))")
    ns['_state_restore']=result
    try:exec(compile(ast.Module(body=[guard],type_ignores=[]),'actual_startup_guard','exec'),ns);allowed=True
    except RuntimeError:allowed=False
    return result,allowed

def run(mode):
    with tempfile.TemporaryDirectory(prefix='r0020-') as td:
        p=str(Path(td)/'state.db');older,claimed=seed(td)
        data=older if mode=='old_valid_head' else claimed
        remote=Remote(mode,data)
        if mode=='deleted_warm':Path(p).write_bytes(data)
        manager=BrainPersistence(p,'research/offline','dummy-offline-token','backups',remote_name='apex_state.db',session=remote)
        with patch('socket.socket.connect',side_effect=AssertionError('network forbidden')):
            result,allowed=wrapper(manager,p)
            if allowed:
                repo=DeliveryClaimRepository(lambda:sqlite3.connect(p));admitted=repo.claim('BTCUSDT:BULLISH:signal',NOW+60,14400)
            else:admitted=None
        keys=[]
        if Path(p).exists():
            with sqlite3.connect(p) as c:
                if c.execute("SELECT 1 FROM sqlite_master WHERE name='delivery_claims'").fetchone():keys=[x[0] for x in c.execute('SELECT cache_key FROM delivery_claims ORDER BY cache_key')]
        return {'mode':mode,'status':result.get('status'),'ready':result.get('ready'),'reason':result.get('reason'),'generation':result.get('generation'),'startup_allowed':allowed,'claim_admitted':admitted,'claim_keys':keys,'remote_puts':remote.puts,'history_reads':remote.history_reads,'remote_was_recreated':remote.created_payload is not None}
