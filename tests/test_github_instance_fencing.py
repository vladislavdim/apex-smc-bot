import base64
import json
import threading
import unittest
from unittest.mock import patch
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import requests

from apex.ops.github_fencing import GithubInstanceLeaseClient
from apex.ops.instance_fencing import LeaseError


class Response:
    def __init__(self,code,data,now):
        self.status_code=code; self.data=data
        self.headers={'Date':format_datetime(now,usegmt=True)}
    def json(self): return self.data


class Store:
    def __init__(self):
        self.now=datetime(2026,10,13,7,tzinfo=timezone.utc)
        self.value=None; self.sha=None; self.generation=0
        self.lock=threading.Lock(); self.lose_ack=False; self.reject_write=False
        self.puts=[]; self.barrier=None; self.reads=0
    def request(self,method,url,**kwargs):
        with self.lock:
            if '/git/ref/heads/main' in url:
                return Response(200,{'object':{'sha':'a'*40}},self.now)
            if method=='POST': return Response(201,{},self.now)
            if method=='GET':
                self.reads+=1
                if self.value is None: result=Response(404,{},self.now)
                else: result=Response(200,{'sha':self.sha,'content':base64.b64encode(json.dumps(self.value).encode()).decode()},self.now)
                barrier=self.barrier if self.reads<=2 else None
            else:
                body=kwargs['json']; self.puts.append(body)
                if self.reject_write: return Response(403,{},self.now)
                if body.get('sha') != self.sha: return Response(409,{},self.now)
                self.value=json.loads(base64.b64decode(body['content']))
                self.generation+=1; self.sha=f'{self.generation:040x}'
                if self.lose_ack:
                    self.lose_ack=False
                    raise requests.Timeout('response lost after commit')
                return Response(200,{},self.now)
        if barrier: barrier.wait(timeout=5)
        return result


class GithubFencingTests(unittest.TestCase):
    def setUp(self): self.store=Store()
    def client(self,identity='worker'):
        return GithubInstanceLeaseClient('owner/repository','fake-token',identity,'a'*40,
            session=self.store,clock=lambda:self.store.now)
    def test_only_one_owner_even_with_equal_render_identity(self):
        a,b=self.client(),self.client()
        self.assertTrue(a.acquire().granted)
        self.assertFalse(b.acquire().granted)
        self.assertEqual(b.state.reason,'LEASE_HELD')
        self.assertTrue(a.renew().granted)
        self.assertFalse(a.release().granted)
        self.assertTrue(b.acquire().granted)
        self.assertEqual(b.state.generation,2)
        self.assertTrue(all(body['branch']=='apex-runtime-lease' for body in self.store.puts))
    def test_expired_owner_cannot_renew_over_new_owner(self):
        a,b=self.client('one'),self.client('two')
        a.acquire(); old=a.state.generation
        self.store.now+=timedelta(seconds=181)
        self.assertTrue(b.acquire().granted)
        self.assertFalse(a.renew().granted)
        self.assertEqual(b.state.generation,old+1)
    def test_lost_ack_is_recovered_by_exact_nonce_without_second_write(self):
        self.store.lose_ack=True
        a=self.client()
        self.assertTrue(a.acquire().granted)
        self.assertEqual(len(self.store.puts),1)
        self.assertTrue(a.state.valid_at(self.store.now))
    def test_rejected_write_never_grants_local_ownership(self):
        self.store.reject_write=True
        self.assertFalse(self.client().acquire().granted)
    def test_failed_renewal_retains_only_committed_lease_until_safety_deadline(self):
        owner=self.client()
        self.assertTrue(owner.acquire().granted)
        old_expiry=self.store.value['expires_at']
        self.store.reject_write=True
        state=owner.renew()
        self.assertTrue(state.granted)
        self.assertEqual(state.reason,'RENEWAL_DEFERRED')
        self.assertEqual(self.store.value['expires_at'],old_expiry)
        self.assertEqual(datetime.fromisoformat(state.expires_at),
                         datetime.fromisoformat(old_expiry)-timedelta(seconds=40))
        self.store.now+=timedelta(seconds=141)
        self.assertFalse(owner.renew().granted)
    def test_failed_renewal_cannot_keep_lease_after_other_owner_takes_over(self):
        owner=self.client('one'); other=self.client('two')
        self.assertTrue(owner.acquire().granted)
        self.store.now+=timedelta(seconds=181)
        self.assertTrue(other.acquire().granted)
        self.store.reject_write=True
        self.assertFalse(owner.renew().granted)
    def test_corrupt_or_clock_skewed_state_blocks_mutation(self):
        a=self.client(); a.acquire()
        self.store.value['generation']=True
        with self.assertRaises(LeaseError): a.renew()
        self.store.value['generation']=1
        a.clock=lambda:self.store.now+timedelta(seconds=11)
        before=len(self.store.puts)
        with self.assertRaises(LeaseError): a.renew()
        self.assertEqual(len(self.store.puts),before)
    def test_local_mutation_window_ends_before_shared_expiry(self):
        a=self.client(); a.acquire()
        remote=datetime.fromisoformat(self.store.value['expires_at'])
        local=datetime.fromisoformat(a.state.expires_at)
        self.assertEqual((remote-local).total_seconds(),40)
    def test_atomic_race_has_exactly_one_winner(self):
        seed=self.client(); seed.acquire(); seed.release()
        self.store.barrier=threading.Barrier(2); self.store.reads=0
        a,b=self.client('one'),self.client('two')
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda c:c.acquire().granted,(a,b)))
        self.assertEqual(sum(results),1)

    def test_monotonic_deadline_blocks_even_when_wall_clock_lease_looks_valid(self):
        from apex.app.runtime import RuntimeSupervisor
        runtime=RuntimeSupervisor(); runtime.activate()
        future=(datetime.now(timezone.utc)+timedelta(seconds=180)).isoformat()
        with patch('apex.app.runtime.time.monotonic',return_value=100):
            runtime.set_instance_lease(1,future)
        with patch('apex.app.runtime.time.monotonic',return_value=301):
            self.assertFalse(runtime.owns_exchange_management)

