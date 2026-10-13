import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from scripts import render_telegram_release as release


class Client:
    def __init__(self,enabled='true'):
        self.events=[]; self.suspended=False; self.number=0
        self.environment={'AUTO_TRADING_ENABLED':enabled,'GITHUB_TOKEN':'fake',
                          'GITHUB_BACKUP_REPO':'vladislavdim/apex-smc-bot'}
    def service(self,identifier):
        return {'id':identifier,'name':'apex-smc-bot-1','repo':release.EXPECTED_REPOSITORY,
                'branch':'main','type':'background_worker',
                'suspended':'suspended' if self.suspended else 'not_suspended'}
    def set_auto_deploy(self,identifier,enabled):
        self.events.append(('auto',enabled)); return {'autoDeploy':'no'}
    def _request(self,method,path,**kwargs):
        if method=='GET':
            return [{'envVar':{'key':key,'value':value}} for key,value in self.environment.items()]
        self.events.append((method,path,kwargs.get('json')))
        if path.endswith('/suspend'): self.suspended=True
        if path.endswith('/resume'): self.suspended=False
        if '/env-vars/' in path: self.environment[path.rsplit('/',1)[1]]=kwargs['json']['value']
        return {}
    def trigger(self,identifier,sha):
        self.number+=1; self.events.append(('deploy',sha,self.environment['AUTO_TRADING_ENABLED']))
        return {'id':str(self.number)}
    def wait_live(self,identifier,deploy): self.events.append(('live',deploy))


class TelegramReleaseTests(unittest.TestCase):
    def test_cold_cutover_and_preserve_original_trading_setting(self):
        client=Client()
        def ready(c,identifier,sha,enabled,started):
            c.events.append(('ready',enabled)); return {'ready':True}
        with patch.object(release,'wait_stopped'),patch.object(release,'wait_ready',side_effect=ready):
            result=release.controlled_telegram_release(client,commit_sha='a'*40)
        self.assertEqual(result['worker_deploy_id'],'2')
        self.assertEqual([event for event in client.events if event[0]=='deploy'],
                         [('deploy','a'*40,'false'),('deploy','a'*40,'true')])
        self.assertTrue(client.environment['APEX_TELEGRAM_ONLY']=='true')
        self.assertEqual(client.environment['APEX_RUNTIME_LEASE_BACKEND'],'github')
        suspend=next(i for i,event in enumerate(client.events) if event[0]=='POST' and event[1].endswith('/suspend'))
        first_setting=next(i for i,event in enumerate(client.events) if event[0]=='PUT')
        self.assertLess(suspend,first_setting)
        self.assertTrue(all('srv-dacboj7avr4c73ftk9s0' not in str(event) for event in client.events))

    def test_disabled_trading_stays_disabled_with_one_deploy(self):
        client=Client('false')
        with patch.object(release,'wait_stopped'),patch.object(release,'wait_ready',return_value={'ready':True}):
            release.controlled_telegram_release(client,commit_sha='a'*40)
        self.assertEqual(client.number,1)
        self.assertEqual(client.environment['AUTO_TRADING_ENABLED'],'false')

    def test_missing_backup_credentials_blocks_before_mutation(self):
        client=Client(); client.environment.pop('GITHUB_TOKEN')
        with self.assertRaises(release.ReleaseError):
            release.controlled_telegram_release(client,commit_sha='a'*40)
        self.assertEqual(client.events,[])

    def test_failed_bootstrap_does_not_enable_trading(self):
        client=Client()
        with patch.object(release,'wait_stopped'),patch.object(release,'wait_ready',side_effect=release.ReleaseError('not ready')):
            with self.assertRaises(release.ReleaseError):
                release.controlled_telegram_release(client,commit_sha='a'*40)
        self.assertEqual(client.environment['AUTO_TRADING_ENABLED'],'false')
        self.assertEqual(client.number,1)

    def test_old_or_unfenced_or_wrong_mode_health_cannot_confirm_release(self):
        now=datetime.now(timezone.utc)
        good={'release_sha':'a'*40,'ready':True,'telegram_only':True,
              'lease_backend':'github','auto_trading_enabled':True}
        def row(value,time):
            return {'timestamp':time.isoformat(),'message':'WARNING:root:[APEX Release Ready] '+json.dumps(value)}
        self.assertIsNone(release.readiness_from_logs([row(good,now-timedelta(seconds=1))],'a'*40,True,now))
        self.assertIsNone(release.readiness_from_logs([row({**good,'lease_backend':'http'},now)],'a'*40,True,now))
        self.assertIsNone(release.readiness_from_logs([row({**good,'auto_trading_enabled':False},now)],'a'*40,True,now))
        self.assertTrue(release.readiness_from_logs([row(good,now)],'a'*40,True,now)['ready'])
