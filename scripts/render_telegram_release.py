"""Controlled Telegram release: one pinned deploy for an already fenced worker.

Only the verified worker is changed. Trading is temporarily disabled while
the previous process is stopped and the new GitHub-fenced worker is verified.
The original enabled setting is restored only after a ready bootstrap.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts.render_client import (
    EXPECTED_REPOSITORY, EXPECTED_SERVICES, RenderReleaseClient, ReleaseError, verify_service,
)


def read_environment(client, service_id):
    rows = client._request('GET',f'/services/{service_id}/env-vars',params={'limit':100})
    return {str((row.get('envVar') or row).get('key') or ''):
            str((row.get('envVar') or row).get('value') or '') for row in rows}


def set_environment(client, service_id, key, value):
    client._request('PUT',f'/services/{service_id}/env-vars/{key}',json={'value':value})


def wait_stopped(client, service_id, *, shutdown_seconds=30):
    deadline = time.monotonic()+120
    while time.monotonic()<deadline:
        if client.service(service_id).get('suspended')=='suspended':
            break
        time.sleep(5)
    else:
        raise ReleaseError('worker suspension was not confirmed')
    # Render enforces its configured shutdown grace; do not start a new backend
    # while an old process might still use the previous lease system.
    deadline=time.monotonic()+shutdown_seconds+5
    while time.monotonic()<deadline:
        time.sleep(min(5,max(0,deadline-time.monotonic())))


def readiness_from_logs(rows, sha, enabled, not_before):
    for row in rows:
        try:
            timestamp=datetime.fromisoformat(str(row.get('timestamp') or '').replace('Z','+00:00'))
            if timestamp.tzinfo is None or timestamp < not_before:
                continue
            message=str(row.get('message') or row.get('text') or '')
            marker='[APEX Release Ready] '
            if marker not in message:
                continue
            value=json.loads(message.split(marker,1)[1])
            if (value.get('release_sha')==sha and value.get('ready') is True
                    and value.get('telegram_only') is True
                    and value.get('lease_backend')=='github'
                    and value.get('auto_trading_enabled') is enabled):
                return {key:value[key] for key in ('release_sha','ready','telegram_only',
                                                  'lease_backend','auto_trading_enabled')}
        except (TypeError,ValueError):
            continue
    return None


def wait_ready(client, service_id, sha, enabled, not_before, *, timeout=1200):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        rows=client.recent_logs(service_id,limit=50,text='APEX Release Ready')
        ready=readiness_from_logs(rows,sha,enabled,not_before)
        if ready:
            return ready
        time.sleep(10)
    raise ReleaseError('Telegram worker did not confirm ready with the requested release and shared GitHub lease')


def controlled_telegram_release(client, *, commit_sha, retire_dashboard=False):
    if not re.fullmatch(r'[0-9a-f]{40}',commit_sha):
        raise ReleaseError('exact immutable release SHA required')
    service_id,name=EXPECTED_SERVICES['worker']
    service=client.service(service_id)
    # A retry may begin while the worker is intentionally suspended.
    verify_service({**service,'suspended':'not_suspended'},service_id=service_id,expected_name=name)
    if service.get('type')!='background_worker':
        raise ReleaseError('Telegram-only release requires the verified background worker')
    shutdown_seconds=int((service.get('serviceDetails') or {}).get('maxShutdownDelaySeconds') or 30)
    if not 1<=shutdown_seconds<=300:
        raise ReleaseError('worker shutdown grace cannot be safely bounded')
    environment=read_environment(client,service_id)
    repository=environment.get('GITHUB_BACKUP_REPO') or environment.get('GITHUB_REPO') or ''
    if repository != EXPECTED_REPOSITORY.removeprefix('https://github.com/') or not environment.get('GITHUB_TOKEN'):
        raise ReleaseError('verified existing GitHub backup credentials are required before stopping the worker')
    original_enabled=environment.get('AUTO_TRADING_ENABLED','false')
    enabled=original_enabled.strip().lower() in {'1','true','yes','on'}
    changed=client.set_auto_deploy(service_id,False)
    if changed.get('autoDeploy')!='no' and changed.get('autoDeployTrigger')!='off':
        raise ReleaseError('worker automatic deployment was not disabled')
    # A backend migration must stop the old owner. Subsequent releases already
    # share the same CAS lease, so a pinned rolling deploy avoids the extra boot
    # caused by resume(). Never roll across different ownership backends.
    fenced = (environment.get('APEX_RUNTIME_LEASE_BACKEND') == 'github'
              and environment.get('APEX_TELEGRAM_ONLY', '').lower() == 'true'
              and service.get('suspended') != 'suspended')
    if not fenced:
        print('Telegram release: stopping the previous worker before lease migration', flush=True)
        client._request('POST', f'/services/{service_id}/suspend')
        wait_stopped(client, service_id, shutdown_seconds=shutdown_seconds)
    desired = {'AUTO_TRADING_ENABLED': 'false', 'APEX_TELEGRAM_ONLY': 'true',
               'APEX_RUNTIME_LEASE_BACKEND': 'github', 'APEX_RUNTIME_LEASE_TTL_SECONDS': '180',
               'PYTHON_VERSION': (Path(__file__).resolve().parents[1] / '.python-version').read_text().strip()}
    for key, value in desired.items():
        if environment.get(key) != value:
            set_environment(client, service_id, key, value)
    if not fenced:
        client._request('POST', f'/services/{service_id}/resume')
    started=datetime.now(timezone.utc)
    deploy=client.trigger(service_id,commit_sha)
    client.wait_live(service_id,deploy['id'])
    ready=wait_ready(client,service_id,commit_sha,False,started)
    if enabled:
        print('Telegram release: fenced bootstrap ready; restoring the original trading setting',flush=True)
        set_environment(client,service_id,'AUTO_TRADING_ENABLED',original_enabled)
        started=datetime.now(timezone.utc)
        deploy=client.trigger(service_id,commit_sha)
        client.wait_live(service_id,deploy['id'])
        ready=wait_ready(client,service_id,commit_sha,True,started)
    result = {'commit_sha':commit_sha,'worker_deploy_id':deploy['id'],'worker_health':ready}
    if retire_dashboard:
        web_id, web_name = EXPECTED_SERVICES['web']
        try:
            web = client.service(web_id)
        except ReleaseError as exc:
            if exc.status_code != 404:
                raise
            # Absence of the exact retired ID is success. Never recreate it or
            # suppress credential, network, or other identity-check failures.
            result['dashboard_deleted'] = True
            return result
        verify_service({**web, 'suspended':'not_suspended'}, service_id=web_id, expected_name=web_name)
        if web.get('type') != 'web_service':
            raise ReleaseError('dashboard retirement requires the verified web service')
        client.set_auto_deploy(web_id, False)
        if web.get('suspended') != 'suspended':
            client._request('POST',f'/services/{web_id}/suspend')
        if client.service(web_id).get('suspended') != 'suspended':
            raise ReleaseError('dashboard suspension was not confirmed; worker is ready')
        result['dashboard_suspended'] = True
    return result


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--sha',required=True)
    parser.add_argument('--retire-dashboard', action='store_true')
    args=parser.parse_args()
    if os.environ.get('APEX_RELEASE_CONFIRMATION')!='DEPLOY_APEX_PRODUCTION':
        raise ReleaseError('exact production confirmation required')
    result=controlled_telegram_release(RenderReleaseClient(os.environ.get('RENDER_API_KEY','')),
                                      commit_sha=args.sha.lower(), retire_dashboard=args.retire_dashboard)
    print('APEX Telegram release ready: '+json.dumps(result,sort_keys=True),flush=True)
    return 0


if __name__=='__main__':
    raise SystemExit(main())
