"""Controlled Telegram-only release with a cold shared-lease backend cutover.

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
from scripts.render_controlled_release import (
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


def controlled_telegram_release(client, *, commit_sha):
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
    print('Telegram release: stopping the verified previous worker before lease migration',flush=True)
    client._request('POST',f'/services/{service_id}/suspend')
    wait_stopped(client,service_id,shutdown_seconds=shutdown_seconds)
    for key,value in {'AUTO_TRADING_ENABLED':'false','APEX_TELEGRAM_ONLY':'true',
                      'APEX_RUNTIME_LEASE_BACKEND':'github','APEX_RUNTIME_LEASE_TTL_SECONDS':'180'}.items():
        set_environment(client,service_id,key,value)
    client._request('POST',f'/services/{service_id}/resume')
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
    return {'commit_sha':commit_sha,'worker_deploy_id':deploy['id'],'worker_health':ready}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--sha',required=True)
    args=parser.parse_args()
    if os.environ.get('APEX_RELEASE_CONFIRMATION')!='DEPLOY_APEX_PRODUCTION':
        raise ReleaseError('exact production confirmation required')
    result=controlled_telegram_release(RenderReleaseClient(os.environ.get('RENDER_API_KEY','')),
                                      commit_sha=args.sha.lower())
    print('APEX Telegram release ready: '+json.dumps(result,sort_keys=True),flush=True)
    return 0


if __name__=='__main__':
    raise SystemExit(main())
