"""Fail-closed Render release orchestration for APEX production.

The script is intentionally separate from the bot.  It verifies the two exact
active services, disables commit-triggered deploys, deploys one immutable main
commit to the web service first and to the worker second, and waits for each
deploy to become live.  It never touches the suspended legacy worker.
"""

from __future__ import annotations

import argparse
import os
import re
import time
from typing import Any
from urllib.parse import urlencode, urlparse

import requests


API_ROOT = "https://api.render.com/v1"
EXPECTED_REPOSITORY = "https://github.com/vladislavdim/apex-smc-bot"
EXPECTED_SERVICES = {
    "web": ("srv-dacboj7avr4c73ftk9s0", "apex-strategy-stats-web"),
    "worker": ("srv-da9f0d6gekts7381reo0", "apex-smc-bot-1"),
}
TERMINAL_FAILURES = {"build_failed", "update_failed", "canceled", "deactivated"}


class ReleaseError(RuntimeError):
    pass


class RenderReleaseClient:
    def __init__(self, token: str, *, session: Any | None = None, timeout: int = 30):
        if not token:
            raise ReleaseError("RENDER_API_KEY is required")
        self.session = session or requests.Session()
        self.timeout = timeout
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self.session.request(
            method,
            f"{API_ROOT}{path}",
            headers=self.headers,
            timeout=self.timeout,
            **kwargs,
        )
        if response.status_code not in {200, 201, 202}:
            raise ReleaseError(f"Render {method} {path} failed with HTTP {response.status_code}")
        if response.status_code == 202 and not getattr(response, "content", b"present"):
            return {}
        return response.json()

    def service(self, service_id: str) -> dict[str, Any]:
        payload = self._request("GET", f"/services/{service_id}")
        return dict(payload.get("service") or payload)

    def set_auto_deploy(self, service_id: str, enabled: bool) -> dict[str, Any]:
        payload = self._request(
            "PATCH", f"/services/{service_id}", json={"autoDeploy": "yes" if enabled else "no"}
        )
        return dict(payload.get("service") or payload)

    def trigger(self, service_id: str, commit_sha: str) -> dict[str, Any]:
        payload = self._request(
            "POST",
            f"/services/{service_id}/deploys",
            json={"commitId": commit_sha, "clearCache": "do_not_clear"},
        )
        deploy = dict(payload.get("deploy") or payload)
        if not deploy.get("id"):
            raise ReleaseError(f"Render returned no deploy id for {service_id}")
        return deploy

    def deploy(self, service_id: str, deploy_id: str) -> dict[str, Any]:
        payload = self._request("GET", f"/services/{service_id}/deploys/{deploy_id}")
        return dict(payload.get("deploy") or payload)

    def recent_logs(self, service_id: str, *, limit: int = 50, text: str = "") -> list[dict[str, Any]]:
        """Return bounded application logs without exposing the API credential."""
        service = self.service(service_id)
        owner = service.get("owner") if isinstance(service.get("owner"), dict) else {}
        owner_id = str(service.get("ownerId") or owner.get("id") or "").strip()
        if not owner_id:
            raise ReleaseError(f"Render returned no owner id for {service_id}")
        payload = self._request(
            "GET",
            "/logs",
            params={
                "ownerId": owner_id,
                "resource": service_id,
                "type": "app",
                "direction": "backward",
                "limit": max(1, min(int(limit), 100)),
                **({"text": text} if text else {}),
            },
        )
        rows = payload.get("logs") if isinstance(payload, dict) else payload
        return [dict(row) for row in (rows or []) if isinstance(row, dict)]

    def wait_live(self, service_id: str, deploy_id: str, *, timeout_seconds: int = 900,
                  interval_seconds: int = 10) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            deploy = self.deploy(service_id, deploy_id)
            status = str(deploy.get("status") or "").lower()
            if status == "live":
                return deploy
            if status in TERMINAL_FAILURES:
                raise ReleaseError(f"Render deploy {deploy_id} ended as {status}")
            time.sleep(interval_seconds)
        raise ReleaseError(f"Render deploy {deploy_id} did not become live in time")


def verify_service(service: dict[str, Any], *, service_id: str, expected_name: str) -> None:
    if str(service.get("id")) != service_id:
        raise ReleaseError(f"unexpected service id: {service.get('id')}")
    if str(service.get("name")) != expected_name:
        raise ReleaseError(f"unexpected service name for {service_id}: {service.get('name')}")
    if str(service.get("branch")) != "main":
        raise ReleaseError(f"{expected_name} is not connected to main")
    repo = str(service.get("repo") or "").rstrip("/").removesuffix(".git")
    if repo != EXPECTED_REPOSITORY:
        raise ReleaseError(f"{expected_name} is connected to an unexpected repository")
    if str(service.get("suspended") or "not_suspended") != "not_suspended":
        raise ReleaseError(f"{expected_name} is suspended")


def check_health(url: str, *, session: Any | None = None, timeout: int = 30) -> dict[str, Any]:
    transport = session or requests.Session()
    response = transport.get(url, timeout=timeout)
    if response.status_code != 200:
        raise ReleaseError(f"web health failed with HTTP {response.status_code}")
    payload = response.json()
    if not (payload.get("ok") is True or str(payload.get("status")).lower() in {"ok", "healthy"}):
        raise ReleaseError("web health payload is not healthy")
    return payload


def check_worker_ready(
    url: str,
    *,
    commit_sha: str,
    session: Any | None = None,
    timeout_seconds: int = 600,
    interval_seconds: int = 5,
) -> dict[str, Any]:
    transport = session or requests.Session()
    separator = "&" if "?" in url else "?"
    probe_url = f"{url}{separator}{urlencode({'sha': commit_sha})}"
    deadline = time.monotonic() + max(1, int(timeout_seconds))
    last_status = "not_requested"
    while time.monotonic() < deadline:
        try:
            response = transport.get(probe_url, timeout=30)
            payload = response.json()
            last_status = str(payload.get("status") or f"HTTP_{response.status_code}")
            if response.status_code == 200 and payload.get("ready") is True:
                if str(payload.get("release_sha") or "") != commit_sha[:12]:
                    raise ReleaseError("worker readiness returned an unexpected release")
                return payload
        except ReleaseError:
            raise
        except Exception as exc:
            last_status = type(exc).__name__
        time.sleep(max(1, int(interval_seconds)))
    raise ReleaseError(f"worker did not report APEX READY for release: {last_status}")


def controlled_release(
    client: RenderReleaseClient,
    *,
    commit_sha: str,
    health_url: str,
    worker_ready_url: str = "",
) -> dict[str, Any]:
    if len(commit_sha) != 40 or any(ch not in "0123456789abcdef" for ch in commit_sha.lower()):
        raise ReleaseError("release SHA must be a full 40-character hexadecimal commit")

    disable_auto_deploy(client)

    web_id = EXPECTED_SERVICES["web"][0]
    web_deploy = client.trigger(web_id, commit_sha)
    client.wait_live(web_id, str(web_deploy["id"]))
    health = check_health(health_url)

    worker_id = EXPECTED_SERVICES["worker"][0]
    worker_deploy = client.trigger(worker_id, commit_sha)
    client.wait_live(worker_id, str(worker_deploy["id"]))
    if not worker_ready_url:
        parsed = urlparse(health_url)
        worker_ready_url = f"{parsed.scheme}://{parsed.netloc}/health/worker"
    worker_health = check_worker_ready(worker_ready_url, commit_sha=commit_sha)
    return {
        "commit_sha": commit_sha,
        "auto_deploy": "disabled",
        "web_deploy_id": web_deploy["id"],
        "worker_deploy_id": worker_deploy["id"],
        "health": health,
        "worker_health": worker_health,
    }


def disable_auto_deploy(client: RenderReleaseClient) -> dict[str, str]:
    """Disable commit deploys only after both production identities pass preflight."""
    services: dict[str, dict[str, Any]] = {}
    for role, (service_id, expected_name) in EXPECTED_SERVICES.items():
        service = client.service(service_id)
        verify_service(service, service_id=service_id, expected_name=expected_name)
        services[role] = service

    # Only after every read-only preflight passes do we replace commit-triggered
    # deployment with the controlled path.
    for role in ("web", "worker"):
        service_id = EXPECTED_SERVICES[role][0]
        updated = client.set_auto_deploy(service_id, False)
        if str(updated.get("autoDeploy") or "no") != "no":
            raise ReleaseError(f"failed to disable auto deploy for {role}")
    return {role: "no" for role in services}


_SECRET_LOG_PATTERNS = (
    re.compile(r"(?i)([a-z][a-z0-9+.-]*://)[^\s/@]+(?::[^\s/@]*)?@"),
    re.compile(r"(?i)(authorization\s*[:=]\s*bearer\s+)[^\s,;]+"),
    re.compile(r"(?i)(api\.telegram\.org/bot)[^/\s]+"),
    re.compile(r"(?i)((?:token|api[_-]?key|secret|password)\s*[:=]\s*)[^\s,;]+"),
)


def sanitize_log_message(value: Any) -> str:
    """Keep diagnostics useful while preventing credentials from reaching CI logs."""
    message = str(value or "").replace("\r", " ").replace("\n", " ")[:2000]
    for pattern in _SECRET_LOG_PATTERNS:
        message = pattern.sub(r"\1[REDACTED]", message)
    return message


def print_worker_diagnostics(client: RenderReleaseClient, *, service_id: str = "", filtered: bool = False) -> None:
    """Best-effort bounded diagnostics for a failed worker readiness check."""
    try:
        options = {"limit": 100, "text": "/(Backup|Persistence|instance_fencing|runtime_lease|OperationalError|Traceback|ERROR|stale instance|FATAL)/"} if filtered else {}
        rows = client.recent_logs(service_id or EXPECTED_SERVICES["worker"][0], **options)
    except Exception as exc:
        print(f"Render worker diagnostics unavailable: {type(exc).__name__}")
        return
    print(f"Render worker diagnostics ({len(rows)} most recent app logs):")
    for row in reversed(rows):
        timestamp = sanitize_log_message(row.get("timestamp") or row.get("time") or "")
        message = sanitize_log_message(
            row.get("message") or row.get("text") or row.get("log") or ""
        )
        if message:
            print(f"{timestamp} {message}".strip())


def print_database_diagnostics(client: RenderReleaseClient) -> dict[str, Any]:
    """Inspect only the configured dashboard DB; never print connection secrets."""
    connection = None
    try:
        rows = client._request("GET", f"/services/{EXPECTED_SERVICES['web'][0]}/env-vars")
        env = {str((row.get("envVar") or row).get("key") or ""):
               str((row.get("envVar") or row).get("value") or "") for row in rows}
        url = env.get("DATABASE_URL", "")
        host = urlparse(url).hostname or ""
        match = re.match(r"(dpg-[a-z0-9]+-[a-z])(?:\.|$)", host)
        if match:
            resource = client._request("GET", f"/postgres/{match.group(1)}")
            resource = resource.get("postgres") or resource
            safe = {key: resource.get(key) for key in ("id", "status", "suspended", "expiresAt")}
            print(f"Configured dashboard database: {safe}")
            if str(safe.get("status")) == "suspended" or str(safe.get("suspended")) == "suspended":
                return safe
            info = client._request("GET", f"/postgres/{match.group(1)}/connection-info")
            url = info.get("externalConnectionString") or url
        import psycopg2
        connection = psycopg2.connect(url, connect_timeout=5,
            options="-c default_transaction_read_only=on -c statement_timeout=5000")
        with connection.cursor() as cursor:
            cursor.execute("SELECT lease_key, release_sha, generation, expires_at, updated_at, expires_at > NOW() FROM apex_runtime_lease")
            print(f"Runtime lease (read only): {cursor.fetchall()}")
            cursor.execute("SELECT kind, MAX(occurred_at), MAX(received_at) FROM apex_stats_events WHERE kind IN ('runtime_health','worker_heartbeat') GROUP BY kind")
            print(f"Runtime heartbeat timestamps (read only): {cursor.fetchall()}")
        return {"status": "available"}
    except ModuleNotFoundError as exc:
        print(f"Dashboard DB local dependency missing: {type(exc).__name__}")
        return {"status": "dependency_missing"}
    except Exception as exc:
        print(f"Dashboard DB diagnostics unavailable: {type(exc).__name__}: {sanitize_log_message(exc)}")
    finally:
        if connection is not None:
            connection.close()
    return {"status": "unavailable"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sha", required=True)
    parser.add_argument("--health-url", default="https://apex-strategy-stats-web.onrender.com/health")
    parser.add_argument("--worker-ready-url", default="https://apex-strategy-stats-web.onrender.com/health/worker")
    parser.add_argument("--confirmation", default=os.environ.get("APEX_RELEASE_CONFIRMATION", ""))
    parser.add_argument("--disable-only", action="store_true")
    parser.add_argument("--diagnostics-only", action="store_true")
    args = parser.parse_args()
    if args.confirmation != "DEPLOY_APEX_PRODUCTION":
        raise ReleaseError("exact confirmation DEPLOY_APEX_PRODUCTION is required")
    client = RenderReleaseClient(os.environ.get("RENDER_API_KEY", ""))
    if args.diagnostics_only:
        for service_id, name in EXPECTED_SERVICES.values():
            verify_service(client.service(service_id), service_id=service_id, expected_name=name)
            print(f"APEX read-only diagnostics: {name}")
            print_worker_diagnostics(client, service_id=service_id, filtered=(name != "apex-strategy-stats-web"))
        print_database_diagnostics(client)
        return 0
    if args.disable_only:
        disabled = disable_auto_deploy(client)
        print(f"APEX auto deploy disabled: {','.join(sorted(disabled))}")
        return 0
    database = print_database_diagnostics(client)
    if database.get("status") != "available":
        raise ReleaseError("Production PostgreSQL is unavailable; restore and verify the configured database before restarting workers")
    try:
        result = controlled_release(
            client,
            commit_sha=args.sha.lower(),
            health_url=args.health_url,
            worker_ready_url=args.worker_ready_url,
        )
    except ReleaseError:
        print_worker_diagnostics(client)
        raise
    print(
        f"APEX controlled release live: {result['commit_sha'][:8]} "
        f"web={result['web_deploy_id']} worker={result['worker_deploy_id']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
