"""Bounded Render API client and strict production identity checks."""
from __future__ import annotations

import time
from typing import Any

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
        before = self._request("GET", f"/services/{service_id}/deploys", params={"limit": 20})
        previous_ids = {str((row.get("deploy") or row).get("id")) for row in before}
        payload = self._request(
            "POST",
            f"/services/{service_id}/deploys",
            json={"commitId": commit_sha, "clearCache": "do_not_clear"},
        )
        deploy = dict(payload.get("deploy") or payload)
        if not deploy.get("id"):
            # A 202 can acknowledge a deployment without an ID. Resolve the
            # exact newly created commit once; never repeat the POST blindly.
            for _ in range(12):
                rows = self._request("GET", f"/services/{service_id}/deploys", params={"limit": 20})
                candidates = [dict(row.get("deploy") or row) for row in rows]
                candidates = [row for row in candidates if row.get("id")
                              and str(row['id']) not in previous_ids
                              and (row.get('commit') or {}).get('id') == commit_sha]
                if len(candidates) == 1:
                    return candidates[0]
                if len(candidates) > 1:
                    raise ReleaseError("Render deploy acknowledgement is ambiguous; no repeat request sent")
                time.sleep(5)
            raise ReleaseError(f"Render returned no identifiable deploy for {service_id}; no repeat request sent")
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
