"""Shared runtime lease client used to fence overlapping Render workers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import threading
from typing import Any, Callable, Mapping
from urllib.request import Request, urlopen
from urllib.error import HTTPError


class LeaseError(RuntimeError):
    pass


PostJson = Callable[[str, Mapping[str, Any], Mapping[str, str], float], Mapping[str, Any]]


def _post_json(
    url: str, payload: Mapping[str, Any], headers: Mapping[str, str], timeout: float,
) -> Mapping[str, Any]:
    body = json.dumps(dict(payload), separators=(",", ":")).encode("utf-8")
    request = Request(url, data=body, headers=dict(headers), method="POST")
    try:
        with urlopen(request, timeout=timeout) as response:  # noqa: S310 - configured APEX endpoint
            value = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        try:
            value = json.loads(exc.read().decode("utf-8"))
        except Exception as parse_exc:
            raise LeaseError(f"runtime_lease_http_{exc.code}") from parse_exc
        if isinstance(value, Mapping):
            return value
        raise LeaseError(f"runtime_lease_http_{exc.code}") from exc
    except Exception as exc:
        raise LeaseError(f"runtime_lease_request_failed:{type(exc).__name__}") from exc
    if not isinstance(value, Mapping):
        raise LeaseError("runtime_lease_invalid_response")
    return value


def derive_lease_url(explicit_url: str = "", ingest_url: str = "") -> str:
    explicit = str(explicit_url or "").strip()
    if explicit:
        return explicit
    source = str(ingest_url or "").strip()
    if source.endswith("/ingest"):
        return source[:-7] + "/runtime/lease"
    return ""


@dataclass(frozen=True)
class LeaseState:
    granted: bool
    instance_id: str
    generation: int | None
    expires_at: str | None
    reason: str

    def valid_at(self, now: datetime | None = None) -> bool:
        if not self.granted or not self.expires_at:
            return False
        try:
            expires = datetime.fromisoformat(self.expires_at.replace("Z", "+00:00"))
        except ValueError:
            return False
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        return expires.astimezone(timezone.utc) > (now or datetime.now(timezone.utc))


class InstanceLeaseClient:
    def __init__(
        self,
        url: str,
        token: str,
        instance_id: str,
        release_sha: str,
        *,
        ttl_seconds: int = 60,
        timeout: float = 2.0,
        post_json: PostJson = _post_json,
    ) -> None:
        self.url = str(url or "").strip()
        self.token = str(token or "").strip()
        self.instance_id = str(instance_id or "").strip()
        self.release_sha = str(release_sha or "unknown").strip()
        self.ttl_seconds = max(30, min(int(ttl_seconds), 300))
        self.timeout = max(0.1, float(timeout))
        self._post = post_json
        self._lock = threading.RLock()
        self._state = LeaseState(False, self.instance_id, None, None, "NOT_ACQUIRED")

    @property
    def configured(self) -> bool:
        return bool(self.url and self.token and self.instance_id)

    @property
    def state(self) -> LeaseState:
        with self._lock:
            return self._state

    def _request(self, action: str, *, generation: int | None = None) -> LeaseState:
        if not self.configured:
            state = LeaseState(False, self.instance_id, None, None, "LEASE_NOT_CONFIGURED")
            with self._lock:
                self._state = state
            return state
        payload = {
            "action": action,
            "lease_key": "apex-production-worker",
            "instance_id": self.instance_id,
            "release_sha": self.release_sha,
            "ttl_seconds": self.ttl_seconds,
        }
        if generation is not None:
            payload["generation"] = int(generation)
        value = self._post(
            self.url, payload,
            {"X-APEX-Ingest-Token": self.token, "Content-Type": "application/json"},
            self.timeout,
        )
        state = LeaseState(
            bool(value.get("granted")), self.instance_id,
            int(value["generation"]) if value.get("generation") is not None else None,
            str(value["expires_at"]) if value.get("expires_at") else None,
            str(value.get("reason") or ""),
        )
        with self._lock:
            self._state = state
        return state

    def acquire(self) -> LeaseState:
        return self._request("acquire")

    def renew(self) -> LeaseState:
        current = self.state
        if not current.granted or current.generation is None:
            return self.acquire()
        return self._request("renew", generation=current.generation)

    def release(self) -> LeaseState:
        current = self.state
        if current.generation is None:
            return current
        return self._request("release", generation=current.generation)


__all__ = [
    "InstanceLeaseClient", "LeaseError", "LeaseState", "PostJson", "derive_lease_url",
]
