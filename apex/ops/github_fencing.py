"""Atomic worker ownership on a dedicated GitHub branch, without PostgreSQL.

Contents updates compare the previous blob SHA. Every write has a new nonce,
so competing acquisitions cannot both succeed. Unknown write results are
recovered by reading that exact nonce; they never grant speculative ownership.
"""
from __future__ import annotations

import base64
import json
import re
import uuid
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import requests

from apex.ops.instance_fencing import InstanceLeaseClient, LeaseError, LeaseState


class GithubInstanceLeaseClient(InstanceLeaseClient):
    BRANCH = "apex-runtime-lease"
    FILE = "worker-lease.json"
    CLOCK_MARGIN = 10
    MUTATION_MARGIN = 40

    def __init__(self, repository, token, instance_id, release_sha, *,
                 ttl_seconds=180, timeout=5, session=None, clock=None):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository or ""):
            raise LeaseError("invalid_lease_repository")
        self.repository = repository
        self.owner_id = str(instance_id) + ":" + uuid.uuid4().hex
        self.session = session or requests.Session()
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        super().__init__(f"https://api.github.com/repos/{repository}", token,
                         instance_id, release_sha, ttl_seconds=ttl_seconds, timeout=timeout)

    def _api(self, method, path, **kwargs):
        try:
            return self.session.request(method, self.url + path, headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "Cache-Control": "no-cache",
            }, timeout=self.timeout, allow_redirects=False, **kwargs)
        except requests.RequestException as exc:
            raise LeaseError("github_lease_transport_unavailable") from exc

    def _server_now(self, response):
        try:
            value = parsedate_to_datetime(response.headers["Date"]).astimezone(timezone.utc)
        except (KeyError, ValueError, TypeError) as exc:
            raise LeaseError("github_lease_server_time_unavailable") from exc
        if abs((self.clock() - value).total_seconds()) > self.CLOCK_MARGIN:
            raise LeaseError("github_lease_clock_skew")
        return value

    def _read(self):
        response = self._api("GET", f"/contents/{self.FILE}", params={"ref": self.BRANCH})
        now = self._server_now(response)
        if response.status_code == 404:
            # Create only the control branch. Never modify main or backup files.
            ref = self._api("GET", "/git/ref/heads/main")
            if ref.status_code != 200:
                raise LeaseError("github_lease_base_unavailable")
            created = self._api("POST", "/git/refs", json={
                "ref": f"refs/heads/{self.BRANCH}", "sha": ref.json()["object"]["sha"]})
            if created.status_code not in {201, 422}:
                raise LeaseError("github_lease_branch_unavailable")
            # The file may still be absent, or another owner may have created it.
            response = self._api("GET", f"/contents/{self.FILE}", params={"ref": self.BRANCH})
            now = self._server_now(response)
            if response.status_code == 404:
                return {}, None, now
        if response.status_code != 200:
            raise LeaseError(f"github_lease_read_http_{response.status_code}")
        document = response.json()
        try:
            value = json.loads(base64.b64decode(document["content"], validate=False))
            if not isinstance(value, dict) or value.get("lease_key") != "apex-production-worker":
                raise ValueError("invalid lease")
            if type(value.get("generation")) is not int or value["generation"] < 1:
                raise ValueError("invalid generation")
            if datetime.fromisoformat(value["expires_at"]).tzinfo is None:
                raise ValueError("timezone required")
        except (KeyError, TypeError, ValueError) as exc:
            raise LeaseError("github_lease_corrupt") from exc
        return value, document["sha"], now

    def _remember(self, value, *, granted, reason):
        expiry = value.get("expires_at")
        # Stop local mutations before the shared expiry even with bounded clock skew.
        if granted:
            expires = datetime.fromisoformat(expiry) - timedelta(seconds=self.MUTATION_MARGIN)
            expiry = expires.isoformat()
            if expires <= self.clock():
                granted, reason = False, "LEASE_EXPIRED"
        self._state = LeaseState(granted, self.instance_id, value.get("generation"), expiry, reason)
        return self._state

    def _request(self, action, *, generation=None):
        with self._lock:
            if not self.configured:
                return self._remember({}, granted=False, reason="LEASE_NOT_CONFIGURED")
            value, sha, now = self._read()
            owner = str(value.get("instance_id") or "")
            valid = bool(owner and datetime.fromisoformat(value["expires_at"]) > now)
            if action == "acquire" and valid and owner != self.owner_id:
                return self._remember(value, granted=False, reason="LEASE_HELD")
            if action != "acquire" and (not valid or owner != self.owner_id
                                        or generation != value.get("generation")):
                return self._remember(value, granted=False, reason="FENCING_TOKEN_MISMATCH")
            proposed = {
                "lease_key": "apex-production-worker",
                "instance_id": "" if action == "release" else self.owner_id,
                "release_sha": self.release_sha,
                "generation": int(value.get("generation") or 0) + (0 if valid else 1),
                "expires_at": (now if action == "release" else now + timedelta(seconds=self.ttl_seconds)).isoformat(),
                "nonce": uuid.uuid4().hex,
            }
            body = {"message": f"APEX runtime lease {action}", "branch": self.BRANCH,
                    "content": base64.b64encode(json.dumps(proposed, sort_keys=True).encode()).decode()}
            if sha:
                body["sha"] = sha
            try:
                response = self._api("PUT", f"/contents/{self.FILE}", json=body)
                committed = response.status_code in {200, 201}
            except LeaseError:
                committed = False
            if not committed:
                observed, _, _ = self._read()
                if observed != proposed:
                    # A failed or ambiguous renewal does not revoke an
                    # already committed lease owned by this process. Keep
                    # only the observed, unexpired generation; never assume
                    # that the proposed extension reached GitHub.
                    if (action == "renew" and observed.get("instance_id") == self.owner_id
                            and observed.get("generation") == generation
                            and datetime.fromisoformat(observed["expires_at"]) > self.clock()):
                        return self._remember(observed, granted=True, reason="RENEWAL_DEFERRED")
                    return self._remember(observed, granted=False,
                                          reason="LEASE_HELD" if action == "acquire" else "FENCING_TOKEN_MISMATCH")
            return self._remember(proposed, granted=action != "release",
                                  reason="RELEASED" if action == "release" else "ACQUIRED")


__all__ = ["GithubInstanceLeaseClient"]
