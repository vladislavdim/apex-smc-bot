"""Canonical V3 durable, rollback-safe database persistence.

Render instances are disposable.  The dedicated GitHub backup branch is the
durable source of truth; a local ``brain.db`` is only a runtime working copy.
This module deliberately does not know anything about trading and can fail
without changing scanner or execution behaviour.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import sqlite3
import tempfile
import threading
import time
from datetime import datetime, timezone
from typing import Any

try:
    import requests
except ImportError:  # Unit tests can inject a transport without runtime deps.
    requests = None


_META_TABLE = "brain_persistence_meta"
# The worker composition root configures SQLite WAL for runtime writers.
# Snapshot and read-only validation connections must use the original DB-API
# constructor so temporary files cannot retain untracked WAL sidecars.
_SQLITE_CONNECT = sqlite3.dbapi2.connect


class BrainPersistence:
    """Restore and snapshot one SQLite database through a GitHub branch."""

    def __init__(
        self,
        db_path: str,
        repository: str,
        token: str,
        branch: str = "brain-backups",
        *,
        remote_name: str = "brain.db",
        session: Any | None = None,
        timeout: int = 30,
    ) -> None:
        self.db_path = os.path.abspath(db_path)
        self.repository = (repository or "").strip().strip("/")
        self.token = (token or "").strip()
        self.branch = (branch or "brain-backups").strip() or "brain-backups"
        self.remote_name = str(remote_name or "brain.db").strip().strip("/")
        if not self.remote_name or ".." in self.remote_name.split("/"):
            raise ValueError("invalid remote database name")
        self.timeout = int(timeout)
        if session is None and requests is None:
            raise RuntimeError("requests is required when no GitHub session is injected")
        self.session = session or requests.Session()
        self._lock = threading.Lock()
        self._ready = False
        self._remote_blob_sha = ""
        self._generation = 0
        self._last_content_hash = ""
        self._last_restore_at = ""
        self._last_backup_at = ""
        self._last_error = ""

    @property
    def configured(self) -> bool:
        return bool(self.repository and self.token)

    @property
    def contents_url(self) -> str:
        return f"https://api.github.com/repos/{self.repository}/contents/{self.remote_name}"

    def _headers(self, *, raw: bool = False) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": (
                "application/vnd.github.raw+json"
                if raw
                else "application/vnd.github+json"
            ),
            "X-GitHub-Api-Version": "2022-11-28",
        }

    @staticmethod
    def _github_error(response: Any) -> tuple[str, str]:
        """Return a safe GitHub error classification without headers/tokens."""
        try:
            payload = response.json()
        except Exception:
            payload = {}
        message = str(payload.get("message") or "")[:240] if isinstance(payload, dict) else ""
        lowered = message.lower()
        transient = (
            int(response.status_code) in {429, 500, 502, 503, 504}
            or (int(response.status_code) == 403 and (
                "rate limit" in lowered
                or "secondary" in lowered
                or "temporarily" in lowered
                or "timed out" in lowered
                or "please try again" in lowered
            ))
        )
        return ("transient" if transient else "permanent"), message

    def _remote_metadata(self, ref: str | None = None) -> tuple[dict[str, Any] | None, str]:
        response = self.session.get(
            self.contents_url,
            params={"ref": ref or self.branch},
            headers=self._headers(),
            timeout=self.timeout,
        )
        if response.status_code == 404:
            return None, "not_found"
        if response.status_code != 200:
            raise RuntimeError(f"GitHub metadata HTTP {response.status_code}")
        payload = response.json()
        if not isinstance(payload, dict) or not payload.get("sha"):
            raise RuntimeError(f"GitHub metadata has no {self.remote_name} blob SHA")
        return payload, "ok"

    def _download_remote(self, ref: str | None = None) -> bytes:
        response = self.session.get(
            self.contents_url,
            params={"ref": ref or self.branch},
            headers=self._headers(raw=True),
            timeout=max(self.timeout, 45),
        )
        if response.status_code != 200:
            raise RuntimeError(f"GitHub raw download HTTP {response.status_code}")
        content = bytes(response.content or b"")
        # Compatibility with API clients that ignore the raw media type.
        if not content or content[:1] == b"{":
            try:
                payload = response.json()
                encoded = payload.get("content", "") if isinstance(payload, dict) else ""
                if encoded:
                    content = base64.b64decode(encoded)
            except Exception:
                pass
        if len(content) < 4096:
            raise RuntimeError(f"GitHub {self.remote_name} is unexpectedly small ({len(content)} bytes)")
        return content

    @property
    def _git_api_url(self) -> str:
        return f"https://api.github.com/repos/{self.repository}/git"

    def _upload_via_git_database(
        self, *, snapshot_path: str, current_blob_sha: str, message: str
    ) -> Any:
        """Atomically replace one file through Git's object API.

        GitHub's Contents endpoint can reject otherwise valid large SQLite
        updates with HTTP 422.  The Git database API accepts the same Base64
        blob while preserving fail-closed compare-and-swap semantics: the new
        commit is parented to the branch head observed immediately before the
        upload and the final ref update is never forced.
        """
        ref_url = f"{self._git_api_url}/ref/heads/{self.branch}"
        ref_response = self.session.get(
            ref_url, headers=self._headers(), timeout=self.timeout,
        )
        if ref_response.status_code != 200:
            return ref_response
        head_sha = str(((ref_response.json() or {}).get("object") or {}).get("sha") or "")
        if not head_sha:
            raise RuntimeError("GitHub branch ref has no commit SHA")

        commit_response = self.session.get(
            f"{self._git_api_url}/commits/{head_sha}",
            headers=self._headers(), timeout=self.timeout,
        )
        if commit_response.status_code != 200:
            return commit_response
        tree_sha = str(((commit_response.json() or {}).get("tree") or {}).get("sha") or "")
        if not tree_sha:
            raise RuntimeError("GitHub branch commit has no tree SHA")

        blob_payload_path = ""
        try:
            fd, blob_payload_path = tempfile.mkstemp(
                dir=os.path.dirname(snapshot_path), prefix="brain-blob-", suffix=".json"
            )
            with os.fdopen(fd, "wb") as payload, open(snapshot_path, "rb") as snapshot:
                payload.write(b'{"content":"')
                while True:
                    chunk = snapshot.read(1_048_575)
                    if not chunk:
                        break
                    payload.write(base64.b64encode(chunk))
                payload.write(b'","encoding":"base64"}')
            headers = {**self._headers(), "Content-Type": "application/json"}
            with open(blob_payload_path, "rb") as body:
                blob_response = self.session.post(
                    f"{self._git_api_url}/blobs", headers=headers, data=body,
                    timeout=max(self.timeout, 60),
                )
        finally:
            if blob_payload_path and os.path.exists(blob_payload_path):
                os.unlink(blob_payload_path)
        if blob_response.status_code not in (200, 201):
            return blob_response
        new_blob_sha = str((blob_response.json() or {}).get("sha") or "")
        if not new_blob_sha:
            raise RuntimeError("GitHub accepted blob but returned no SHA")

        # Abort if the file changed while the blob was uploaded.  The final
        # non-forced ref update also protects against any later branch race.
        refreshed, refreshed_state = self._remote_metadata()
        refreshed_blob_sha = str((refreshed or {}).get("sha") or "")
        if refreshed_state != "ok" or refreshed_blob_sha != current_blob_sha:
            raise RuntimeError("GitHub branch changed during atomic backup")

        tree_response = self.session.post(
            f"{self._git_api_url}/trees", headers=self._headers(),
            json={
                "base_tree": tree_sha,
                "tree": [{
                    "path": self.remote_name, "mode": "100644",
                    "type": "blob", "sha": new_blob_sha,
                }],
            },
            timeout=self.timeout,
        )
        if tree_response.status_code not in (200, 201):
            return tree_response
        new_tree_sha = str((tree_response.json() or {}).get("sha") or "")
        commit_create = self.session.post(
            f"{self._git_api_url}/commits", headers=self._headers(),
            json={"message": message, "tree": new_tree_sha, "parents": [head_sha]},
            timeout=self.timeout,
        )
        if commit_create.status_code not in (200, 201):
            return commit_create
        new_commit_sha = str((commit_create.json() or {}).get("sha") or "")
        return self.session.patch(
            f"{self._git_api_url}/refs/heads/{self.branch}",
            headers=self._headers(), json={"sha": new_commit_sha, "force": False},
            timeout=self.timeout,
        )

    def _historical_refs(self, limit: int = 8) -> list[str]:
        response = self.session.get(
            f"https://api.github.com/repos/{self.repository}/commits",
            params={"sha": self.branch, "path": self.remote_name, "per_page": limit},
            headers=self._headers(),
            timeout=self.timeout,
        )
        if response.status_code != 200:
            return []
        payload = response.json()
        if not isinstance(payload, list):
            return []
        refs = []
        for item in payload[1:]:  # current branch head was already attempted
            sha = str((item or {}).get("sha") or "") if isinstance(item, dict) else ""
            if sha:
                refs.append(sha)
        return refs

    @staticmethod
    def _integrity(path: str) -> None:
        connection = _SQLITE_CONNECT(f"file:{path}?mode=ro", uri=True, timeout=15)
        try:
            result = connection.execute("PRAGMA integrity_check").fetchone()
            tables = connection.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
            ).fetchone()
        finally:
            connection.close()
        if not result or str(result[0]).lower() != "ok":
            raise RuntimeError(f"SQLite integrity check failed: {result}")
        if not tables or int(tables[0]) == 0:
            raise RuntimeError("SQLite backup contains no tables")

    @staticmethod
    def _read_meta(path: str) -> tuple[int, str, str]:
        connection = _SQLITE_CONNECT(f"file:{path}?mode=ro", uri=True, timeout=10)
        try:
            exists = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (_META_TABLE,),
            ).fetchone()
            if not exists:
                return 0, "", ""
            row = connection.execute(
                f"SELECT generation, content_hash, backed_up_at FROM {_META_TABLE} WHERE id=1"
            ).fetchone()
            if not row:
                return 0, "", ""
            return int(row[0] or 0), str(row[1] or ""), str(row[2] or "")
        finally:
            connection.close()

    @staticmethod
    def _ensure_meta(connection: sqlite3.Connection) -> None:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_META_TABLE} (
                id INTEGER PRIMARY KEY CHECK(id=1),
                generation INTEGER NOT NULL DEFAULT 0,
                parent_blob_sha TEXT NOT NULL DEFAULT '',
                content_hash TEXT NOT NULL DEFAULT '',
                backed_up_at TEXT NOT NULL DEFAULT '',
                reason TEXT NOT NULL DEFAULT ''
            )
            """
        )
        connection.execute(
            f"""
            INSERT INTO {_META_TABLE}
                (id, generation, parent_blob_sha, content_hash, backed_up_at, reason)
            VALUES (1, 0, '', '', '', '')
            ON CONFLICT(id) DO NOTHING
            """
        )
        connection.commit()

    @staticmethod
    def _logical_hash(path: str) -> str:
        """Hash durable SQLite content, excluding bookkeeping-only rows.

        Hashing the database file itself is incorrect: SQLite header counters
        and persistence metadata change even when APEX learned nothing.  A
        logical dump is stable across restore/deploy cycles.  Heartbeat rows
        are deliberately excluded because they prove liveness, not knowledge.
        """
        digest = hashlib.sha256()
        connection = _SQLITE_CONNECT(f"file:{path}?mode=ro", uri=True, timeout=30)
        try:
            for statement in connection.iterdump():
                lowered = statement.lower()
                if "brain_persistence_meta" in lowered or "heartbeat" in lowered:
                    continue
                digest.update(statement.encode("utf-8", "surrogatepass"))
                digest.update(b"\n")
        finally:
            connection.close()
        return digest.hexdigest()

    @staticmethod
    def _counts(path: str) -> dict[str, int]:
        result: dict[str, int] = {}
        connection = _SQLITE_CONNECT(f"file:{path}?mode=ro", uri=True, timeout=10)
        try:
            for table in ("knowledge",):
                exists = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (table,),
                ).fetchone()
                result[table] = int(
                    connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                ) if exists else 0
        finally:
            connection.close()
        return result

    def restore(self) -> dict[str, Any]:
        """Always restore the verified branch snapshot; never compare file sizes."""
        with self._lock:
            if not self.configured:
                self._last_error = "GitHub persistence is not configured"
                return {"status": "not_configured", "ready": False}
            try:
                metadata, state = self._remote_metadata()
                if state != "ok" or metadata is None:
                    raise FileNotFoundError(f"{self.remote_name} is absent on branch {self.branch}")
                directory = os.path.dirname(self.db_path)
                os.makedirs(directory, exist_ok=True)
                selected_ref = ""
                selected_content = b""
                generation = 0
                content_hash = ""
                backed_up_at = ""
                last_candidate_error = ""
                for ref in [self.branch, *self._historical_refs()]:
                    temp_path = ""
                    try:
                        candidate_meta = metadata if ref == self.branch else self._remote_metadata(ref)[0]
                        if not candidate_meta:
                            continue
                        content = self._download_remote(ref)
                        with tempfile.NamedTemporaryFile(
                            dir=directory, prefix="brain-restore-", suffix=".db", delete=False
                        ) as target:
                            target.write(content)
                            target.flush()
                            os.fsync(target.fileno())
                            temp_path = target.name
                        self._integrity(temp_path)
                        generation, content_hash, backed_up_at = self._read_meta(temp_path)
                        # Sidecars belong to the disposable pre-restore database
                        # and must not be replayed against the downloaded file.
                        for suffix in ("-wal", "-shm"):
                            sidecar = self.db_path + suffix
                            if os.path.exists(sidecar):
                                os.unlink(sidecar)
                        os.replace(temp_path, self.db_path)
                        temp_path = ""
                        selected_ref = ref
                        selected_content = content
                        break
                    except Exception as candidate_error:
                        last_candidate_error = f"{ref[:12]}: {candidate_error}"
                    finally:
                        if temp_path and os.path.exists(temp_path):
                            os.unlink(temp_path)
                if not selected_ref:
                    raise RuntimeError(
                        f"no valid {self.remote_name} in recent backup history ({last_candidate_error})"
                    )
                now = datetime.now(timezone.utc).isoformat()
                self._ready = True
                # CAS always targets the current branch blob.  When a previous
                # valid commit was recovered, the next backup safely replaces
                # the corrupt head with a new verified commit.
                self._remote_blob_sha = str(metadata["sha"])
                self._generation = generation
                self._last_content_hash = content_hash if selected_ref == self.branch else ""
                self._last_restore_at = now
                self._last_backup_at = backed_up_at
                self._last_error = ""
                return {
                    "status": "restored",
                    "ready": True,
                    "blob_sha": self._remote_blob_sha,
                    "generation": generation,
                    "size": len(selected_content),
                    "counts": self._counts(self.db_path),
                    "restored_at": now,
                    "recovered_from": "" if selected_ref == self.branch else selected_ref,
                }
            except Exception as exc:
                self._ready = False
                self._last_error = str(exc)
                return {
                    "status": "restore_failed", "ready": False, "error": str(exc),
                    "reason": "REMOTE_MISSING" if isinstance(exc, FileNotFoundError) else "RESTORE_INVALID",
                }

    def initialize(self, reason: str = "initial") -> dict[str, Any]:
        """Create a missing remote database without overwriting an existing one."""
        with self._lock:
            if not self.configured:
                return {"status": "not_configured", "ready": False}
            temp_path = ""
            try:
                metadata, state = self._remote_metadata()
                if state == "ok" and metadata is not None:
                    return {"status": "remote_exists", "ready": False}
                directory = os.path.dirname(self.db_path)
                os.makedirs(directory, exist_ok=True)
                fd, temp_path = tempfile.mkstemp(
                    dir=directory, prefix="database-initial-", suffix=".db"
                )
                os.close(fd)
                source = _SQLITE_CONNECT(self.db_path, timeout=30, check_same_thread=False)
                target = _SQLITE_CONNECT(temp_path, timeout=30)
                try:
                    source.backup(target)
                finally:
                    target.close()
                    source.close()
                self._integrity(temp_path)
                logical_hash = self._logical_hash(temp_path)
                now = datetime.now(timezone.utc).isoformat()
                target = _SQLITE_CONNECT(temp_path, timeout=15)
                try:
                    self._ensure_meta(target)
                    target.execute(
                        f"""UPDATE {_META_TABLE}
                            SET generation=1,parent_blob_sha='',content_hash=?,
                                backed_up_at=?,reason=? WHERE id=1""",
                        (logical_hash, now, str(reason)[:120]),
                    )
                    target.commit()
                finally:
                    target.close()
                self._integrity(temp_path)
                with open(temp_path, "rb") as snapshot:
                    content = snapshot.read()
                response = self.session.put(
                    self.contents_url,
                    headers=self._headers(),
                    json={
                        "message": f"{self.remote_name} initial {now[:16]} [skip ci]",
                        "content": base64.b64encode(content).decode("ascii"),
                        "branch": self.branch,
                    },
                    timeout=max(self.timeout, 30),
                )
                if response.status_code in (409, 422):
                    return {"status": "concurrent_initialize", "ready": False}
                if response.status_code not in (200, 201):
                    category, message = self._github_error(response)
                    raise RuntimeError(
                        f"GitHub initialize HTTP {response.status_code} {category} {message}".strip()
                    )
                payload = response.json() if hasattr(response, "json") else {}
                new_sha = str((payload.get("content") or {}).get("sha") or "")
                if not new_sha:
                    refreshed, _ = self._remote_metadata()
                    new_sha = str((refreshed or {}).get("sha") or "")
                if not new_sha:
                    raise RuntimeError("GitHub accepted initialize but returned no blob SHA")
                self._ready = True
                self._remote_blob_sha = new_sha
                self._generation = 1
                self._last_content_hash = logical_hash
                self._last_restore_at = now
                self._last_backup_at = now
                self._last_error = ""
                return {
                    "status": "initialized", "ready": True, "saved": True,
                    "blob_sha": new_sha, "generation": 1, "size": len(content),
                }
            except Exception as exc:
                self._ready = False
                self._last_error = str(exc)
                return {"status": "initialize_failed", "ready": False, "error": str(exc)}
            finally:
                if temp_path and os.path.exists(temp_path):
                    os.unlink(temp_path)

    def backup(self, reason: str = "scheduled") -> dict[str, Any]:
        """Upload one consistent snapshot if data changed and this instance is current."""
        with self._lock:
            if not self.configured:
                return {"status": "not_configured", "saved": False}
            if not self._ready:
                # Critical fail-safe: a fresh/old local DB can never overwrite
                # the last known-good remote snapshot after a restore failure.
                return {
                    "status": "blocked_unrestored",
                    "saved": False,
                    "error": self._last_error or "verified restore has not completed",
                }
            temp_path = ""
            payload_path = ""
            try:
                metadata, state = self._remote_metadata()
                if state != "ok" or metadata is None:
                    raise RuntimeError("remote brain.db disappeared after restore")
                current_sha = str(metadata["sha"])
                if current_sha != self._remote_blob_sha:
                    # Another Render instance saved a newer generation.  Never
                    # retry with its SHA: doing so would recreate the rollback.
                    self._last_error = (
                        f"stale instance: restored {self._remote_blob_sha[:12]}, "
                        f"remote is {current_sha[:12]}"
                    )
                    return {
                        "status": "stale_remote",
                        "saved": False,
                        "remote_blob_sha": current_sha,
                        "local_base_sha": self._remote_blob_sha,
                    }

                directory = os.path.dirname(self.db_path)
                fd, temp_path = tempfile.mkstemp(
                    dir=directory, prefix="brain-backup-", suffix=".db"
                )
                os.close(fd)
                source = _SQLITE_CONNECT(self.db_path, timeout=30, check_same_thread=False)
                target = _SQLITE_CONNECT(temp_path, timeout=30)
                try:
                    source.backup(target)
                finally:
                    target.close()
                    source.close()
                self._integrity(temp_path)
                logical_hash = self._logical_hash(temp_path)
                if logical_hash == self._last_content_hash:
                    self._last_error = ""
                    return {"status": "unchanged", "saved": False}

                generation = self._generation + 1
                now = datetime.now(timezone.utc).isoformat()
                target = _SQLITE_CONNECT(temp_path, timeout=15)
                try:
                    self._ensure_meta(target)
                    target.execute(
                        f"""
                        UPDATE {_META_TABLE}
                        SET generation=?, parent_blob_sha=?, content_hash=?,
                            backed_up_at=?, reason=?
                        WHERE id=1
                        """,
                        (generation, current_sha, logical_hash, now, str(reason)[:120]),
                    )
                    target.commit()
                finally:
                    target.close()
                self._integrity(temp_path)
                snapshot_size = os.path.getsize(temp_path)
                message = (
                    f"{self.remote_name} backup {now[:16].replace('T', ' ')} "
                    f"g{generation} [{str(reason)[:32]}] [skip ci]"
                )
                # GitHub's Contents API requires Base64 inside JSON. Reading a
                # 40+ MiB database, encoding it, and then letting requests
                # encode the JSON held roughly three full copies in RAM and
                # caused periodic Render OOM restarts. Production sessions use
                # a disk-backed streaming JSON body; injected test transports
                # keep the small legacy dictionary contract.
                streaming = bool(requests is not None and isinstance(self.session, requests.Session))
                upload = None
                if streaming:
                    fd, payload_path = tempfile.mkstemp(
                        dir=os.path.dirname(temp_path), prefix="brain-upload-", suffix=".json"
                    )
                    with os.fdopen(fd, "wb") as payload, open(temp_path, "rb") as snapshot:
                        payload.write((
                            '{"message":' + json.dumps(message) + ',"content":"'
                        ).encode("utf-8"))
                        # Multiple of three: only the final Base64 chunk may
                        # contain padding, so concatenation stays valid.
                        while True:
                            chunk = snapshot.read(1_048_575)
                            if not chunk:
                                break
                            payload.write(base64.b64encode(chunk))
                        payload.write((
                            '","branch":' + json.dumps(self.branch) +
                            ',"sha":' + json.dumps(current_sha) + '}'
                        ).encode("utf-8"))
                else:
                    with open(temp_path, "rb") as snapshot:
                        content = snapshot.read()
                    upload = {
                        "message": message,
                        "content": base64.b64encode(content).decode("ascii"),
                        "branch": self.branch,
                        "sha": current_sha,
                    }

                def put_snapshot():
                    if not streaming:
                        return self.session.put(
                            self.contents_url, headers=self._headers(), json=upload,
                            timeout=max(self.timeout, 30),
                        )
                    headers = {**self._headers(), "Content-Type": "application/json"}
                    with open(payload_path, "rb") as body:
                        return self.session.put(
                            self.contents_url, headers=headers, data=body,
                            timeout=max(self.timeout, 30),
                        )

                response = None
                # GitHub can return 422 while a previous Contents API commit
                # on the same backup branch is still being validated.  The
                # file blob SHA remains unchanged in that case, so retrying
                # the same CAS write is safe.  This is common during a Render
                # rolling deploy because brain/state/memory share the branch.
                for attempt in range(5):
                    response = put_snapshot()
                    category, _message = self._github_error(response)
                    if response.status_code in (200, 201):
                        break
                    if response.status_code in (409, 422):
                        refreshed, refreshed_state = self._remote_metadata()
                        refreshed_sha = str((refreshed or {}).get("sha") or "")
                        if refreshed_state != "ok" or refreshed_sha != current_sha:
                            self._last_error = (
                                f"concurrent update: restored {current_sha[:12]}, "
                                f"remote is {refreshed_sha[:12] or 'unavailable'}"
                            )
                            return {
                                "status": "stale_remote",
                                "saved": False,
                                "remote_blob_sha": refreshed_sha,
                                "local_base_sha": current_sha,
                            }
                        if attempt < 4:
                            # 409 is normally a short branch-head race.  A 422
                            # validation collision needs a little more time.
                            if response.status_code == 422:
                                time.sleep(2 ** attempt)
                            continue
                        break
                    if category != "transient":
                        break
                    if attempt < 4:
                        time.sleep(2 ** attempt)
                assert response is not None
                if response.status_code in (409, 422) and streaming:
                    response = self._upload_via_git_database(
                        snapshot_path=temp_path,
                        current_blob_sha=current_sha,
                        message=message,
                    )
                if response.status_code not in (200, 201):
                    if response.status_code in (409, 422):
                        self._last_error = f"GitHub concurrent update HTTP {response.status_code}"
                        return {"status": "concurrent_update", "saved": False}
                    category, message = self._github_error(response)
                    suffix = f" ({message})" if message else ""
                    raise RuntimeError(
                        f"GitHub backup HTTP {response.status_code} {category}{suffix}"
                    )
                payload = response.json() if hasattr(response, "json") else {}
                new_sha = str((payload.get("content") or {}).get("sha") or "")
                if not new_sha:
                    refreshed, _ = self._remote_metadata()
                    new_sha = str((refreshed or {}).get("sha") or "")
                if not new_sha:
                    raise RuntimeError("GitHub accepted backup but returned no blob SHA")
                self._remote_blob_sha = new_sha
                self._generation = generation
                self._last_content_hash = logical_hash
                self._last_backup_at = now
                self._last_error = ""
                return {
                    "status": "saved",
                    "saved": True,
                    "blob_sha": new_sha,
                    "generation": generation,
                    "size": snapshot_size,
                    "counts": self._counts(temp_path),
                    "backed_up_at": now,
                }
            except Exception as exc:
                self._last_error = str(exc)
                return {"status": "backup_failed", "saved": False, "error": str(exc)}
            finally:
                if payload_path and os.path.exists(payload_path):
                    os.unlink(payload_path)
                if temp_path and os.path.exists(temp_path):
                    os.unlink(temp_path)

    def status(self) -> dict[str, Any]:
        return {
            "configured": self.configured,
            "ready": self._ready,
            "branch": self.branch,
            "generation": self._generation,
            "remote_blob_sha": self._remote_blob_sha,
            "last_restore_at": self._last_restore_at,
            "last_backup_at": self._last_backup_at,
            "last_error": self._last_error,
        }
