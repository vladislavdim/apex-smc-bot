"""Bounded-memory Git transport for snapshots rejected by GitHub's JSON APIs."""
from __future__ import annotations

import base64
import os
import re
import subprocess
import tempfile
from types import SimpleNamespace


def push_snapshot(*, remote_url: str, branch: str, remote_name: str,
                  snapshot_path: str, expected_blob_sha: str, message: str,
                  token: str = "", timeout: int = 90):
    """Replace one blob with file CAS and a non-forced branch push.

    Credentials live only in the subprocess environment. Git output is never
    propagated to logs; it can contain authentication information.
    """
    if not re.fullmatch(r"[0-9a-f]{40}", expected_blob_sha):
        raise ValueError("invalid expected snapshot SHA")
    if not remote_name or remote_name.startswith("-") or any(
        part in {"", ".", ".."} for part in remote_name.split("/")
    ):
        raise ValueError("invalid snapshot path")
    if os.path.getsize(snapshot_path) >= 100 * 1024 * 1024:
        raise RuntimeError("compressed snapshot exceeds GitHub's 100 MiB file limit")
    env = {"PATH": os.defpath, "LANG": "C.UTF-8"}
    env.update(GIT_TERMINAL_PROMPT="0", GIT_CONFIG_NOSYSTEM="1",
               GIT_CONFIG_GLOBAL=os.devnull, GIT_AUTHOR_NAME="APEX Backup",
               GIT_AUTHOR_EMAIL="apex-backup@localhost", GIT_COMMITTER_NAME="APEX Backup",
               GIT_COMMITTER_EMAIL="apex-backup@localhost")
    settings = [("pack.threads", "1"), ("pack.windowMemory", "16m"),
                ("http.followRedirects", "false")]
    if token:
        if not re.fullmatch(r"https://github\.com/[\w.-]+/[\w.-]+\.git", remote_url):
            raise ValueError("authenticated Git backup requires a GitHub repository URL")
        credential = base64.b64encode(f"x-access-token:{token}".encode()).decode()
        settings.append(("http.extraHeader", f"Authorization: Basic {credential}"))
    env["GIT_CONFIG_COUNT"] = str(len(settings))
    for index, (key, value) in enumerate(settings):
        env[f"GIT_CONFIG_KEY_{index}"] = key
        env[f"GIT_CONFIG_VALUE_{index}"] = value

    def response(code, sha=""):
        return SimpleNamespace(status_code=code, _apex_backup_phase="git_push",
                               json=lambda: {"content": {"sha": sha}}, text="")

    with tempfile.TemporaryDirectory(prefix="apex-git-backup-") as directory:
        def run(*args, input=None, check=True):
            try:
                result = subprocess.run(["git", *args], cwd=directory, env=env,
                                        input=input, capture_output=True, timeout=timeout)
            except (OSError, subprocess.TimeoutExpired) as exc:
                raise RuntimeError(f"Git snapshot transport unavailable: {type(exc).__name__}") from None
            if check and result.returncode:
                raise RuntimeError(f"Git snapshot transport failed at {args[0]}")
            return result
        run("check-ref-format", f"refs/heads/{branch}")
        run("init", "--bare", ".")
        run("fetch", "--depth=1", "--no-tags", remote_url, f"refs/heads/{branch}")
        observed = run("ls-tree", "FETCH_HEAD", "--", remote_name).stdout.decode().split()
        if len(observed) < 3 or observed[2] != expected_blob_sha:
            return response(409)
        run("read-tree", "FETCH_HEAD")
        blob = run("hash-object", "-w", "--", os.path.abspath(snapshot_path)).stdout.decode().strip()
        run("update-index", "--add", "--cacheinfo", "100644", blob, remote_name)
        tree = run("write-tree").stdout.decode().strip()
        parent = run("rev-parse", "FETCH_HEAD").stdout.decode().strip()
        commit = run("commit-tree", tree, "-p", parent, input=message.encode()).stdout.decode().strip()
        pushed = run("push", remote_url, f"{commit}:refs/heads/{branch}", check=False)
        if pushed.returncode:
            # Caller re-reads metadata to resolve either a race or a lost ack.
            return response(409)
        return response(201, blob)
