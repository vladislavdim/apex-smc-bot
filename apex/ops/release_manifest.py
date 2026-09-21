"""Immutable, secret-free manifest for a production APEX release."""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Mapping

from apex.config.settings import ApexConfig
from apex.config.versions import (
    GROQ_PROMPT_VERSION,
    MANAGER_VERSION,
    REGIME_VERSION,
    RISK_VERSION,
    SCHEMA_VERSION,
)


class ReleaseManifestError(RuntimeError):
    pass


@dataclass(frozen=True)
class ReleaseManifest:
    release_sha: str
    strategy_versions: Mapping[str, str]
    manager_version: str
    risk_version: str
    regime_version: str
    groq_prompt_version: str
    schema_version: str
    config_hash: str
    strategy_config_hash: str
    deployed_at: str

    @property
    def production_valid(self) -> bool:
        return bool(re.fullmatch(r"[0-9a-f]{40}", self.release_sha))

    def canonical_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))


def build_release_manifest(
    config: ApexConfig,
    *,
    release_sha: str | None = None,
    deployed_at: str | None = None,
) -> ReleaseManifest:
    moment = deployed_at or datetime.now(timezone.utc).isoformat(timespec="seconds")
    sha = str(release_sha or config.runtime.release_sha).strip().lower()
    versions = dict(config.strategies.versions)
    return ReleaseManifest(
        release_sha=sha,
        strategy_versions=dict(sorted(versions.items())),
        manager_version=MANAGER_VERSION,
        risk_version=RISK_VERSION,
        regime_version=REGIME_VERSION,
        groq_prompt_version=GROQ_PROMPT_VERSION,
        schema_version=SCHEMA_VERSION,
        config_hash=config.safe_config_hash(),
        strategy_config_hash=config.strategies.manifest_hash(),
        deployed_at=moment,
    )


def persist_release_manifest(conn: sqlite3.Connection, manifest: ReleaseManifest) -> None:
    if not manifest.production_valid:
        raise ReleaseManifestError("release_sha_invalid")
    encoded = manifest.canonical_json()
    existing = conn.execute(
        "SELECT manifest_json FROM release_manifests WHERE release_sha=?",
        (manifest.release_sha,),
    ).fetchone()
    if existing is not None and str(existing[0]) != encoded:
        raise ReleaseManifestError("release_manifest_conflict")
    conn.execute(
        """INSERT OR IGNORE INTO release_manifests(
               release_sha,manifest_json,config_hash,strategy_config_hash,deployed_at
           ) VALUES(?,?,?,?,?)""",
        (
            manifest.release_sha,
            encoded,
            manifest.config_hash,
            manifest.strategy_config_hash,
            manifest.deployed_at,
        ),
    )
    conn.commit()


__all__ = [
    "ReleaseManifest", "ReleaseManifestError", "build_release_manifest",
    "persist_release_manifest",
]
