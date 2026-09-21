"""Typed configuration boundary for the separately deployed Dashboard."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Mapping

from apex.config.settings import ConfigParseError


DEFAULT_STATS_BASELINE_UTC = "2026-09-10T07:55:47+00:00"


def _port(env: Mapping[str, str]) -> int:
    raw = str(env.get("PORT", "10000")).strip()
    try:
        value = int(raw)
    except ValueError:
        raise ConfigParseError("PORT_INVALID") from None
    if not 1 <= value <= 65535:
        raise ConfigParseError("PORT_OUT_OF_RANGE")
    return value


def _baseline(env: Mapping[str, str]) -> datetime:
    raw = str(env.get("APEX_STATS_BASELINE_UTC") or DEFAULT_STATS_BASELINE_UTC).strip()
    try:
        value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        raise ConfigParseError("APEX_STATS_BASELINE_UTC_INVALID") from None
    if value.tzinfo is None:
        raise ConfigParseError("APEX_STATS_BASELINE_UTC_TZ_REQUIRED")
    return value.astimezone(timezone.utc)


def _bounded_int(env: Mapping[str, str], name: str, default: int, minimum: int, maximum: int) -> int:
    raw = str(env.get(name, default)).strip()
    try:
        value = int(raw)
    except ValueError:
        raise ConfigParseError(f"{name}_INVALID") from None
    if not minimum <= value <= maximum:
        raise ConfigParseError(f"{name}_OUT_OF_RANGE")
    return value


@dataclass(frozen=True)
class DashboardSettings:
    database_url: str = field(repr=False)
    dashboard_token: str = field(repr=False)
    ingest_token: str = field(repr=False)
    market_database_url: str = field(default="", repr=False)
    release_sha: str = ""
    port: int = 10000
    stats_baseline_utc: datetime = datetime.fromisoformat(DEFAULT_STATS_BASELINE_UTC)
    cache_ttl_seconds: int = 45
    cache_max_entries: int = 16

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "DashboardSettings":
        source = os.environ if env is None else env
        return cls(
            database_url=str(source.get("DATABASE_URL") or "").strip(),
            dashboard_token=str(source.get("DASHBOARD_TOKEN") or "").strip(),
            ingest_token=str(source.get("INGEST_TOKEN") or "").strip(),
            market_database_url=str(source.get("APEX_MARKET_DATABASE_URL") or "").strip(),
            release_sha=str(source.get("RENDER_GIT_COMMIT") or source.get("GIT_COMMIT") or "").strip(),
            port=_port(source),
            stats_baseline_utc=_baseline(source),
            cache_ttl_seconds=_bounded_int(source, "APEX_DASHBOARD_CACHE_TTL_SECONDS", 45, 5, 600),
            cache_max_entries=_bounded_int(source, "APEX_DASHBOARD_CACHE_MAX_ENTRIES", 16, 2, 128),
        )

    def validate_startup(self) -> None:
        missing = tuple(
            name
            for name, value in (
                ("DATABASE_URL", self.database_url),
                ("DASHBOARD_TOKEN", self.dashboard_token),
                ("INGEST_TOKEN", self.ingest_token),
            )
            if not value
        )
        if missing:
            raise ConfigParseError("DASHBOARD_REQUIRED_MISSING:" + ",".join(missing))


__all__ = ["DEFAULT_STATS_BASELINE_UTC", "DashboardSettings"]
