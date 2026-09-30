"""Injected legacy-to-V3 cutover orchestration.

The transport launcher may request a refresh, but import/parity policy lives
here and has no dependency on Telegram, aiohttp or the monolithic worker.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class CutoverSpec:
    label: str
    inhibit_code: str
    parity_error: str
    importer: Callable[..., dict[str, Any]]
    parity_report: Callable[..., dict[str, Any]]
    parity_counts: tuple[str, ...] = ()


def sync_cutover(
    spec: CutoverSpec,
    legacy_factory: Callable[[], Any],
    state_factory: Callable[[], Any],
) -> dict[str, Any]:
    """Import only missing history and verify its immutable State subset."""
    imported = spec.importer(legacy_factory, state_factory, refresh=True)
    parity = spec.parity_report(legacy_factory, state_factory)
    if not parity.get("ok"):
        mismatches = tuple(parity.get("mismatches") or ())[:10]
        raise RuntimeError(spec.parity_error + ":" + ",".join(map(str, mismatches)))
    return {
        **imported,
        **{
            f"parity_{field}": parity[field]
            for field in spec.parity_counts
            if field in parity
        },
        "parity_ok": True,
    }


async def refresh_cutover(
    spec: CutoverSpec,
    sync_call: Callable[[], dict[str, Any]],
    *,
    runtime: Any,
    failed_state: Any,
    report_incident: Callable[..., Any],
    recover_incident: Callable[..., Any],
) -> dict[str, Any]:
    """Run one cutover refresh and apply the shared fail-closed policy."""
    try:
        result = await asyncio.to_thread(sync_call)
    except Exception as exc:
        runtime.fail_component(
            "state_db", spec.inhibit_code, failed_state,
            f"{spec.label} mirror failed: {type(exc).__name__}",
        )
        details = {"error_type": type(exc).__name__}
        # SQLite's structured codes identify lock/schema/I/O failures without
        # publishing SQL, database contents or credentials in an incident.
        for field in ("sqlite_errorcode", "sqlite_errorname"):
            value = getattr(exc, field, None)
            if value is not None:
                details[field] = value
        report_incident(
            spec.inhibit_code, "state_db", "CRITICAL",
            details,
        )
        raise
    runtime.recover_component("state_db", spec.inhibit_code)
    recover_incident(spec.inhibit_code, "state_db")
    return result


__all__ = ["CutoverSpec", "refresh_cutover", "sync_cutover"]
