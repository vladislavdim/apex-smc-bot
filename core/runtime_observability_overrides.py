"""Refinements for release-cohort and FAST timing observability.

Loaded immediately after ``runtime_observability.install()`` and before the
application module defines its handlers. The code is fail-open and observability
only; it never changes a trading predicate or market calculation.
"""
from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from core import runtime_observability as ro

_APPLIED = False


def _mark_elapsed(context: dict[str, Any], field: str, now: float) -> None:
    try:
        timing = context.setdefault("telemetry", {}).setdefault("fast_stage_timing", {})
        anchor = float(context.get("_fast_stage_anchor") or context.get("started_monotonic") or now)
        if field not in timing:
            timing[field] = round(max(0.0, now - anchor) * 1000.0, 1)
        context["_fast_stage_anchor"] = now
    except Exception:
        pass


def _patch_fast_boundaries() -> None:
    try:
        import core.setup_audit as audit
    except Exception:
        return
    if getattr(audit, "_release_cohort_boundary_patch", False):
        return

    current_audit_test = audit.audit_test
    current_to_thread = asyncio.to_thread

    def audit_test(code: str, value: Any, label: str = "", condition: str = "", line: int | None = None) -> Any:
        try:
            context = audit._current()
            if isinstance(context, dict) and str(context.get("strategy") or "").upper() == "FAST":
                code_s = str(code)
                now = time.monotonic()
                if code_s == "FAST_LTF_CONTEXT_DATA":
                    _mark_elapsed(context, "context_15m_ms", now)
                elif code_s.startswith("FAST_DETECT_FAST_DEAL_") and code_s != "FAST_DETECT_FAST_DEAL_G9242":
                    # If a 4h/context gate terminates before the normal 9242
                    # boundary, preserve the elapsed zone-stage timing.
                    try:
                        numeric = int(code_s.rsplit("G", 1)[-1]) if "G" in code_s else 0
                    except Exception:
                        numeric = 0
                    if 9192 <= numeric < 9242 and bool(value):
                        _mark_elapsed(context, "zone_4h_ms", now)
        except Exception:
            pass
        return current_audit_test(code, value, label, condition, line)

    async def timed_to_thread(func: Any, /, *args: Any, **kwargs: Any) -> Any:
        fn_name = str(getattr(func, "__name__", ""))
        runtime = ro._runtime_scan_context(audit)
        if fn_name != "check_session_liquidity" or str(runtime.get("scanner") or "") != "auto_fast_deal_scan":
            return await current_to_thread(func, *args, **kwargs)
        symbol = str(args[0] if args else kwargs.get("symbol") or "").upper()
        run_key = str(runtime.get("run_id") or "no-run")
        result = await current_to_thread(func, *args, **kwargs)
        try:
            if isinstance(result, dict) and not bool(result.get("ok")):
                with ro._FAST_TIMING_LOCK:
                    pending = ro._FAST_LIQUIDITY_PENDING.pop((run_key, symbol), None) or {}
                liquidity_ms = float(pending.get("liquidity_ms") or 0.0)
                pair_index = int(pending.get("pair_index") or 0)
                audit.emit_event(
                    "fast_stage_timing", "FAST", symbol,
                    {
                        "run_id": runtime.get("run_id"),
                        "scanner": runtime.get("scanner"),
                        "started_at": ro._utc_now(),
                        "outcome": "FILTERED",
                        "reason": "LOW_LIQUIDITY",
                        "telemetry": {
                            "fast_stage_timing": {
                                "liquidity_ms": round(liquidity_ms, 1),
                                "total_pair_ms": round(liquidity_ms, 1),
                                "pair_index": pair_index,
                            }
                        },
                    },
                )
        except Exception:
            pass
        return result

    audit.audit_test = audit_test
    asyncio.to_thread = timed_to_thread
    audit._release_cohort_boundary_patch = True


def _patch_stats_globals() -> None:
    original_html_patch = ro._patch_stats_html

    def release_rows(mod: Any) -> list[dict[str, Any]]:
        try:
            conn = mod._connect()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT payload->>'release_sha' AS sha,
                               MIN(occurred_at) AS first_seen,
                               MAX(occurred_at) AS last_seen
                        FROM apex_stats_events
                        WHERE COALESCE(payload->>'release_sha','') <> ''
                        GROUP BY 1
                        ORDER BY first_seen DESC
                        LIMIT 50
                    """)
                    return [
                        {"sha": str(row[0]), "first_seen": row[1], "last_seen": row[2]}
                        for row in cur.fetchall() if row and row[0]
                    ]
            finally:
                conn.close()
        except Exception:
            return []

    def fast_timing_summary_db(mod: Any, mode: str, release_sha: str, symbol: str = "",
                               from_date: str = "", to_date: str = "") -> dict[str, Any]:
        where = ["strategy='FAST'", "kind IN ('attempt','fast_stage_timing')"]
        params: list[Any] = []
        if release_sha:
            where.append("payload->>'release_sha'=%s")
            params.append(release_sha)
        elif mode == "24h":
            where.append("occurred_at >= NOW() - INTERVAL '1 day'")
        elif mode == "all":
            where.append("occurred_at >= %s::timestamptz")
            params.append(mod.STATS_BASELINE_UTC)
        if symbol:
            where.append("symbol=%s")
            params.append(str(symbol).upper())
        if from_date:
            where.append("occurred_at >= %s::date")
            params.append(from_date)
        if to_date:
            where.append("occurred_at < (%s::date + INTERVAL '1 day')")
            params.append(to_date)
        try:
            conn = mod._connect()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT payload FROM apex_stats_events WHERE " + " AND ".join(where) +
                        " ORDER BY occurred_at DESC LIMIT 50000", params,
                    )
                    payloads = [row[0] for row in cur.fetchall() if row and isinstance(row[0], dict)]
            finally:
                conn.close()
            return ro._summarize_fast_payloads(mod, payloads)
        except Exception:
            return ro._summarize_fast_payloads(mod, [])

    def patch_html(html: str) -> str:
        rendered = original_html_patch(html)
        rendered = rendered.replace(
            "const mode=LAST.cohort_mode||'current',sha=(LAST.release_sha||LAST.current_release_sha||'').slice(0,8),since=(LAST.release_started_at||'').replace('T',' ').slice(0,19);",
            "const mode=LAST.cohort_mode||'current',sha=(LAST.release_sha||LAST.current_release_sha||'').slice(0,8),since=LAST.release_started_at?new Date(LAST.release_started_at).toLocaleString('ru-RU',{timeZone:'Europe/Warsaw',hour12:false}):'';",
        )
        rendered = rendered.replace("+' · since '+since+' UTC'", "+' · since '+since+' Warsaw'")
        return rendered

    def patch_stats_module(mod: Any) -> None:
        if getattr(mod, "_release_cohort_runtime_patch", False):
            return
        original = mod.build_dashboard

        def build_dashboard(days: int = 1, strategy: str = "", symbol: str = "", outcome: str = "", groq: str = "",
                            min_rr: float | None = None, max_rr: float | None = None, from_date: str = "", to_date: str = "",
                            page: int = 1, page_size: int = 100, release: str = "current") -> dict[str, Any]:
            mode = str(release or "current").strip().lower()
            releases = release_rows(mod)
            current = ro._release_sha() or (releases[0]["sha"] if releases else "")
            ordered = [row["sha"] for row in releases]
            previous = ""
            if current in ordered:
                idx = ordered.index(current)
                if idx + 1 < len(ordered):
                    previous = ordered[idx + 1]
            elif ordered:
                previous = ordered[0]

            selected = ""
            effective_days = days
            effective_from = from_date
            if mode in {"current", "latest"}:
                selected = current
            elif mode == "previous":
                selected = previous
            elif mode == "24h":
                effective_days = 1
            elif mode == "all":
                if not effective_from:
                    effective_from = mod.STATS_BASELINE_UTC.date().isoformat()
            else:
                selected = str(release or "")
                mode = "release"

            selected_row = next((row for row in releases if row["sha"] == selected), None)
            if selected and not effective_from:
                first_seen = selected_row.get("first_seen") if selected_row else None
                if first_seen is not None:
                    try:
                        effective_from = first_seen.date().isoformat()
                    except Exception:
                        effective_from = str(first_seen)[:10]
                else:
                    effective_from = mod.STATS_BASELINE_UTC.date().isoformat()

            result = original(
                effective_days, strategy, symbol, outcome, groq, min_rr, max_rr,
                effective_from, to_date, page, page_size, selected,
            )
            result["cohort_mode"] = mode
            result["current_release_sha"] = current
            result["previous_release_sha"] = previous
            result["available_releases"] = ordered[:20]
            result["release_sha"] = selected
            if selected_row and selected_row.get("first_seen"):
                try:
                    result["release_started_at"] = selected_row["first_seen"].isoformat()
                except Exception:
                    result["release_started_at"] = str(selected_row["first_seen"])
            else:
                result["release_started_at"] = ""
            result["fast_stage_timing"] = fast_timing_summary_db(
                mod, mode, selected, symbol=symbol, from_date=from_date, to_date=to_date,
            )
            return result

        def ingest_without_history_purge(raw: Any) -> int:
            items = raw if isinstance(raw, list) else [raw]
            events = [x for x in (mod._safe_event(value) for value in items[:500]) if x]
            if not events:
                return 0
            conn = mod._connect()
            try:
                with conn, conn.cursor() as cur:
                    for event in events:
                        cur.execute("""INSERT INTO apex_stats_events(event_key,kind,strategy,symbol,occurred_at,payload)
                            VALUES (%s,%s,%s,%s,%s,%s::jsonb)
                            ON CONFLICT(event_key) DO UPDATE SET kind=EXCLUDED.kind,strategy=EXCLUDED.strategy,
                            symbol=EXCLUDED.symbol,occurred_at=EXCLUDED.occurred_at,payload=EXCLUDED.payload,received_at=NOW()""",
                            (event["event_key"], event["kind"], event["strategy"], event["symbol"], event["occurred_at"],
                             mod.json.dumps(event["payload"], ensure_ascii=False, default=str)))
            finally:
                conn.close()
            return len(events)

        mod.build_dashboard = build_dashboard
        mod.ingest = ingest_without_history_purge
        try:
            mod.HTML = patch_html(mod.HTML)
        except Exception:
            pass
        mod._release_cohort_runtime_patch = True

    ro._release_rows = release_rows
    ro._fast_timing_summary_db = fast_timing_summary_db
    ro._patch_stats_html = patch_html
    ro._patch_stats_module = patch_stats_module


def apply() -> None:
    global _APPLIED
    if _APPLIED:
        return
    _APPLIED = True
    try:
        _patch_fast_boundaries()
    except Exception:
        pass
    try:
        _patch_stats_globals()
    except Exception:
        pass
