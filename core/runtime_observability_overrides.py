"""Refinements for release-cohort and FAST timing observability.

Loaded immediately after ``runtime_observability.install()`` and before the
application module defines its handlers. The code is fail-open and observability
only; it never changes a trading predicate or market calculation.
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict
import os
import threading
import time
from typing import Any

from core import runtime_observability as ro

_APPLIED = False
_DASHBOARD_CACHE: "OrderedDict[tuple[Any, ...], tuple[float, dict[str, Any]]]" = OrderedDict()
_DASHBOARD_CACHE_LOCK = threading.Lock()
_DASHBOARD_BUILD_LOCK = threading.Lock()
_DASHBOARD_CACHE_TTL_SECONDS = max(5.0, float(os.environ.get("APEX_DASHBOARD_CACHE_TTL_SECONDS", "45")))
_DASHBOARD_CACHE_MAX_ENTRIES = max(2, int(os.environ.get("APEX_DASHBOARD_CACHE_MAX_ENTRIES", "16")))


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
            # Routine deploys keep one stable denominator. A formula change
            # requires a deliberate baseline bump. Dashboard aggregation can
            # scan tens of thousands of JSON events, so only one request may
            # build it at a time; concurrent browser refreshes get the last
            # completed value instead of multiplying memory/DB load.
            mode = "stable"
            effective_from = from_date
            if not effective_from:
                effective_from = mod.STATS_BASELINE_UTC.date().isoformat()
            cache_key = (
                str(strategy or "").upper(), str(symbol or "").upper(), str(outcome or "").upper(),
                str(groq or "").upper(), min_rr, max_rr, effective_from, to_date,
                max(1, int(page)), max(1, min(500, int(page_size))),
            )

            def cached(now: float, *, fresh_only: bool) -> dict[str, Any] | None:
                with _DASHBOARD_CACHE_LOCK:
                    item = _DASHBOARD_CACHE.get(cache_key)
                    if not item or (fresh_only and now - item[0] > _DASHBOARD_CACHE_TTL_SECONDS):
                        return None
                    _DASHBOARD_CACHE.move_to_end(cache_key)
                    value = dict(item[1])
                    value["dashboard_cache"] = {
                        "status": "HIT" if fresh_only else "STALE_WHILE_REVALIDATE",
                        "age_seconds": round(max(0.0, now - item[0]), 1),
                    }
                    return value

            now = time.monotonic()
            hit = cached(now, fresh_only=True)
            if hit is not None:
                return hit
            acquired = _DASHBOARD_BUILD_LOCK.acquire(blocking=False)
            if not acquired:
                stale = cached(now, fresh_only=False)
                if stale is not None:
                    return stale
                acquired = _DASHBOARD_BUILD_LOCK.acquire(timeout=12.0)
                if not acquired:
                    raise TimeoutError("dashboard aggregation is busy; retry shortly")
            try:
                # A different request might have filled the cache while this
                # request waited for the single-flight lock.
                hit = cached(time.monotonic(), fresh_only=True)
                if hit is not None:
                    return hit
                started = time.monotonic()
                releases = release_rows(mod)
                current = ro._release_sha() or (releases[0]["sha"] if releases else "")
                result = original(
                    30, strategy, symbol, outcome, groq, min_rr, max_rr,
                    effective_from, to_date, page, page_size, "",
                )
                result["cohort_mode"] = mode
                result["current_release_sha"] = current
                result["previous_release_sha"] = ""
                result["available_releases"] = [current] if current else []
                result["release_sha"] = current
                result["release_started_at"] = mod.STATS_BASELINE_UTC.isoformat()
                result["runtime_release_sha"] = current
                result["baseline_started_at"] = mod.STATS_BASELINE_UTC.isoformat()
                result["fast_stage_timing"] = fast_timing_summary_db(
                    mod, mode, "", symbol=symbol, from_date=from_date, to_date=to_date,
                )
                result["dashboard_cache"] = {
                    "status": "MISS",
                    "build_ms": round((time.monotonic() - started) * 1000.0, 1),
                }
                stored_at = time.monotonic()
                with _DASHBOARD_CACHE_LOCK:
                    _DASHBOARD_CACHE[cache_key] = (stored_at, dict(result))
                    _DASHBOARD_CACHE.move_to_end(cache_key)
                    while len(_DASHBOARD_CACHE) > _DASHBOARD_CACHE_MAX_ENTRIES:
                        _DASHBOARD_CACHE.popitem(last=False)
                return result
            except Exception:
                stale = cached(time.monotonic(), fresh_only=False)
                if stale is not None:
                    stale["dashboard_cache"]["status"] = "STALE_AFTER_ERROR"
                    return stale
                raise
            finally:
                _DASHBOARD_BUILD_LOCK.release()

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
