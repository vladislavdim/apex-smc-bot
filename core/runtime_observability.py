"""Passive runtime observability patches for APEX.

This module changes telemetry and the Strategy Lab presentation only. It must
never alter a trading decision, entry, stop, target, RR, or strategy gate.
All patches are fail-open.
"""
from __future__ import annotations

import builtins
import os
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any

_INSTALLED = False
_FAST_TIMING_LOCK = threading.Lock()
_FAST_LIQUIDITY_PENDING: dict[tuple[str, str], dict[str, Any]] = {}
_FAST_BATCH_COUNTER: dict[str, int] = {}

FAST_TIMING_FIELDS = (
    "liquidity_ms",
    "context_15m_ms",
    "htf_ms",
    "btc_ms",
    "zone_4h_ms",
    "trigger_ms",
    "total_pair_ms",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _release_sha() -> str:
    return (os.environ.get("RENDER_GIT_COMMIT") or os.environ.get("GIT_COMMIT") or "").strip()


def _service_instance() -> str:
    return (
        os.environ.get("RENDER_INSTANCE_ID")
        or os.environ.get("HOSTNAME")
        or f"pid:{os.getpid()}"
    ).strip()


def _deploy_id() -> str:
    return (
        os.environ.get("RENDER_DEPLOY_ID")
        or os.environ.get("DEPLOY_ID")
        or _release_sha()
        or "unknown"
    ).strip()


def _metadata(started_at: str | None = None) -> dict[str, str]:
    return {
        "release_sha": _release_sha() or "unknown",
        "service_instance": _service_instance() or "unknown",
        "deploy_id": _deploy_id() or "unknown",
        "started_at": started_at or _utc_now(),
    }


def _runtime_scan_context(audit_module: Any) -> dict[str, Any]:
    try:
        fn = getattr(audit_module, "_runtime_scan_context", None)
        if callable(fn):
            return fn() or {}
    except Exception:
        pass
    return {}


def _install_audit_patch() -> None:
    """Add release identity and passive FAST timing without touching gates."""
    try:
        import asyncio
        import core.setup_audit as audit
    except Exception:
        return

    if getattr(audit, "_release_cohort_runtime_patch", False):
        return

    orig_emit_event = audit.emit_event
    orig_new_attempt = audit._new_attempt
    orig_finish_attempt = audit._finish_attempt
    orig_audit_test = audit.audit_test
    orig_to_thread = asyncio.to_thread

    def emit_event(kind: str, strategy: str, symbol: str, payload: dict[str, Any], *, event_key: str | None = None) -> str:
        try:
            data = dict(payload) if isinstance(payload, dict) else {}
            started = str(data.get("started_at") or "") or _utc_now()
            for key, value in _metadata(started).items():
                data.setdefault(key, value)
            return orig_emit_event(kind, strategy, symbol, data, event_key=event_key)
        except Exception:
            return orig_emit_event(kind, strategy, symbol, payload, event_key=event_key)

    async def audited_to_thread(func: Any, /, *args: Any, **kwargs: Any) -> Any:
        fn_name = str(getattr(func, "__name__", ""))
        runtime = _runtime_scan_context(audit)
        scanner = str(runtime.get("scanner") or "")
        if fn_name != "check_session_liquidity" or scanner != "auto_fast_deal_scan":
            return await orig_to_thread(func, *args, **kwargs)

        symbol = str(args[0] if args else kwargs.get("symbol") or "").upper()
        run_key = str(runtime.get("run_id") or "no-run")
        started = time.monotonic()
        try:
            return await orig_to_thread(func, *args, **kwargs)
        finally:
            elapsed_ms = round((time.monotonic() - started) * 1000.0, 1)
            try:
                with _FAST_TIMING_LOCK:
                    next_index = _FAST_BATCH_COUNTER.get(run_key, 0) + 1
                    _FAST_BATCH_COUNTER[run_key] = next_index
                    _FAST_LIQUIDITY_PENDING[(run_key, symbol)] = {
                        "liquidity_ms": elapsed_ms,
                        "pair_index": next_index,
                    }
                    if len(_FAST_LIQUIDITY_PENDING) > 200:
                        for old_key in list(_FAST_LIQUIDITY_PENDING)[:50]:
                            _FAST_LIQUIDITY_PENDING.pop(old_key, None)
                    if len(_FAST_BATCH_COUNTER) > 100:
                        for old_run in list(_FAST_BATCH_COUNTER)[:25]:
                            _FAST_BATCH_COUNTER.pop(old_run, None)
            except Exception:
                pass

    def new_attempt(strategy: str, subtype: str, fn_name: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
        context = orig_new_attempt(strategy, subtype, fn_name, args, kwargs)
        try:
            context.update(_metadata(str(context.get("started_at") or "") or _utc_now()))
            if str(strategy).upper() == "FAST":
                runtime = _runtime_scan_context(audit)
                run_key = str(runtime.get("run_id") or context.get("run_id") or "no-run")
                symbol = str(context.get("symbol") or "").upper()
                pending = None
                with _FAST_TIMING_LOCK:
                    pending = _FAST_LIQUIDITY_PENDING.pop((run_key, symbol), None)
                timing = context.setdefault("telemetry", {}).setdefault("fast_stage_timing", {})
                if isinstance(pending, dict):
                    timing.update(pending)
                context["_fast_stage_anchor"] = float(context.get("started_monotonic") or time.monotonic())
        except Exception:
            pass
        return context

    def _mark_fast_stage(context: dict[str, Any], field: str, now: float) -> None:
        try:
            timing = context.setdefault("telemetry", {}).setdefault("fast_stage_timing", {})
            anchor = float(context.get("_fast_stage_anchor") or context.get("started_monotonic") or now)
            if field not in timing:
                timing[field] = round(max(0.0, now - anchor) * 1000.0, 1)
            context["_fast_stage_anchor"] = now
        except Exception:
            pass

    def audit_test(code: str, value: Any, label: str = "", condition: str = "", line: int | None = None) -> Any:
        try:
            context = audit._current()
            if isinstance(context, dict) and str(context.get("strategy") or "").upper() == "FAST":
                now = time.monotonic()
                code_s = str(code)
                if code_s == "FAST_LTF_CONTEXT_STRUCTURE":
                    _mark_fast_stage(context, "context_15m_ms", now)
                elif code_s == "FAST_HTF_SUPPORT":
                    _mark_fast_stage(context, "htf_ms", now)
                elif code_s == "FAST_BTC_HARD_CONFLICT":
                    _mark_fast_stage(context, "btc_ms", now)
                elif code_s == "FAST_DETECT_FAST_DEAL_G9172":
                    timing = context.setdefault("telemetry", {}).setdefault("fast_stage_timing", {})
                    if "btc_ms" not in timing:
                        _mark_fast_stage(context, "btc_ms", now)
                elif code_s == "FAST_DETECT_FAST_DEAL_G9242":
                    _mark_fast_stage(context, "zone_4h_ms", now)
                    context["_fast_trigger_started"] = now
        except Exception:
            pass
        return orig_audit_test(code, value, label, condition, line)

    def finish_attempt(context: dict[str, Any], outcome: str, *, candidate: dict[str, Any] | None = None, error: str = "") -> None:
        try:
            if str(context.get("strategy") or "").upper() == "FAST":
                now = time.monotonic()
                timing = context.setdefault("telemetry", {}).setdefault("fast_stage_timing", {})
                trigger_started = context.get("_fast_trigger_started")
                if trigger_started is not None and "trigger_ms" not in timing:
                    timing["trigger_ms"] = round(max(0.0, now - float(trigger_started)) * 1000.0, 1)
                detector_ms = max(0.0, now - float(context.get("started_monotonic") or now)) * 1000.0
                timing["total_pair_ms"] = round(float(timing.get("liquidity_ms") or 0.0) + detector_ms, 1)
        except Exception:
            pass
        return orig_finish_attempt(context, outcome, candidate=candidate, error=error)

    audit.emit_event = emit_event
    audit._new_attempt = new_attempt
    audit._finish_attempt = finish_attempt
    audit.audit_test = audit_test
    asyncio.to_thread = audited_to_thread
    audit._release_cohort_runtime_patch = True


def _release_rows(mod: Any) -> list[dict[str, Any]]:
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
                    ORDER BY last_seen DESC
                    LIMIT 50
                """)
                return [
                    {"sha": str(r[0]), "first_seen": r[1], "last_seen": r[2]}
                    for r in cur.fetchall() if r and r[0]
                ]
        finally:
            conn.close()
    except Exception:
        return []


def _summarize_fast_payloads(mod: Any, payloads: list[dict[str, Any]]) -> dict[str, Any]:
    values: dict[str, list[float]] = {name: [] for name in FAST_TIMING_FIELDS}
    pair_indices: list[float] = []
    attempts = 0
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        telemetry = payload.get("telemetry") if isinstance(payload.get("telemetry"), dict) else {}
        timing = telemetry.get("fast_stage_timing") if isinstance(telemetry.get("fast_stage_timing"), dict) else {}
        if timing:
            attempts += 1
        for name in FAST_TIMING_FIELDS:
            value = mod._num(timing.get(name))
            if value is not None:
                values[name].append(float(value))
        pair_index = mod._num(timing.get("pair_index"))
        if pair_index is not None:
            pair_indices.append(float(pair_index))
    return {
        "metrics": {name: mod._metric_summary(v) for name, v in values.items()},
        "max_pair_index": int(max(pair_indices)) if pair_indices else 0,
        "attempts_with_timing": attempts,
    }


def _fast_timing_summary_db(mod: Any, mode: str, release_sha: str, symbol: str = "",
                            from_date: str = "", to_date: str = "") -> dict[str, Any]:
    where = ["kind='attempt'", "strategy='FAST'"]
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
                cur.execute("SELECT payload FROM apex_stats_events WHERE " + " AND ".join(where) + " ORDER BY occurred_at DESC LIMIT 50000", params)
                payloads = [r[0] for r in cur.fetchall() if r and isinstance(r[0], dict)]
        finally:
            conn.close()
        return _summarize_fast_payloads(mod, payloads)
    except Exception:
        return _summarize_fast_payloads(mod, [])


def _patch_stats_html(html: str) -> str:
    old_tabs = '<div class=tabs id=periods><button class="btn active" data-days=1>24 часа</button><button class=btn data-days=7>7 дней</button><button class=btn data-days=30>30 дней</button><button class=btn id=latestRelease>После последнего deploy</button></div>'
    new_tabs = '<div class=tabs id=periods><button class="btn active" id=currentRelease>Current release</button><button class=btn id=previousRelease>Previous</button><button class=btn id=last24>24h</button><button class=btn id=allHistory>All history</button></div>'
    html = html.replace(old_tabs, new_tabs)
    html = html.replace('Актуальная статистика после #97 · с 14:54:22 UTC 03.09.2026', '<span id=cohortLabel>Current release</span>')
    html = html.replace("let DAYS=1,STRATEGY='',PAGE=1,LAST=null,RELEASE=''", "let DAYS=1,STRATEGY='',PAGE=1,LAST=null,RELEASE='current'")
    html = html.replace(
        '<div class="section card"><h2>Numeric funnel diagnostics</h2><div id=numericDiag></div></div>',
        '<div class="section card"><h2>FAST stage timing</h2><div id=fastTiming></div></div><div class="section card"><h2>Numeric funnel diagnostics</h2><div id=numericDiag></div></div>'
    )
    html = html.replace('det={stop:r.stop,checks:r.checks,candidate:r.candidate,groq_review:r.groq_review,decisions:r.decisions,subtype:r.subtype,function:r.function,run_id:r.run_id,duration_ms:r.duration_ms}',
                        'det={telemetry:r.telemetry,release_sha:r.release_sha,service_instance:r.service_instance,deploy_id:r.deploy_id,started_at:r.started_at,stop:r.stop,checks:r.checks,candidate:r.candidate,groq_review:r.groq_review,decisions:r.decisions,subtype:r.subtype,function:r.function,run_id:r.run_id,duration_ms:r.duration_ms}')
    render_marker = "function render(){updated.textContent='Обновлено '+LAST.generated_at.replace('T',' ').slice(0,19)+' UTC'+(LAST.release_sha?' · '+LAST.release_sha.slice(0,8):'');summary.innerHTML=cards(LAST.summary);renderFunnels();renderBosAge();renderWyCompare();renderNumericDiag();"
    render_repl = "function renderFastTiming(){const ft=LAST.fast_stage_timing||{},m=ft.metrics||{},labels={liquidity_ms:'Liquidity',context_15m_ms:'15m context',htf_ms:'HTF 1h/4h',btc_ms:'BTC context',zone_4h_ms:'4h zone/context',trigger_ms:'15m trigger',total_pair_ms:'Total pair'};fastTiming.innerHTML=Object.entries(labels).map(([k,l])=>{const x=m[k]||{};return `<div class=crit><b>${l}</b><div class=muted>n=${x.count||0}</div><div>P50 ${num(x.median)} ms · P75 ${num(x.p75)} · P90 ${num(x.p90)}</div></div>`}).join('')||'<span class=muted>Нет данных</span>'}function render(){const mode=LAST.cohort_mode||'current',sha=(LAST.release_sha||LAST.current_release_sha||'').slice(0,8),since=(LAST.release_started_at||'').replace('T',' ').slice(0,19);cohortLabel.textContent=mode==='current'?`Current release: ${sha||'—'}${since?' · since '+since+' UTC':''}`:mode==='previous'?`Previous release: ${sha||'—'}`:mode==='24h'?'24h · mixed releases':'All history · mixed releases';updated.textContent='Обновлено '+LAST.generated_at.replace('T',' ').slice(0,19)+' UTC';summary.innerHTML=cards(LAST.summary);renderFunnels();renderBosAge();renderWyCompare();renderFastTiming();renderNumericDiag();"
    html = html.replace(render_marker, render_repl)
    old_handlers = "document.querySelectorAll('#periods .btn').forEach(b=>b.onclick=()=>{document.querySelectorAll('#periods .btn').forEach(x=>x.classList.remove('active'));b.classList.add('active');DAYS=Number(b.dataset.days);PAGE=1;load()});"
    new_handlers = "function setCohort(mode,b){document.querySelectorAll('#periods .btn').forEach(x=>x.classList.remove('active'));b.classList.add('active');RELEASE=mode;DAYS=mode==='24h'?1:30;PAGE=1;load()}currentRelease.onclick=()=>setCohort('current',currentRelease);previousRelease.onclick=()=>setCohort('previous',previousRelease);last24.onclick=()=>setCohort('24h',last24);allHistory.onclick=()=>setCohort('all',allHistory);"
    html = html.replace(old_handlers, new_handlers)
    html = html.replace("latestRelease.onclick=()=>{RELEASE=RELEASE?'':'latest';latestRelease.classList.toggle('active',!!RELEASE);PAGE=1;load()};", "")
    return html


def _patch_stats_module(mod: Any) -> None:
    if getattr(mod, "_release_cohort_runtime_patch", False):
        return
    original = mod.build_dashboard

    def build_dashboard(days: int = 1, strategy: str = "", symbol: str = "", outcome: str = "", groq: str = "",
                        min_rr: float | None = None, max_rr: float | None = None, from_date: str = "", to_date: str = "",
                        page: int = 1, page_size: int = 100, release: str = "current") -> dict[str, Any]:
        mode = str(release or "current").strip().lower()
        releases = _release_rows(mod)
        current = _release_sha() or (releases[0]["sha"] if releases else "")
        ordered = [r["sha"] for r in releases]
        previous = ""
        if current in ordered:
            idx = ordered.index(current)
            if idx + 1 < len(ordered):
                previous = ordered[idx + 1]
        elif ordered:
            previous = ordered[0]

        effective_release = ""
        effective_days = days
        effective_from = from_date
        if mode in {"current", "latest"}:
            effective_release = current
        elif mode == "previous":
            effective_release = previous
        elif mode == "24h":
            effective_days = 1
        elif mode == "all":
            if not effective_from:
                effective_from = mod.STATS_BASELINE_UTC.date().isoformat()
        else:
            effective_release = str(release or "")
            mode = "release"

        result = original(effective_days, strategy, symbol, outcome, groq, min_rr, max_rr,
                          effective_from, to_date, page, page_size, effective_release)
        result["cohort_mode"] = mode
        result["current_release_sha"] = current
        result["previous_release_sha"] = previous
        result["available_releases"] = ordered[:20]
        selected = effective_release
        result["release_sha"] = selected
        selected_row = next((r for r in releases if r["sha"] == selected), None)
        if selected_row and selected_row.get("first_seen"):
            try:
                result["release_started_at"] = selected_row["first_seen"].isoformat()
            except Exception:
                result["release_started_at"] = str(selected_row["first_seen"])
        else:
            result["release_started_at"] = ""
        result["fast_stage_timing"] = _fast_timing_summary_db(
            mod, mode, selected, symbol=symbol, from_date=from_date, to_date=to_date
        )
        return result

    mod.build_dashboard = build_dashboard
    try:
        mod.HTML = _patch_stats_html(mod.HTML)
    except Exception:
        pass
    mod._release_cohort_runtime_patch = True


def _install_stats_patch() -> None:
    if os.path.basename(sys.argv[0] or "") != "stats_server.py":
        return
    original_build_class = builtins.__build_class__

    def patched_build_class(func: Any, name: str, *bases: Any, **kwargs: Any) -> Any:
        cls = original_build_class(func, name, *bases, **kwargs)
        if name == "Handler" and getattr(func, "__module__", "") == "__main__":
            try:
                mod = sys.modules.get("__main__")
                if mod is not None and hasattr(mod, "build_dashboard"):
                    _patch_stats_module(mod)
            except Exception:
                pass
            finally:
                builtins.__build_class__ = original_build_class
        return cls

    builtins.__build_class__ = patched_build_class


def install() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    try:
        _install_audit_patch()
    except Exception:
        pass
    try:
        _install_stats_patch()
    except Exception:
        pass
