"""Passive strategy-attempt telemetry for APEX.

The module is intentionally fail-open and is not part of signal calculation.
It records what the existing detector actually evaluated, where it stopped, and
what values existed at that moment. Network/database failures are swallowed and
must never affect a trading decision.
"""
from __future__ import annotations

import functools
import json
import logging
import os
import queue
import sqlite3
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

DB_PATH = os.environ.get("APEX_SETUP_AUDIT_DB_PATH", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "setup_audit.db"))
RELEASE_SHA = (os.environ.get("RENDER_GIT_COMMIT") or os.environ.get("GIT_COMMIT") or "").strip()
_MAX_PAYLOAD_CHARS = 60000
_EVENT_QUEUE: "queue.Queue[dict[str, Any]]" = queue.Queue(maxsize=10000)
_WORKER_LOCK = threading.Lock()
_WORKER_STARTED = False
_TLS = threading.local()
_PRIORITY_LOCAL_NAMES = {
    "symbol", "timeframe", "direction", "price", "price_now", "live_price", "current_price",
    "entry", "sl", "tp", "tp1", "tp2", "tp3", "rr", "rr_check", "risk", "reward",
    "score", "q_score", "confluence_score", "confirms", "fast_score", "zone_type", "zone_desc",
    "in_zone", "btc_trend", "direction_4h", "direction_1h", "direction_1d", "htf_dir", "htf_1d",
    "spring_found", "sos_found", "utad_found", "sow_found", "drawdown_pct", "pump_pct",
    "acc_range_pct", "dist_range_pct", "vol_compression", "vol_compressed", "vol_expanding",
    "higher_lows", "entry_drift_pct", "sl_pct", "tp_pct", "tp2_pct", "weekly_warning",
    "_swing_1h_choch", "_swing_fvg_ok", "_swing_pd_ok", "_swing_15m_confirms",
    "_sw_confirms", "_zone_ltf_structure", "_acceptance", "_sweep_candles_ago",
    "_fast_funding_warning", "_swing_funding_warning", "_zone_funding_warning",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _stack() -> list[dict[str, Any]]:
    stack = getattr(_TLS, "audit_stack", None)
    if stack is None:
        stack = []
        _TLS.audit_stack = stack
    return stack


def _current() -> dict[str, Any] | None:
    stack = _stack()
    return stack[-1] if stack else None


def _safe_scalar(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        if isinstance(value, str) and len(value) > 1000:
            return value[:1000] + "…"
        return value
    return None


def _safe_value(value: Any, depth: int = 0) -> Any:
    if depth > 2:
        return None
    scalar = _safe_scalar(value)
    if scalar is not None or value is None:
        return scalar
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in list(value.items())[:40]:
            safe = _safe_value(item, depth + 1)
            if safe is not None:
                out[str(key)[:100]] = safe
        return out
    if isinstance(value, (tuple, list)):
        if len(value) > 20:
            return {"type": type(value).__name__, "count": len(value)}
        out = []
        for item in value[:20]:
            safe = _safe_value(item, depth + 1)
            if safe is not None:
                out.append(safe)
        return out
    return None


def _snapshot_locals(values: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(values, dict):
        return {}
    result: dict[str, Any] = {}
    ordered = [name for name in _PRIORITY_LOCAL_NAMES if name in values]
    ordered.extend(name for name in values if name not in _PRIORITY_LOCAL_NAMES)
    for name in ordered:
        if name.startswith("__") or len(result) >= 90:
            continue
        value = values.get(name)
        if callable(value) or isinstance(value, type(sys)):
            continue
        safe = _safe_value(value)
        if safe is not None:
            result[name] = safe
    while len(json.dumps(result, ensure_ascii=False, default=str)) > 24000 and result:
        result.pop(next(reversed(result)))
    return result


def _candidate_snapshot(candidate: dict[str, Any]) -> dict[str, Any]:
    result = _safe_value(candidate)
    return result if isinstance(result, dict) else {}


def _runtime_scan_context() -> dict[str, Any]:
    try:
        main = sys.modules.get("__main__")
        return {"run_id": getattr(main, "_active_scan_run_id", None), "scanner": getattr(main, "_active_market_scan", None)}
    except Exception:
        return {"run_id": None, "scanner": None}


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("""CREATE TABLE IF NOT EXISTS setup_audit_events (
        event_key TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        strategy TEXT,
        symbol TEXT,
        occurred_at TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        synced INTEGER NOT NULL DEFAULT 0,
        sync_attempts INTEGER NOT NULL DEFAULT 0,
        last_sync_error TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_setup_audit_recent ON setup_audit_events(occurred_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_setup_audit_lookup ON setup_audit_events(strategy,symbol,occurred_at DESC)")
    return conn


def _payload_text(payload: dict[str, Any]) -> str:
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
    if len(text) <= _MAX_PAYLOAD_CHARS:
        return text
    compact = dict(payload)
    compact.pop("context", None)
    compact.pop("market_memory", None)
    compact.pop("historical_zones", None)
    compact.pop("closed_loop_learning", None)
    compact["payload_truncated"] = True
    return json.dumps(compact, ensure_ascii=False, separators=(",", ":"), default=str)[:_MAX_PAYLOAD_CHARS]


def _persist_event(event: dict[str, Any]) -> None:
    try:
        conn = _connect()
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        conn.execute("""INSERT OR REPLACE INTO setup_audit_events
            (event_key,kind,strategy,symbol,occurred_at,payload_json,synced,sync_attempts,last_sync_error)
            VALUES (?,?,?,?,?,?,COALESCE((SELECT synced FROM setup_audit_events WHERE event_key=?),0),
                    COALESCE((SELECT sync_attempts FROM setup_audit_events WHERE event_key=?),0),
                    (SELECT last_sync_error FROM setup_audit_events WHERE event_key=?))""",
            (event["event_key"], event["kind"], event.get("strategy", ""), event.get("symbol", ""),
             event["occurred_at"], _payload_text(payload), event["event_key"], event["event_key"], event["event_key"]))
        conn.execute("DELETE FROM setup_audit_events WHERE occurred_at < datetime('now','-90 days')")
        conn.commit(); conn.close()
    except Exception as exc:
        logging.debug("[SetupAudit] local persistence skipped: %s", exc)


def _post_event(event: dict[str, Any]) -> bool:
    url = os.environ.get("APEX_STATS_INGEST_URL", "").strip()
    token = os.environ.get("APEX_STATS_INGEST_TOKEN", "").strip()
    if not url or not token:
        return False
    try:
        import requests
        response = requests.post(url, json=event,
            headers={"X-APEX-Ingest-Token": token, "Content-Type": "application/json"}, timeout=4)
        if 200 <= response.status_code < 300:
            return True
        raise RuntimeError(f"HTTP {response.status_code}")
    except Exception as exc:
        try:
            conn = _connect()
            conn.execute("UPDATE setup_audit_events SET sync_attempts=sync_attempts+1,last_sync_error=? WHERE event_key=?",
                         (str(exc)[:500], event.get("event_key")))
            conn.commit(); conn.close()
        except Exception:
            pass
        return False


def _mark_synced(event_key: str) -> None:
    try:
        conn = _connect()
        conn.execute("UPDATE setup_audit_events SET synced=1,last_sync_error=NULL WHERE event_key=?", (event_key,))
        conn.commit(); conn.close()
    except Exception:
        pass


def _flush_unsynced(limit: int = 100) -> None:
    if not os.environ.get("APEX_STATS_INGEST_URL") or not os.environ.get("APEX_STATS_INGEST_TOKEN"):
        return
    try:
        conn = _connect()
        rows = conn.execute("""SELECT event_key,kind,strategy,symbol,occurred_at,payload_json
            FROM setup_audit_events WHERE synced=0 ORDER BY occurred_at LIMIT ?""", (int(limit),)).fetchall()
        conn.close()
        for key, kind, strategy, symbol, occurred_at, payload_json in rows:
            try:
                payload = json.loads(payload_json or "{}")
            except Exception:
                payload = {}
            event = {"event_key": key, "kind": kind, "strategy": strategy, "symbol": symbol,
                     "occurred_at": occurred_at, "payload": payload}
            if _post_event(event):
                _mark_synced(key)
            else:
                break
    except Exception:
        return


def _worker() -> None:
    last_retry = 0.0
    while True:
        try:
            event = _EVENT_QUEUE.get(timeout=1.0)
        except queue.Empty:
            event = None
        if event:
            _persist_event(event)
            if _post_event(event):
                _mark_synced(str(event.get("event_key")))
            _EVENT_QUEUE.task_done()
        now = time.monotonic()
        if now - last_retry >= 30.0:
            _flush_unsynced(100)
            last_retry = now


def _ensure_worker() -> None:
    global _WORKER_STARTED
    if _WORKER_STARTED:
        return
    with _WORKER_LOCK:
        if _WORKER_STARTED:
            return
        threading.Thread(target=_worker, name="apex-setup-audit", daemon=True).start()
        _WORKER_STARTED = True


def emit_event(kind: str, strategy: str, symbol: str, payload: dict[str, Any], *, event_key: str | None = None) -> str:
    key = event_key or str(uuid.uuid4())
    payload_data = dict(payload) if isinstance(payload, dict) else {}
    if RELEASE_SHA:
        payload_data.setdefault("release_sha", RELEASE_SHA)
    event = {"event_key": key, "kind": str(kind), "strategy": str(strategy or "").upper(),
             "symbol": str(symbol or "").upper(), "occurred_at": _utc_now(),
             "payload": payload_data}
    try:
        _ensure_worker(); _EVENT_QUEUE.put_nowait(event)
    except Exception:
        pass
    return key


def _new_attempt(strategy: str, subtype: str, fn_name: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    symbol = str(kwargs.get("symbol") or (args[0] if args else "") or "").upper()
    timeframe = str(kwargs.get("timeframe") or (args[1] if len(args) > 1 and isinstance(args[1], str) else "") or "")
    runtime = _runtime_scan_context()
    return {"attempt_key": str(uuid.uuid4()), "strategy": str(strategy).upper(), "subtype": str(subtype or "").upper(),
            "function": fn_name, "symbol": symbol, "timeframe": timeframe, "run_id": runtime.get("run_id"),
            "scanner": runtime.get("scanner"), "started_at": _utc_now(), "started_monotonic": time.monotonic(),
            "checks": [], "telemetry": {}, "finished": False, "stop": None}


def _finish_attempt(context: dict[str, Any], outcome: str, *, candidate: dict[str, Any] | None = None, error: str = "") -> None:
    if context.get("finished"):
        return
    context["finished"] = True
    payload = {"attempt_key": context["attempt_key"], "strategy": context["strategy"], "subtype": context.get("subtype") or "",
               "function": context.get("function") or "", "symbol": context.get("symbol") or "", "timeframe": context.get("timeframe") or "",
               "run_id": context.get("run_id"), "scanner": context.get("scanner"), "started_at": context.get("started_at"),
               "finished_at": _utc_now(), "duration_ms": round((time.monotonic() - float(context.get("started_monotonic") or time.monotonic())) * 1000, 1),
               "outcome": outcome, "stop": context.get("stop"), "checks": context.get("checks", []),
               "telemetry": _safe_value(context.get("telemetry") or {}) or {},
               "candidate": _candidate_snapshot(candidate or {}), "error": str(error)[:2000] if error else ""}
    emit_event("attempt", context["strategy"], context.get("symbol", ""), payload, event_key=context["attempt_key"])


def audit_observe(key: str, value: Any, *, append: bool = False) -> None:
    """Attach fail-open, decision-neutral telemetry to the current strategy attempt."""
    context = _current()
    if context is None:
        return
    try:
        safe = _safe_value(value)
        if safe is None:
            return
        telemetry = context.setdefault("telemetry", {})
        name = str(key or "")[:100]
        if not name:
            return
        if append:
            bucket = telemetry.setdefault(name, [])
            if isinstance(bucket, list):
                bucket.append(safe)
        elif isinstance(telemetry.get(name), dict) and isinstance(safe, dict):
            telemetry[name].update(safe)
        else:
            telemetry[name] = safe
    except Exception:
        pass


def _compact_label(label: str, condition: str = "") -> str:
    text = " ".join(str(label or "").replace("#", "").split())
    if not text:
        text = " ".join(str(condition or "").split())
    return text[:300]


def audit_test(code: str, value: Any, label: str = "", condition: str = "", line: int | None = None) -> Any:
    context = _current()
    if context is not None:
        try:
            context["checks"].append({"code": str(code), "label": _compact_label(label, condition),
                "condition": str(condition)[:800], "line": int(line) if line else None,
                "state": "FAIL" if bool(value) else "PASS", "predicate": bool(value)})
        except Exception:
            pass
    return value


def audit_fail(code: str, label: str = "", values: dict[str, Any] | None = None, condition: str = "", line: int | None = None) -> None:
    context = _current()
    if context is not None and not context.get("finished"):
        context["stop"] = {"code": str(code), "label": _compact_label(label, condition), "condition": str(condition)[:1000],
                           "line": int(line) if line else None, "snapshot": _snapshot_locals(values)}
        _finish_attempt(context, "FILTERED")
    return None


def audit_strategy(strategy: str, subtype: str = "") -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
        if getattr(fn, "_apex_setup_audited", False):
            return fn
        @functools.wraps(fn)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            context = _new_attempt(strategy, subtype, fn.__name__, args, kwargs)
            stack = _stack(); stack.append(context)
            try:
                result = fn(*args, **kwargs)
                if isinstance(result, dict):
                    result.setdefault("_audit_attempt_key", context["attempt_key"])
                    _finish_attempt(context, "PENDING_LTF" if result.get("_pending_ltf") else "CANDIDATE", candidate=result)
                elif result is None:
                    if not context.get("finished"):
                        context["stop"] = {"code": f"{strategy}_UNLABELED", "label": "Unlabelled return None", "condition": "", "line": None, "snapshot": {}}
                        _finish_attempt(context, "FILTERED")
                else:
                    _finish_attempt(context, "OTHER", candidate={"result": _safe_value(result)})
                return result
            except Exception as exc:
                _finish_attempt(context, "ERROR", error=f"{type(exc).__name__}: {exc}"); raise
            finally:
                try:
                    if stack and stack[-1] is context: stack.pop()
                    elif context in stack: stack.remove(context)
                except Exception: pass
        wrapped._apex_setup_audited = True
        return wrapped
    return decorate


def _candidate_attempt_key(candidate: dict[str, Any]) -> str:
    return str(candidate.get("_audit_attempt_key") or "") if isinstance(candidate, dict) else ""


def emit_decision_event(candidate: dict[str, Any], outcome: str, stage: str, reason: str = "", evidence: dict[str, Any] | None = None) -> None:
    try:
        strategy = str(candidate.get("scan_type") or candidate.get("grade") or candidate.get("strategy") or "UNKNOWN").upper()
        payload = {"attempt_key": _candidate_attempt_key(candidate), "symbol": candidate.get("symbol"), "strategy": strategy,
                   "timeframe": candidate.get("timeframe"), "direction": candidate.get("direction"), "outcome": str(outcome).upper(),
                   "stage": str(stage), "reason": str(reason)[:2000], "entry": candidate.get("entry"), "sl": candidate.get("sl"),
                   "tp1": candidate.get("tp1") or candidate.get("tp"), "tp2": candidate.get("tp2"), "tp3": candidate.get("tp3"),
                   "rr": candidate.get("rr"), "evidence": _safe_value(evidence or {})}
        emit_event("decision", strategy, str(candidate.get("symbol") or ""), payload)
    except Exception: pass


def emit_groq_review_event(candidate: dict[str, Any], review: dict[str, Any]) -> None:
    try:
        strategy = str(candidate.get("scan_type") or candidate.get("grade") or candidate.get("strategy") or "UNKNOWN").upper()
        payload = {"attempt_key": _candidate_attempt_key(candidate), "symbol": candidate.get("symbol"), "strategy": strategy,
                   "timeframe": candidate.get("timeframe"), "direction": candidate.get("direction"), "entry": candidate.get("entry"),
                   "sl": candidate.get("sl"), "tp1": candidate.get("tp1") or candidate.get("tp"), "tp2": candidate.get("tp2"),
                   "tp3": candidate.get("tp3"), "rr": candidate.get("rr"), "decision": review.get("decision"),
                   "confidence": review.get("confidence"), "degraded": bool(review.get("degraded")),
                   "reasons": _safe_value(review.get("reasons") or []), "risks": _safe_value(review.get("risks") or []),
                   "setup_assessment": _safe_value(review.get("setup_assessment") or {}), "context": _safe_value(review.get("context") or {}),
                   "news_context": _safe_value(review.get("news_context") or {}), "market_memory": _safe_value(review.get("market_memory") or {}),
                   "historical_zones": _safe_value(review.get("historical_zones") or {}),
                   "closed_loop_learning": _safe_value(review.get("closed_loop_learning") or {})}
        emit_event("groq_review", strategy, str(candidate.get("symbol") or ""), payload)
    except Exception: pass


def emit_scan_event(run_id: int, strategy: str, symbol: str, stage: str, outcome: str, reason_code: str = "", detail: dict[str, Any] | None = None) -> None:
    try:
        payload = {"run_id": int(run_id), "strategy": str(strategy).upper(), "symbol": symbol, "stage": stage,
                   "outcome": outcome, "reason_code": reason_code, "detail": _safe_value(detail or {})}
        emit_event("scan_event", strategy, symbol, payload)
    except Exception: pass
