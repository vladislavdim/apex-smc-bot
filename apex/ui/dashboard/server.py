"""Protected read-only APEX strategy statistics service.

Canonical V3 Dashboard backend.

This process is deployed separately from the Telegram polling worker but lives in
the same repository/project. It never imports or executes trading code. Passive
telemetry is accepted at /ingest and persisted in Postgres.
"""
from __future__ import annotations

import hmac
import hashlib
import json
import logging
import re
import threading
import time
from collections import Counter, OrderedDict, defaultdict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

import psycopg2
import psycopg2.extras

from apex.strategies.specifications import STRATEGY_CATALOG
from apex.telemetry.dashboard_projection import normalize_incident_snapshot
from apex.ui.dashboard.config import DashboardSettings

_SETTINGS = DashboardSettings.from_env()
DATABASE_URL = _SETTINGS.database_url
DASHBOARD_TOKEN = _SETTINGS.dashboard_token
INGEST_TOKEN = _SETTINGS.ingest_token
MARKET_DATABASE_URL = _SETTINGS.market_database_url
PORT = _SETTINGS.port
# Routine deploys must not reset the production cohort. Bump this
# baseline manually only when a validated formula/settings change is promoted.
STATS_BASELINE_UTC = _SETTINGS.stats_baseline_utc
_DASHBOARD_CACHE: "OrderedDict[tuple[Any, ...], tuple[float, dict[str, Any]]]" = OrderedDict()
_DASHBOARD_CACHE_LOCK = threading.Lock()
_DASHBOARD_BUILD_LOCK = threading.Lock()
_DASHBOARD_PERSIST_CHECKED: set[str] = set()
# Bound one aggregation pass so the free 512 MB web instance cannot be killed
# while materializing tens of thousands of JSON telemetry payloads at once.
# JSONB audit payloads can be large (full strategy evidence/check paths).  A
# 20k fetch has repeatedly exceeded Render Free's 512 MiB limit while Python
# and psycopg2 held both the raw rows and the normalized projection.  The rows
# are newest-first, so 5k retains the current scanner window without allowing
# an authenticated refresh to kill the web process.
MAX_DASHBOARD_EVENTS = 5_000



def _connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    return psycopg2.connect(DATABASE_URL, connect_timeout=8)


def ensure_schema() -> None:
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS apex_stats_events (
                event_key TEXT PRIMARY KEY, kind TEXT NOT NULL, strategy TEXT, symbol TEXT,
                occurred_at TIMESTAMPTZ NOT NULL, payload JSONB NOT NULL,
                received_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_apex_stats_recent ON apex_stats_events(occurred_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_apex_stats_lookup ON apex_stats_events(strategy,symbol,occurred_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_apex_stats_kind ON apex_stats_events(kind,occurred_at DESC)")
            cur.execute("""CREATE TABLE IF NOT EXISTS apex_runtime_lease (
                lease_key TEXT PRIMARY KEY,
                instance_id TEXT NOT NULL,
                release_sha TEXT NOT NULL,
                generation BIGINT NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS apex_stats_dashboard_cache (
                cache_key TEXT PRIMARY KEY, payload JSONB NOT NULL,
                built_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
    finally:
        conn.close()


def _safe_event(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    key, kind = str(raw.get("event_key") or "")[:100], str(raw.get("kind") or "")[:40]
    if not key or not kind:
        return None
    payload = raw.get("payload") if isinstance(raw.get("payload"), dict) else {}
    return {"event_key": key, "kind": kind, "strategy": str(raw.get("strategy") or "")[:40].upper(),
            "symbol": str(raw.get("symbol") or "")[:40].upper(),
            "occurred_at": str(raw.get("occurred_at") or datetime.now(timezone.utc).isoformat())[:64],
            "payload": payload}


def ingest(raw: Any) -> int:
    items = raw if isinstance(raw, list) else [raw]
    events = [x for x in (_safe_event(v) for v in items[:500]) if x]
    if not events:
        return 0
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            for e in events:
                cur.execute("""INSERT INTO apex_stats_events(event_key,kind,strategy,symbol,occurred_at,payload)
                    VALUES (%s,%s,%s,%s,%s,%s::jsonb)
                    ON CONFLICT(event_key) DO UPDATE SET kind=EXCLUDED.kind,strategy=EXCLUDED.strategy,
                    symbol=EXCLUDED.symbol,occurred_at=EXCLUDED.occurred_at,payload=EXCLUDED.payload,received_at=NOW()""",
                    (e["event_key"], e["kind"], e["strategy"], e["symbol"], e["occurred_at"],
                     json.dumps(e["payload"], ensure_ascii=False, default=str)))
    finally:
        conn.close()
    return len(events)


def runtime_lease(raw: Any) -> tuple[dict[str, Any], int]:
    """Atomically acquire/renew/release the one production worker lease."""
    if not isinstance(raw, dict):
        return {"granted": False, "reason": "INVALID_PAYLOAD"}, 400
    action = str(raw.get("action") or "").lower()
    lease_key = str(raw.get("lease_key") or "")[:80]
    instance_id = str(raw.get("instance_id") or "")[:160]
    release_sha = str(raw.get("release_sha") or "").lower()
    try:
        ttl_seconds = max(30, min(int(raw.get("ttl_seconds") or 60), 300))
        requested_generation = int(raw["generation"]) if raw.get("generation") is not None else None
    except (TypeError, ValueError):
        return {"granted": False, "reason": "INVALID_LEASE_VALUES"}, 400
    if action not in {"acquire", "renew", "release"}:
        return {"granted": False, "reason": "INVALID_ACTION"}, 400
    if lease_key != "apex-production-worker" or not instance_id:
        return {"granted": False, "reason": "INVALID_LEASE_IDENTITY"}, 400
    if not re.fullmatch(r"[0-9a-f]{40}", release_sha):
        return {"granted": False, "reason": "INVALID_RELEASE_SHA"}, 400

    now = datetime.now(timezone.utc)
    expires_at = now.timestamp() + ttl_seconds
    expires_dt = datetime.fromtimestamp(expires_at, timezone.utc)
    conn = _connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM apex_runtime_lease WHERE lease_key=%s FOR UPDATE",
                (lease_key,),
            )
            current = cur.fetchone()
            if action == "acquire":
                if current is None:
                    generation = 1
                    cur.execute(
                        """INSERT INTO apex_runtime_lease
                           (lease_key,instance_id,release_sha,generation,expires_at)
                           VALUES(%s,%s,%s,%s,%s)""",
                        (lease_key, instance_id, release_sha, generation, expires_dt),
                    )
                elif str(current["instance_id"]) == instance_id:
                    generation = int(current["generation"])
                    cur.execute(
                        """UPDATE apex_runtime_lease SET release_sha=%s,expires_at=%s,updated_at=NOW()
                           WHERE lease_key=%s""",
                        (release_sha, expires_dt, lease_key),
                    )
                elif current["expires_at"] <= now:
                    generation = int(current["generation"]) + 1
                    cur.execute(
                        """UPDATE apex_runtime_lease SET instance_id=%s,release_sha=%s,
                           generation=%s,expires_at=%s,updated_at=NOW() WHERE lease_key=%s""",
                        (instance_id, release_sha, generation, expires_dt, lease_key),
                    )
                else:
                    return {
                        "granted": False, "reason": "LEASE_HELD",
                        "generation": int(current["generation"]),
                        "expires_at": current["expires_at"].isoformat(),
                    }, 409
                return {
                    "granted": True, "reason": "ACQUIRED", "generation": generation,
                    "expires_at": expires_dt.isoformat(),
                }, 200

            if current is None:
                return {"granted": False, "reason": "LEASE_MISSING"}, 409
            generation = int(current["generation"])
            if str(current["instance_id"]) != instance_id or requested_generation != generation:
                return {
                    "granted": False, "reason": "FENCING_TOKEN_MISMATCH",
                    "generation": generation,
                }, 409
            if action == "renew":
                cur.execute(
                    """UPDATE apex_runtime_lease SET release_sha=%s,expires_at=%s,updated_at=NOW()
                       WHERE lease_key=%s""",
                    (release_sha, expires_dt, lease_key),
                )
                return {
                    "granted": True, "reason": "RENEWED", "generation": generation,
                    "expires_at": expires_dt.isoformat(),
                }, 200
            released_generation = generation + 1
            cur.execute(
                """UPDATE apex_runtime_lease SET generation=%s,expires_at=NOW(),updated_at=NOW()
                   WHERE lease_key=%s""",
                (released_generation, lease_key),
            )
            return {
                "granted": False, "reason": "RELEASED", "generation": released_generation,
                "expires_at": now.isoformat(),
            }, 200
    finally:
        conn.close()


def worker_readiness(expected_sha: str, *, max_age_seconds: int = 90) -> tuple[dict[str, Any], int]:
    """Return only a fresh worker-authored READY heartbeat for this release."""
    sha = str(expected_sha or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        return {"ready": False, "status": "INVALID_RELEASE_SHA"}, 400
    conn = _connect()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT occurred_at,received_at,payload FROM apex_stats_events
                   WHERE kind='runtime_status'
                     AND LOWER(payload->>'release_sha')=%s
                     AND received_at >= NOW() - (%s * INTERVAL '1 second')
                   ORDER BY received_at DESC LIMIT 1""",
                (sha, max(15, min(int(max_age_seconds), 300))),
            )
            row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return {"ready": False, "status": "WORKER_HEARTBEAT_MISSING", "release_sha": sha[:12]}, 503
    payload = row["payload"] if isinstance(row["payload"], dict) else {}
    ready = payload.get("ready") is True and str(payload.get("status") or "") == "READY"
    response = {
        "ready": ready,
        "status": str(payload.get("status") or "UNKNOWN"),
        "health": str(payload.get("health") or "UNKNOWN"),
        "release_sha": sha[:12],
        "new_entries": payload.get("new_entries"),
        "reason_codes": payload.get("reason_codes") or [],
        "started_at": payload.get("started_at"),
        "fencing_generation": payload.get("fencing_generation"),
        "fencing_expires_at": payload.get("fencing_expires_at"),
        "components": payload.get("components") if isinstance(payload.get("components"), dict) else {},
        "occurred_at": row["occurred_at"].isoformat(),
        "received_at": row["received_at"].isoformat(),
    }
    return response, 200 if ready else 503


def _num(value: Any) -> float | None:
    try: return float(value)
    except (TypeError, ValueError): return None


def _reason(text: str) -> str:
    text = " ".join(str(text or "").split())
    text = re.sub(r"\b\d+(?:\.\d+)?%", "#%", text)
    return text[:260] or "без причины"


def _parse_utc(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def _timeframe_seconds(value: Any) -> int | None:
    match = re.fullmatch(r"(\d+)\s*([mhdwMHDW])", str(value or "").strip())
    if not match:
        return None
    unit = match.group(2).lower()
    return int(match.group(1)) * {"m": 60, "h": 3600, "d": 86400, "w": 604800}[unit]


def _market_freshness(last_success: Any, timeframe: Any, now: datetime) -> dict[str, Any]:
    timestamp = _parse_utc(last_success)
    period = _timeframe_seconds(timeframe)
    if timestamp is None or period is None:
        return {"freshness_status": "UNKNOWN", "age_seconds": None, "freshness_sla_seconds": period * 3 if period else None}
    age = max(0, int((now.astimezone(timezone.utc) - timestamp).total_seconds()))
    # Three working candles is deliberately diagnostic-only. It never changes
    # a strategy decision and avoids calling an old successful request fresh.
    sla = max(300, period * 3)
    return {"freshness_status": "FRESH" if age <= sla else "STALE", "age_seconds": age, "freshness_sla_seconds": sla}


def _fetch(days: int, strategy: str, symbol: str, from_date: str = "", to_date: str = "") -> list[dict[str, Any]]:
    days = max(1, min(int(days), 30)); where: list[str] = []; params: list[Any] = []
    if from_date and re.fullmatch(r"\d{4}-\d{2}-\d{2}", from_date):
        where.append("occurred_at >= GREATEST(%s::date, %s::timestamptz)"); params.extend([from_date, STATS_BASELINE_UTC])
    else:
        where.append("occurred_at >= GREATEST(NOW() - (%s * INTERVAL '1 day'), %s::timestamptz)"); params.extend([days, STATS_BASELINE_UTC])
    if to_date and re.fullmatch(r"\d{4}-\d{2}-\d{2}", to_date): where.append("occurred_at < (%s::date + INTERVAL '1 day')"); params.append(to_date)
    # Market-data health is operational telemetry and must remain visible while
    # Strategy Lab is filtered to a concrete strategy.
    operational = "'market_data','incident_snapshot','runtime_status','apex_v2_snapshot'"
    if strategy: where.append(f"(strategy=%s OR kind IN ({operational}))"); params.append(strategy.upper())
    if symbol: where.append(f"(symbol=%s OR kind IN ({operational}))"); params.append(symbol.upper())
    conn = _connect()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT event_key,kind,strategy,symbol,occurred_at,payload "
                "FROM apex_stats_events WHERE " + " AND ".join(where) +
                " ORDER BY occurred_at DESC LIMIT %s",
                [*params, MAX_DASHBOARD_EVENTS],
            )
            rows = cur.fetchall()
    finally: conn.close()
    return [{"event_key": r["event_key"], "kind": r["kind"], "strategy": r["strategy"], "symbol": r["symbol"],
             "occurred_at": r["occurred_at"].isoformat(), "payload": r["payload"] if isinstance(r["payload"], dict) else {}} for r in rows]


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(v) for v in values)
    pos = (len(ordered) - 1) * max(0.0, min(float(fraction), 1.0))
    lo = int(pos); hi = min(lo + 1, len(ordered) - 1)
    if lo == hi:
        return round(ordered[lo], 3)
    weight = pos - lo
    return round(ordered[lo] * (1 - weight) + ordered[hi] * weight, 3)


def _metric_summary(values: list[float]) -> dict[str, Any]:
    clean = [float(v) for v in values if _num(v) is not None]
    return {
        "count": len(clean),
        "min": round(min(clean), 3) if clean else None,
        "p25": _percentile(clean, 0.25), "median": _percentile(clean, 0.50),
        "p75": _percentile(clean, 0.75), "p90": _percentile(clean, 0.90),
        "max": round(max(clean), 3) if clean else None,
    }


def _stop_owner(checks: list[dict[str, Any]], stop: dict[str, Any]) -> tuple[int | None, str]:
    """Resolve one blocking STOP without blaming a check by list order."""
    failed = [(idx, check) for idx, check in enumerate(checks)
              if str(check.get("state") or "").upper() == "FAIL"]
    if not failed:
        return None, "NO_FAILED_CHECK"
    explicit_index = stop.get("blocking_check_index")
    if explicit_index is not None:
        try:
            idx = int(explicit_index)
            if 0 <= idx < len(checks) and str(checks[idx].get("state") or "").upper() == "FAIL":
                return idx, "EXPLICIT"
        except (TypeError, ValueError):
            pass
    explicit_code = str(stop.get("blocking_check_code") or "").strip()
    if explicit_code:
        matches = [(idx, check) for idx, check in failed
                   if str(check.get("code") or "").strip() == explicit_code]
        if len(matches) == 1:
            return matches[0][0], "EXPLICIT_CODE"
    condition = str(stop.get("condition") or "").strip()
    label = str(stop.get("label") or "").strip()
    matches = [(idx, check) for idx, check in failed
               if condition and str(check.get("condition") or "").strip() == condition]
    if not matches:
        matches = [(idx, check) for idx, check in failed
                   if label and str(check.get("label") or "").strip() == label]
    if len(matches) == 1:
        return matches[0][0], "EXACT_TEXT"
    if len(failed) == 1:
        return failed[0][0], "SINGLE_FAILED_CHECK"
    return None, "AMBIGUOUS"


def _live_decision_path(row: dict[str, Any]) -> dict[str, Any]:
    """Return the literal, decision-neutral order recorded by the live detector.

    The path is derived after the strategy decision. It never re-runs a
    predicate or changes a gate, level, RR, risk rule, or execution outcome.
    """
    raw_checks = row.get("checks") if isinstance(row.get("checks"), list) else []
    checks = [check for check in raw_checks if isinstance(check, dict)]
    stop = row.get("stop") if isinstance(row.get("stop"), dict) else {}
    owner_index, owner_status = _stop_owner(checks, stop) if stop else (None, "NO_STOP")
    steps = []
    for index, check in enumerate(checks):
        steps.append({
            "order": index + 1,
            "code": str(check.get("code") or ""),
            "label": str(check.get("label") or check.get("condition") or check.get("code") or "check")[:300],
            "state": str(check.get("state") or "UNKNOWN").upper(),
            "role": str(check.get("role") or "OBSERVED_CHECK").upper(),
            "actual_value": check.get("actual_value"),
            "required_value": check.get("required_value"),
            "blocking_stop": index == owner_index,
        })
    passed = [step for step in steps if step["state"] == "PASS"]
    return {
        "source": "LIVE_AUDIT_ORDER",
        "first_reached": steps[0] if steps else None,
        "last_reached": steps[-1] if steps else None,
        "last_passed": passed[-1] if passed else None,
        "blocking_step": steps[owner_index] if owner_index is not None and owner_index < len(steps) else None,
        "blocking_mapping": owner_status,
        "final_outcome": str(row.get("outcome") or "UNKNOWN").upper(),
        "steps": steps,
    }


def _observed_funnels(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("strategy") or "UNKNOWN")].append(row)
    result = []
    for strategy, items in sorted(grouped.items()):
        steps: dict[str, dict[str, Any]] = {}
        ambiguous_stops = 0
        for row in items:
            seen = set()
            raw_checks = row.get("checks", []) if isinstance(row.get("checks"), list) else []
            stop = row.get("stop") if isinstance(row.get("stop"), dict) else {}
            owner_index, owner_status = _stop_owner(raw_checks, stop) if stop else (None, "NO_STOP")
            if stop and owner_status in {"AMBIGUOUS", "NO_FAILED_CHECK"}:
                ambiguous_stops += 1
            for idx, check in enumerate(raw_checks):
                if not isinstance(check, dict):
                    continue
                label = str(check.get("label") or check.get("condition") or check.get("code") or "check")[:180]
                if label in seen:
                    continue
                seen.add(label)
                bucket = steps.setdefault(label, {"label": label, "reached": 0, "passed": 0, "failed": 0, "positions": [], "role": check.get("role", "UNKNOWN"), "blocking_stops": 0})
                bucket["reached"] += 1
                state = str(check.get("state") or "").upper()
                bucket["passed"] += int(state == "PASS")
                bucket["failed"] += int(state == "FAIL")
                bucket["positions"].append(idx)
                # Old cohorts sometimes marked several checks or a passed
                # check as blocking.  Count exactly one owner only when the
                # stop can be justified from a failed predicate.
                bucket["blocking_stops"] += int(owner_index == idx)
        min_reached = max(2, int(len(items) * 0.02))
        ordered = []
        for bucket in steps.values():
            if bucket["reached"] < min_reached:
                continue
            avg_pos = sum(bucket["positions"]) / len(bucket["positions"]) if bucket["positions"] else 999
            ordered.append({
                "label": bucket["label"], "reached": bucket["reached"], "passed": bucket["passed"], "failed": bucket["failed"],
                "pass_rate": round(bucket["passed"] / bucket["reached"] * 100, 1) if bucket["reached"] else None,
                "from_attempts": round(bucket["passed"] / len(items) * 100, 1) if items else None,
                "avg_position": round(avg_pos, 2), "role": bucket["role"], "blocking_stops": bucket["blocking_stops"],
            })
        ordered.sort(key=lambda x: (x["avg_position"], -x["reached"]))
        result.append({
            "strategy": strategy, "attempts": len(items), "steps": ordered[:30],
            "ambiguous_stop_mappings": ambiguous_stops,
            "candidates": sum(str(x.get("outcome") or "").upper() == "CANDIDATE" for x in items),
            "pending_ltf": sum(str(x.get("outcome") or "").upper() == "PENDING_LTF" for x in items),
            "filtered": sum(str(x.get("outcome") or "").upper() == "FILTERED" for x in items),
            "errors": sum(str(x.get("outcome") or "").upper() == "ERROR" for x in items),
            "groq": sum(bool(x.get("groq_review")) for x in items),
            "delivered": sum(any(str(d.get("stage") or "").lower() == "delivered" or str(d.get("outcome") or "").upper() == "ACCEPT" for d in x.get("decisions", [])) for x in items),
        })
    return result


def _build_dashboard_uncached(days: int = 1, strategy: str = "", symbol: str = "", outcome: str = "", groq: str = "",
                    min_rr: float | None = None, max_rr: float | None = None, from_date: str = "", to_date: str = "",
                    page: int = 1, page_size: int = 100, release: str = "") -> dict[str, Any]:
    events = _fetch(days, strategy, symbol, from_date, to_date)
    available_releases = []
    for e in events:
        sha = str((e.get("payload") or {}).get("release_sha") or "").strip()
        if sha and sha not in available_releases:
            available_releases.append(sha)
    market_success_history = {}
    for e in events:
        payload = e.get("payload") or {}
        if e.get("kind") != "market_data" or str(payload.get("status") or "").upper() != "OK":
            continue
        key = (str(e.get("symbol") or ""), str(payload.get("timeframe") or ""))
        timestamp = payload.get("last_success_at") or e.get("occurred_at")
        if timestamp and (key not in market_success_history or str(timestamp) > str(market_success_history[key])):
            market_success_history[key] = timestamp
    active_release = available_releases[0] if release == "latest" and available_releases else str(release or "").strip()
    if active_release:
        events = [e for e in events if str((e.get("payload") or {}).get("release_sha") or "").strip() == active_release]
    attempts=[]; reviews={}; decisions=defaultdict(list); scan_events=[]; trade_events=[]; market_data_events=[]; ltf_watch_events=[]
    manager_events=[]; apex_v2_snapshots=[]; incident_snapshots=[]
    for e in events:
        p=e["payload"]; key=str(p.get("attempt_key") or "")
        if e["kind"]=="attempt":
            row=dict(p); row.setdefault("attempt_key",e["event_key"]); row.setdefault("strategy",e["strategy"]); row.setdefault("symbol",e["symbol"]); row["occurred_at"]=e["occurred_at"]; attempts.append(row)
        elif e["kind"]=="groq_review" and key: reviews[key]={**p,"occurred_at":e["occurred_at"]}
        elif e["kind"]=="decision" and key: decisions[key].append({**p,"occurred_at":e["occurred_at"]})
        elif e["kind"]=="scan_event": scan_events.append({**p,"occurred_at":e["occurred_at"]})
        elif e["kind"]=="trade_event": trade_events.append({**p,"strategy":e["strategy"],"symbol":e["symbol"],"occurred_at":e["occurred_at"]})
        elif e["kind"]=="market_data": market_data_events.append({**p,"symbol":e["symbol"],"occurred_at":e["occurred_at"]})
        elif e["kind"]=="ltf_watch": ltf_watch_events.append({**p,"strategy":e["strategy"],"symbol":e["symbol"],"occurred_at":e["occurred_at"]})
        elif e["kind"]=="manager_event": manager_events.append({**p,"strategy":e["strategy"],"symbol":e["symbol"],"occurred_at":e["occurred_at"]})
        elif e["kind"]=="apex_v2_snapshot": apex_v2_snapshots.append({**p,"occurred_at":e["occurred_at"]})
        elif e["kind"]=="incident_snapshot": incident_snapshots.append({**p,"occurred_at":e["occurred_at"]})
    joined=[]
    for a in attempts:
        key=str(a.get("attempt_key") or ""); c=a.get("candidate") if isinstance(a.get("candidate"),dict) else {}; stop=a.get("stop") if isinstance(a.get("stop"),dict) else {}; snap=stop.get("snapshot") if isinstance(stop.get("snapshot"),dict) else {}
        rr=_num(c.get("rr")); rr=rr if rr is not None else _num(snap.get("rr") or snap.get("rr_check") or snap.get("_wy_rr") or snap.get("_wyd_rr"))
        row={**a,"groq_review":reviews.get(key),"decisions":sorted(decisions.get(key,[]),key=lambda x:x.get("occurred_at","")),"rr_value":rr}
        row["near_setup"]=bool(a.get("outcome")=="FILTERED" and snap.get("entry") is not None and snap.get("sl") is not None)
        row["decision_path"] = _live_decision_path(row)
        joined.append(row)
    if outcome: joined=[r for r in joined if str(r.get("outcome") or "").upper()==outcome.upper()]
    if groq: joined=[r for r in joined if str((r.get("groq_review") or {}).get("decision") or "").upper()==groq.upper()]
    if min_rr is not None: joined=[r for r in joined if r.get("rr_value") is not None and float(r["rr_value"])>=min_rr]
    if max_rr is not None: joined=[r for r in joined if r.get("rr_value") is not None and float(r["rr_value"])<=max_rr]
    failures=Counter(); by_strategy=defaultdict(Counter); checks=defaultdict(Counter); groq_reasons=Counter(); groq_risks=Counter(); groq_counts=Counter(); conf=defaultdict(list)
    for r in joined:
        name=str(r.get("strategy") or "UNKNOWN"); stop=r.get("stop") if isinstance(r.get("stop"),dict) else {}
        if r.get("outcome")=="FILTERED": label=str(stop.get("label") or stop.get("code") or "UNLABELED"); failures[label]+=1; by_strategy[name][label]+=1
        for ch in r.get("checks",[]) if isinstance(r.get("checks"),list) else []:
            if isinstance(ch,dict):
                label=str(ch.get("label") or ch.get("condition") or ch.get("code") or "check")[:300]
                checks[(name,label)][str(ch.get("state") or "UNKNOWN").upper()]+=1
        g=r.get("groq_review") or {}
        if g:
            d=str(g.get("decision") or "UNKNOWN").upper(); groq_counts[d]+=1
            v=_num(g.get("confidence"));
            if v is not None: conf[d].append(v)
            if d in {"WAIT","REJECT"}:
                for x in g.get("reasons",[]) if isinstance(g.get("reasons"),list) else []: groq_reasons[_reason(str(x))]+=1
                for x in g.get("risks",[]) if isinstance(g.get("risks"),list) else []: groq_risks[_reason(str(x))]+=1
    opened=[t for t in trade_events if str(t.get("action") or "").upper()=="OPEN"]
    closed=[t for t in trade_events if str(t.get("action") or "").upper()=="CLOSE"]
    wins=[t for t in closed if str(t.get("result") or "").lower() in {"tp1","tp2","tp3"}]
    losses=[t for t in closed if str(t.get("result") or "").lower()=="sl"]
    pnl_vals=[float(t["pnl_pct"]) for t in closed if _num(t.get("pnl_pct")) is not None]
    r_vals=[float(t["realized_r"]) for t in closed if _num(t.get("realized_r")) is not None]
    trade_by_strategy=defaultdict(lambda:{"opened":0,"closed":0,"wins":0,"losses":0,"pnl_pct":0.0,"r_sum":0.0,"r_n":0})
    for t in opened:
        trade_by_strategy[str(t.get("strategy") or "UNKNOWN")]["opened"]+=1
    for t in closed:
        st=str(t.get("strategy") or "UNKNOWN"); d=trade_by_strategy[st]; d["closed"]+=1
        res=str(t.get("result") or "").lower()
        if res in {"tp1","tp2","tp3"}: d["wins"]+=1
        elif res=="sl": d["losses"]+=1
        pv=_num(t.get("pnl_pct")); rv=_num(t.get("realized_r"))
        if pv is not None: d["pnl_pct"]+=pv
        if rv is not None: d["r_sum"]+=rv; d["r_n"]+=1
    trade_rows=[]
    for st,d in sorted(trade_by_strategy.items()):
        decided=d["wins"]+d["losses"]
        trade_rows.append({"strategy":st,"opened":d["opened"],"closed":d["closed"],"wins":d["wins"],"losses":d["losses"],
            "win_rate":round(d["wins"]/decided*100,1) if decided else None,"pnl_pct":round(d["pnl_pct"],3),
            "avg_r":round(d["r_sum"]/d["r_n"],3) if d["r_n"] else None})
    trade_stats={"opened":len(opened),"closed":len(closed),"wins":len(wins),"losses":len(losses),
        "win_rate":round(len(wins)/(len(wins)+len(losses))*100,1) if (wins or losses) else None,
        "pnl_pct":round(sum(pnl_vals),3) if pnl_vals else 0.0,"avg_pnl_pct":round(sum(pnl_vals)/len(pnl_vals),3) if pnl_vals else None,
        "avg_r":round(sum(r_vals)/len(r_vals),3) if r_vals else None,"by_strategy":trade_rows,
        "recent":sorted(closed,key=lambda x:x.get("occurred_at",""),reverse=True)[:50]}

    funnels = _observed_funnels(joined)
    bos_age_stats = {}
    for bos_strategy in ("SWING", "FAST"):
        buckets = {label: {"bucket": label, "events": 0, "retest": 0, "displacement": 0, "volume": 0, "rr": 0, "groq": 0, "delivered": 0}
                   for label in ("1", "2", "3", "4", "5+")}
        for r in joined:
            if str(r.get("strategy") or "").upper() != bos_strategy:
                continue
            telemetry = r.get("telemetry") if isinstance(r.get("telemetry"), dict) else {}
            events_for_attempt = []
            for telemetry_key in ("bos_event", "bos_execution_event"):
                telemetry_event = telemetry.get(telemetry_key)
                if isinstance(telemetry_event, dict):
                    events_for_attempt.append(telemetry_event)
            progress = telemetry.get("bos_progress") if isinstance(telemetry.get("bos_progress"), dict) else {}
            reached_groq = bool(r.get("groq_review"))
            reached_delivery = any(
                str(d.get("stage") or "").lower() == "delivered" or str(d.get("outcome") or "").upper() == "ACCEPT"
                for d in r.get("decisions", []) if isinstance(d, dict)
            )
            for event in events_for_attempt:
                if not isinstance(event, dict):
                    continue
                age = _num(event.get("age_bars"))
                if age is None or age < 1:
                    continue
                age_i = int(age)
                label = str(age_i) if age_i <= 4 else "5+"
                b = buckets[label]
                b["events"] += 1
                b["retest"] += int(bool(progress.get("retest_confirmed")))
                b["displacement"] += int(bool(progress.get("displacement_confirmed")))
                b["volume"] += int(bool(progress.get("volume_confirmed")))
                b["rr"] += int(bool(progress.get("rr_reached")))
                b["groq"] += int(reached_groq)
                b["delivered"] += int(reached_delivery)
        rows = []
        for label in ("1", "2", "3", "4", "5+"):
            b = buckets[label]
            n = b["events"]
            rows.append({**b, "groq_pct": round(b["groq"] / n * 100, 1) if n else 0.0,
                         "delivered_pct": round(b["delivered"] / n * 100, 1) if n else 0.0})
        bos_age_stats[bos_strategy] = rows

    wy_dist_values = []
    wy_box_values = []
    for r in joined:
        if str(r.get("strategy") or "").upper() != "WYCKOFF" or str(r.get("subtype") or "").upper() != "DISTRIBUTION":
            continue
        c = r.get("candidate") if isinstance(r.get("candidate"), dict) else {}
        stop = r.get("stop") if isinstance(r.get("stop"), dict) else {}
        snap = stop.get("snapshot") if isinstance(stop.get("snapshot"), dict) else {}
        value = _num(c.get("dist_range") if c.get("dist_range") is not None else snap.get("dist_range_pct"))
        if value is not None:
            wy_dist_values.append(value)
        telemetry = r.get("telemetry") if isinstance(r.get("telemetry"), dict) else {}
        wy_telemetry = telemetry.get("wyckoff_distribution") if isinstance(telemetry.get("wyckoff_distribution"), dict) else {}
        box_value = _num(wy_telemetry.get("distribution_box_width_pct"))
        if box_value is not None:
            wy_box_values.append(box_value)
    wy_dist_range = {
        "count": len(wy_dist_values),
        "min": round(min(wy_dist_values), 3) if wy_dist_values else None,
        "p25": _percentile(wy_dist_values, 0.25), "median": _percentile(wy_dist_values, 0.50),
        "p75": _percentile(wy_dist_values, 0.75), "p90": _percentile(wy_dist_values, 0.90),
        "max": round(max(wy_dist_values), 3) if wy_dist_values else None,
    }
    wy_box_range = {
        "count": len(wy_box_values),
        "min": round(min(wy_box_values), 3) if wy_box_values else None,
        "p25": _percentile(wy_box_values, 0.25), "median": _percentile(wy_box_values, 0.50),
        "p75": _percentile(wy_box_values, 0.75), "p90": _percentile(wy_box_values, 0.90),
        "max": round(max(wy_box_values), 3) if wy_box_values else None,
    }

    numeric_specs = {
        "SWING": ("swing_numeric", ("displacement_body_ratio", "directional_displacement_ratio", "volume_ratio", "retest_distance_atr")),
        "MTF": ("mtf_numeric", ("pd_position_pct", "pd_mid_distance_pct", "positive_confluence_count", "core_tf_match", "rr_value")),
        "ZONE": ("zone_numeric", ("range_position_pct", "range_atr", "zone_distance_atr", "test_count", "best_directional_displacement_ratio", "best_directional_body_atr", "quality_score")),
    }
    numeric_values = {strategy: {metric: [] for metric in metrics} for strategy, (_, metrics) in numeric_specs.items()}
    wy_observation = {"observed": 0, "old_pass": 0, "structural_pass": 0, "both_pass": 0, "structural_only": 0, "old_only": 0, "phase_ready": 0}
    wy_acc_observation = {"observed": 0, "old_pass": 0, "structural_pass": 0, "both_pass": 0, "structural_only": 0, "old_only": 0, "phase_ready": 0}
    swing_volume_observation = {"observed": 0, "pass_1_2": 0, "pass_1_1": 0, "observed_only": 0}
    fast_target_reasons = Counter()
    for r in joined:
        strategy_name = str(r.get("strategy") or "").upper()
        telemetry = r.get("telemetry") if isinstance(r.get("telemetry"), dict) else {}
        if strategy_name in numeric_specs:
            telemetry_key, metrics = numeric_specs[strategy_name]
            payload = telemetry.get(telemetry_key) if isinstance(telemetry.get(telemetry_key), dict) else {}
            for metric in metrics:
                value = _num(payload.get(metric))
                if value is not None:
                    numeric_values[strategy_name][metric].append(value)
        if strategy_name == "SWING":
            payload = telemetry.get("swing_numeric") if isinstance(telemetry.get("swing_numeric"), dict) else {}
            current = payload.get("volume_pass_1_2")
            observed = payload.get("volume_pass_1_1_observed")
            if isinstance(current, bool) and isinstance(observed, bool):
                swing_volume_observation["observed"] += 1
                swing_volume_observation["pass_1_2"] += int(current)
                swing_volume_observation["pass_1_1"] += int(observed)
                swing_volume_observation["observed_only"] += int(observed and not current)
        if strategy_name == "FAST":
            payload = telemetry.get("fast_target_geometry") if isinstance(telemetry.get("fast_target_geometry"), dict) else {}
            if payload.get("reason"):
                fast_target_reasons[str(payload.get("reason"))] += 1
        if strategy_name == "WYCKOFF" and str(r.get("subtype") or "").upper() == "DISTRIBUTION":
            payload = telemetry.get("wyckoff_distribution") if isinstance(telemetry.get("wyckoff_distribution"), dict) else {}
            old_pass = payload.get("old_range_under_25")
            structural_pass = payload.get("structural_box_under_25")
            if isinstance(old_pass, bool) and isinstance(structural_pass, bool):
                wy_observation["observed"] += 1
                wy_observation["old_pass"] += int(old_pass)
                wy_observation["structural_pass"] += int(structural_pass)
                wy_observation["both_pass"] += int(old_pass and structural_pass)
                wy_observation["structural_only"] += int(structural_pass and not old_pass)
                wy_observation["old_only"] += int(old_pass and not structural_pass)
                wy_observation["phase_ready"] += int(bool(payload.get("observed_phase_ready")))
        if strategy_name == "WYCKOFF" and str(r.get("subtype") or "").upper() == "SPRING":
            payload = telemetry.get("wyckoff_accumulation") if isinstance(telemetry.get("wyckoff_accumulation"), dict) else {}
            old_pass = payload.get("old_range_under_25")
            structural_pass = payload.get("structural_box_under_25")
            if isinstance(old_pass, bool) and isinstance(structural_pass, bool):
                wy_acc_observation["observed"] += 1
                wy_acc_observation["old_pass"] += int(old_pass)
                wy_acc_observation["structural_pass"] += int(structural_pass)
                wy_acc_observation["both_pass"] += int(old_pass and structural_pass)
                wy_acc_observation["structural_only"] += int(structural_pass and not old_pass)
                wy_acc_observation["old_only"] += int(old_pass and not structural_pass)
                wy_acc_observation["phase_ready"] += int(bool(payload.get("observed_phase_ready")))
    numeric_telemetry = {
        strategy_name: {metric: _metric_summary(values) for metric, values in metrics.items()}
        for strategy_name, metrics in numeric_values.items()
    }

    latest_market_data = {}
    last_market_success = {}
    for item in sorted(market_data_events, key=lambda x: x.get("occurred_at", ""), reverse=True):
        key = (str(item.get("symbol") or ""), str(item.get("timeframe") or ""))
        if key not in latest_market_data:
            latest_market_data[key] = item
        if str(item.get("status") or "").upper() == "OK" and key not in last_market_success:
            last_market_success[key] = item.get("last_success_at") or item.get("occurred_at")
    market_rows = []
    now_utc = datetime.now(timezone.utc)
    for key, item in latest_market_data.items():
        status = str(item.get("status") or "UNKNOWN").upper()
        success_at = item.get("last_success_at") or last_market_success.get(key) or market_success_history.get(key)
        closed_candle_at = item.get("last_closed_candle_at")
        market_rows.append({
            "symbol": key[0], "timeframe": key[1], "status": status,
            "source": item.get("source") or item.get("provider") or "Gate",
            "reason": item.get("reason") or "", "candle_count": item.get("candle_count") or 0,
            "last_success_at": success_at,
            "last_closed_candle_at": closed_candle_at,
            "freshness_basis": "LAST_CLOSED_CANDLE" if closed_candle_at else "UNKNOWN",
            "last_update_at": item.get("last_update_at") or item.get("occurred_at"),
            **_market_freshness(closed_candle_at, key[1], now_utc),
        })
    market_rows.sort(key=lambda x: (x["status"] == "OK" and x["freshness_status"] == "FRESH", x["symbol"], x["timeframe"]))
    market_data = {
        "ok": sum(row["status"] == "OK" for row in market_rows),
        "failed": sum(row["status"] != "OK" for row in market_rows),
        "stale": sum(row["freshness_status"] == "STALE" for row in market_rows),
        "fresh": sum(row["freshness_status"] == "FRESH" for row in market_rows),
        "freshness_unknown": sum(row["freshness_status"] == "UNKNOWN" for row in market_rows),
        "total": len(market_rows),
        "last_update": max((row.get("last_update_at") or "" for row in market_rows), default=""),
        "rows": market_rows[:100],
    }

    latest_ltf = {}
    for item in sorted(ltf_watch_events, key=lambda x: x.get("occurred_at", ""), reverse=True):
        key = str(item.get("setup_id") or "").strip() or (
            str(item.get("strategy") or ""), str(item.get("symbol") or ""),
            str(item.get("direction") or ""), str(item.get("required_timeframe") or ""),
        )
        latest_ltf.setdefault(key, item)
    def _not_expired(item: dict[str, Any]) -> bool:
        value = str(item.get("expires_at") or "").strip()
        if not value:
            return True
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed > now_utc
        except (TypeError, ValueError):
            return True
    ltf_rows = sorted(
        (item for item in latest_ltf.values()
         if str(item.get("state") or "").upper() == "WAITING" and _not_expired(item)),
        key=lambda x: (str(x.get("strategy") or ""), str(x.get("symbol") or "")),
    )
    ltf_watch = {"waiting": len(ltf_rows), "unique_setups": len(ltf_rows), "rows": ltf_rows[:100]}
    for funnel in funnels:
        funnel["pending_ltf_attempts"] = funnel.get("pending_ltf", 0)
        funnel["pending_ltf"] = sum(str(x.get("strategy", "")).upper() == funnel["strategy"] for x in ltf_rows)

    # Dashboard V2 joins the complete control path without being able to alter it.
    latest_v2 = max(apex_v2_snapshots, key=lambda x: x.get("occurred_at", ""), default={})
    manager_actions = Counter()
    manager_states = Counter()
    groq_manager_calls = 0
    manager_failures = 0
    recent_manager = []
    for item in sorted(manager_events, key=lambda x: x.get("occurred_at", ""), reverse=True):
        review = item.get("review") if isinstance(item.get("review"), dict) else {}
        execution = item.get("execution") if isinstance(item.get("execution"), dict) else {}
        facts = item.get("facts") if isinstance(item.get("facts"), dict) else {}
        action = str(review.get("action") or "HOLD").upper()
        state = str(item.get("manager_state") or facts.get("manager_state_before") or "UNKNOWN").upper()
        manager_actions[action] += 1
        manager_states[state] += 1
        groq_manager_calls += int(bool(review.get("groq_called")))
        manager_failures += int(str(execution.get("status") or "").upper() in {
            "ERROR", "REJECTED", "RATE_LIMITED", "RECONCILIATION_REQUIRED"
        })
        if len(recent_manager) < 100:
            recent_manager.append({
                "occurred_at": item.get("occurred_at"), "signal_id": item.get("signal_id"),
                "strategy": item.get("strategy"), "symbol": item.get("symbol"),
                "events": item.get("events") or [], "action": action, "state": state,
                "confidence": review.get("confidence"), "reason": review.get("reason"),
                "execution_status": execution.get("status"),
            })
    manager_summary = {
        "cycles": len(manager_events), "groq_calls": groq_manager_calls,
        "failures": manager_failures, "actions": dict(manager_actions),
        "states": dict(manager_states), "recent": recent_manager,
    }

    opportunity_states = Counter()
    opportunities_by_identity = {}
    for row in joined:
        if not row.get("near_setup"):
            continue
        candidate = row.get("candidate") if isinstance(row.get("candidate"), dict) else {}
        stop = row.get("stop") if isinstance(row.get("stop"), dict) else {}
        snap = stop.get("snapshot") if isinstance(stop.get("snapshot"), dict) else {}
        entry = _num(candidate.get("entry") if candidate.get("entry") is not None else snap.get("entry"))
        sl = _num(candidate.get("sl") if candidate.get("sl") is not None else snap.get("sl"))
        tp1 = _num(candidate.get("tp1") or candidate.get("tp") or snap.get("tp1") or snap.get("tp"))
        direction = str(candidate.get("direction") or snap.get("direction") or "").upper()
        current = None
        for field in ("decision_price", "current_price", "latest_price", "_integrity_current", "price"):
            current = _num(candidate.get(field) if candidate.get(field) is not None else snap.get(field))
            if current is not None:
                break
        state = "UNASSESSED"
        target_passed = None
        if entry is not None and tp1 is not None and current is not None and direction in {"BULLISH", "BEARISH"}:
            target_passed = current >= tp1 if direction == "BULLISH" else current <= tp1
            state = "TARGET_ALREADY_PASSED" if target_passed else "AWAITING_LIVE_OUTCOME"
        telemetry = row.get("telemetry") if isinstance(row.get("telemetry"), dict) else {}
        target_geometry = telemetry.get("fast_rr_geometry") or telemetry.get("fast_target_geometry") or {}
        structure = candidate.get("structure_event") or snap.get("structure_event") or {}
        identity = (
            str(row.get("strategy") or "").upper(), str(row.get("symbol") or "").upper(), direction,
            str(candidate.get("setup_id") or snap.get("setup_id") or candidate.get("zone_id") or
                snap.get("zone_id") or (structure.get("candle_time") if isinstance(structure, dict) else "") or ""),
            round(entry, 10) if entry is not None else None,
            round(sl, 10) if sl is not None else None,
            round(tp1, 10) if tp1 is not None else None,
            str(stop.get("code") or stop.get("label") or ""),
        )
        opportunity = {
            "attempt_id": row.get("attempt_key"), "occurred_at": row.get("finished_at") or row.get("occurred_at"),
            "strategy": row.get("strategy"), "symbol": row.get("symbol"), "direction": direction,
            "stop_reason": stop.get("label") or stop.get("code"), "entry": entry, "sl": sl,
            "tp1": tp1, "rr": row.get("rr_value"), "decision_price": current,
            "execution_state": state, "target_already_passed": target_passed,
            "target_geometry": target_geometry, "setup_identity": "|".join(map(str, identity[:4])),
        }
        # Events arrive newest-first. Repeated scans of the same immutable
        # geometry are observations of one setup, not new opportunities.
        opportunities_by_identity.setdefault(identity, opportunity)
    opportunities = list(opportunities_by_identity.values())
    opportunity_states.update(item["execution_state"] for item in opportunities)
    opportunities.sort(key=lambda x: x.get("occurred_at") or "", reverse=True)
    opportunity_review = {"counts": dict(opportunity_states), "recent": opportunities[:100]}

    portfolio = latest_v2.get("portfolio") if isinstance(latest_v2.get("portfolio"), dict) else {}
    execution_mode = latest_v2.get("execution_mode") if isinstance(latest_v2.get("execution_mode"), dict) else {}
    execution_health = latest_v2.get("execution_health") if isinstance(latest_v2.get("execution_health"), dict) else {}
    manager_db = latest_v2.get("manager_db") if isinstance(latest_v2.get("manager_db"), dict) else {}
    learning_v2 = latest_v2.get("learning") if isinstance(latest_v2.get("learning"), dict) else {}
    latest_incident_snapshot = max(
        incident_snapshots, key=lambda x: x.get("occurred_at", ""), default=None,
    )
    if latest_incident_snapshot is not None:
        incidents = normalize_incident_snapshot(latest_incident_snapshot.get("incidents"))
    else:
        # Compatibility only until every running worker emits V3 snapshots.
        incidents = normalize_incident_snapshot(latest_v2.get("open_incidents"))
    versions = latest_v2.get("versions") if isinstance(latest_v2.get("versions"), dict) else {}
    budget_plan = latest_v2.get("api_budget_plan") if isinstance(latest_v2.get("api_budget_plan"), dict) else {}
    observability_reasons = []
    if str(budget_plan.get("status") or "").upper() in {"", "UNCONFIGURED", "UNAVAILABLE", "INVALID_PLAN"}:
        observability_reasons.append("API_BUDGET_PLAN_INCOMPLETE")
    if market_data["freshness_unknown"]:
        observability_reasons.append("MARKET_DATA_FRESHNESS_UNKNOWN")
    system_state = "CRITICAL" if any(str(x.get("severity") or "").upper() == "CRITICAL" for x in incidents) else (
        "DEGRADED" if market_data["failed"] or manager_failures or incidents else "HEALTHY"
    )
    system_overview = {
        "state": system_state, "market_data_ok": market_data["ok"],
        "market_data_total": market_data["total"], "open_trades": max(0, len(opened)-len(closed)),
        "manager_cycles": len(manager_events), "groq_entry_reviews": sum(groq_counts.values()),
        "incidents": len(incidents), "execution_mode": execution_mode,
        "api_budget": latest_v2.get("api_budget", []),
        "api_budget_plan": budget_plan,
        "api_budget_error": latest_v2.get("api_budget_error"),
        "source_registry": latest_v2.get("source_registry", []),
        "gate_microstructure": latest_v2.get("gate_microstructure", []),
        "versions": versions, "snapshot_at": latest_v2.get("generated_at") or latest_v2.get("occurred_at"),
        "observability_state": "INCOMPLETE" if observability_reasons else "COMPLETE",
        "observability_reasons": observability_reasons,
    }
    integration_health = _integration_health(latest_v2, market_data)
    function_health = _function_health(latest_v2, integration_health)

    total=len(joined); page_size=max(20,min(int(page_size),200)); page=max(1,int(page)); start=(page-1)*page_size
    # Keep the attempt denominator for release comparisons; the user-facing
    # pending_ltf value is the unique active-watch count above.
    legacy_pending_ltf = {"pending_ltf":sum(r.get("outcome")=="PENDING_LTF" for r in joined)}
    reviews_n=sum(groq_counts.values()); delivered_attempts=sum(1 for r in joined if any(str(d.get("stage") or "").lower()=="delivered" or str(d.get("outcome") or "").upper()=="ACCEPT" for d in r.get("decisions",[])))
    delivered_signal_ids = {
        str(t.get("signal_id")) for t in opened if t.get("signal_id") not in (None, "")
    }
    delivered = max(delivered_attempts, len(delivered_signal_ids), len(opened))
    return {"period_days":days,"baseline":"production-live-v3","baseline_utc":STATS_BASELINE_UTC.isoformat(),"generated_at":datetime.now(timezone.utc).isoformat(timespec="seconds"),
      "release_filter":release,"release_sha":active_release,"available_releases":available_releases[:12],"funnels":funnels,
      "bos_choch_age":bos_age_stats,"wyckoff_dist_range":wy_dist_range,"wyckoff_box_width":wy_box_range,
      "numeric_telemetry":numeric_telemetry,"wyckoff_observation":wy_observation,"wyckoff_accumulation_observation":wy_acc_observation,
      "swing_volume_observation":swing_volume_observation,"fast_target_diagnostics":dict(fast_target_reasons),
      "market_data":market_data,"ltf_watch":ltf_watch,"system_overview":system_overview,
      "integration_health":integration_health,
      "function_health":function_health,
      "source_registry":integration_health.get("display_sources",[]),
      "portfolio_dependency":latest_v2.get("portfolio_dependency",{}),
      "gate_microstructure":system_overview.get("gate_microstructure",[]),
      "api_budget_plan":system_overview.get("api_budget_plan",{}),
      "manager_v2":manager_summary,"opportunity_review":opportunity_review,
      "portfolio_risk":portfolio,"execution_mode":execution_mode,"incidents":incidents,"versions":versions,
      "execution_health":execution_health,"manager_db":manager_db,"learning_v2":learning_v2,
      "summary":{"attempts":total,"candidates":sum(r.get("outcome")=="CANDIDATE" for r in joined),"pending_ltf":len(ltf_rows),"pending_ltf_attempts":legacy_pending_ltf["pending_ltf"],"near_setups":len(opportunities),"groq_total":reviews_n,"groq_approve":groq_counts.get("APPROVE",0),"groq_wait":groq_counts.get("WAIT",0),"groq_reject":groq_counts.get("REJECT",0),"delivered":delivered,"scan_events":len(scan_events)},
      "strategy_counts":dict(Counter(str(r.get("strategy") or "UNKNOWN") for r in joined)),
      "failures":[{"label":k,"count":v} for k,v in failures.most_common(30)],
      "failures_by_strategy":{k:[{"label":a,"count":b} for a,b in v.most_common(30)] for k,v in by_strategy.items()},
      "criterion_stats":[{"strategy":k[0],"label":k[1],"pass":v.get("PASS",0),"fail":v.get("FAIL",0),"total":v.get("PASS",0)+v.get("FAIL",0)} for k,v in sorted(checks.items(),key=lambda x:sum(x[1].values()),reverse=True)[:300]],
      "groq":{"decisions":dict(groq_counts),"reasons":[{"label":k,"count":v} for k,v in groq_reasons.most_common(30)],"risks":[{"label":k,"count":v} for k,v in groq_risks.most_common(30)],"avg_confidence":{k:(sum(v)/len(v) if v else None) for k,v in conf.items()}},
      "trade_stats":trade_stats,"catalog":STRATEGY_CATALOG,"rows":joined[start:start+page_size],"pagination":{"page":page,"page_size":page_size,"total":total,"pages":max(1,(total+page_size-1)//page_size)}}


def _runtime_release_sha() -> str:
    return _SETTINGS.release_sha


def _integration_health(snapshot: dict[str, Any], market_data: dict[str, Any]) -> dict[str, Any]:
    """Project secret-free provider, feature and local-budget diagnostics."""
    registry = snapshot.get("source_registry")
    registry = registry if isinstance(registry, list) else []
    budgets = snapshot.get("api_budget")
    budgets = budgets if isinstance(budgets, list) else []
    budget_by_source = {
        str(row.get("source") or "").lower(): row
        for row in budgets if isinstance(row, dict)
    }
    sources = []
    for raw in registry:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        source = str(row.get("source") or "unknown").lower()
        budget = budget_by_source.get(str(row.get("budget_key") or source).lower(), {})
        used = budget.get("used") if isinstance(budget.get("used"), dict) else {}
        health = budget.get("health") if isinstance(budget.get("health"), dict) else {}
        blocked_until = float(health.get("blocked_until") or 0)
        failures = int(health.get("failures") or 0)
        day_used = int(used.get("day") or 0)
        if blocked_until > time.time():
            status, reason = "BLOCKED", "CIRCUIT_OPEN_OR_RATE_LIMIT"
        elif failures:
            status, reason = "DEGRADED", "RECENT_PROVIDER_FAILURES"
        elif day_used:
            status, reason = "ACTIVE", "REQUESTS_OBSERVED"
        elif row.get("mode") in {"PRIMARY_MARKET", "EXECUTION", "INTERNAL", "PROXY"}:
            status, reason = "AVAILABLE", "NO_BUDGETED_REQUEST_OBSERVED"
        else:
            status, reason = "IDLE", "NO_REQUEST_OBSERVED"
        sources.append({
            **row, "status": status, "reason_code": reason,
            "used_minute": int(used.get("minute") or 0),
            "used_hour": int(used.get("hour") or 0),
            "used_day": day_used, "remaining_day": budget.get("remaining_day"),
            "allocation": budget.get("allocation") or {}, "failures": failures,
            "rate_limits": int(health.get("rate_limits") or 0),
            "denied": int(health.get("denied") or 0),
            "blocked_until": blocked_until or None,
            "provenance": " · ".join(filter(None, (
                str(row.get("provenance") or ""), reason,
                f"used day {day_used}" if budget else "budget not observed",
                f"remaining day {budget.get('remaining_day')}" if budget.get("remaining_day") is not None else "",
                f"rate limits {int(health.get('rate_limits') or 0)}" if budget else "",
                f"denied {int(health.get('denied') or 0)}" if budget else "",
            ))),
        })

    micro = snapshot.get("gate_microstructure")
    micro = micro if isinstance(micro, list) else []
    latest_micro = micro[0] if micro and isinstance(micro[0], dict) else {}
    live_status = str(
        latest_micro.get("freshness_status") or latest_micro.get("status") or "NO_TELEMETRY"
    ).upper()
    features = [
        {
            "feature": "structural_liquidity_heatmap",
            "label": "Structural liquidity heatmap",
            "status": "READY" if int(market_data.get("ok") or 0) else "WAITING_FOR_CANDLES",
            "source": "Gate closed candles", "reason_code": "CANDLE_DERIVED_LEVELS",
        },
        {
            "feature": "live_orderbook_heatmap", "label": "Live Gate order-book heatmap",
            "status": live_status, "source": "Gate WebSocket depth",
            "reason_code": str(latest_micro.get("sequence_status") or "NO_SEQUENCE_VERIFIED_DEPTH"),
            "updated_at": latest_micro.get("created_at") or latest_micro.get("updated_at"),
            "levels": len(latest_micro.get("heatmap_levels") or []),
        },
    ]
    display_sources = sources + [
        {
            "source": feature["label"], "mode": "FEATURE",
            "status": feature["status"],
            "provenance": " · ".join(filter(None, (
                str(feature.get("source") or ""), str(feature.get("reason_code") or ""),
                f"levels {feature.get('levels')}" if feature.get("levels") is not None else "",
                str(feature.get("updated_at") or ""),
            ))),
        }
        for feature in features
    ]
    return {
        "sources": sources, "display_sources": display_sources,
        "budgets": budgets, "features": features,
    }


def _function_health(
    snapshot: dict[str, Any], integration_health: dict[str, Any],
) -> dict[str, Any]:
    """Build one truthful, secret-free health matrix for the Dashboard.

    Runtime rows are worker-observed. Provider rows expose local request and
    rate-limit telemetry. Request-driven context features are explicitly
    labelled ON_DEMAND when no periodic production probe exists, so the UI
    never presents "configured" as proof that a provider is healthy.
    """
    runtime = snapshot.get("runtime_health")
    runtime = runtime if isinstance(runtime, dict) else {}
    components = runtime.get("components")
    components = components if isinstance(components, dict) else {}
    rows: list[dict[str, Any]] = []
    for name, raw in sorted(components.items()):
        item = raw if isinstance(raw, dict) else {}
        status = str(item.get("state") or "UNKNOWN").upper()
        reason_code = str(item.get("detail") or "") or None
        if status == "UNKNOWN" and name in {"groq", "risk_engine"}:
            status = "ON_DEMAND"
            reason_code = "CANDIDATE_DRIVEN_NO_PERIODIC_PROBE"
        elif status == "UNKNOWN" and name.startswith("scanner_"):
            status = "WAITING_FIRST_RUN"
            reason_code = "SCHEDULED_RUN_NOT_OBSERVED_FOR_INSTANCE"
        rows.append({
            "category": "runtime", "function": str(name),
            "status": status,
            "reason_code": reason_code,
            "updated_at": item.get("updated_at"),
            "required": bool(item.get("required", False)),
        })

    provider_names: set[str] = set()
    for raw in integration_health.get("sources") or []:
        if not isinstance(raw, dict):
            continue
        name = str(raw.get("source") or "unknown").lower()
        provider_names.add(name)
        rows.append({
            "category": "provider", "function": name,
            "status": str(raw.get("status") or "UNKNOWN").upper(),
            "reason_code": raw.get("reason_code"),
            "updated_at": raw.get("updated_at"),
            "used_day": int(raw.get("used_day") or 0),
            "remaining_day": raw.get("remaining_day"),
            "failures": int(raw.get("failures") or 0),
            "rate_limits": int(raw.get("rate_limits") or 0),
            "required": False,
        })
    for raw in integration_health.get("features") or []:
        if not isinstance(raw, dict):
            continue
        rows.append({
            "category": "feature", "function": raw.get("feature") or "unknown",
            "status": str(raw.get("status") or "UNKNOWN").upper(),
            "reason_code": raw.get("reason_code"),
            "updated_at": raw.get("updated_at"),
            "required": False,
        })

    for name in ("news_rss", "economic_calendar", "dxy", "fear_greed"):
        if name not in provider_names:
            rows.append({
                "category": "context", "function": name,
                "status": "ON_DEMAND",
                "reason_code": "REQUEST_DRIVEN_NO_PERIODIC_PROBE",
                "updated_at": None, "required": False,
            })

    counts = Counter(str(row.get("status") or "UNKNOWN") for row in rows)
    return {
        "runtime_status": str(runtime.get("status") or "UNKNOWN").upper(),
        "health": str(runtime.get("health") or "UNKNOWN").upper(),
        "ready": runtime.get("ready") is True,
        "new_entries": str(runtime.get("new_entries") or "UNKNOWN").upper(),
        "release_sha": str(runtime.get("release_sha") or "")[:12],
        "reason_codes": list(runtime.get("reason_codes") or []),
        "counts": dict(counts), "rows": rows,
    }


def _cache_key(strategy: str, symbol: str, outcome: str, groq: str,
               min_rr: float | None, max_rr: float | None, from_date: str,
               to_date: str, page: int, page_size: int) -> tuple[Any, ...]:
    return (
        str(strategy or "").upper(), str(symbol or "").upper(),
        str(outcome or "").upper(), str(groq or "").upper(), min_rr, max_rr,
        from_date, to_date, max(1, int(page)), max(20, min(200, int(page_size))),
    )


def _cached_dashboard(key: tuple[Any, ...], *, fresh_only: bool) -> dict[str, Any] | None:
    now = time.monotonic()
    with _DASHBOARD_CACHE_LOCK:
        item = _DASHBOARD_CACHE.get(key)
        if item is None:
            return None
        age = max(0.0, now - item[0])
        if fresh_only and age > _SETTINGS.cache_ttl_seconds:
            return None
        _DASHBOARD_CACHE.move_to_end(key)
        value = dict(item[1])
    value["dashboard_cache"] = {
        "status": "HIT" if fresh_only else "STALE_WHILE_REVALIDATE",
        "age_seconds": round(age, 1),
    }
    return value


def _persistent_cache_key(key: tuple[Any, ...]) -> str:
    return hashlib.sha256(
        json.dumps(key, ensure_ascii=True, default=str, separators=(",", ":")).encode()
    ).hexdigest()


def _load_persisted_dashboard(key: tuple[Any, ...]) -> dict[str, Any] | None:
    persistent_key = _persistent_cache_key(key)
    with _DASHBOARD_CACHE_LOCK:
        if persistent_key in _DASHBOARD_PERSIST_CHECKED:
            return None
        _DASHBOARD_PERSIST_CHECKED.add(persistent_key)
    try:
        conn = _connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT payload,EXTRACT(EPOCH FROM (NOW()-built_at)) "
                    "FROM apex_stats_dashboard_cache WHERE cache_key=%s",
                    (persistent_key,),
                )
                row = cur.fetchone()
        finally:
            conn.close()
        if row and isinstance(row[0], dict):
            value = dict(row[0])
            value["dashboard_cache"] = {
                "status": "PERSISTED_STALE",
                "age_seconds": round(max(0.0, float(row[1] or 0)), 1),
            }
            return value
    except Exception:
        return None
    return None


def _store_dashboard(key: tuple[Any, ...], value: dict[str, Any]) -> None:
    stored_at = time.monotonic()
    with _DASHBOARD_CACHE_LOCK:
        _DASHBOARD_CACHE[key] = (stored_at, dict(value))
        _DASHBOARD_CACHE.move_to_end(key)
        while len(_DASHBOARD_CACHE) > _SETTINGS.cache_max_entries:
            _DASHBOARD_CACHE.popitem(last=False)
    try:
        conn = _connect()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO apex_stats_dashboard_cache(cache_key,payload,built_at)
                       VALUES (%s,%s::jsonb,NOW())
                       ON CONFLICT(cache_key) DO UPDATE SET
                         payload=EXCLUDED.payload,built_at=EXCLUDED.built_at""",
                    (_persistent_cache_key(key), json.dumps(value, ensure_ascii=False, default=str)),
                )
        finally:
            conn.close()
    except Exception:
        pass


def _build_and_store_dashboard(key: tuple[Any, ...], args: tuple[Any, ...]) -> dict[str, Any]:
    started = time.monotonic()
    result = _build_dashboard_uncached(*args)
    release_sha = _runtime_release_sha()
    result.update({
        "cohort_mode": "stable",
        "current_release_sha": release_sha,
        "release_sha": release_sha,
        "runtime_release_sha": release_sha,
        "release_started_at": STATS_BASELINE_UTC.isoformat(),
        "baseline_started_at": STATS_BASELINE_UTC.isoformat(),
        "available_releases": [release_sha] if release_sha else [],
        "dashboard_cache": {
            "status": "MISS",
            "build_ms": round((time.monotonic() - started) * 1000.0, 1),
        },
    })
    _store_dashboard(key, result)
    return result


def _refresh_dashboard_background(key: tuple[Any, ...], args: tuple[Any, ...]) -> None:
    if not _DASHBOARD_BUILD_LOCK.acquire(blocking=False):
        return

    def run() -> None:
        try:
            _build_and_store_dashboard(key, args)
        except Exception as exc:
            # A failed refresh must be observable.  Silently swallowing this
            # exception left an old persisted cohort looking authoritative for
            # days even though worker ingestion continued normally.
            logging.exception(
                "Dashboard background refresh failed: %s", type(exc).__name__
            )
        finally:
            _DASHBOARD_BUILD_LOCK.release()

    threading.Thread(target=run, name="dashboard-cache-refresh", daemon=True).start()


def build_dashboard(days: int = 1, strategy: str = "", symbol: str = "", outcome: str = "", groq: str = "",
                    min_rr: float | None = None, max_rr: float | None = None, from_date: str = "", to_date: str = "",
                    page: int = 1, page_size: int = 100, release: str = "current") -> dict[str, Any]:
    """Return one stable, single-flight Dashboard cohort across routine deploys."""
    del days, release
    effective_from = from_date or STATS_BASELINE_UTC.date().isoformat()
    key = _cache_key(strategy, symbol, outcome, groq, min_rr, max_rr, effective_from, to_date, page, page_size)
    args = (30, strategy, symbol, outcome, groq, min_rr, max_rr,
            effective_from, to_date, page, page_size, "")
    hit = _cached_dashboard(key, fresh_only=True)
    if hit is not None:
        return hit
    stale = _cached_dashboard(key, fresh_only=False) or _load_persisted_dashboard(key)
    if stale is not None:
        with _DASHBOARD_CACHE_LOCK:
            _DASHBOARD_CACHE.setdefault(
                key, (time.monotonic() - _SETTINGS.cache_ttl_seconds - 1, dict(stale))
            )
        _refresh_dashboard_background(key, args)
        return stale
    if not _DASHBOARD_BUILD_LOCK.acquire(blocking=False):
        raise TimeoutError("dashboard aggregation is warming; retry shortly")
    try:
        return _build_and_store_dashboard(key, args)
    finally:
        _DASHBOARD_BUILD_LOCK.release()


# The compact V3 UI remains separate from the read-only telemetry backend.
from apex.ui.dashboard import HTML as V3_DASHBOARD_HTML
HTML = V3_DASHBOARD_HTML


class Handler(BaseHTTPRequestHandler):
    server_version="APEXStats/1.0"
    def _write(self,body,content_type,status=200):
        try:
            self.send_response(status); self.send_header("Content-Type",content_type); self.send_header("Content-Length",str(len(body))); self.send_header("Cache-Control","no-store"); self.send_header("X-Content-Type-Options","nosniff"); self.end_headers(); self.wfile.write(body)
            return True
        except (BrokenPipeError,ConnectionResetError):
            # A browser closed or refreshed the tab while a large dashboard
            # response was being written. The request is over; do not attempt
            # a second error response to the already closed socket.
            return False
    def _json(self,data,status=200):
        body=json.dumps(data,ensure_ascii=False,default=str).encode(); return self._write(body,"application/json; charset=utf-8",status)
    def _html(self,text,status=200):
        body=text.encode()
        try:
            self.send_response(status); self.send_header("Content-Type","text/html; charset=utf-8"); self.send_header("Content-Length",str(len(body))); self.send_header("Cache-Control","no-store"); self.send_header("Referrer-Policy","no-referrer"); self.send_header("X-Frame-Options","DENY"); self.send_header("X-Content-Type-Options","nosniff"); self.send_header("Content-Security-Policy","default-src 'self' 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'"); self.end_headers(); self.wfile.write(body)
            return True
        except (BrokenPipeError,ConnectionResetError):
            return False
    def _auth(self,q):
        supplied=(q.get("key") or [""])[0]; return bool(DASHBOARD_TOKEN and hmac.compare_digest(supplied,DASHBOARD_TOKEN))
    def do_HEAD(self): self.send_response(200); self.end_headers()
    def do_GET(self):
        p=urlparse(self.path); q=parse_qs(p.query)
        # Treat a trailing slash as the same Dashboard route. Mobile browsers,
        # saved bookmarks and reverse proxies may normalize /stats to /stats/.
        # Keep / itself unchanged while canonicalizing every other path.
        route = p.path if p.path == "/" else p.path.rstrip("/")
        if route=="/health": self._json({"ok":True,"service":"apex-strategy-stats"}); return
        if route=="/health/worker":
            payload,status=worker_readiness((q.get("sha") or [""])[0]); self._json(payload,status); return
        if not self._auth(q): self._html("<!doctype html><meta charset=utf-8><h2>403 · закрытая статистика APEX</h2>",403); return
        if route in {"/","/stats"}: self._html(HTML); return
        if route=="/api/dashboard":
            try:
                val=lambda k,d="":(q.get(k) or [d])[0]; data=build_dashboard(int(val("days","1")),val("strategy"),val("symbol"),val("outcome"),val("groq"),float(val("min_rr")) if val("min_rr") else None,float(val("max_rr")) if val("max_rr") else None,val("fromdate"),val("todate"),int(val("page","1")),int(val("page_size","100")),val("release")); self._json(data)
            except Exception as exc: self._json({"error":f"{type(exc).__name__}: {exc}"},500)
            return
        self._json({"error":"not found"},404)
    def do_POST(self):
        path=urlparse(self.path).path
        if path not in {"/ingest","/runtime/lease"}: self._json({"error":"not found"},404); return
        if not INGEST_TOKEN or not hmac.compare_digest(self.headers.get("X-APEX-Ingest-Token",""),INGEST_TOKEN): self._json({"error":"forbidden"},403); return
        try:
            max_bytes=16_384 if path=="/runtime/lease" else 2_000_000
            n=min(int(self.headers.get("Content-Length","0") or 0),max_bytes)
            raw=json.loads(self.rfile.read(n).decode())
            if path=="/runtime/lease":
                payload,status=runtime_lease(raw); self._json(payload,status)
            else:
                count=ingest(raw); self._json({"ok":True,"accepted":count})
        except Exception as exc: self._json({"error":f"{type(exc).__name__}: {exc}"},400)
    def log_message(self,fmt,*args): print(f"[stats] {self.command} {urlparse(self.path).path}")


class APEXStatsServer(ThreadingHTTPServer):
    # A burst of ingest and browser refreshes must not fill the tiny stdlib
    # accept queue and make /health unreachable at Render's proxy.
    daemon_threads = True
    request_queue_size = 128


def main():
    _SETTINGS.validate_startup()
    ensure_schema()
    # Do not eagerly aggregate the telemetry cohort at process startup. On the
    # 512 MB web instance that work can race port startup and trigger a restart
    # loop. The first authenticated dashboard request uses persisted cache when
    # available and otherwise performs one bounded single-flight build.
    print(f"APEX Strategy Stats listening on :{PORT}")
    APEXStatsServer(("0.0.0.0",PORT),Handler).serve_forever()


if __name__=="__main__": main()
