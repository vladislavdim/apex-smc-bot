"""Protected read-only APEX strategy statistics service.

This process is deployed separately from the Telegram polling worker but lives in
the same repository/project. It never imports or executes trading code. Passive
telemetry is accepted at /ingest and persisted in Postgres.
"""
from __future__ import annotations

import hmac
import gzip
import hashlib
import io
import json
import os
import re
import threading
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

import psycopg2
import psycopg2.extras

from core.strategy_catalog import STRATEGY_CATALOG
from research.store import ResearchStore

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
DASHBOARD_TOKEN = os.environ.get("DASHBOARD_TOKEN", "").strip()
INGEST_TOKEN = os.environ.get("INGEST_TOKEN", "").strip()
MARKET_DATABASE_URL = os.environ.get("APEX_MARKET_DATABASE_URL", "").strip()
RESEARCH_GITHUB_REPO = os.environ.get("APEX_RESEARCH_GITHUB_REPO", "vladislavdim/apex-smc-bot").strip()
RESEARCH_RELEASE_TAG = os.environ.get("APEX_RESEARCH_RELEASE_TAG", "apex-research-btc-data").strip()
_RESEARCH_RELEASE_CACHE: dict[str, Any] = {"at": 0.0, "value": None}
PORT = int(os.environ.get("PORT", "10000"))
# Routine deploys must not reset the Strategy Lab denominator. Bump this
# baseline manually only when a validated formula/settings change is promoted.
STATS_BASELINE_UTC = datetime.fromisoformat(
    os.environ.get("APEX_STATS_BASELINE_UTC", "2026-09-10T07:55:47+00:00")
)



def _connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    return psycopg2.connect(DATABASE_URL, connect_timeout=8)


def _github_research_dashboard() -> dict[str, Any]:
    """Read the compact public snapshot; never download the SQLite asset here."""
    now = time.monotonic()
    if _RESEARCH_RELEASE_CACHE["value"] is not None and now-_RESEARCH_RELEASE_CACHE["at"] < 300:
        return _RESEARCH_RELEASE_CACHE["value"]

    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", RESEARCH_GITHUB_REPO):
        raise RuntimeError("BTC research GitHub repository setting is invalid")
    if not RESEARCH_RELEASE_TAG:
        raise RuntimeError("BTC research GitHub release tag is empty")

    def download(url: str, limit: int, timeout: int) -> bytes:
        if not url.startswith(("https://github.com/", "https://api.github.com/")):
            raise RuntimeError("BTC research asset URL is not an approved GitHub URL")
        request = urllib.request.Request(
            url,
            headers={"Accept": "application/octet-stream", "User-Agent": "APEX-Research-Dashboard"},
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            chunks: list[bytes] = []; size = 0
            while True:
                chunk = response.read(min(1024 * 1024, limit - size + 1))
                if not chunk:
                    break
                chunks.append(chunk); size += len(chunk)
                if size > limit:
                    raise RuntimeError("BTC research dashboard asset exceeds configured size limit")
        return b"".join(chunks)

    encoded_tag = quote(RESEARCH_RELEASE_TAG, safe="")
    direct_base = f"https://github.com/{RESEARCH_GITHUB_REPO}/releases/download/{encoded_tag}"
    manifest_url = f"{direct_base}/BTCUSDT.manifest.json"
    dashboard_url = f"{direct_base}/BTCUSDT.dashboard.json.gz"
    source = "DIRECT_RELEASE"
    try:
        # Direct release URLs do not consume the unauthenticated GitHub REST
        # quota shared by Render egress IPs.  The API lookup remains a fallback
        # for repositories whose release assets were renamed or redirected.
        manifest_payload = download(manifest_url, 1024 * 1024, 12)
        compressed = download(dashboard_url, 20 * 1024 * 1024, 30)
    except (urllib.error.HTTPError, urllib.error.URLError):
        api = f"https://api.github.com/repos/{RESEARCH_GITHUB_REPO}/releases/tags/{encoded_tag}"
        request = urllib.request.Request(
            api,
            headers={"Accept": "application/vnd.github+json", "User-Agent": "APEX-Research-Dashboard"},
        )
        with urllib.request.urlopen(request, timeout=12) as response:
            release = json.loads(response.read())
        assets = {str(x.get("name")): x for x in release.get("assets", []) if isinstance(x, dict)}
        asset = assets.get("BTCUSDT.dashboard.json.gz")
        manifest_asset = assets.get("BTCUSDT.manifest.json")
        if not asset or not manifest_asset:
            raise RuntimeError("BTC research snapshot manifest or dashboard asset is not published")
        manifest_payload = download(str(manifest_asset.get("browser_download_url") or ""), 1024 * 1024, 12)
        compressed = download(str(asset.get("browser_download_url") or ""), 20 * 1024 * 1024, 30)
        source = "RELEASE_API_FALLBACK"

    try:
        manifest = json.loads(manifest_payload)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("BTC research snapshot manifest is invalid") from exc
    if not isinstance(manifest, dict) or manifest.get("snapshot_version") != "research-snapshot-v2":
        raise RuntimeError("BTC research snapshot manifest version is unsupported")
    if str(manifest.get("symbol") or "").upper() != "BTCUSDT" or tuple(manifest.get("timeframes") or ()) != ("15m", "1h", "4h", "1d"):
        raise RuntimeError("BTC research snapshot manifest scope is invalid")
    if manifest.get("no_real_execution") is not True or manifest.get("live_activation") != "FORBIDDEN":
        raise RuntimeError("BTC research snapshot live-execution flag is invalid")

    expected_hash = str((manifest.get("dashboard_gz") or {}).get("sha256") or "").lower()
    actual_hash = hashlib.sha256(compressed).hexdigest().lower()
    if not expected_hash or not hmac.compare_digest(expected_hash, actual_hash):
        raise RuntimeError("BTC research dashboard checksum does not match manifest")
    if len(compressed) > 20 * 1024 * 1024:
        raise RuntimeError("BTC research dashboard asset exceeds 20 MiB")
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(compressed), mode="rb") as stream:
            decompressed = stream.read(20 * 1024 * 1024 + 1)
    except (OSError, EOFError) as exc:
        raise RuntimeError("BTC research dashboard gzip is invalid") from exc
    if len(decompressed) > 20 * 1024 * 1024:
        raise RuntimeError("BTC research dashboard decompressed payload exceeds 20 MiB")
    try:
        value = json.loads(decompressed)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("BTC research dashboard JSON is invalid") from exc
    if not isinstance(value, dict):
        raise RuntimeError("BTC research dashboard payload is not an object")
    storage = value.get("storage") if isinstance(value.get("storage"), dict) else {}
    runs = value.get("runs") if isinstance(value.get("runs"), list) else []
    latest_run = runs[0] if runs and isinstance(runs[0], dict) else {}
    manifest_run = manifest.get("latest_run") if isinstance(manifest.get("latest_run"), dict) else {}
    if latest_run.get("status") != "COMPLETED" or float(latest_run.get("progress") or 0) < 100:
        raise RuntimeError("BTC research dashboard run is not complete")
    if manifest_run.get("research_run_id") and latest_run.get("research_run_id") != manifest_run.get("research_run_id"):
        raise RuntimeError("BTC research dashboard run does not match manifest")
    if storage.get("snapshot_version") and storage.get("snapshot_version") != manifest.get("snapshot_version"):
        raise RuntimeError("BTC research dashboard storage generation mismatch")
    value.setdefault("storage", {}).update({"source": "GITHUB_RELEASE", "release_transport": source,
                                             "cached_seconds": 300,
                                             "manifest_verified": True})
    _RESEARCH_RELEASE_CACHE.update({"at": now, "value": value})
    return value


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
            cur.execute("DELETE FROM apex_stats_events WHERE occurred_at < NOW() - INTERVAL '95 days'")
    finally:
        conn.close()
    return len(events)


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
    if strategy: where.append("(strategy=%s OR kind='market_data')"); params.append(strategy.upper())
    if symbol: where.append("symbol=%s"); params.append(symbol.upper())
    conn = _connect()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT event_key,kind,strategy,symbol,occurred_at,payload FROM apex_stats_events WHERE " + " AND ".join(where) + " ORDER BY occurred_at DESC LIMIT 50000", params)
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


def build_dashboard(days: int = 1, strategy: str = "", symbol: str = "", outcome: str = "", groq: str = "",
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
    manager_events=[]; apex_v2_snapshots=[]; lab_profile_events=[]
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
        elif e["kind"]=="lab_profile_shadow": lab_profile_events.append({**p,"strategy":e["strategy"],"symbol":e["symbol"],"occurred_at":e["occurred_at"]})
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
    wy_shadow = {"observed": 0, "old_pass": 0, "structural_pass": 0, "both_pass": 0, "structural_only": 0, "old_only": 0, "phase_ready": 0}
    wy_acc_shadow = {"observed": 0, "old_pass": 0, "structural_pass": 0, "both_pass": 0, "structural_only": 0, "old_only": 0, "phase_ready": 0}
    swing_volume_shadow = {"observed": 0, "pass_1_2": 0, "pass_1_1": 0, "shadow_only": 0}
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
            shadow = payload.get("volume_pass_1_1_shadow")
            if isinstance(current, bool) and isinstance(shadow, bool):
                swing_volume_shadow["observed"] += 1
                swing_volume_shadow["pass_1_2"] += int(current)
                swing_volume_shadow["pass_1_1"] += int(shadow)
                swing_volume_shadow["shadow_only"] += int(shadow and not current)
        if strategy_name == "FAST":
            payload = telemetry.get("fast_target_geometry") if isinstance(telemetry.get("fast_target_geometry"), dict) else {}
            if payload.get("reason"):
                fast_target_reasons[str(payload.get("reason"))] += 1
        if strategy_name == "WYCKOFF" and str(r.get("subtype") or "").upper() == "DISTRIBUTION":
            payload = telemetry.get("wyckoff_distribution") if isinstance(telemetry.get("wyckoff_distribution"), dict) else {}
            old_pass = payload.get("old_range_under_25")
            structural_pass = payload.get("structural_box_under_25")
            if isinstance(old_pass, bool) and isinstance(structural_pass, bool):
                wy_shadow["observed"] += 1
                wy_shadow["old_pass"] += int(old_pass)
                wy_shadow["structural_pass"] += int(structural_pass)
                wy_shadow["both_pass"] += int(old_pass and structural_pass)
                wy_shadow["structural_only"] += int(structural_pass and not old_pass)
                wy_shadow["old_only"] += int(old_pass and not structural_pass)
                wy_shadow["phase_ready"] += int(bool(payload.get("shadow_phase_ready")))
        if strategy_name == "WYCKOFF" and str(r.get("subtype") or "").upper() == "SPRING":
            payload = telemetry.get("wyckoff_accumulation") if isinstance(telemetry.get("wyckoff_accumulation"), dict) else {}
            old_pass = payload.get("old_range_under_25")
            structural_pass = payload.get("structural_box_under_25")
            if isinstance(old_pass, bool) and isinstance(structural_pass, bool):
                wy_acc_shadow["observed"] += 1
                wy_acc_shadow["old_pass"] += int(old_pass)
                wy_acc_shadow["structural_pass"] += int(structural_pass)
                wy_acc_shadow["both_pass"] += int(old_pass and structural_pass)
                wy_acc_shadow["structural_only"] += int(structural_pass and not old_pass)
                wy_acc_shadow["old_only"] += int(old_pass and not structural_pass)
                wy_acc_shadow["phase_ready"] += int(bool(payload.get("shadow_phase_ready")))
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
            state = "TARGET_ALREADY_PASSED" if target_passed else "AWAITING_REPLAY"
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
    incidents = latest_v2.get("open_incidents") if isinstance(latest_v2.get("open_incidents"), list) else []
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

    lab_summary = {}
    for row in lab_profile_events:
        name = str(row.get("strategy") or "UNKNOWN")
        item = lab_summary.setdefault(name, {"strategy": name, "runs": 0, "lab_candidates": 0,
            "live_candidates": 0, "same_outcome": 0, "unavailable": 0, "match_total": 0.0,
            "match_count": 0, "candidate_agreement": 0, "pending_vs_filtered": 0})
        item["runs"] += 1
        lab_outcome = str(row.get("lab_outcome") or "").upper()
        live_outcome = str(row.get("live_outcome") or "").upper()
        item["lab_candidates"] += lab_outcome == "CANDIDATE"
        item["live_candidates"] += live_outcome == "CANDIDATE"
        item["same_outcome"] += bool(lab_outcome and live_outcome and lab_outcome == live_outcome)
        item["candidate_agreement"] += ((lab_outcome == "CANDIDATE") == (live_outcome == "CANDIDATE"))
        item["pending_vs_filtered"] += {lab_outcome, live_outcome} == {"PENDING_LTF", "FILTERED"}
        item["unavailable"] += lab_outcome == "UNAVAILABLE"
        match = _num(row.get("hard_gate_match_pct"))
        if match is not None:
            item["match_total"] += match; item["match_count"] += 1
    for item in lab_summary.values():
        item["outcome_agreement_pct"] = round(item["same_outcome"] / item["runs"] * 100, 2) if item["runs"] else None
        item["candidate_agreement_pct"] = round(item["candidate_agreement"] / item["runs"] * 100, 2) if item["runs"] else None
        item["avg_hard_gate_match_pct"] = round(item["match_total"] / item["match_count"], 2) if item["match_count"] else None
        item.pop("match_total", None); item.pop("match_count", None)

    compact_lab_events = []
    for row in sorted(lab_profile_events, key=lambda x:x.get("occurred_at", ""), reverse=True)[:100]:
        lab_checks = row.get("checks") if isinstance(row.get("checks"), list) else []
        ordered = []
        for check in lab_checks:
            if not isinstance(check, dict): continue
            role = str(check.get("role") or "")
            status = str(check.get("status") or check.get("state") or "")
            if role == "HARD_GATE" or status in {"UNAVAILABLE", "ERROR"}:
                ordered.append({key: check.get(key) for key in
                    ("check_order", "check_code", "label", "role", "status", "measured", "threshold") if check.get(key) is not None})
        candidate = row.get("candidate") if isinstance(row.get("candidate"), dict) else {}
        compact_lab_events.append({
            "occurred_at": row.get("occurred_at"), "strategy": row.get("strategy"), "symbol": row.get("symbol"),
            "profile_id": row.get("profile_id"), "live_outcome": row.get("live_outcome"),
            "lab_outcome": row.get("lab_outcome"), "stop_code": row.get("stop_code"),
            "hard_gate_match_pct": row.get("hard_gate_match_pct"), "error": row.get("error"),
            "candidate": {key: candidate.get(key) for key in ("direction", "entry", "sl", "tp1", "tp2", "terminal_tp", "rr")},
            "ordered_hard_gates": ordered[:30],
        })

    total=len(joined); page_size=max(20,min(int(page_size),200)); page=max(1,int(page)); start=(page-1)*page_size
    # Keep the legacy attempt denominator available for release comparisons;
    # the user-facing pending_ltf value is the unique active-watch count above.
    legacy_pending_ltf = {"pending_ltf":sum(r.get("outcome")=="PENDING_LTF" for r in joined)}
    reviews_n=sum(groq_counts.values()); delivered_attempts=sum(1 for r in joined if any(str(d.get("stage") or "").lower()=="delivered" or str(d.get("outcome") or "").upper()=="ACCEPT" for d in r.get("decisions",[])))
    delivered_signal_ids = {
        str(t.get("signal_id")) for t in opened if t.get("signal_id") not in (None, "")
    }
    delivered = max(delivered_attempts, len(delivered_signal_ids), len(opened))
    return {"period_days":days,"baseline":"post97","baseline_utc":STATS_BASELINE_UTC.isoformat(),"generated_at":datetime.now(timezone.utc).isoformat(timespec="seconds"),
      "release_filter":release,"release_sha":active_release,"available_releases":available_releases[:12],"funnels":funnels,
      "bos_choch_age":bos_age_stats,"wyckoff_dist_range":wy_dist_range,"wyckoff_box_width":wy_box_range,
      "numeric_telemetry":numeric_telemetry,"wyckoff_shadow":wy_shadow,"wyckoff_accumulation_shadow":wy_acc_shadow,
      "swing_volume_shadow":swing_volume_shadow,"fast_target_diagnostics":dict(fast_target_reasons),
      "market_data":market_data,"ltf_watch":ltf_watch,"system_overview":system_overview,
      "source_registry":system_overview.get("source_registry",[]),
      "portfolio_dependency":latest_v2.get("portfolio_dependency",{}),
      "gate_microstructure":system_overview.get("gate_microstructure",[]),
      "api_budget_plan":system_overview.get("api_budget_plan",{}),
      "manager_v2":manager_summary,"opportunity_review":opportunity_review,
      "lab_profile_shadow":{"total":len(lab_profile_events),"by_strategy":sorted(lab_summary.values(),key=lambda x:x["strategy"]),"recent":compact_lab_events},
      "portfolio_risk":portfolio,"execution_mode":execution_mode,"incidents":incidents,"versions":versions,
      "execution_health":execution_health,"manager_db":manager_db,"learning_v2":learning_v2,
      "summary":{"attempts":total,"candidates":sum(r.get("outcome")=="CANDIDATE" for r in joined),"pending_ltf":len(ltf_rows),"pending_ltf_attempts":legacy_pending_ltf["pending_ltf"],"near_setups":len(opportunities),"groq_total":reviews_n,"groq_approve":groq_counts.get("APPROVE",0),"groq_wait":groq_counts.get("WAIT",0),"groq_reject":groq_counts.get("REJECT",0),"delivered":delivered,"scan_events":len(scan_events)},
      "strategy_counts":dict(Counter(str(r.get("strategy") or "UNKNOWN") for r in joined)),
      "failures":[{"label":k,"count":v} for k,v in failures.most_common(30)],
      "failures_by_strategy":{k:[{"label":a,"count":b} for a,b in v.most_common(30)] for k,v in by_strategy.items()},
      "criterion_stats":[{"strategy":k[0],"label":k[1],"pass":v.get("PASS",0),"fail":v.get("FAIL",0),"total":v.get("PASS",0)+v.get("FAIL",0)} for k,v in sorted(checks.items(),key=lambda x:sum(x[1].values()),reverse=True)[:300]],
      "groq":{"decisions":dict(groq_counts),"reasons":[{"label":k,"count":v} for k,v in groq_reasons.most_common(30)],"risks":[{"label":k,"count":v} for k,v in groq_risks.most_common(30)],"avg_confidence":{k:(sum(v)/len(v) if v else None) for k,v in conf.items()}},
      "trade_stats":trade_stats,"catalog":STRATEGY_CATALOG,"rows":joined[start:start+page_size],"pagination":{"page":page,"page_size":page_size,"total":total,"pages":max(1,(total+page_size-1)//page_size)}}


HTML=r'''<!doctype html><html lang=ru><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><meta name=robots content="noindex,nofollow"><meta name=referrer content=no-referrer><title>APEX Strategy Lab</title><style>
:root{color-scheme:dark;--bg:#0c0e12;--card:#151921;--muted:#8e98a8;--line:#29303c;--text:#eef2f7;--good:#4fd18b;--bad:#ff6b78;--warn:#f0c760;--accent:#77a7ff}*{box-sizing:border-box}body{margin:0;font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:var(--bg);color:var(--text)}main{max-width:1500px;margin:auto;padding:18px}.top{display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap}.title{font-size:24px;font-weight:800}.muted{color:var(--muted)}.controls,.tabs{display:flex;gap:8px;flex-wrap:wrap;margin:14px 0}.controls input,.controls select,.btn{background:#10141b;border:1px solid var(--line);color:var(--text);border-radius:9px;padding:9px 11px}.btn{cursor:pointer}.btn.active{border-color:var(--accent);background:#17243a}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px}.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:13px}.num{font-size:24px;font-weight:800}.section{margin-top:16px}.section h2{font-size:16px;margin:0 0 10px}.cols{display:grid;grid-template-columns:1fr 1fr;gap:12px}.barrow{display:grid;grid-template-columns:minmax(170px,1.8fr) 4fr 55px;gap:8px;align-items:center;margin:7px 0}.bar{height:8px;background:#232a35;border-radius:6px;overflow:hidden}.bar i{display:block;height:100%;background:var(--accent)}.tablewrap{overflow:auto;border:1px solid var(--line);border-radius:12px}table{width:100%;border-collapse:collapse;min-width:1180px;background:var(--card)}th,td{padding:9px 10px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}th{position:sticky;top:0;background:#171c25;z-index:2}.badge{display:inline-block;padding:2px 7px;border-radius:999px;border:1px solid var(--line);font-size:12px}.good{color:var(--good)}.bad{color:var(--bad)}.warn{color:var(--warn)}.details{display:none}.details.open{display:table-row}.detailbox{white-space:pre-wrap;max-height:620px;overflow:auto;background:#0e1218;padding:12px;border-radius:8px}.criteria{display:grid;grid-template-columns:repeat(2,minmax(280px,1fr));gap:8px}.crit{padding:9px;border:1px solid var(--line);border-radius:9px}.footer{margin:18px 0;color:var(--muted)}@media(max-width:1000px){.grid{grid-template-columns:repeat(2,1fr)}.cols{grid-template-columns:1fr}.criteria{grid-template-columns:1fr}}
</style></head><body><main><div class=top><div><div class=title>📊 APEX · Strategy Lab</div><div class=muted>Актуальная статистика после #97 · с 14:54:22 UTC 03.09.2026</div></div><div id=updated class=muted></div></div>
<div class=tabs id=v2tabs><button class="btn active" data-anchor=overviewV2>Overview</button><button class=btn data-anchor=strategyLabV2>Strategy Lab</button><button class=btn data-anchor=managerSection>Manager</button><button class=btn data-anchor=opportunitySection>Opportunities</button><button class=btn data-anchor=healthSection>Health</button><button class=btn data-anchor=learningSection>Learning</button><button class=btn data-anchor=researchShadowV2 id=researchTab>Research / Shadow</button></div><div id=liveDashboardV2><div class=tabs id=periods><button class="btn active" data-days=1>24 часа</button><button class=btn data-days=7>7 дней</button><button class=btn data-days=30>30 дней</button><button class=btn id=latestRelease>После последнего deploy</button></div><div class=tabs id=strategies><button class="btn active" data-strategy="">Все</button><button class=btn data-strategy=FAST>FAST</button><button class=btn data-strategy=MTF>MTF</button><button class=btn data-strategy=SWING>SWING</button><button class=btn data-strategy=ZONE>ZONE</button><button class=btn data-strategy=WYCKOFF>WYCKOFF</button></div>
<div class=controls><input id=symbol placeholder="Пара, напр. BTCUSDT"><input id=fromdate type=date title="Дата от"><input id=todate type=date title="Дата до"><select id=outcome><option value="">Все исходы</option><option>FILTERED</option><option>CANDIDATE</option><option>PENDING_LTF</option><option>ERROR</option></select><select id=groq><option value="">Любой Groq</option><option>APPROVE</option><option>WAIT</option><option>REJECT</option></select><input id=minrr type=number step=.1 placeholder="RR от"><input id=maxrr type=number step=.1 placeholder="RR до"><button class=btn id=apply>Применить</button><button class=btn id=refresh>↻</button></div><div id=overviewV2><div class=grid id=summary></div><div class="section card"><h2>APEX V2 · System overview</h2><div id=systemOverview></div><div id=apiBudget></div></div><div class="cols section"><div class=card><h2>Portfolio & Risk</h2><div id=portfolioRisk></div></div><div class=card><h2>Execution & Groq</h2><div id=executionHealth></div></div></div><div class="section card"><h2>Versions</h2><div id=versionsV2></div></div></div>
<div id=healthSection class="cols section"><div class=card><h2>Market Data / Gate</h2><div id=marketData></div></div><div class=card><h2>PENDING LTF lifecycle</h2><div id=ltfWatch></div></div></div><div class="section card"><h2>Open incidents</h2><div id=incidentsV2></div></div>
<div id=managerSection class="section card"><h2>Trade Manager 2.0</h2><div id=managerV2></div></div><div id=opportunitySection class="section card"><h2>Opportunity Review</h2><div id=opportunityV2></div></div>
<div id=strategyLabV2 class="section card"><h2>Сквозная воронка по стратегиям</h2><div id=funnels></div><div id=wyRange class=muted style="margin-top:10px"></div></div><div class="section card"><h2>Live ↔ лабораторный профиль</h2><div class=warn>Параллельный расчёт на тех же закрытых свечах; не имеет доступа к исполнению.</div><div id=labProfileShadow></div></div><div id=learningSection class="cols section"><div class=card><h2>BOS/CHoCH age telemetry</h2><div id=bosAge></div></div><div class=card><h2>WYCKOFF Distribution width telemetry</h2><div id=wyCompare></div></div></div><div class="section card"><h2>Counterfactual & Shadow</h2><div id=learningV2></div></div><div class="section card"><h2>Numeric funnel diagnostics</h2><div id=numericDiag></div></div><div class="cols section"><div class=card><h2>Где чаще всего останавливаются</h2><div id=failures></div></div><div class=card><h2>Groq: причины WAIT/REJECT</h2><div id=groqReasons></div></div></div><div class="section card"><h2>Проходимость критериев</h2><div id=criteriaStats></div></div><div class="section card"><h2>Статистика сделок</h2><div id=tradeStats></div></div><div class=section><h2>Все проверки / потенциальные сделки</h2><div class=tablewrap><table><thead><tr><th>Дата</th><th>Стратегия</th><th>Пара</th><th>Напр.</th><th>Статус</th><th>Где остановилась</th><th>Entry</th><th>SL</th><th>TP1</th><th>TP2</th><th>RR</th><th>Groq</th><th></th></tr></thead><tbody id=rows></tbody></table></div><div class=controls><button class=btn id=prev>←</button><span id=pageinfo class=muted></span><button class=btn id=next>→</button></div></div></div><div id=researchShadowV2 class="section card" style="display:none"><h2>🧪 Research / Backtest</h2><div class=muted>Откройте вкладку для загрузки отдельной Research DB.</div></div><div class=footer>Dashboard V2 только читает статистику. Он не меняет Entry / SL / TP / RR, стратегии, Groq, Manager или Binance.</div></main><script>
const TOKEN=new URLSearchParams(location.search).get('key')||'';let DAYS=1,STRATEGY='',PAGE=1,LAST=null,RELEASE='';const esc=s=>String(s??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));const num=v=>v===null||v===undefined||v===''?'—':Number(v).toLocaleString('ru-RU',{maximumFractionDigits:8});function params(){const p=new URLSearchParams({key:TOKEN,days:DAYS,strategy:STRATEGY,page:PAGE,page_size:100});if(RELEASE)p.set('release',RELEASE);for(const id of ['symbol','outcome','groq','fromdate','todate']){const v=document.getElementById(id).value.trim();if(v)p.set(id,v)}const a=minrr.value,b=maxrr.value;if(a)p.set('min_rr',a);if(b)p.set('max_rr',b);return p}async function load(){const r=await fetch('/api/dashboard?'+params());if(!r.ok){document.body.innerHTML='<main><h2>Статистика недоступна</h2><p>'+r.status+'</p></main>';return}LAST=await r.json();render()}function cards(s){const t=LAST.trade_stats||{};const wr=t.win_rate===null||t.win_rate===undefined?'—':t.win_rate+'%';const pnl=(Number(t.pnl_pct||0)>=0?'+':'')+Number(t.pnl_pct||0).toFixed(2)+'%';const ar=t.avg_r===null||t.avg_r===undefined?'—':Number(t.avg_r).toFixed(2)+'R';return [['Проверок',s.attempts],['Кандидатов',s.candidates],['Ждут LTF',s.pending_ltf||0],['Почти сделок',s.near_setups],['До Groq',s.groq_total],['Groq APPROVE',s.groq_approve],['WAIT / REJECT',s.groq_wait+' / '+s.groq_reject],['Отправлено',s.delivered],['Открыто',t.opened||0],['Закрыто',t.closed||0],['Win rate',wr],['P&L',pnl],['Средний R',ar]].map(x=>`<div class=card><div class=muted>${x[0]}</div><div class=num>${x[1]}</div></div>`).join('')}function bars(items,id){const el=document.getElementById(id),max=Math.max(1,...items.map(x=>x.count));el.innerHTML=items.slice(0,15).map(x=>`<div class=barrow><div title="${esc(x.label)}">${esc(x.label).slice(0,90)}</div><div class=bar><i style="width:${100*x.count/max}%"></i></div><b>${x.count}</b></div>`).join('')||'<span class=muted>Нет данных</span>'}function renderChecks(){const a=LAST.criterion_stats.filter(x=>!STRATEGY||x.strategy===STRATEGY);criteriaStats.innerHTML=a.slice(0,80).map(x=>{const p=x.total?Math.round(x.pass/x.total*100):0;return `<div class=barrow><div><b>${esc(x.strategy)}</b> · ${esc(x.label).slice(0,90)}<div class=muted>✅ ${x.pass} · ❌ ${x.fail} · всего ${x.total}</div></div><div class=bar><i style="width:${p}%"></i></div><b>${p}%</b></div>`}).join('')||'<span class=muted>Нет данных</span>'}function renderTradeStats(){const t=LAST.trade_stats||{},rows=t.by_strategy||[];const head=`<div class=muted style="margin-bottom:10px">Открыто: <b>${t.opened||0}</b> · Закрыто: <b>${t.closed||0}</b> · TP: <b class=good>${t.wins||0}</b> · SL: <b class=bad>${t.losses||0}</b> · Win rate: <b>${t.win_rate==null?'—':t.win_rate+'%'}</b> · P&L: <b class="${Number(t.pnl_pct||0)>=0?'good':'bad'}">${Number(t.pnl_pct||0)>=0?'+':''}${Number(t.pnl_pct||0).toFixed(3)}%</b> · Avg R: <b>${t.avg_r==null?'—':Number(t.avg_r).toFixed(3)+'R'}</b></div>`;const body=rows.length?`<div class=tablewrap><table style="min-width:760px"><thead><tr><th>Стратегия</th><th>Открыто</th><th>Закрыто</th><th>TP</th><th>SL</th><th>Win rate</th><th>P&L %</th><th>Avg R</th></tr></thead><tbody>${rows.map(r=>`<tr><td><b>${esc(r.strategy)}</b></td><td>${r.opened}</td><td>${r.closed}</td><td class=good>${r.wins}</td><td class=bad>${r.losses}</td><td>${r.win_rate==null?'—':r.win_rate+'%'}</td><td class="${Number(r.pnl_pct||0)>=0?'good':'bad'}">${Number(r.pnl_pct||0)>=0?'+':''}${Number(r.pnl_pct||0).toFixed(3)}%</td><td>${r.avg_r==null?'—':Number(r.avg_r).toFixed(3)+'R'}</td></tr>`).join('')}</tbody></table></div>`:'<span class=muted>Закрытых сделок пока нет</span>';tradeStats.innerHTML=head+body}function renderCatalog(){const names=STRATEGY?[STRATEGY]:['FAST','MTF','SWING','ZONE','WYCKOFF'];catalogTitle.textContent=STRATEGY?'Критерии '+STRATEGY:'Полный ромб критериев всех стратегий';catalog.innerHTML=names.map(n=>{const c=LAST.catalog[n];return `<div class=crit style="grid-column:1/-1"><b>${n}</b> · ${esc(c.timeframes)} · RR: ${esc(c.rr)}</div>`+c.criteria.map(q=>`<div class=crit><b>${q.required?'●':'○'} ${esc(q.label)}</b><div class=muted>${esc(q.category)}${q.required?' · обязательный':' · контекст/бонус'}</div>${q.detail?`<div>${esc(q.detail)}</div>`:''}</div>`).join('')}).join('')}function levels(r){const c=r.candidate||{},s=(r.stop||{}).snapshot||{};return {entry:c.entry??s.entry,sl:c.sl??s.sl,tp1:c.tp1??c.tp??s.tp1??s.tp,tp2:c.tp2??s.tp2,tp3:c.tp3??s.tp3,rr:r.rr_value}}
function normText(v){return String(v||'').toLowerCase().replace(/[_/.-]+/g,' ').replace(/[^a-zа-я0-9 ]/gi,' ').replace(/\s+/g,' ').trim()}
function words(v){return new Set(normText(v).split(' ').filter(x=>x.length>2&&!['the','and','for','with','not','или','для','при','что','это'].includes(x)))}
function matchCriterion(q,checks,stop){const qc=normText(q.code),ql=normText(q.label),qw=words(q.code+' '+q.label);let best=null,bestScore=0;for(const ch of checks){const raw=[ch.code,ch.label,ch.condition].filter(Boolean).join(' '),n=normText(raw);let score=0;if(qc&&n.includes(qc))score+=12;if(ql&&n.includes(ql))score+=10;const cw=words(raw);for(const w of qw)if(cw.has(w))score+=1;if(score>bestScore){bestScore=score;best=ch}}if(bestScore>=3)return {state:String(best.state||'UNKNOWN').toUpperCase(),source:best};const st=normText((stop?.code||'')+' '+(stop?.label||'')+' '+(stop?.condition||''));if(qc==='fast rr'&&/rr 2 0|rr less 2 0/.test(st.replace('<',' less ')))return {state:'FAIL',source:stop};let ss=0;for(const w of qw)if(st.includes(w))ss++;if((qc&&st.includes(qc))||(ql&&st.includes(ql))||ss>=2)return {state:'FAIL',source:stop};return {state:'NOT_REACHED',source:null}}
function sectionName(cat,required){const c=normText(cat);if(/final deterministic|geometry/.test(c))return 'FINAL / GEOMETRY';if(/trigger|structure|phase/.test(c))return 'TRIGGER / STRUCTURE';if(/confirmation|risk context|market context|learning/.test(c))return required?'CORE CONTEXT':'ADDITIONAL';if(/ai quality/.test(c))return 'GROQ';return required?'CORE':'ADDITIONAL'}
const detailTextWithoutLivePath=detailText;
detailText=function(r){const p=r.decision_path||{},label=x=>x?`${x.order}. ${x.label||x.code} [${x.state}]`:'—',lines=['[ACTUAL LIVE EXECUTION ORDER]',`FIRST: ${label(p.first_reached)}`,`LAST REACHED: ${label(p.last_reached)}`,`LAST PASSED: ${label(p.last_passed)}`,`BLOCKING STEP: ${label(p.blocking_step)}`,`FINAL OUTCOME: ${p.final_outcome||r.outcome||'—'}`];for(const x of (p.steps||[]))lines.push(`${x.blocking_stop?'🛑':x.state==='PASS'?'✅':x.state==='FAIL'?'❌':'⚠️'} ${x.order}. ${x.label||x.code} · ${x.state}`);return lines.join('\n')+'\n\n'+detailTextWithoutLivePath(r)}
function detailText(r){const l=levels(r),g=r.groq_review||{},stop=r.stop||{},snap=stop.snapshot||{},checks=Array.isArray(r.checks)?r.checks:[],catalog=(LAST.catalog||{})[r.strategy]||{criteria:[]};const dir=String(r.candidate?.direction??snap.direction??'').toUpperCase()||'—';const lines=[`${r.symbol||'—'} · ${r.strategy||'—'} · ${dir}`,(r.finished_at||r.occurred_at||'').replace('T',' ').slice(0,19)+' UTC',''];const groups={};let pass=0,fail=0,nr=0,opt=0;for(const q of (catalog.criteria||[])){let m=matchCriterion(q,checks,stop);if(q.code.endsWith('_groq'))m=g.decision?{state:String(g.decision).toUpperCase()==='APPROVE'?'PASS':'FAIL'}:{state:'NOT_REACHED'};const group=sectionName(q.category,q.required);if(!groups[group])groups[group]=[];const state=m.state,icon=state==='PASS'?'✅':state==='FAIL'?'❌':'⏭';if(state==='PASS')pass++;else if(state==='FAIL')fail++;else nr++;if(!q.required)opt++;groups[group].push(`${icon} ${q.required?'[MAIN]':'[ADD]'} ${q.label}${state==='NOT_REACHED'?' · not reached':''}`)}lines.push(`[SUMMARY]`,`✅ Passed: ${pass} · ❌ Failed: ${fail} · ⏭ Not reached: ${nr} · ADD criteria: ${opt}`,'');for(const k of ['CORE','CORE CONTEXT','TRIGGER / STRUCTURE','ADDITIONAL','FINAL / GEOMETRY','GROQ'])if(groups[k]?.length)lines.push(`[${k}]`,...groups[k],'');const matched=new Set();for(const q of (catalog.criteria||[])){const m=matchCriterion(q,checks,stop);if(m.source)matched.add(m.source)}const extras=checks.filter(ch=>!matched.has(ch)&&!String(ch.code||'').startsWith('_')&&!String(ch.label||'').startsWith('_'));if(extras.length){lines.push('[OTHER RECORDED CHECKS]');for(const ch of extras){const st=String(ch.state||'UNKNOWN').toUpperCase();lines.push(`${st==='PASS'?'✅':st==='FAIL'?'❌':'⚠️'} ${ch.label||ch.condition||ch.code}`)}lines.push('')}lines.push('[LEVELS]',`Entry: ${num(l.entry)}`,`SL:    ${num(l.sl)}`,`TP1:   ${num(l.tp1)}`,`TP2:   ${num(l.tp2)}`,`TP3:   ${num(l.tp3)}`,`RR:    ${num(l.rr)}`,'');if(g.decision){lines.push('[GROQ RESULT]',`Decision: ${g.decision}`,`Confidence: ${g.confidence!==undefined?Math.round(Number(g.confidence)*100)+'%':'—'}`);if(Array.isArray(g.reasons)&&g.reasons.length)lines.push('Reasons:',...g.reasons.map(x=>'❌ '+x));if(Array.isArray(g.risks)&&g.risks.length)lines.push('Risks:',...g.risks.map(x=>'⚠️ '+x));lines.push('')}else lines.push('[GROQ RESULT]','⏭ Not reached','');lines.push('[STOP]',stop.label||stop.code||'—');return lines.join('\n')}
function renderV2(){const o=LAST.system_overview||{},p=LAST.portfolio_risk||{},m=LAST.manager_v2||{},mdb=LAST.manager_db||{},e=LAST.execution_mode||{},eh=LAST.execution_health||{},v=LAST.versions||{},op=LAST.opportunity_review||{},learn=LAST.learning_v2||{},incs=LAST.incidents||[];const state=o.state||'UNKNOWN',sc=state==='HEALTHY'?'good':state==='CRITICAL'?'bad':'warn';systemOverview.innerHTML=`<div class=criteria><div class=crit><b class=${sc}>${esc(state)}</b><div class=muted>Состояние всей системы</div></div><div class=crit><b>Gate ${o.market_data_ok||0}/${o.market_data_total||0}</b><div class=muted>последний release</div></div><div class=crit><b>Manager cycles ${o.manager_cycles||0}</b><div class=muted>Groq entry reviews ${o.groq_entry_reviews||0}</div></div><div class=crit><b>Incidents ${o.incidents||0}</b><div class=muted>snapshot ${esc((o.snapshot_at||'—').replace('T',' ').slice(0,19))}</div></div></div>`;apiBudget.innerHTML='<h3>External API budget · rolling 24h</h3><div class=muted>Локальное распределение APEX; лимиты подписки и общего IP проверяются отдельно. Binance execution не входит.</div>'+((o.api_budget||[]).map(x=>`<div class=crit><b>${esc(x.source)}</b> · units ${num((x.used||{}).day)}/${num((x.allocation||{}).day)} · remaining ${num(x.remaining_day)}<div class=muted>minute ${num((x.used||{}).minute)}/${num((x.allocation||{}).minute)} · hour ${num((x.used||{}).hour)}/${num((x.allocation||{}).hour)} · 429/418 ${num((x.health||{}).rate_limits)} · deferred ${num((x.health||{}).denied)}</div></div>`).join('')||'<span class=muted>Ledger snapshot ещё не получен</span>');portfolioRisk.innerHTML=p.observed_at?`<b class="${p.risk_state==='OK'?'good':'bad'}">${esc(p.risk_state)}</b> · positions ${p.open_positions||0}<div>Risk ${num(p.total_risk_pct)}% · LONG ${num(p.long_risk_pct)}% · SHORT ${num(p.short_risk_pct)}%</div><div class=muted>${esc((p.reasons||[]).join(', ')||'No portfolio blocker')}</div>`:'<span class=muted>Snapshot ещё не получен</span>';executionHealth.innerHTML=`<div><b>${esc(String(e.mode||'unknown').toUpperCase())}</b> · enabled ${e.enabled?'yes':'no'} · live armed <span class="${e.live_armed?'warn':'muted'}">${e.live_armed?'yes':'no'}</span> · kill switch ${e.kill_switch?'ON':'off'}</div><div class=muted>Manager Groq calls ${m.groq_calls||0} · execution failures ${m.failures||0}</div><div>${Object.entries(eh.statuses||{}).map(([k,n])=>`<span class=badge style="margin:3px">${esc(k)} ${n}</span>`).join('')}</div>`;versionsV2.innerHTML=Object.entries(v).map(([k,x])=>`<span class=badge style="margin:3px">${esc(k)}: ${esc(x)}</span>`).join('')||'<span class=muted>Version snapshot ещё не получен</span>';incidentsV2.innerHTML=incs.length?incs.map(x=>`<div class=crit><b class="${x.severity==='CRITICAL'?'bad':'warn'}">${esc(x.severity)} · ${esc(x.component)}</b><div>${esc(x.impact)}</div><div class=muted>${esc(x.started_at)}</div></div>`).join(''):'<span class=good>Открытых инцидентов нет</span>';managerV2.innerHTML=`<div class=muted>cycles ${m.cycles||0} · Groq calls ${m.groq_calls||0} · failures ${m.failures||0}</div><div>${Object.entries(m.actions||{}).map(([k,n])=>`<span class=badge style="margin:3px">${esc(k)} ${n}</span>`).join('')}${Object.entries(mdb.states||{}).map(([k,n])=>`<span class=badge style="margin:3px">${esc(k)} ${n}</span>`).join('')}</div>`+(m.recent||[]).slice(0,20).map(x=>`<div class=crit style="margin-top:7px"><b>${esc(x.strategy)} · ${esc(x.symbol)} · ${esc(x.action)}</b><div>${esc((x.events||[]).join(', '))}</div><div class=muted>${esc(x.state)} · ${esc(x.execution_status||'no exchange action')} · ${esc((x.occurred_at||'').replace('T',' ').slice(0,19))}</div></div>`).join('');opportunityV2.innerHTML=`<div>${Object.entries(op.counts||{}).map(([k,n])=>`<span class=badge style="margin:3px">${esc(k)} ${n}</span>`).join('')||'<span class=muted>Почти-сделок нет</span>'}</div>`+(op.recent||[]).slice(0,30).map(x=>`<div class=crit style="margin-top:7px"><b>${esc(x.strategy)} · ${esc(x.symbol)} · ${esc(x.execution_state)}</b><div>${esc(x.stop_reason)} · Entry ${num(x.entry)} · TP1 ${num(x.tp1)} · RR ${num(x.rr)}</div><div class=muted>Decision price ${num(x.decision_price)} · ${esc((x.occurred_at||'').replace('T',' ').slice(0,19))}</div></div>`).join('');learningV2.innerHTML=`<div class=muted>Replay trades ${(learn.replay||[]).length} · shadow rules ${(learn.shadow_rules||[]).length}</div>`+(learn.replay||[]).slice(0,20).map(x=>`<div class=crit style="margin-top:7px"><b>Trade ${x.signal_id}</b><div>Groq edge ${num(x.groq_edge_r)}R · vs rules ${num(x.groq_vs_rules_r)}R · Playbook edge ${num(x.playbook_edge_r)}R</div></div>`).join('')+(learn.shadow_rules||[]).slice(0,20).map(x=>`<div class=crit style="margin-top:7px"><b>${esc(x.strategy)} · ${esc(x.rule_id)}</b><div>eligible ${x.eligible||0} · old/new ${x.old_pass||0}/${x.new_pass||0} · promotion ${x.promotion_eligible?'review only':'no'}</div></div>`).join('')}
function renderNumericDiag(){const data=LAST.numeric_telemetry||{};const labels={displacement_body_ratio:'SWING body/range (raw; direction ignored)',directional_displacement_ratio:'SWING directional displacement (actual gate ratio)',volume_ratio:'SWING volume/avg',retest_distance_atr:'SWING retest distance/ATR',pd_position_pct:'MTF Premium/Discount position %',pd_mid_distance_pct:'MTF distance from mid (% range)',positive_confluence_count:'MTF positive confluence',core_tf_match:'MTF 1h/4h match count',rr_value:'MTF RR',range_position_pct:'ZONE range position %',range_atr:'ZONE range/ATR',zone_distance_atr:'ZONE distance/ATR',test_count:'ZONE test count',best_directional_displacement_ratio:'ZONE best displacement',best_directional_body_atr:'ZONE best body/ATR',quality_score:'ZONE quality score'};const blocks=[];for(const st of ['SWING','MTF','ZONE']){for(const [key,x] of Object.entries(data[st]||{})){if(!x||!x.count)continue;blocks.push(`<div class=crit><b>${esc(labels[key]||st+' '+key)}</b><div class=muted>n=${x.count}</div><div>P25 ${num(x.p25)} · P50 ${num(x.median)} · P75 ${num(x.p75)} · P90 ${num(x.p90)}</div></div>`)}}numericDiag.innerHTML=`<div class=criteria>${blocks.join('')||'<span class=muted>Нет данных</span>'}</div>`}function renderBosAge(){const data=LAST.bos_choch_age||{};const blocks=['SWING','FAST'].map(st=>{const items=data[st]||[],max=Math.max(1,...items.map(x=>x.events||0));const body=items.map(x=>`<div class=barrow><div><b>${st} · age ${esc(x.bucket)}</b><div class=muted>events ${x.events} · retest ${x.retest} · disp ${x.displacement} · vol ${x.volume} · RR ${x.rr} · Groq ${x.groq_pct}% · sent ${x.delivered_pct}%</div></div><div class=bar><i style="width:${100*(x.events||0)/max}%"></i></div><b>${x.events}</b></div>`).join('');return `<div style="margin-bottom:12px">${body||'<span class=muted>Нет данных</span>'}</div>`}).join('');bosAge.innerHTML=blocks||'<span class=muted>Нет данных</span>'}function renderWyCompare(){const old=LAST.wyckoff_dist_range||{},box=LAST.wyckoff_box_width||{},sh=LAST.wyckoff_shadow||{};const fmt=(name,x)=>x.count?`<div class=crit><b>${name}</b><div class=muted>n=${x.count}</div><div>P25 ${num(x.p25)}% · P50 ${num(x.median)}% · P75 ${num(x.p75)}% · P90 ${num(x.p90)}%</div></div>`:`<div class=crit><b>${name}</b><div class=muted>Нет данных</div></div>`;const shadow=sh.observed?`<div class=crit><b>Shadow @ 25%</b><div class=muted>n=${sh.observed}</div><div>old pass ${sh.old_pass} · structural pass ${sh.structural_pass} · structural-only ${sh.structural_only} · old-only ${sh.old_only}</div></div>`:'';wyCompare.innerHTML=`<div class=criteria>${fmt('Старый 30d high-low',old)}${fmt('Новый BC/AR/ST box',box)}${shadow}</div>`}function renderRows(){rows.innerHTML=LAST.rows.map((r,i)=>{const l=levels(r),g=r.groq_review||{},st=r.outcome||'—',stop=r.stop||{},cls=st==='CANDIDATE'?'good':st==='ERROR'?'bad':'warn',dir=String(r.candidate?.direction??stop.snapshot?.direction??'').toUpperCase(),det={stop:r.stop,checks:r.checks,candidate:r.candidate,groq_review:r.groq_review,decisions:r.decisions,subtype:r.subtype,function:r.function,run_id:r.run_id,duration_ms:r.duration_ms};return `<tr><td>${esc((r.finished_at||r.occurred_at||'').replace('T',' ').slice(0,19))}</td><td><b>${esc(r.strategy)}</b>${r.subtype?`<div class=muted>${esc(r.subtype)}</div>`:''}</td><td>${esc(r.symbol)}</td><td>${esc(dir)}</td><td><span class="badge ${cls}">${esc(st)}</span>${r.near_setup?'<div class=warn>почти сделка</div>':''}</td><td>${esc(stop.label||stop.code||'—').slice(0,120)}</td><td>${num(l.entry)}</td><td>${num(l.sl)}</td><td>${num(l.tp1)}</td><td>${num(l.tp2)}</td><td>${num(l.rr)}</td><td>${g.decision?`<b>${esc(g.decision)}</b><div>${g.confidence!==undefined?Math.round(Number(g.confidence)*100)+'%':''}</div>`:'—'}</td><td><button class=btn onclick="toggle(${i})">детали</button></td></tr><tr class=details id=d${i}><td colspan=13><div class=detailbox>${esc(detailText(r))}</div><details><summary class=muted style="cursor:pointer;margin-top:8px">Raw data</summary><div class=detailbox>${esc(JSON.stringify(det,null,2))}</div></details></td></tr>`}).join('')||'<tr><td colspan=13 class=muted>Нет строк</td></tr>';pageinfo.textContent=`Страница ${LAST.pagination.page}/${LAST.pagination.pages} · строк ${LAST.pagination.total}`}window.toggle=i=>document.getElementById('d'+i).classList.toggle('open');function renderFunnels(){const data=(LAST.funnels||[]).filter(x=>!STRATEGY||x.strategy===STRATEGY);funnels.innerHTML=data.map(f=>`<div style="margin-bottom:14px"><b>${esc(f.strategy)}</b> · старт ${f.attempts} → кандидаты ${f.candidates} → ждут LTF ${f.pending_ltf||0} → Groq ${f.groq} → отправлено ${f.delivered}<div class=muted>${f.steps.map(x=>`${esc(x.label)} [${esc(x.role||'UNKNOWN')}]: ${x.passed}/${x.reached} (${x.pass_rate??0}%) · STOP ${x.blocking_stops||0}`).join(' → ')}</div></div>`).join('')||'<span class=muted>Нет данных</span>';const w=LAST.wyckoff_dist_range||{};wyRange.textContent=w.count?`WYCKOFF Distribution range: n=${w.count} · P25=${w.p25}% · median=${w.median}% · P75=${w.p75}% · P90=${w.p90}%`:'';}function render(){updated.textContent='Обновлено '+LAST.generated_at.replace('T',' ').slice(0,19)+' UTC'+(LAST.release_sha?' · '+LAST.release_sha.slice(0,8):'');summary.innerHTML=cards(LAST.summary);renderV2();renderFunnels();renderBosAge();renderWyCompare();renderNumericDiag();bars(LAST.failures,'failures');bars(LAST.groq.reasons,'groqReasons');renderChecks();renderTradeStats();renderRows()}document.querySelectorAll('#periods .btn').forEach(b=>b.onclick=()=>{document.querySelectorAll('#periods .btn').forEach(x=>x.classList.remove('active'));b.classList.add('active');DAYS=Number(b.dataset.days);PAGE=1;load()});function switchDashboard(anchor){const research=anchor==='researchShadowV2';liveDashboardV2.style.display=research?'none':'';researchShadowV2.style.display=research?'':'none';periods.style.display=research?'none':'';strategies.style.display=research?'none':'';if(research&&RESEARCH===null)loadResearch();if(!research)document.getElementById(anchor)?.scrollIntoView({behavior:'smooth'})}document.querySelectorAll('#v2tabs .btn').forEach(b=>b.onclick=()=>{document.querySelectorAll('#v2tabs .btn').forEach(x=>x.classList.remove('active'));b.classList.add('active');switchDashboard(b.dataset.anchor)});document.querySelectorAll('#strategies .btn').forEach(b=>b.onclick=()=>{document.querySelectorAll('#strategies .btn').forEach(x=>x.classList.remove('active'));b.classList.add('active');STRATEGY=b.dataset.strategy;PAGE=1;load()});apply.onclick=()=>{PAGE=1;load()};refresh.onclick=load;latestRelease.onclick=()=>{RELEASE=RELEASE?'':'latest';latestRelease.classList.toggle('active',!!RELEASE);PAGE=1;load()};prev.onclick=()=>{if(PAGE>1){PAGE--;load()}};next.onclick=()=>{if(LAST&&PAGE<LAST.pagination.pages){PAGE++;load()}};load();setInterval(load,60000);
function dashboardLoadWarning(message){let el=document.getElementById('dashboardLoadWarning');if(!el){el=document.createElement('div');el.id='dashboardLoadWarning';el.className='warn';el.style.cssText='position:sticky;top:0;z-index:20;padding:10px 16px;background:#3b2608;border-bottom:1px solid #f59e0b';document.body.prepend(el)}el.textContent=message}
function clearDashboardLoadWarning(){document.getElementById('dashboardLoadWarning')?.remove()}
async function load(){try{const r=await fetch('/api/dashboard?'+params());if(!r.ok){dashboardLoadWarning('Dashboard HTTP '+r.status+' · последняя успешная статистика сохранена; нули не подставляются.');return}const next=await r.json();LAST=next;clearDashboardLoadWarning();render()}catch(e){dashboardLoadWarning('Dashboard временно недоступен · последняя успешная статистика сохранена; нули не подставляются.')}}
</script><script>
function renderExtensions(){if(!LAST)return;let box=document.getElementById('extensionsV2');if(!box){box=document.createElement('div');box.id='extensionsV2';box.className='section card';document.getElementById('overviewV2').appendChild(box)}const o=LAST.system_overview||{},l=LAST.learning_v2||{},reg=o.source_registry||LAST.source_registry||[],dep=LAST.portfolio_dependency||{},cal=l.groq_calibration||LAST.groq_calibration||{},replays=l.replay_v2||LAST.replay_v2||[],sh=l.shadow_evaluations||LAST.shadow_evaluations||[];const regHtml=reg.slice(0,20).map(x=>`<span class="badge" style="margin:3px" title="${esc(x.provenance||'')}">${esc(x.source)} · ${esc(x.mode)} · ${x.market_data?'market':'context'} · ${esc(x.fallback||'')}</span>`).join('')||'<span class="muted">Реестр ещё не загружен</span>';const clusters=(dep.clusters||[]).map(x=>`<span class="badge" style="margin:3px">${esc(x.join(' · '))}</span>`).join('')||'<span class="muted">Нет Gate dependency snapshot</span>';const replayHtml=replays.slice(0,10).map(x=>`<div class="crit"><b>Replay ${esc(x.signal_id)}</b> · Groq edge ${num(x.groq_edge_r)}R · vs rules ${num(x.groq_vs_rules_r)}R · Playbook edge ${num(x.playbook_edge_r)}R</div>`).join('')||'<span class="muted">Нет сохранённых replay bundles</span>';const shadowHtml=sh.slice(0,10).map(x=>`<div class="crit"><b>${esc(x.strategy)} · ${esc(x.rule_id)}</b> · n=${num(x.eligible)} · mean/median ΔR ${num(x.mean_delta_r)}/${num(x.median_delta_r)} · ${x.promotion_proposed?'только review':'не готово'}</div>`).join('')||'<span class="muted">Нет shadow evidence</span>';box.innerHTML=`<h2>APEX V2 · Source / Learning diagnostics</h2><div class="muted">Новые источники и книжные правила не меняют входы, RR, риск или исполнение.</div><div class="crit"><b>Source Registry</b><div>${regHtml}</div></div><div class="crit"><b>Gate dependency clusters</b><div>${clusters}</div></div><div class="crit"><b>Groq calibration</b><div>calls ${num(cal.calls)} · resolved ${num(cal.resolved)} · Brier ${num(cal.brier)} · mean reward ${num(cal.mean_reward_r)}R</div></div><div class="crit"><b>Replay edges</b>${replayHtml}</div><div class="crit"><b>Shadow promotion evidence</b>${shadowHtml}</div>`}
setInterval(renderExtensions,2000);renderExtensions();
</script><script>
function renderMicrostructure(){if(!LAST)return;let box=document.getElementById('extensionsV2');if(!box)return;let node=document.getElementById('microstructureV2');if(!node){node=document.createElement('div');node.id='microstructureV2';node.className='crit';box.appendChild(node)}const rows=(LAST.gate_microstructure||[]);node.innerHTML='<b>Gate WS microstructure shadow</b><div>'+((rows.slice(0,10).map(x=>esc(x.symbol)+' · update '+esc(x.update_id)).join('<br>'))||'<span class="muted">Нет наблюдений</span>')+'</div><div class="muted">Только spread/depth/flow; это не доказательство намерений или охоты за стопами.</div>'}
setInterval(renderMicrostructure,2000);renderMicrostructure();
</script><script>
let RESEARCH=null;
async function loadResearch(){const box=document.getElementById('researchShadowV2');if(!box)return;box.innerHTML='<h2>🧪 Research / Shadow</h2><span class=muted>Загрузка…</span>';const r=await fetch('/api/research?key='+encodeURIComponent(TOKEN));if(!r.ok){box.innerHTML='<h2>🧪 Research / Shadow</h2><span class=warn>База Research пока недоступна: HTTP '+r.status+'</span>';return}RESEARCH=await r.json();renderResearch();renderResearchDiagnostics();renderResearchLifecycle();renderResearchExperiments();renderResearchAttemptDetails();renderResearchDecisionPaths();renderResearchMarketContext()}
function researchLoadWarning(message){const box=document.getElementById('researchShadowV2');if(!box)return;let el=document.getElementById('researchLoadWarning');if(!el){el=document.createElement('div');el.id='researchLoadWarning';el.className='warn';el.style.marginBottom='10px';box.prepend(el)}el.textContent=message}
function clearResearchLoadWarning(){document.getElementById('researchLoadWarning')?.remove()}
async function loadResearch(){const box=document.getElementById('researchShadowV2');if(!box)return;if(RESEARCH===null)box.innerHTML='<h2>🧪 Research / Shadow</h2><span class=muted>Загрузка…</span>';try{const r=await fetch('/api/research?key='+encodeURIComponent(TOKEN));if(!r.ok){if(RESEARCH!==null)researchLoadWarning('Research HTTP '+r.status+' · сохранён последний успешный снимок.');else box.innerHTML='<h2>🧪 Research / Shadow</h2><span class=warn>База Research пока недоступна: HTTP '+r.status+'</span>';return}const next=await r.json();RESEARCH=next;renderResearch();renderResearchDiagnostics();renderResearchLifecycle();renderResearchExperiments();renderResearchAttemptDetails();renderResearchDecisionPaths();renderResearchMarketContext();clearResearchLoadWarning()}catch(e){if(RESEARCH!==null)researchLoadWarning('Research временно недоступен · сохранён последний успешный снимок.');else box.innerHTML='<h2>🧪 Research / Shadow</h2><span class=warn>База Research временно недоступна</span>'}}
function researchMeta(v){try{return typeof v==='string'?JSON.parse(v):v||{}}catch(_){return {raw:String(v||'')}}}
function renderResearch(){const d=RESEARCH||{},meta=d.meta||{},runs=d.runs||[],jobs=d.jobs||[],candles=d.candles||[],funnels=d.funnels||[],trades=d.trades||[],quality=d.quality||[],evals=d.evaluations||[],active=d.active_shadow||[],pair=researchMeta(meta.pair_progress),worker=researchMeta(meta.worker_state),load=researchMeta(meta.research_load),api=d.api_usage||{},day=api.day||{},minute=api.minute||{};const state=worker.state||worker.raw||'NOT_STARTED',pct=Math.max(0,Math.min(100,Number(pair.pair_percent||0))),overall=Math.max(0,Math.min(100,Number(pair.overall_percent||0))),strategies=pair.strategy_status||{};researchShadowV2.innerHTML=`<h2>🧪 APEX Research / Shadow Lab</h2><div class=warn><b>NO REAL EXECUTION</b> · Research не меняет production-профили, Entry/SL/TP/RR, риск и Binance.</div><div class="cols section"><div class=card><h2>Текущая пара · ${esc(pair.current_symbol||'—')}</h2><div><b>${num(pair.pair_index||0)}/${num(pair.pair_total||0)} · ${num(pct)}%</b> · ${esc(pair.stage||state)}</div><div style="height:12px;background:#202a38;border-radius:8px;overflow:hidden;margin:8px 0"><div style="height:100%;width:${pct}%;background:#32d583"></div></div><div class=muted>Общий прогресс ${num(overall)}% · RSS ${num(pair.rss_mb||load.rss_mb)} MB</div><div>${Object.entries(strategies).map(([k,v])=>`<span class=badge style="margin:3px">${esc(k)} · ${esc(v)}</span>`).join('')}</div></div><div class=card><h2>Лимиты нагрузки</h2><div>Gate сегодня: <b>${num(day.used||0)}/${num(day.limit_value||0)}</b></div><div>Текущая минута: ${num(minute.used||0)}/${num(minute.limit_value||0)}</div><div class=muted>CPU duty ${num(load.cpu_duty_percent)}% · RSS guard ${num(load.max_rss_mb)} MB · denied ${num(day.denied||0)} · 429 ${num(day.rate_limited||0)}</div></div></div><div class="grid section">${candles.map(x=>`<div class=card><div class=muted>${esc(x.timeframe)} · ${num(x.symbols)} пар</div><div class=num>${num(x.candles)}</div><div class=muted>${x.coverage_start?new Date(Number(x.coverage_start)*1000).toISOString().slice(0,10):'—'} → ${x.coverage_end?new Date(Number(x.coverage_end)*1000).toISOString().slice(0,10):'—'}</div></div>`).join('')||'<div class=card><span class=muted>Backfill ещё не начат</span></div>'}</div><div class="cols section"><div class=card><h2>Research Health / Progress</h2><div class=muted>Worker: ${esc(state)}</div>${jobs.slice(0,20).map(x=>`<div class=crit><b>${esc(x.job_type)} · ${esc(x.symbol)} ${esc(x.timeframe)}</b><div>${num(x.progress)}% · ${esc(x.status)}</div><div class=muted>checkpoint ${esc(x.last_timestamp||'—')} · ${esc(x.error||'')}</div></div>`).join('')||'<span class=muted>Нет задач</span>'}</div><div class=card><h2>Dataset / Replay versions</h2>${runs.map(x=>`<div class=crit><b>${esc(x.run_type)}</b><div>${esc(x.strategy_version)} · ${num(x.progress)}% · ${esc(x.status)}</div><div class=muted>${esc(x.dataset_version)} · ${esc(x.feature_version)} · ${esc(String(x.code_sha||'').slice(0,8))}</div></div>`).join('')||'<span class=muted>Replay ещё не запускался</span>'}</div></div><div class="cols section"><div class=card><h2>Research Funnel</h2>${funnels.map(x=>`<span class=badge style="margin:3px">${esc(x.parent_strategy)} · ${esc(x.outcome)} ${num(x.count)}</span>`).join('')||'<span class=muted>Нет point-in-time attempts</span>'}</div><div class=card><h2>Shadow outcomes</h2>${trades.map(x=>`<div class=crit><b>${esc(x.parent_strategy)} · ${esc(x.track)} · ${esc(x.status)}</b><div>n=${num(x.count)} · WR ${num(x.win_rate)}% · expectancy ${num(x.expectancy)}R</div></div>`).join('')||'<span class=muted>Нет завершённых shadow-сделок</span>'}</div></div><div class="cols section"><div class=card><h2>Indicator / Segment Lab</h2>${evals.slice(0,30).map(x=>`<div class=crit><b>${esc(x.feature)} · ${esc(x.segment_json)}</b><div>n=${num(x.sample_size)} · expectancy ${num(x.expectancy)}R · uplift ${num(x.uplift)}R · OOS ${num(x.oos_uplift)}R</div><div class=muted>${esc(x.status)} · auto activation: forbidden</div></div>`).join('')||'<span class=muted>Нужны завершённые replay-сделки</span>'}</div><div class=card><h2>Data Quality</h2>${quality.map(x=>`<div class=crit><b class="${x.severity==='ERROR'?'bad':'warn'}">${esc(x.issue_type)}</b> · ${num(x.count)}</div>`).join('')||'<span class=good>Открытых ошибок истории нет</span>'}<h2 style="margin-top:14px">Shadow Active</h2>${active.map(x=>`<div class=crit><b>${esc(x.parent_strategy)} · ${esc(x.symbol)} · ${esc(x.direction)}</b><div>Entry ${num(x.entry)} · SL ${num(x.initial_sl)} · TP1 ${num(x.tp1)} · MFE ${num(x.mfe_r)}R</div></div>`).join('')||'<span class=muted>Нет активных виртуальных позиций</span>'}</div></div>`}
function renderResearchDiagnostics(){const d=RESEARCH||{},box=document.getElementById('researchShadowV2');if(!box)return;const checks=d.checks||[],sources=d.sources||[],edges=d.track_edges||[],parity=d.context_parity||[];const grouped=checks.reduce((m,x)=>{const k=(x.parent_strategy||'UNKNOWN')+' · '+(x.label||x.check_code||'CHECK');(m[k]||(m[k]=[])).push(x);return m},{});const checkHtml=Object.entries(grouped).slice(0,80).map(([k,v])=>{const p=v.find(x=>x.status==='PASS'),f=v.find(x=>x.status==='FAIL'),n=v.find(x=>x.status==='NOT_REACHED');return '<div class=crit><b>'+esc(k)+'</b><div>✅ '+num(p&&p.count||0)+' · ❌ '+num(f&&f.count||0)+' · ⏭ '+num(n&&n.count||0)+'</div></div>'}).join('')||'<span class=muted>Журнал критериев ещё не накоплен</span>';const sourceHtml=sources.map(x=>'<div class=crit><b>'+esc(x.source)+'</b> · '+esc(x.status)+'<div class=muted>'+esc(x.authority)+' · SLA '+num(x.freshness_sla_seconds)+'s · '+esc(x.fallback_source||'no fallback')+'</div></div>').join('')||'<span class=muted>Source Registry ещё не инициализирован</span>';const edgeHtml=edges.slice(0,60).map(x=>'<div class=crit><b>'+esc(x.parent_strategy)+' · '+esc(x.symbol)+'</b><div>ACTUAL '+esc(x.actual_status||'UNAVAILABLE')+' · Groq edge '+num(x.groq_edge_r)+'R · rules edge '+num(x.groq_vs_rules_edge_r)+'R</div></div>').join('')||'<span class=muted>Нужны связанные ACTUAL fills и виртуальные треки</span>';const parityHtml=parity.map(x=>'<div class=crit><b>'+esc(x.parent_strategy)+' · candidate n='+num(x.candidates)+'</b><div>Research ↔ live 1h exact: '+num(x.live_v1_exact_pct)+'% · live 4h family: '+num(x.live_v2_family_pct)+'% · all agree: '+num(x.all_agree_pct)+'%</div></div>').join('')||'<span class=muted>Нужен новый v4 replay; совпадение режима не предполагается автоматически.</span>';box.insertAdjacentHTML('beforeend','<div class="cols section"><div class=card><h2>Check journal · PASS / FAIL / NOT REACHED</h2>'+checkHtml+'</div><div class=card><h2>Source Registry / provenance</h2>'+sourceHtml+'</div></div><div class="section card"><h2>Research ↔ live market-context parity</h2><div class=warn>Процент считается только на candidates и не меняет live-решение.</div>'+parityHtml+'</div><div class="section card"><h2>Counterfactual edges</h2>'+edgeHtml+'</div>')}
function renderResearchExperiments(){
 const box=document.getElementById('researchShadowV2');if(!box)return;
 const rows=((RESEARCH||{}).evaluations||[]).filter(x=>x.comparison_kind==='PAIRED_FIXED_POLICY');
 const html=rows.map(x=>{let m={};try{m=JSON.parse(x.metrics_json||'{}')}catch(e){}
 const o=m.overall||{};return '<div class=crit><b>'+esc(x.profile_id)+'</b><div>'+esc(m.baseline_track)+' → '+esc(m.candidate_track)+' · paired n='+num(o.n)+' · mean ΔR '+num(o.mean_delta_r)+' · median ΔR '+num(o.median_delta_r)+'</div><div>Improved '+num(o.improved)+' · worsened '+num(o.worsened)+'</div>'+((m.folds||[]).map(f=>'<div class=muted>Test '+new Date(f.test_start*1000).toISOString().slice(0,10)+' · train n='+num(f.train.n)+' · test n='+num(f.test.n)+' · test ΔR '+num(f.test.mean_delta_r)+' · late labels excluded '+num(f.purged_late_train)+'</div>').join(''))+'<div class=warn>Fixed-policy research · не доказательство улучшения LIVE detector · auto activation forbidden</div></div>'}).join('');
 box.insertAdjacentHTML('beforeend','<div class="section card"><h2>Paired policy / Walk-forward</h2>'+(html||'<span class=muted>Нужны завершённые пары независимых треков</span>')+'</div>');
}
function renderResearchAttemptDetails(){
 const d=RESEARCH||{},box=document.getElementById('researchShadowV2');if(!box)return;
 const attempts=d.attempts||[],trades=d.attempt_trades||[],checks=d.attempt_check_rows||[],byAttempt=trades.reduce((m,x)=>{(m[x.attempt_id]||(m[x.attempt_id]=[])).push(x);return m},{}),checksByAttempt=checks.reduce((m,x)=>{(m[x.attempt_id]||(m[x.attempt_id]=[])).push(x);return m},{});
 const body=attempts.map((a,i)=>{const linked=byAttempt[a.attempt_id]||[],audit=checksByAttempt[a.attempt_id]||[],t=linked.find(x=>x.track==='PLAYBOOK_ONLY')||linked[0]||{},auditHtml=audit.map(x=>'<div class=crit><b>'+esc(x.role)+' · '+esc(x.label)+'</b><div>'+esc(x.status)+' · '+esc(x.domain)+'</div><div class=muted>measured '+esc(x.measured_json)+' · threshold '+esc(x.threshold_json)+'</div></div>').join('')||'<span class=muted>Нет журнала признаков</span>';return '<tr><td>'+new Date(Number(a.decision_time)*1000).toISOString().slice(0,19).replace('T',' ')+'</td><td><b>'+esc(a.parent_strategy)+'</b><div class=muted>'+esc(a.profile_id)+'</div></td><td>'+esc(a.symbol)+'</td><td>'+esc(a.direction)+'</td><td>'+esc(a.outcome)+'</td><td>'+num(a.entry)+'</td><td>'+num(a.sl)+'</td><td>'+num(a.tp1)+'</td><td>'+num(a.tp2)+'</td><td>'+num(a.rr)+'</td><td>'+esc(t.status||'—')+'<div>'+num(t.net_r)+'R</div></td><td><button class=btn onclick="toggleResearchAttempt('+i+')">детали</button></td></tr><tr class=details id=ra'+i+'><td colspan=12><div class=detailbox><b>Паспорт решения · главные и вспомогательные признаки</b>'+auditHtml+'<div class=warn>SHADOW_CONTEXT не блокирует и не разрешает live-сделку.</div>'+esc(JSON.stringify({attempt:a,tracks:linked},null,2))+'</div></td></tr>'}).join('');
 box.insertAdjacentHTML('beforeend','<div class="section"><h2>Все найденные сетапы и результаты</h2><div class=muted>Снимок рынка, проверки, Entry/SL/TP/RR, причина STOP и независимые replay-треки.</div><div class=tablewrap style="margin-top:8px"><table><thead><tr><th>Когда</th><th>Стратегия</th><th>Пара</th><th>Напр.</th><th>Решение</th><th>Entry</th><th>SL</th><th>TP1</th><th>TP2</th><th>RR</th><th>Результат</th><th></th></tr></thead><tbody>'+body+'</tbody></table></div></div>');
 window.toggleResearchAttempt=i=>document.getElementById('ra'+i).classList.toggle('open');
}
function renderResearchDecisionPaths(){
 const d=RESEARCH||{},box=document.getElementById('researchShadowV2');if(!box)return;
 const paths=d.decision_paths||[];
 const stepValue=v=>{try{return JSON.stringify(v)}catch(e){return String(v??'—')}};
 const stepHtml=(step,tag)=>step?'<div class=crit><b>'+esc(tag)+' · '+esc(step.label||step.code)+'</b><div>'+esc(step.domain)+' · '+esc(step.status)+' · TF '+esc(step.source_timeframe||'—')+'</div><div class=muted>получено '+esc(stepValue(step.measured))+' · требуется '+esc(stepValue(step.threshold))+'</div></div>':'<div class=crit><b>'+esc(tag)+'</b><div class=muted>Не определено</div></div>';
 const html=paths.map((p,i)=>{const steps=p.steps||[],chain=steps.map((x,n)=>'<div class=crit><b>'+(n+1)+'. '+esc(x.label||x.code)+'</b><div>'+esc(x.domain)+' · '+esc(x.status)+' · TF '+esc(x.source_timeframe||'—')+'</div><div class=muted>получено '+esc(stepValue(x.measured))+' · требуется '+esc(stepValue(x.threshold))+'</div></div>').join('');return '<details class=crit '+(i<3?'open':'')+'><summary><b>'+esc(p.parent_strategy)+' · '+esc(p.symbol)+' · '+esc(p.direction)+'</b> · '+new Date(Number(p.decision_time)*1000).toISOString().slice(0,19).replace('T',' ')+' · RR '+num(p.rr)+'</summary><div class="cols" style="margin-top:8px">'+stepHtml(p.first_basis,'FIRST · первое основание')+stepHtml(p.final_trigger,'LAST TRIGGER · последний сигнал')+stepHtml(p.final_validation,'FINAL CHECK · финальная проверка')+'</div><div class=criteria style="margin-top:8px">'+chain+'</div><div class=muted>Entry '+num(p.entry)+' · SL '+num(p.sl)+' · TP1 '+num(p.tp1)+' · TP2 '+num(p.tp2)+' · terminal TP '+num(p.terminal_tp)+'</div></details>'}).join('')||'<span class=muted>Пути решений появятся после нового candidate replay.</span>';
 box.insertAdjacentHTML('beforeend','<div class="section card"><h2>Порядок построения найденных сделок</h2><div class=warn>FIRST — первое торговое основание после проверки данных. LAST TRIGGER — последний сигнал, сформировавший candidate. FINAL CHECK — последняя защита геометрии/RR. Порядок показывает фактический Research-профиль и не меняет live.</div>'+html+'</div>');
}
function renderResearchMarketContext(){
 const d=RESEARCH||{},box=document.getElementById('researchShadowV2');if(!box)return;const rows=d.market_context||[],recent=d.market_context_recent||[];
 const expected=['TRADE_CVD_REAL','OPEN_INTEREST','FUNDING_RATE','LIQUIDATIONS','ORDER_BOOK_LIQUIDITY','LONG_SHORT_RATIO'],coverage=d.coverage||[];const html=expected.map(name=>{const x=rows.find(r=>r.feature===name)||coverage.find(r=>r.feature===name)||{feature:name,source:'GATE',quality:'UNAVAILABLE',availability:'NOT_COLLECTED',samples:0};return '<div class=crit><b>'+esc(x.feature)+' · '+esc(x.source)+'</b><div>'+esc(x.quality)+' · '+esc(x.availability)+' · samples '+num(x.samples)+'</div><div class=muted>'+esc(x.coverage_start||'—')+' → '+esc(x.coverage_end||'—')+' · '+num(x.symbols)+' symbols</div></div>'}).join('');
 const latest={};recent.forEach(x=>{if(!latest[x.feature])latest[x.feature]=x});
 const values=Object.values(latest).map(x=>{let v={};try{v=JSON.parse(x.value_json||'{}')}catch(e){};return '<div class=crit><b>'+esc(x.feature)+' · '+esc(x.symbol)+'</b><div>'+esc(JSON.stringify(v))+'</div><div class=muted>as-of '+new Date(Number(x.event_time)*1000).toISOString()+' · '+esc(x.quality)+' · '+esc(x.availability)+'</div></div>'}).join('')||'<span class=muted>Нет point-in-time значений</span>';
 let book=(latest.ORDER_BOOK_LIQUIDITY||{}),bookValue={};try{bookValue=JSON.parse(book.value_json||'{}')}catch(e){}const ladder=bookValue.heatmap_levels||[],maxSize=Math.max(1,...ladder.map(x=>Number(x.size)||0));const heat=ladder.map(x=>'<div style="padding:3px 6px;margin:2px 0;background:rgba('+(x.side==='BID'?'50,213,131':'240,68,82')+','+(0.12+0.65*(Number(x.size)||0)/maxSize)+')">'+esc(x.side)+' · '+num(x.price)+' · size '+num(x.size)+'</div>').join('')||'<span class=muted>Depth сохраняется только с момента включения; историческая книга не выдумывается.</span>';
 box.insertAdjacentHTML('beforeend','<div class="section card"><h2>Derivatives / Microstructure · Shadow only</h2><div class=warn>Trade CVD, OI, funding, liquidations, depth и long/short ratio не меняют Entry/SL/TP/RR/gates/risk.</div><div class=criteria>'+html+'</div><h2 style="margin-top:14px">Последние значения · point-in-time</h2><div class=criteria>'+values+'</div><h2 style="margin-top:14px">Gate liquidity heatmap · latest ladder</h2>'+heat+'</div>');
}
function renderResearchLifecycle(){const d=RESEARCH||{},box=document.getElementById('researchShadowV2');if(!box)return;const unique=d.unique_funnels||[],setups=d.setups||[],levels=d.levels||[],coverage=d.coverage||[];const uniqueHtml=unique.map(x=>'<span class="badge" style="margin:3px">'+esc(x.parent_strategy)+' · '+esc(x.state)+' '+num(x.count)+' ('+num(x.symbols)+' пар)</span>').join('')||'<span class=muted>Уникальные setup ещё не накоплены</span>';const setupHtml=setups.slice(0,40).map(x=>'<div class=crit><b>'+esc(x.parent_strategy)+' · '+esc(x.symbol)+' · '+esc(x.direction||'—')+'</b><div>'+esc(x.state)+' · '+esc(x.timeframe||'—')+' · checks '+num(x.checks_count)+'</div><div class=muted>'+esc(x.first_seen)+' → '+esc(x.last_seen)+' · '+esc(x.terminal_reason||'нет terminal reason')+'</div></div>').join('')||'<span class=muted>Нет setup lifecycle</span>';const levelHtml=levels.map(x=>'<span class="badge" style="margin:3px">'+esc(x.timeframe)+' · '+esc(x.level_type)+' · '+esc(x.status)+' '+num(x.count)+'</span>').join('')||'<span class=muted>Уровни ещё не материализованы</span>';const missing=coverage.filter(x=>String(x.quality||'').toUpperCase()!=='VALID').slice(0,20).map(x=>'<div class=crit><b>'+esc(x.feature)+' · '+esc(x.source)+'</b><div>'+esc(x.quality)+' · '+esc(x.availability)+' · samples '+num(x.samples)+'</div></div>').join('')||'<span class=good>Нет заявленных неполных feature-источников</span>';box.insertAdjacentHTML('beforeend','<div class="section card"><h2>Unique Setup Lifecycle</h2><div>'+uniqueHtml+'</div><div class="criteria" style="margin-top:8px">'+setupHtml+'</div></div><div class="cols section"><div class=card><h2>Materialized levels</h2>'+levelHtml+'</div><div class=card><h2>Feature coverage / unavailable is not zero</h2>'+missing+'</div></div>')}

</script><script>
function renderLabProfileShadow(){
 const data=LAST&&LAST.lab_profile_shadow||{},summary=data.by_strategy||[],recent=data.recent||[];
 const cardsHtml=summary.map(x=>'<div class=crit><b>'+esc(x.strategy)+'</b> · runs '+num(x.runs)+'<div>LAB candidates '+num(x.lab_candidates)+' · LIVE candidates '+num(x.live_candidates)+' · exact outcome '+num(x.outcome_agreement_pct)+'%</div><div>Candidate/non-candidate agreement '+num(x.candidate_agreement_pct)+'% · pending↔filtered '+num(x.pending_vs_filtered)+'</div><div class=muted>Среднее совпадение достигнутых hard gates '+num(x.avg_hard_gate_match_pct)+'% · unavailable '+num(x.unavailable)+'</div></div>').join('');
 const rowsHtml=recent.slice(0,25).map(x=>{const c=x.candidate||{},steps=x.ordered_hard_gates||[],first=steps[0],last=steps.length?steps[steps.length-1]:null,chain=steps.map((s,i)=>(i+1)+'. '+(s.status==='PASS'?'✅':s.status==='FAIL'?'❌':'⏭')+' '+esc(s.label||s.code)).join(' → ');return '<details class=crit><summary><b>'+esc(x.strategy)+' · '+esc(x.symbol)+'</b> · LIVE '+esc(x.live_outcome)+' ↔ LAB '+esc(x.lab_outcome)+' · match '+num(x.hard_gate_match_pct)+'%</summary><div>'+esc((x.occurred_at||'').replace('T',' ').slice(0,19))+' UTC · stop '+esc(x.stop_code||'—')+'</div><div>Entry '+num(c.entry)+' · SL '+num(c.sl)+' · TP1 '+num(c.tp1)+' · TP2 '+num(c.tp2)+' · TP3/terminal '+num(c.terminal_tp)+' · RR '+num(c.rr)+'</div><div class=muted>FIRST '+esc(first&&(first.label||first.code)||'—')+' · LAST '+esc(last&&(last.label||last.code)||'—')+'</div><div class=muted>'+((chain)||esc(x.error||'Нет журнала'))+'</div></details>'}).join('');
 labProfileShadow.innerHTML='<div class=muted>Всего shadow-сравнений '+num(data.total||0)+'. Они не могут отправлять ордера.</div><div class=criteria>'+(cardsHtml||'<span class=muted>Ожидаются новые live-проверки</span>')+'</div>'+rowsHtml;
}
const renderWithoutLabProfile=render;
render=function(){renderWithoutLabProfile();renderLabProfileShadow()};
</script></body></html>'''


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
        if p.path=="/health": self._json({"ok":True,"service":"apex-strategy-stats"}); return
        if not self._auth(q): self._html("<!doctype html><meta charset=utf-8><h2>403 · закрытая статистика APEX</h2>",403); return
        if p.path in {"/","/stats"}: self._html(HTML); return
        if p.path=="/api/dashboard":
            try:
                val=lambda k,d="":(q.get(k) or [d])[0]; data=build_dashboard(int(val("days","1")),val("strategy"),val("symbol"),val("outcome"),val("groq"),float(val("min_rr")) if val("min_rr") else None,float(val("max_rr")) if val("max_rr") else None,val("fromdate"),val("todate"),int(val("page","1")),int(val("page_size","100")),val("release")); self._json(data)
            except Exception as exc: self._json({"error":f"{type(exc).__name__}: {exc}"},500)
            return
        if p.path=="/api/research":
            try:
                # The web process may observe a database created by an older
                # research worker.  Additive/idempotent migrations are safe at
                # the read boundary and prevent a stale schema from hiding
                # the Research tab after a restart.
                if MARKET_DATABASE_URL:
                    research_store = ResearchStore(MARKET_DATABASE_URL)
                    research_store.ensure_schema()
                    data = research_store.dashboard()
                else:
                    data = _github_research_dashboard()
                self._json(data)
            except Exception as exc:
                print(f"[stats] research unavailable: {type(exc).__name__}")
                self._json({"error":"research database unavailable"},503)
            return
        self._json({"error":"not found"},404)
    def do_POST(self):
        if urlparse(self.path).path!="/ingest": self._json({"error":"not found"},404); return
        if not INGEST_TOKEN or not hmac.compare_digest(self.headers.get("X-APEX-Ingest-Token",""),INGEST_TOKEN): self._json({"error":"forbidden"},403); return
        try:
            n=min(int(self.headers.get("Content-Length","0") or 0),2_000_000); count=ingest(json.loads(self.rfile.read(n).decode())); self._json({"ok":True,"accepted":count})
        except Exception as exc: self._json({"error":f"{type(exc).__name__}: {exc}"},400)
    def log_message(self,fmt,*args): print(f"[stats] {self.command} {urlparse(self.path).path}")


class APEXStatsServer(ThreadingHTTPServer):
    # A burst of ingest and browser refreshes must not fill the tiny stdlib
    # accept queue and make /health unreachable at Render's proxy.
    daemon_threads = True
    request_queue_size = 128


def main():
    if not DATABASE_URL or not DASHBOARD_TOKEN or not INGEST_TOKEN: raise SystemExit("DATABASE_URL, DASHBOARD_TOKEN and INGEST_TOKEN are required")
    ensure_schema()
    # Warm the default cache without delaying port binding. Until it completes,
    # /health and ingest remain responsive and duplicate dashboard builds fail
    # fast instead of occupying every request thread.
    threading.Thread(target=lambda: build_dashboard(), name="dashboard-cache-warm", daemon=True).start()
    print(f"APEX Strategy Stats listening on :{PORT}")
    APEXStatsServer(("0.0.0.0",PORT),Handler).serve_forever()


if __name__=="__main__": main()
