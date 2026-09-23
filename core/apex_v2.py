"""APEX V2 control plane.

This module joins the existing strategy, Groq, Trade Manager V2 and execution
layers without changing a single entry formula or strategy gate.  It owns the
immutable trade thesis, normalized market/portfolio snapshots and read-only
telemetry consumed by Dashboard V2.

The control plane is deliberately deterministic.  AI may propose an action,
but this module never calls an exchange and never mutates Entry/SL/TP.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from apex.db.connection import connect_compatibility as _connect_compatibility_db
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping
from apex.config.settings import ApexConfig


APEX_VERSION = "2.0"
SCHEMA_VERSION = 2
STRATEGIES = ("FAST", "MTF", "SWING", "ZONE", "WYCKOFF")
MATERIAL_MANAGER_EVENTS = {
    "TP1_HIT", "TP2_HIT", "TP3_HIT", "INVALIDATION_HIT", "BOS", "CHOCH",
    "EXTERNAL_CONFLICT", "NO_PROGRESS", "MARKET_DATA_DEGRADED",
    "EXCHANGE_FILL", "RECONCILIATION_REQUIRED", "VOLATILITY_SHOCK",
}
DB_PATH = ApexConfig.from_env().database.compatibility_db_path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _float(value: Any, default: float | None = None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _connect(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = _connect_compatibility_db(db_path, timeout=20, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def ensure_apex_v2_schema(db_path: str = DB_PATH) -> None:
    """Create only additive, restart-safe APEX V2 tables."""
    conn = _connect(db_path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS apex_v2_runtime (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS apex_v2_theses (
            signal_id INTEGER PRIMARY KEY,
            thesis_hash TEXT NOT NULL,
            strategy TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry REAL NOT NULL,
            initial_sl REAL NOT NULL,
            initial_tp1 REAL NOT NULL,
            initial_tp2 REAL,
            terminal_tp REAL NOT NULL,
            snapshot_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_apex_v2_theses_lookup
          ON apex_v2_theses(strategy,symbol,created_at DESC);
        CREATE TABLE IF NOT EXISTS apex_v2_market_states (
            snapshot_key TEXT PRIMARY KEY,
            regime TEXT NOT NULL DEFAULT 'UNKNOWN',
            btc_direction TEXT NOT NULL DEFAULT 'UNKNOWN',
            freshness_state TEXT NOT NULL DEFAULT 'UNKNOWN',
            snapshot_json TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_apex_v2_market_states_recent
          ON apex_v2_market_states(observed_at DESC);
        CREATE TABLE IF NOT EXISTS apex_v2_portfolio_snapshots (
            snapshot_key TEXT PRIMARY KEY,
            open_positions INTEGER NOT NULL DEFAULT 0,
            total_risk_pct REAL NOT NULL DEFAULT 0,
            long_risk_pct REAL NOT NULL DEFAULT 0,
            short_risk_pct REAL NOT NULL DEFAULT 0,
            risk_state TEXT NOT NULL DEFAULT 'UNKNOWN',
            snapshot_json TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_apex_v2_portfolio_recent
          ON apex_v2_portfolio_snapshots(observed_at DESC);
        CREATE TABLE IF NOT EXISTS apex_v2_decisions (
            action_id TEXT PRIMARY KEY,
            signal_id INTEGER,
            strategy TEXT NOT NULL DEFAULT '',
            symbol TEXT NOT NULL DEFAULT '',
            decision_source TEXT NOT NULL,
            action TEXT NOT NULL,
            confidence REAL,
            manager_state TEXT,
            outcome TEXT,
            latency_ms REAL,
            prompt_version TEXT,
            model_version TEXT,
            context_hash TEXT,
            payload_json TEXT NOT NULL DEFAULT '{}',
            occurred_at TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_apex_v2_decisions_recent
          ON apex_v2_decisions(occurred_at DESC);
        CREATE TABLE IF NOT EXISTS apex_v2_opportunities (
            attempt_id TEXT PRIMARY KEY,
            release_sha TEXT NOT NULL DEFAULT '',
            strategy TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL DEFAULT '',
            stop_reason TEXT NOT NULL DEFAULT '',
            entry REAL,
            initial_sl REAL,
            tp1 REAL,
            rr REAL,
            execution_state TEXT NOT NULL DEFAULT 'UNASSESSED',
            entry_available INTEGER,
            target_already_passed INTEGER,
            mfe_r REAL,
            mae_r REAL,
            hypothetical_result TEXT,
            payload_json TEXT NOT NULL DEFAULT '{}',
            observed_at TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_apex_v2_opportunities_lookup
          ON apex_v2_opportunities(strategy,execution_state,observed_at DESC);
        CREATE TABLE IF NOT EXISTS apex_v2_incidents (
            incident_key TEXT PRIMARY KEY,
            component TEXT NOT NULL,
            severity TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'OPEN',
            impact TEXT NOT NULL DEFAULT '',
            detail_json TEXT NOT NULL DEFAULT '{}',
            started_at TEXT NOT NULL,
            resolved_at TEXT,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_apex_v2_incidents_recent
          ON apex_v2_incidents(status,severity,started_at DESC);
        """
    )
    conn.execute("""CREATE TABLE IF NOT EXISTS apex_release_manifests (
        release_sha TEXT PRIMARY KEY, manifest_json TEXT NOT NULL,
        first_seen TEXT DEFAULT CURRENT_TIMESTAMP)""")
    manifest = version_manifest()
    conn.execute("INSERT OR IGNORE INTO apex_release_manifests(release_sha,manifest_json) VALUES(?,?)",
                 (manifest["release_sha"], _canonical(manifest)))
    conn.execute(
        """INSERT INTO apex_v2_runtime(key,value_json) VALUES('version_manifest',?)
           ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=CURRENT_TIMESTAMP""",
        (_canonical(manifest),),
    )
    conn.execute(
        """INSERT INTO apex_v2_runtime(key,value_json) VALUES('schema_version',?)
           ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=CURRENT_TIMESTAMP""",
        (_canonical(SCHEMA_VERSION),),
    )
    conn.commit()
    conn.close()


def version_manifest(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    config = ApexConfig.from_env(env)
    return {
        "apex_version": APEX_VERSION,
        "schema_version": SCHEMA_VERSION,
        "release_sha": config.runtime.release_sha,
        "manager_version": 2,
        "strategy_version": config.strategies.manifest_hash(),
        "prompt_version": "legacy-parity-v1",
        "playbook_version": "v3-live-manager",
        "groq_model": config.integrations.groq_model or "configured-runtime",
    }


def build_trade_thesis(signal: Mapping[str, Any], assessment: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Freeze the original reason and immutable levels for one trade."""
    evidence = dict(assessment or signal.get("setup_assessment") or {})
    entry = _float(signal.get("entry"), 0.0) or 0.0
    sl = _float(signal.get("sl"), 0.0) or 0.0
    tp1 = _float(signal.get("tp1", signal.get("tp")), 0.0) or 0.0
    tp2 = _float(signal.get("tp2"), tp1) or tp1
    terminal = _float(signal.get("tp3"), tp2) or tp2
    direction = str(signal.get("direction") or "").upper()
    strategy = str(signal.get("strategy") or signal.get("scan_type") or signal.get("grade") or "MTF").upper()
    if strategy == "FAST_DEAL":
        strategy = "FAST"
    risk = abs(entry - sl)
    expected = [
        {"stage": "ENTRY", "price": entry},
        {"stage": "TP1", "price": tp1, "r": round(abs(tp1 - entry) / risk, 4) if risk else None},
        {"stage": "TP2", "price": tp2, "r": round(abs(tp2 - entry) / risk, 4) if risk else None},
    ]
    if terminal != tp2:
        expected.append({"stage": "TERMINAL_TP", "price": terminal, "r": round(abs(terminal-entry)/risk, 4) if risk else None})
    core_evidence = evidence.get("CORE") or (evidence.get("evidence_roles") or {}).get("CORE") or []
    trigger_evidence = evidence.get("TRIGGER") or (evidence.get("evidence_roles") or {}).get("TRIGGER") or []
    return {
        "version": APEX_VERSION,
        "frozen_at": _utc_now(),
        "symbol": str(signal.get("symbol") or "").upper(),
        "strategy": strategy,
        "direction": direction,
        "working_timeframe": str(signal.get("timeframe") or ""),
        "entry": entry,
        "initial_sl": sl,
        "initial_tp1": tp1,
        "initial_tp2": tp2,
        "terminal_tp": terminal,
        "initial_rr": _float(signal.get("rr")),
        "invalidation": {"type": "INITIAL_STRUCTURAL_SL", "price": sl},
        "expected_path": expected,
        "setup_class": evidence.get("state") or evidence.get("setup_class") or "UNKNOWN",
        "source": evidence.get("source") or "strategy_candidate",
        "thesis": evidence.get("thesis") or signal.get("logic") or "",
        "core": core_evidence,
        "trigger": trigger_evidence,
        # Upper-case aliases preserve the Manager V2 prompt contract and make
        # the migration transparent to existing active trades.
        "CORE": core_evidence,
        "TRIGGER": trigger_evidence,
        "conflicts": evidence.get("conflicts") or [],
        "market_regime": signal.get("regime") or "UNKNOWN",
        "versions": version_manifest(),
    }


def freeze_trade_thesis(
    signal_id: int, signal: Mapping[str, Any], assessment: Mapping[str, Any] | None = None,
    db_path: str = DB_PATH,
) -> dict[str, Any]:
    ensure_apex_v2_schema(db_path)
    thesis = build_trade_thesis(signal, assessment)
    required = (thesis["entry"], thesis["initial_sl"], thesis["initial_tp1"])
    if int(signal_id) <= 0 or min(required) <= 0:
        return thesis
    encoded = _canonical(thesis)
    conn = _connect(db_path)
    conn.execute(
        """INSERT OR IGNORE INTO apex_v2_theses
           (signal_id,thesis_hash,strategy,symbol,direction,entry,initial_sl,initial_tp1,
            initial_tp2,terminal_tp,snapshot_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
        (int(signal_id), _digest(thesis), thesis["strategy"], thesis["symbol"], thesis["direction"],
         thesis["entry"], thesis["initial_sl"], thesis["initial_tp1"], thesis["initial_tp2"],
         thesis["terminal_tp"], encoded),
    )
    conn.commit()
    row = conn.execute("SELECT snapshot_json FROM apex_v2_theses WHERE signal_id=?", (int(signal_id),)).fetchone()
    conn.close()
    try:
        return json.loads(row[0]) if row else thesis
    except (TypeError, json.JSONDecodeError):
        return thesis


def assess_entry_execution(
    candidate: Mapping[str, Any], *, decision_price: float | None = None,
    post_signal_high: float | None = None, post_signal_low: float | None = None,
) -> dict[str, Any]:
    """Classify executability without changing or inventing a level."""
    direction = str(candidate.get("direction") or "").upper()
    entry = _float(candidate.get("entry"))
    tp1 = _float(candidate.get("tp1", candidate.get("tp")))
    sl = _float(candidate.get("sl"))
    current = _float(decision_price)
    high, low = _float(post_signal_high), _float(post_signal_low)
    result = {
        "state": "INSUFFICIENT_DATA", "entry_available": None,
        "target_already_passed": None, "price_chase_pct": None,
    }
    if entry is None or sl is None or tp1 is None or direction not in {"BULLISH", "BEARISH"}:
        return result
    if current is not None:
        result["target_already_passed"] = current >= tp1 if direction == "BULLISH" else current <= tp1
        result["price_chase_pct"] = round(abs(current-entry)/entry*100, 4) if entry else None
        if result["target_already_passed"]:
            result["state"] = "TARGET_ALREADY_PASSED"
    if high is not None and low is not None:
        touched = low <= entry <= high
        result["entry_available"] = touched
        if result["state"] != "TARGET_ALREADY_PASSED":
            result["state"] = "ENTRY_AVAILABLE" if touched else "ENTRY_NOT_AVAILABLE"
    elif current is not None and result["state"] != "TARGET_ALREADY_PASSED":
        result["state"] = "AWAITING_POST_SIGNAL_CANDLES"
    return result


def normalize_market_state(raw: Mapping[str, Any], *, observed_at: str | None = None) -> dict[str, Any]:
    """Build one shared, read-only market context for all consumers."""
    quality = raw.get("data_quality") if isinstance(raw.get("data_quality"), Mapping) else {}
    regime = str(raw.get("regime") or raw.get("market_regime") or "UNKNOWN").upper()
    btc = str(raw.get("btc_direction") or raw.get("btc_trend") or "UNKNOWN").upper()
    stale = bool(raw.get("stale") or quality.get("stale"))
    return {
        "version": APEX_VERSION,
        "observed_at": observed_at or _utc_now(),
        "regime": regime,
        "btc_direction": btc,
        "volatility": raw.get("volatility") or raw.get("volatility_regime") or "UNKNOWN",
        "alt_regime": raw.get("alt_regime") or "UNKNOWN",
        "session": raw.get("session") or "UNKNOWN",
        "funding": raw.get("funding"),
        "open_interest": raw.get("open_interest"),
        "correlation": raw.get("correlation") or {},
        "htf_levels": raw.get("htf_levels") or [],
        "news_risk": raw.get("news_risk") or "UNKNOWN",
        "data_quality": {**dict(quality), "stale": stale},
        "freshness_state": "STALE" if stale else str(quality.get("state") or "FRESH").upper(),
    }


def store_market_state(raw: Mapping[str, Any], db_path: str = DB_PATH) -> dict[str, Any]:
    ensure_apex_v2_schema(db_path)
    state = normalize_market_state(raw)
    key = str(raw.get("snapshot_key") or f"market:{state['observed_at']}:{_digest(state)[:16]}")
    conn = _connect(db_path)
    conn.execute(
        """INSERT OR IGNORE INTO apex_v2_market_states
           (snapshot_key,regime,btc_direction,freshness_state,snapshot_json,observed_at)
           VALUES(?,?,?,?,?,?)""",
        (key, state["regime"], state["btc_direction"], state["freshness_state"],
         _canonical(state), state["observed_at"]),
    )
    conn.commit(); conn.close()
    return state


def portfolio_risk_snapshot(
    positions: Iterable[Mapping[str, Any]], *, max_positions: int = 3,
    max_total_risk_pct: float = 3.0, max_same_side_risk_pct: float = 2.0,
    daily_pnl_pct: float = 0.0, max_daily_loss_pct: float = 2.0,
) -> dict[str, Any]:
    """Aggregate portfolio exposure from cached manager/exchange state only."""
    rows = []
    for item in positions:
        risk = max(0.0, _float(item.get("risk_pct"), 0.0) or 0.0)
        direction = str(item.get("direction") or item.get("side") or "").upper()
        rows.append({
            "signal_id": item.get("signal_id"), "symbol": str(item.get("symbol") or "").upper(),
            "strategy": str(item.get("strategy") or "UNKNOWN").upper(),
            "direction": direction, "risk_pct": risk,
            "btc_beta": _float(item.get("btc_beta")), "protected": bool(item.get("protected", True)),
        })
    long_risk = sum(x["risk_pct"] for x in rows if x["direction"] in {"BULLISH", "LONG"})
    short_risk = sum(x["risk_pct"] for x in rows if x["direction"] in {"BEARISH", "SHORT"})
    total = long_risk + short_risk
    reasons = []
    if len(rows) >= max_positions:
        reasons.append("MAX_OPEN_POSITIONS")
    if total >= max_total_risk_pct:
        reasons.append("MAX_TOTAL_RISK")
    if max(long_risk, short_risk) >= max_same_side_risk_pct:
        reasons.append("MAX_SAME_SIDE_RISK")
    if daily_pnl_pct <= -abs(max_daily_loss_pct):
        reasons.append("MAX_DAILY_LOSS")
    if any(not x["protected"] for x in rows):
        reasons.append("UNPROTECTED_POSITION")
    return {
        "version": APEX_VERSION, "observed_at": _utc_now(), "open_positions": len(rows),
        "total_risk_pct": round(total, 4), "long_risk_pct": round(long_risk, 4),
        "short_risk_pct": round(short_risk, 4), "daily_pnl_pct": round(float(daily_pnl_pct), 4),
        "risk_state": "BLOCKED" if reasons else "OK", "allow_new_open": not reasons,
        "reasons": reasons, "positions": rows,
    }


def store_portfolio_snapshot(snapshot: Mapping[str, Any], db_path: str = DB_PATH) -> str:
    ensure_apex_v2_schema(db_path)
    observed = str(snapshot.get("observed_at") or _utc_now())
    key = str(snapshot.get("snapshot_key") or f"portfolio:{observed}:{_digest(snapshot)[:16]}")
    conn = _connect(db_path)
    conn.execute(
        """INSERT OR IGNORE INTO apex_v2_portfolio_snapshots
           (snapshot_key,open_positions,total_risk_pct,long_risk_pct,short_risk_pct,
            risk_state,snapshot_json,observed_at) VALUES(?,?,?,?,?,?,?,?)""",
        (key, int(snapshot.get("open_positions") or 0), _float(snapshot.get("total_risk_pct"), 0.0),
         _float(snapshot.get("long_risk_pct"), 0.0), _float(snapshot.get("short_risk_pct"), 0.0),
         str(snapshot.get("risk_state") or "UNKNOWN"), _canonical(snapshot), observed),
    )
    conn.commit(); conn.close()
    return key


def material_events(events: Iterable[Any]) -> list[str]:
    """Return a stable, deduplicated event list used to decide Groq calls."""
    seen: set[str] = set()
    result = []
    for event in events:
        name = str(event or "").upper()
        if name in MATERIAL_MANAGER_EVENTS and name not in seen:
            seen.add(name); result.append(name)
    return result


def confidence_calibration(records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Measure whether stated confidence agrees with resolved outcomes."""
    buckets = {(0.0, 0.6): [], (0.6, 0.8): [], (0.8, 1.01): []}
    for record in records:
        confidence = _float(record.get("confidence"))
        helped = record.get("helped")
        if confidence is None or not isinstance(helped, bool):
            continue
        for bounds, values in buckets.items():
            if bounds[0] <= confidence < bounds[1]:
                values.append(helped); break
    result = []
    for (low, high), values in buckets.items():
        result.append({
            "bucket": f"{int(low*100)}-{int(min(high,1.0)*100)}%",
            "count": len(values),
            "success_rate": round(sum(values)/len(values)*100, 1) if values else None,
        })
    return result


def record_decision(
    *, action_id: str, decision_source: str, action: str, signal_id: int | None = None,
    strategy: str = "", symbol: str = "", confidence: float | None = None,
    manager_state: str | None = None, outcome: str | None = None,
    latency_ms: float | None = None, context: Mapping[str, Any] | None = None,
    payload: Mapping[str, Any] | None = None, db_path: str = DB_PATH,
) -> bool:
    ensure_apex_v2_schema(db_path)
    manifest = version_manifest()
    context_data = dict(context or {})
    conn = _connect(db_path)
    inserted = conn.execute(
        """INSERT OR IGNORE INTO apex_v2_decisions
           (action_id,signal_id,strategy,symbol,decision_source,action,confidence,manager_state,
            outcome,latency_ms,prompt_version,model_version,context_hash,payload_json,occurred_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (str(action_id), signal_id, str(strategy).upper(), str(symbol).upper(), str(decision_source),
         str(action).upper(), confidence, manager_state, outcome, latency_ms,
         manifest["prompt_version"], manifest["groq_model"], _digest(context_data),
         _canonical(payload or {}), _utc_now()),
    ).rowcount
    conn.commit(); conn.close()
    return bool(inserted)


def upsert_incident(
    incident_key: str, component: str, severity: str, impact: str,
    detail: Mapping[str, Any] | None = None, *, resolved: bool = False,
    db_path: str = DB_PATH,
) -> None:
    ensure_apex_v2_schema(db_path)
    now = _utc_now()
    conn = _connect(db_path)
    conn.execute(
        """INSERT INTO apex_v2_incidents
           (incident_key,component,severity,status,impact,detail_json,started_at,resolved_at)
           VALUES(?,?,?,?,?,?,?,?)
           ON CONFLICT(incident_key) DO UPDATE SET component=excluded.component,
             severity=excluded.severity,status=excluded.status,impact=excluded.impact,
             detail_json=excluded.detail_json,
             resolved_at=CASE WHEN excluded.status='RESOLVED' THEN excluded.resolved_at ELSE NULL END,
             updated_at=CURRENT_TIMESTAMP""",
        (str(incident_key), str(component).upper(), str(severity).upper(),
         "RESOLVED" if resolved else "OPEN", str(impact), _canonical(detail or {}), now,
         now if resolved else None),
    )
    conn.commit(); conn.close()


def dashboard_snapshot(db_path: str = DB_PATH) -> dict[str, Any]:
    """Return a secret-free local snapshot suitable for telemetry emission."""
    ensure_apex_v2_schema(db_path)
    conn = _connect(db_path)
    result: dict[str, Any] = {"versions": version_manifest(), "generated_at": _utc_now()}
    # Publish the canonical supervisor state with the other worker telemetry.
    # The web process must not probe production integrations independently:
    # it renders the last worker-observed state instead.
    try:
        from apex.app.runtime import runtime_supervisor
        result["runtime_health"] = runtime_supervisor.snapshot()
    except Exception:
        result["runtime_health"] = {}
    # Source registry is declarative and secret-free.  It makes the Gate-only
    # market-data boundary visible beside the rolling request ledger.
    try:
        from core.source_registry import registry_snapshot
        result["source_registry"] = registry_snapshot()
    except Exception:
        result["source_registry"] = []
    try:
        from external_sources.budget import SourceBudget
        result["api_budget"] = SourceBudget(db_path).snapshot()
        raw_plan = ApexConfig.from_env().integrations.external_source_plan_json
        if raw_plan:
            try:
                from external_sources.budget import plan_daily_load
                result["api_budget_plan"] = plan_daily_load(json.loads(raw_plan))
            except (TypeError, ValueError, json.JSONDecodeError):
                result["api_budget_plan"] = {"status": "invalid_plan"}
        else:
            result["api_budget_plan"] = {
                "status": "UNCONFIGURED",
                "accounting_scope": "external_context_adapters_only",
                "scanner_gate_rest_covered": False,
                "binance_execution_reserved_separately": True,
                "reason": "APEX_EXTERNAL_SOURCE_PLAN_JSON is not configured",
            }
    except Exception:
        result["api_budget"] = []
        result["api_budget_error"] = "ledger_unavailable"
        result["api_budget_plan"] = {"status": "UNAVAILABLE", "reason": "budget_ledger_unavailable"}
    for key, table, order in (
        ("market_state", "apex_v2_market_states", "observed_at"),
        ("portfolio", "apex_v2_portfolio_snapshots", "observed_at"),
    ):
        row = conn.execute(f"SELECT snapshot_json FROM {table} ORDER BY {order} DESC LIMIT 1").fetchone()
        try:
            result[key] = json.loads(row[0]) if row else {}
        except (TypeError, json.JSONDecodeError):
            result[key] = {}
    result["open_incidents"] = [dict(row) for row in conn.execute(
        """SELECT incident_key,component,severity,status,impact,started_at,updated_at
           FROM apex_v2_incidents WHERE status='OPEN' ORDER BY started_at DESC LIMIT 30"""
    ).fetchall()]
    result["recent_decisions"] = [dict(row) for row in conn.execute(
        """SELECT action_id,signal_id,strategy,symbol,decision_source,action,confidence,
                  manager_state,outcome,latency_ms,occurred_at
           FROM apex_v2_decisions ORDER BY occurred_at DESC LIMIT 30"""
    ).fetchall()]
    result["opportunity_counts"] = {
        str(row[0]): int(row[1]) for row in conn.execute(
            "SELECT execution_state,COUNT(*) FROM apex_v2_opportunities GROUP BY execution_state"
        ).fetchall()
    }
    table_names = {str(row[0]) for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    if "trade_manager_state" in table_names:
        historical_states = {str(row[0]): int(row[1]) for row in conn.execute(
            "SELECT manager_state,COUNT(*) FROM trade_manager_state GROUP BY manager_state"
        ).fetchall()}
        active_states = {str(row[0]): int(row[1]) for row in conn.execute(
            """SELECT m.manager_state,COUNT(*) FROM trade_manager_state m
                 JOIN signals s ON s.id=m.signal_id
                WHERE COALESCE(m.status,'ACTIVE')!='CLOSED'
                  AND LOWER(COALESCE(s.result,'pending'))='pending'
                GROUP BY m.manager_state"""
        ).fetchall()} if "signals" in table_names else {}
        result["manager_db"] = {
            "states": active_states,
            "historical_states": historical_states,
            "active_count": sum(active_states.values()),
            "trades": [dict(row) for row in conn.execute(
                """SELECT signal_id,symbol,strategy,direction,manager_state,status,last_price,current_r,
                          tp1_seen,tp2_seen,tp3_seen,position_fraction,last_event,last_action,
                          last_confidence,data_failure_count,updated_at,closed_at
                   FROM trade_manager_state ORDER BY updated_at DESC LIMIT 100"""
            ).fetchall()],
        }
    else:
        result["manager_db"] = {"states": {}, "trades": []}
    result["learning"] = {"scope": "CONFIRMED_LIVE_ONLY"}
    try:
        from core.groq_calibration import calibration_summary
        result["groq_calibration"] = calibration_summary(db_path)
    except Exception:
        result["groq_calibration"] = {"calls": 0, "resolved": 0, "scope": "CONFIRMED_LIVE_ONLY"}
    try:
        from apex.risk.dependency import latest_dependency_snapshot
        result["portfolio_dependency"] = latest_dependency_snapshot(db_path)
    except Exception:
        result["portfolio_dependency"] = {}
    result["learning"].update({
        "groq_calibration": result.get("groq_calibration", {}),
    })
    if "trade_executions" in table_names:
        execution_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(trade_executions)").fetchall()}
        active_stop_expr = "active_stop_price" if "active_stop_price" in execution_columns else "sl AS active_stop_price"
        result["execution_health"] = {
            "statuses": {str(row[0]): int(row[1]) for row in conn.execute(
                "SELECT status,COUNT(*) FROM trade_executions GROUP BY status"
            ).fetchall()},
            "recent": [dict(row) for row in conn.execute(
                f"""SELECT signal_id,mode,symbol,direction,status,entry,sl,{active_stop_expr},
                           tp1,tp2,quantity,last_error,updated_at
                    FROM trade_executions ORDER BY updated_at DESC LIMIT 50"""
            ).fetchall()],
        }
    else:
        result["execution_health"] = {"statuses": {}, "recent": []}
    conn.close()
    config = ApexConfig.from_env()
    live_mode = config.execution.mode
    enabled = config.execution.enabled
    confirmed = config.execution.live_confirmation == "ENABLE_LIVE_BINANCE_FUTURES"
    result["execution_mode"] = {
        "mode": live_mode, "enabled": enabled,
        "live_armed": bool(enabled and live_mode == "live" and confirmed),
        "kill_switch": config.execution.kill_switch,
    }
    return result


def emit_dashboard_snapshot(db_path: str = DB_PATH) -> None:
    """Best-effort bridge to the existing durable Strategy Lab ingest queue."""
    try:
        from core.setup_audit import emit_event
        snap = dashboard_snapshot(db_path)
        release = str((snap.get("versions") or {}).get("release_sha") or "unknown")
        key = f"apex-v2:{release}:{datetime.now(timezone.utc).strftime('%Y%m%d%H%M')}"
        emit_event("apex_v2_snapshot", "SYSTEM", "", snap, event_key=key)
    except Exception:
        pass
