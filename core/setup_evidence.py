"""Deterministic, qualitative setup evidence engine.

The engine never calculates or changes trade levels.  It explains why a
strategy candidate exists, groups correlated observations into domains and
classifies the causal chain instead of adding indicator points.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any


SUPPORTED_STRATEGIES = {"MTF", "SWING", "ZONE", "FAST", "WYCKOFF"}
STATE_ORDER = {"INVALID": 0, "DEVELOPING": 1, "VALID": 2, "STRONG": 3, "EXCEPTIONAL": 4}
STRATEGY_CAUSAL_MATRIX = {
    "MTF": {"core": "1h+4h direction and structural OB/FVG location", "trigger": "fresh closed 15m BOS/CHoCH"},
    "SWING": {"core": "HTF thesis and major liquidity/OB/FVG reaction", "trigger": "4h plus fresh closed 1h BOS/CHoCH"},
    "ZONE": {"core": "qualified Discount/Premium extreme", "trigger": "fresh closed 1h BOS/CHoCH from the zone"},
    "FAST": {"core": "session-qualified 15m OB/FVG retest with supportive HTF context", "trigger": "15m displacement and fresh BOS/CHoCH"},
    "WYCKOFF": {"core": "qualified accumulation/distribution range", "trigger": "Spring→SOS, UTAD→SOW or re-accumulation release"},
}


def _strategy(candidate: dict[str, Any]) -> str:
    return str(candidate.get("scan_type") or candidate.get("grade") or candidate.get("signal_type") or "").upper()


def _direction(candidate: dict[str, Any]) -> str:
    raw = str(candidate.get("direction") or "").upper()
    return "BULLISH" if raw in {"BULLISH", "LONG", "BUY"} else "BEARISH" if raw in {"BEARISH", "SHORT", "SELL"} else raw


def _truth(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, dict):
        return bool(value) and value.get("found", True) is not False
    return str(value or "").strip().lower() not in {"", "none", "false", "0", "unknown", "n/a", "—"}


def _agrees(value: Any, direction: str) -> bool:
    raw = str(value or "").upper()
    if direction == "BULLISH":
        return raw in {"BULLISH", "LONG", "BUY", "UP", "BULLISH_BOS", "BULLISH_CHOCH"} or "BULL" in raw
    if direction == "BEARISH":
        return raw in {"BEARISH", "SHORT", "SELL", "DOWN", "BEARISH_BOS", "BEARISH_CHOCH"} or "BEAR" in raw
    return False


def _geometry(candidate: dict[str, Any], direction: str) -> tuple[bool, str]:
    try:
        entry = float(candidate["entry"])
        sl = float(candidate["sl"])
        tp = float(candidate.get("tp1") if candidate.get("tp1") is not None else candidate["tp"])
        rr = float(candidate["rr"])
    except (KeyError, TypeError, ValueError):
        return False, "missing or non-numeric Entry/SL/TP/RR"
    if min(entry, sl, tp, rr) <= 0:
        return False, "Entry/SL/TP/RR must be positive"
    correct = sl < entry < tp if direction == "BULLISH" else tp < entry < sl if direction == "BEARISH" else False
    if not correct:
        return False, "trade geometry contradicts direction"
    if rr < 2.0:
        return False, "RR is below the universal 2.0 minimum"
    return True, "structural invalidation and target are directionally valid"


def _external_domain(context: dict[str, Any] | None, direction: str) -> dict[str, Any]:
    context = context if isinstance(context, dict) else {}
    observations: list[str] = []
    conflicts: list[str] = []
    independent = set()

    oi = context.get("open_interest") if isinstance(context.get("open_interest"), dict) else {}
    oi_change = oi.get("change_1h_pct")
    try:
        oi_usable = str(oi.get("status") or "").lower() not in {"stale", "error", "unavailable", "not_configured"}
        if oi_usable and oi_change is not None and float(oi_change) >= 1.0:
            observations.append(f"OI 1h {float(oi_change):+.2f}%")
            independent.add("derivatives")
    except (TypeError, ValueError):
        pass

    directional_fields = (
        ("large_orders", "bias", "participation"),
        ("whale_activity", "bias", "participation"),
        ("smart_money", "bias", "participation"),
        ("live_tape", "bias", "participation"),
        ("exchange_flow", "bias", "flow"),
    )
    # All order-flow-like observations form one participation domain.
    participation_seen = False
    opposition_domains = set()
    opposition_sources = set()
    for section, key, domain in directional_fields:
        item = context.get(section) if isinstance(context.get(section), dict) else {}
        if str(item.get("status") or "").lower() in {"stale", "error", "unavailable", "not_configured"}:
            continue
        bias = item.get(key)
        if _agrees(bias, direction):
            observations.append(f"{section}: {bias}")
            participation_seen = True
        elif _truth(bias) and str(bias).lower() not in {"neutral", "unknown", "balanced"}:
            conflicts.append(f"{section} opposes thesis: {bias}")
            opposition_domains.add(domain)
            if item.get("source"):
                opposition_sources.add(str(item["source"]))
    if participation_seen:
        independent.add("participation")

    liq = context.get("liquidations") if isinstance(context.get("liquidations"), dict) else {}
    dominance = str(liq.get("dominance") or "").lower()
    expected = "short" if direction == "BULLISH" else "long"
    liq_usable = str(liq.get("status") or "").lower() not in {"stale", "error", "unavailable", "not_configured"}
    if liq_usable and expected in dominance:
        observations.append(f"liquidations: {dominance}")
        independent.add("derivatives")

    return {
        "role": "TIER2",
        "quality": "CONFIRMING" if observations else "NEUTRAL",
        "observations": observations,
        "conflicts": conflicts,
        "conflict_domains": sorted(opposition_domains),
        "conflict_sources": sorted(opposition_sources),
        "independent_domains": sorted(independent),
        "note": "correlated tape/CVD/order-flow facts are one participation domain",
    }


def _technical_backbone(strategy: str, evidence: dict[str, Any], direction: str) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    missing: list[str] = []
    core: list[str] = []
    trigger: list[str] = []
    core_complete = True

    if strategy == "MTF":
        alignment = evidence.get("timeframe_alignment") if isinstance(evidence.get("timeframe_alignment"), dict) else {}
        aligned = _agrees(alignment.get("1h"), direction) and _agrees(alignment.get("4h"), direction)
        location = _truth(evidence.get("ob")) or _truth(evidence.get("fvg"))
        structure = _truth(evidence.get("structure_event")) or bool(evidence.get("bos_choch"))
        if aligned: core.append("1h and 4h structure agree with direction")
        else:
            missing.append("1h+4h directional alignment")
            core_complete = False
        if location: core.append("candidate is anchored to OB/FVG location")
        else:
            missing.append("structural OB/FVG location")
            core_complete = False
        if structure: trigger.append("fresh 15m BOS/CHoCH confirms intent")
        else: missing.append("fresh 15m BOS/CHoCH trigger")
    elif strategy == "SWING":
        htf = evidence.get("htf_dir")
        location = _truth(evidence.get("ob")) or _truth(evidence.get("fvg")) or _truth(evidence.get("confirms"))
        structure_4h = _truth(evidence.get("structure_event"))
        structure_1h = _truth(evidence.get("structure_event_1h"))
        if _agrees(htf, direction) or str(htf or "").upper() in {"MIXED", "NEUTRAL", ""}:
            core.append("HTF location does not invalidate swing direction")
        else:
            missing.append("non-conflicting HTF structure")
            core_complete = False
        if location: core.append("liquidity/OB/FVG reaction is present")
        else:
            missing.append("major liquidity reaction at structural location")
            core_complete = False
        if structure_4h and structure_1h: trigger.append("4h and fresh 1h structure confirm realization")
        else: missing.append("4h plus fresh 1h BOS/CHoCH trigger")
    elif strategy == "ZONE":
        expected = "discount" if direction == "BULLISH" else "premium"
        zone_ok = expected in str(evidence.get("zone") or "").lower()
        location = _truth(evidence.get("zone_type"))
        structure = _truth(evidence.get("structure_event"))
        if zone_ok and location: core.append(f"price is in {expected} at a qualified zone")
        else:
            missing.append(f"qualified {expected} location")
            core_complete = False
        if structure: trigger.append("fresh closed 1h structure confirms zone reaction")
        else: missing.append("fresh closed 1h BOS/CHoCH trigger")
    elif strategy == "FAST":
        location = _truth(evidence.get("zone")) and (_truth(evidence.get("ob")) or _truth(evidence.get("fvg")))
        structure = _truth(evidence.get("structure_event"))
        if location: core.append("15m OB/FVG retest location and session setup are present")
        else:
            missing.append("15m OB/FVG retest location")
            core_complete = False
        if structure: trigger.append("15m displacement and fresh BOS/CHoCH confirm intent")
        else: missing.append("fresh 15m displacement/structure trigger")
    elif strategy == "WYCKOFF":
        phases = str(evidence.get("phases") or "").upper()
        if evidence.get("spring"):
            core.append("accumulation range and sell-side liquidity event")
            if evidence.get("sos"): trigger.append("Spring followed by SOS")
            else: missing.append("SOS after Spring")
        elif evidence.get("utad"):
            core.append("distribution range and buy-side liquidity event")
            if evidence.get("sow"): trigger.append("UTAD followed by SOW")
            else: missing.append("SOW after UTAD")
        elif "RE-ACCUMULATION" in phases or "REACCUMULATION" in phases:
            core.append("re-accumulation range with higher lows")
            if evidence.get("reacc_trigger_validated"): trigger.append("compression resolved toward structural liquidity")
            else: missing.append("re-accumulation release trigger")
        else:
            missing.extend(["recognized Wyckoff phase sequence", "Spring/SOS, UTAD/SOW or re-accumulation trigger"])
            core_complete = False
    return (
        {"role": "CORE", "quality": "STRONG" if core and core_complete else "WEAK", "observations": core},
        {"role": "TRIGGER", "quality": "STRONG" if trigger else "MISSING", "observations": trigger},
        missing,
    )


def assess_candidate(candidate: dict[str, Any], external_context: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return a causal assessment without mutating ``candidate``."""
    snapshot = deepcopy(candidate)
    strategy = _strategy(snapshot)
    direction = _direction(snapshot)
    evidence = snapshot.get("technical_evidence") if isinstance(snapshot.get("technical_evidence"), dict) else {}
    geometry_ok, geometry_reason = _geometry(snapshot, direction)
    external = _external_domain(external_context, direction)

    if strategy not in SUPPORTED_STRATEGIES:
        return {"version": 1, "strategy": strategy, "direction": direction, "state": "NOT_APPLICABLE", "blocking": False,
                "thesis": "No causal matrix is registered for this candidate type", "dimensions": {}, "domains": {}, "conflicts": []}

    core, trigger, missing = _technical_backbone(strategy, evidence, direction)
    conflicts: list[dict[str, str]] = []
    if not geometry_ok:
        conflicts.append({"severity": "FATAL", "reason": geometry_reason})
    external_severity = "MATERIAL" if (
        len(external.get("conflict_domains", [])) >= 2
        and len(external.get("conflict_sources", [])) >= 2
    ) else "ADVISORY"
    for item in external.get("conflicts", []):
        conflicts.append({"severity": external_severity, "reason": item})
    warning = evidence.get("funding_warning") or evidence.get("weekly_warning")
    if _truth(warning):
        conflicts.append({"severity": "ADVISORY", "reason": str(warning)})

    confirmation_observations: list[str] = []
    domains: dict[str, dict[str, Any]] = {
        "location_structure": {"quality": core["quality"], "observations": core["observations"]},
        "trigger_impulse": {"quality": trigger["quality"], "observations": trigger["observations"]},
        "participation_derivatives": external,
        "risk_geometry": {"quality": "STRONG" if geometry_ok else "INVALID", "observations": [geometry_reason]},
    }
    if evidence.get("volume_confirmed") or _truth(evidence.get("confirms")):
        confirmation_observations.append("technical participation confirms the move")
    if evidence.get("btc_confirmed"):
        confirmation_observations.append("BTC market context agrees")
    if _truth(evidence.get("fvg")) and _truth(evidence.get("ob")):
        confirmation_observations.append("displacement inefficiency and order block are coherent")

    if any(item["severity"] == "FATAL" for item in conflicts):
        state = "INVALID"
    elif missing:
        state = "DEVELOPING"
    else:
        state = "VALID"
        # A strong setup requires a complete strong backbone and good entry;
        # weak context cannot promote it.
        if core["quality"] == "STRONG" and trigger["quality"] == "STRONG" and geometry_ok and (
            confirmation_observations or strategy == "WYCKOFF"
        ):
            state = "STRONG"
        # Exceptional requires independent evidence outside price structure.
        independent = set(external.get("independent_domains", []))
        if state == "STRONG" and len(independent) >= 2 and not conflicts:
            state = "EXCEPTIONAL"
    if any(item["severity"] == "ADVISORY" for item in conflicts) and state == "EXCEPTIONAL":
        state = "STRONG"
    if any(item["severity"] == "MATERIAL" for item in conflicts) and STATE_ORDER.get(state, 0) > STATE_ORDER["VALID"]:
        state = "VALID"

    dimensions = {
        "context_quality": core["quality"],
        "trigger_quality": trigger["quality"],
        "confirmation_quality": "STRONG" if confirmation_observations and external["quality"] == "CONFIRMING" else "CONFIRMING" if confirmation_observations or external["quality"] == "CONFIRMING" else "NEUTRAL",
        "entry_quality": "STRONG" if geometry_ok else "INVALID",
        "conflict_risk": "FATAL" if any(item["severity"] == "FATAL" for item in conflicts) else "MATERIAL" if any(item["severity"] == "MATERIAL" for item in conflicts) else "ADVISORY" if conflicts else "CLEAR",
    }
    thesis = f"{strategy} {direction}: " + (" → ".join(core["observations"] + trigger["observations"]) if not missing else "waiting for " + "; ".join(missing))
    return {
        "version": 1, "matrix": STRATEGY_CAUSAL_MATRIX[strategy], "strategy": strategy,
        "regime": str(snapshot.get("regime") or "UNKNOWN"), "direction": direction, "state": state,
        "blocking": state in {"INVALID", "DEVELOPING"}, "thesis": thesis[:1000],
        "dimensions": dimensions, "domains": domains, "confirmations": confirmation_observations,
        "evidence_roles": {
            "CORE": core["observations"],
            "TRIGGER": trigger["observations"],
            "TIER1": confirmation_observations,
            "TIER2": external.get("observations", []),
            "TIER3": [str(warning)] if _truth(warning) else [],
            "CONFLICT": conflicts,
        },
        "missing": missing, "conflicts": conflicts,
        "policy": "weak facts adjust context; only a complete causal backbone defines the class",
    }


def candidate_key(candidate: dict[str, Any]) -> str:
    payload = {key: candidate.get(key) for key in ("symbol", "direction", "timeframe", "entry", "sl", "tp1", "tp2", "tp3", "rr")}
    payload["strategy"] = _strategy(candidate)
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()[:24]


def ensure_setup_evidence_schema(db_path: str) -> None:
    with sqlite3.connect(db_path, timeout=20) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS setup_assessments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id INTEGER,
            candidate_key TEXT NOT NULL,
            stage TEXT NOT NULL,
            symbol TEXT NOT NULL,
            strategy TEXT NOT NULL,
            direction TEXT,
            timeframe TEXT,
            state TEXT NOT NULL,
            thesis TEXT,
            assessment_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(candidate_key, stage)
        );
        CREATE INDEX IF NOT EXISTS idx_setup_assessments_recent ON setup_assessments(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_setup_assessments_strategy_state ON setup_assessments(strategy,state,created_at DESC);
        """)
        try:
            conn.execute("ALTER TABLE setup_assessments ADD COLUMN signal_id INTEGER")
        except sqlite3.OperationalError:
            pass
        conn.execute("CREATE INDEX IF NOT EXISTS idx_setup_assessments_signal ON setup_assessments(signal_id,stage)")


def persist_assessment(candidate: dict[str, Any], assessment: dict[str, Any], stage: str, db_path: str) -> bool:
    if assessment.get("state") == "NOT_APPLICABLE":
        return True
    try:
        ensure_setup_evidence_schema(db_path)
        now = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(db_path, timeout=20) as conn:
            conn.execute(
                """INSERT INTO setup_assessments
                   (candidate_key,stage,symbol,strategy,direction,timeframe,state,thesis,assessment_json,created_at,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(candidate_key,stage) DO UPDATE SET state=excluded.state,thesis=excluded.thesis,
                   assessment_json=excluded.assessment_json,updated_at=excluded.updated_at""",
                (candidate_key(candidate), stage.upper(), str(candidate.get("symbol") or ""), _strategy(candidate),
                 _direction(candidate), str(candidate.get("timeframe") or ""), str(assessment.get("state") or ""),
                 str(assessment.get("thesis") or ""), json.dumps(assessment, ensure_ascii=False, default=str), now, now),
            )
            conn.execute("DELETE FROM setup_assessments WHERE created_at < datetime('now','-90 days')")
        return True
    except sqlite3.Error as exc:
        logging.warning("[SetupEvidence] persistence unavailable; assessment retained in memory: %s", exc)
        return False


def bind_assessment_to_signal(candidate: dict[str, Any], signal_id: int, db_path: str) -> None:
    """Attach immutable technical/final assessments to the delivered signal."""
    if int(signal_id or 0) <= 0:
        return
    ensure_setup_evidence_schema(db_path)
    with sqlite3.connect(db_path, timeout=20) as conn:
        conn.execute(
            "UPDATE setup_assessments SET signal_id=?,updated_at=? WHERE candidate_key=?",
            (int(signal_id), datetime.now(timezone.utc).isoformat(), candidate_key(candidate)),
        )


def setup_evidence_dashboard(db_path: str, hours: int = 24, limit: int = 12) -> dict[str, Any]:
    ensure_setup_evidence_schema(db_path)
    with sqlite3.connect(db_path, timeout=20) as conn:
        conn.row_factory = sqlite3.Row
        summary = conn.execute(
            """SELECT strategy,state,COUNT(*) count FROM setup_assessments
               WHERE stage='FINAL' AND created_at >= datetime('now', ?)
               GROUP BY strategy,state ORDER BY strategy,state""", (f"-{int(hours)} hours",)
        ).fetchall()
        recent = conn.execute(
            """SELECT symbol,strategy,direction,state,thesis,assessment_json,updated_at
               FROM setup_assessments WHERE stage='FINAL' ORDER BY updated_at DESC LIMIT ?""", (int(limit),)
        ).fetchall()
    return {"hours": hours, "summary": [dict(row) for row in summary], "recent": [dict(row) for row in recent]}
