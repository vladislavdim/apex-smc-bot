"""Event-driven advisory trade manager for active APEX signals.

The manager is deliberately downstream of entry strategies. It never creates a
signal and never rewrites immutable initial entry/SL/TP/RR. It observes an
already-active thesis, detects meaningful management events, asks Groq for a
bounded advisory action, persists the decision for learning/audit, and may
publish a *management target* when a fresh structural objective is supplied by
market structure. A management target is never an invented TP4/TP5 and never
replaces the original targets.
"""
from __future__ import annotations

import html
import json
import os
import sqlite3
from typing import Any, Callable

from core.market_structure import analyze_market_structure

DB_PATH = os.environ.get(
    "APEX_DB_PATH",
    os.environ.get(
        "APEX_BRAIN_DB_PATH",
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "brain.db"),
    ),
)
ALLOWED_ACTIONS = {
    "HOLD", "PROTECT", "PARTIAL_EXIT", "LET_RUN", "EXIT", "WAIT_CONFIRMATION"
}
MANAGEMENT_TF = {
    "FAST": "5m",
    "MTF": "15m",
    "ZONE": "15m",
    "SWING": "1h",
    "WYCKOFF": "1h",
}
MANAGEMENT_MATRIX = {
    "FAST": {
        "cadence": "every closed 5m candle",
        "protect": "only after TP1 or a fresh 5m continuation BOS; use the confirmed 5m HL/LH",
        "exit": "confirmed opposite 5m CHoCH or immutable SL; FAST momentum failure matters quickly",
        "partial": "TP1 or a confirmed loss of FAST momentum after positive excursion",
        "let_run": "fresh continuation BOS plus non-conflicting participation",
    },
    "MTF": {
        "cadence": "every closed 15m candle",
        "protect": "after TP1 or confirmed 15m continuation; use the latest confirmed 15m HL/LH",
        "exit": "confirmed 15m reversal aligned against the original 1h/4h thesis, or immutable SL",
        "partial": "TP1 or material conflict after the 15m trigger has failed",
        "let_run": "15m continuation structure remains aligned with the original MTF thesis",
    },
    "ZONE": {
        "cadence": "every closed 15m candle",
        "protect": "only after the zone reaction has produced continuation structure or TP1",
        "exit": "confirmed failure back through the zone thesis or immutable SL",
        "partial": "TP1 or a confirmed opposite reaction while leaving the source zone",
        "let_run": "the original Premium/Discount reaction continues toward structural liquidity",
    },
    "SWING": {
        "cadence": "every closed 1h candle",
        "protect": "after TP1 or a confirmed 1h continuation swing; use the latest 1h HL/LH",
        "exit": "confirmed 1h thesis reversal or immutable SL; ignore isolated lower-timeframe noise",
        "partial": "at an original target or after confirmed 1h deterioration, never from one wick",
        "let_run": "1h continuation agrees with the original HTF core and has room to liquidity",
    },
    "WYCKOFF": {
        "cadence": "every closed 1h candle",
        "protect": "after SOS/SOW continuation or TP1, at the latest confirmed phase HL/LH",
        "exit": "confirmed failure of Spring/SOS, UTAD/SOW or re-accumulation thesis, or immutable SL",
        "partial": "at an original target or a confirmed opposing phase transition",
        "let_run": "phase progression and structure both confirm continuation",
    },
}


def _connect(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=20, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def ensure_trade_manager_schema(db_path: str = DB_PATH) -> None:
    conn = _connect(db_path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS trade_manager_state (
            signal_id INTEGER PRIMARY KEY,
            symbol TEXT NOT NULL,
            strategy TEXT NOT NULL,
            direction TEXT NOT NULL,
            management_tf TEXT NOT NULL,
            initial_entry REAL NOT NULL,
            initial_sl REAL NOT NULL,
            initial_tp1 REAL NOT NULL,
            initial_tp2 REAL,
            initial_tp3 REAL,
            initial_rr REAL,
            last_price REAL,
            best_price REAL,
            current_r REAL NOT NULL DEFAULT 0,
            tp1_seen INTEGER NOT NULL DEFAULT 0,
            tp2_seen INTEGER NOT NULL DEFAULT 0,
            tp3_seen INTEGER NOT NULL DEFAULT 0,
            manager_target REAL,
            manager_protect_level REAL,
            last_event TEXT,
            last_action TEXT,
            last_confidence REAL,
            last_reviewed_candle TEXT,
            thesis_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS trade_manager_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            action TEXT,
            confidence REAL,
            price REAL,
            r_multiple REAL,
            manager_target REAL,
            manager_protect_level REAL,
            facts_json TEXT NOT NULL DEFAULT '{}',
            reason TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_trade_manager_events_signal
          ON trade_manager_events(signal_id, created_at);
        """
    )
    # Safe additive migration for databases created by the first manager build.
    for table, column, typedef in (
        ("trade_manager_state", "manager_target", "REAL"),
        ("trade_manager_state", "manager_protect_level", "REAL"),
        ("trade_manager_events", "manager_target", "REAL"),
        ("trade_manager_events", "manager_protect_level", "REAL"),
        ("trade_manager_state", "tp3_seen", "INTEGER NOT NULL DEFAULT 0"),
    ):
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {typedef}")
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()


def normalize_strategy(value: Any) -> str:
    raw = str(value or "MTF").upper()
    if "FAST" in raw:
        return "FAST"
    if "SWING" in raw:
        return "SWING"
    if "ZONE" in raw:
        return "ZONE"
    if "WYCKOFF" in raw:
        return "WYCKOFF"
    return "MTF"


def management_matrix(strategy: Any) -> dict[str, str]:
    return dict(MANAGEMENT_MATRIX[normalize_strategy(strategy)])


def r_multiple(direction: str, entry: float, sl: float, price: float) -> float:
    risk = abs(entry - sl)
    if risk <= 0:
        return 0.0
    move = price - entry if str(direction).upper() == "BULLISH" else entry - price
    return round(move / risk, 4)


def _closed_candles(candles: list[Any]) -> list[dict[str, Any]]:
    clean: list[dict[str, Any]] = []
    for raw in candles or []:
        try:
            if isinstance(raw, dict):
                clean.append({
                    "timestamp": raw.get("timestamp", raw.get("time", raw.get("open_time"))),
                    "open": float(raw["open"]),
                    "high": float(raw["high"]),
                    "low": float(raw["low"]),
                    "close": float(raw["close"]),
                    "volume": float(raw.get("volume") or 0),
                })
            else:
                values = list(raw)
                clean.append({
                    "timestamp": values[0],
                    "open": float(values[1]),
                    "high": float(values[2]),
                    "low": float(values[3]),
                    "close": float(values[4]),
                    "volume": float(values[5]) if len(values) > 5 else 0.0,
                })
        except (KeyError, TypeError, ValueError, IndexError):
            continue
    # Exchange candle loaders normally include the mutable edge candle. The
    # manager reasons about close-confirmed structure only.
    return clean[:-1] if len(clean) >= 3 else []


def build_structure_facts(
    candles: list[Any],
    trade_direction: str,
    last_reviewed_candle: Any = None,
) -> dict[str, Any]:
    closed = _closed_candles(candles)
    if len(closed) < 15:
        return {"closed_candle": False, "new_management_candle": False}
    analysis = analyze_market_structure(closed, swing_lookback=3, max_break_age=1)
    event = analysis.get("event") or {}
    classified = analysis.get("classified") or []
    candle_id = closed[-1].get("timestamp")
    new_candle = str(candle_id) != str(last_reviewed_candle) if candle_id is not None else True
    direction = str(trade_direction or "").upper()
    protection_kind = "HL" if direction == "BULLISH" else "LH"
    target_kind = "HH" if direction == "BULLISH" else "LL"
    protection = next((float(s["price"]) for s in reversed(classified) if s.get("kind") == protection_kind), None)
    structural_target = next((float(s["price"]) for s in reversed(classified) if s.get("kind") == target_kind), None)
    event_direction = str(event.get("direction") or "").upper()
    event_type = str(event.get("type") or "").upper()
    against_trade = bool(event_type == "CHOCH" and event_direction and event_direction != direction)
    with_trade = bool(event_type in {"BOS", "CHOCH"} and event_direction == direction)
    return {
        "closed_candle": True,
        "new_management_candle": new_candle,
        "management_candle_id": candle_id,
        "structure_event": event_type or None,
        "structure_direction": event_direction or analysis.get("direction"),
        "structure_level": event.get("level"),
        "structure_with_trade": with_trade,
        "structure_against_trade": against_trade,
        "confirmed_protection_level": protection,
        "structural_target": structural_target,
        "latest_close": float(closed[-1]["close"]),
        "latest_closed_high": float(closed[-1]["high"]),
        "latest_closed_low": float(closed[-1]["low"]),
    }


def detect_events(state: dict[str, Any], price: float, facts: dict[str, Any]) -> list[str]:
    events: list[str] = []
    bullish = str(state["direction"]).upper() == "BULLISH"
    tp1 = float(state["initial_tp1"])
    tp2 = float(state.get("initial_tp2") or tp1)
    tp3 = float(state.get("initial_tp3") or tp2)
    sl = float(state["initial_sl"])
    use_bar = bool(facts.get("closed_candle") and facts.get("new_management_candle"))
    high_value = facts.get("latest_closed_high") if use_bar else price
    low_value = facts.get("latest_closed_low") if use_bar else price
    observed_high = float(price if high_value is None else high_value)
    observed_low = float(price if low_value is None else low_value)
    tp1_hit = (bullish and observed_high >= tp1) or (not bullish and observed_low <= tp1)
    tp2_hit = (bullish and observed_high >= tp2) or (not bullish and observed_low <= tp2)
    tp3_hit = (bullish and observed_high >= tp3) or (not bullish and observed_low <= tp3)
    sl_hit = (bullish and observed_low <= sl) or (not bullish and observed_high >= sl)
    tp1_new = not int(state.get("tp1_seen") or 0) and tp1_hit
    if tp1_new:
        events.append("TP1_HIT")
    if (int(state.get("tp1_seen") or 0) or tp1_new) and not int(state.get("tp2_seen") or 0) and tp2_hit:
        events.append("TP2_HIT")
    if (int(state.get("tp2_seen") or 0) or tp2_hit) and not int(state.get("tp3_seen") or 0) and tp3_hit:
        events.append("TP3_HIT")
    if sl_hit:
        events.append("INVALIDATION_HIT")
    if use_bar and sl_hit and (tp1_hit or tp2_hit or tp3_hit):
        events.append("AMBIGUOUS_BARRIERS")
    structure = str(facts.get("structure_event") or "").upper()
    if structure in {"BOS", "CHOCH"} and facts.get("new_management_candle"):
        events.append(structure)
    if facts.get("closed_candle") and facts.get("new_management_candle"):
        events.append("MANAGEMENT_CANDLE_CLOSE")
    if facts.get("external_conflict"):
        events.append("EXTERNAL_CONFLICT")
    return list(dict.fromkeys(events))


def compact_external_context(context: dict[str, Any], direction: str) -> dict[str, Any]:
    """Keep fresh decision evidence while excluding bulky provider payloads."""
    context = context if isinstance(context, dict) else {}
    result: dict[str, Any] = {}
    fields = {
        "open_interest": ("value", "change_1h_pct", "change_4h_pct", "trend", "status", "age_seconds", "source"),
        "funding": ("rate", "extreme", "bias", "status", "age_seconds", "source"),
        "liquidations": ("long_usd", "short_usd", "dominance", "status", "age_seconds", "source"),
        "large_orders": ("buy_pressure", "sell_pressure", "bias", "status", "age_seconds", "source"),
        "exchange_flow": ("inflow_usd", "outflow_usd", "bias", "status", "age_seconds", "source"),
        "smart_money": ("buy_usd", "sell_usd", "bias", "confidence", "status", "age_seconds", "source"),
        "live_tape": ("buy_usd_60s", "sell_usd_60s", "long_liq_usd_300s", "short_liq_usd_300s", "bias", "status", "age_seconds"),
    }
    for section, keys in fields.items():
        source = context.get(section)
        if isinstance(source, dict):
            compact = {key: source.get(key) for key in keys if source.get(key) is not None}
            if compact:
                result[section] = compact
    for key in ("external_bias", "external_confidence", "conflicts"):
        if context.get(key) not in (None, "", [], {}):
            result[key] = context.get(key)
    quality = context.get("data_quality")
    if isinstance(quality, dict):
        result["data_quality"] = {
            "available_sources": quality.get("available_sources") or [],
            "failed_sources": quality.get("failed_sources") or [],
        }
    expected = "bullish" if str(direction).upper() == "BULLISH" else "bearish"
    bias = str(context.get("external_bias") or "unknown").lower()
    try:
        confidence = float(context.get("external_confidence") or 0)
    except (TypeError, ValueError):
        confidence = 0.0
    explicit = bool(context.get("conflict") or context.get("significant_conflict") or context.get("conflicts"))
    result["significant_conflict"] = explicit or (
        bias not in {"unknown", "neutral", expected} and confidence >= .6
    )
    return result


def _prompt(state: dict[str, Any], events: list[str], facts: dict[str, Any]) -> str:
    try:
        original_thesis = json.loads(state.get("thesis_json") or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        original_thesis = {}
    payload = {
        "trade": {
            key: state.get(key)
            for key in (
                "signal_id", "symbol", "strategy", "direction", "management_tf",
                "initial_entry", "initial_sl", "initial_tp1", "initial_tp2",
                "initial_tp3", "initial_rr", "last_price", "best_price",
                "current_r", "tp1_seen", "tp2_seen", "manager_target",
                "tp3_seen", "manager_protect_level",
            )
        },
        "events": events,
        "original_thesis": original_thesis,
        "management_matrix": facts.get("management_matrix") or management_matrix(state.get("strategy")),
        "facts": facts,
    }
    return """You are APEX Trade Manager. Manage an already-open trading thesis; do not create a new trade.
Initial entry, initial SL, TP1/TP2/TP3 and initial RR are immutable historical facts. Never rewrite them.
Use the supplied original CORE, TRIGGER, setup class and conflicts as the immutable thesis context. Do not silently replace that thesis.
Use only supplied facts; never invent candles, structure, volume, OI, funding, news, levels or probabilities.
Choose exactly one action: HOLD, PROTECT, PARTIAL_EXIT, LET_RUN, EXIT, WAIT_CONFIRMATION.
A management_target is NOT a replacement for the original TP. It may be returned only when facts.structural_target is present, lies beyond the current continuation direction, and continuation is structurally confirmed. Otherwise return null.
A protect_level may be returned only from facts.confirmed_protection_level. Never invent or numerically adjust a level.
LET_RUN requires continuation evidence, not price alone. EXIT requires invalidation or strong confirmed reversal evidence. A wick alone is not a confirmed BOS/CHoCH.
Return strict JSON only: {"action":"HOLD","confidence":0.0,"reason":"...","protect_level":null,"management_target":null,"next_trigger":"..."}.
DATA:
""" + json.dumps(payload, ensure_ascii=False, default=str)[:14000]


def _numeric_or_none(value: Any) -> float | None:
    try:
        number = float(value)
        return number if number > 0 else None
    except (TypeError, ValueError):
        return None


def _parse_review(raw: Any, facts: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    try:
        text = str(raw or "").strip().replace("```json", "").replace("```", "").strip()
        start, end = text.find("{"), text.rfind("}")
        obj = json.loads(text[start:end + 1]) if start >= 0 and end > start else {}
    except Exception:
        obj = {}
    action = str(obj.get("action") or "WAIT_CONFIRMATION").upper()
    if action not in ALLOWED_ACTIONS:
        action = "WAIT_CONFIRMATION"
    try:
        confidence = max(0.0, min(1.0, float(obj.get("confidence") or 0)))
    except (TypeError, ValueError):
        confidence = 0.0

    # Groq cannot invent levels. Returned values are accepted only if they
    # exactly match a deterministic level supplied in facts (within tolerance).
    supplied_protect = _numeric_or_none(facts.get("confirmed_protection_level"))
    proposed_protect = _numeric_or_none(obj.get("protect_level"))
    protect_level = None
    current_price = (_numeric_or_none(facts.get("current_price"))
                     or _numeric_or_none(facts.get("latest_close"))
                     or _numeric_or_none(state.get("last_price")))
    initial_sl = float(state.get("initial_sl") or 0)
    bullish = str(state.get("direction") or "").upper() == "BULLISH"
    previous_protect = _numeric_or_none(state.get("manager_protect_level"))
    if supplied_protect and proposed_protect and current_price:
        tol = max(abs(supplied_protect), 1.0) * 1e-8
        geometry_ok = initial_sl < supplied_protect < current_price if bullish else current_price < supplied_protect < initial_sl
        improves = previous_protect is None or (supplied_protect > previous_protect if bullish else supplied_protect < previous_protect)
        if abs(supplied_protect - proposed_protect) <= tol and geometry_ok and improves:
            protect_level = supplied_protect

    supplied_target = _numeric_or_none(facts.get("structural_target"))
    proposed_target = _numeric_or_none(obj.get("management_target"))
    management_target = None
    if supplied_target and proposed_target and current_price and facts.get("structure_with_trade"):
        tol = max(abs(supplied_target), 1.0) * 1e-8
        geometry_ok = supplied_target > current_price if bullish else supplied_target < current_price
        if abs(supplied_target - proposed_target) <= tol and geometry_ok:
            management_target = supplied_target

    return {
        "action": action,
        "confidence": confidence,
        "reason": str(obj.get("reason") or "")[:1500],
        "protect_level": protect_level,
        "management_target": management_target,
        "next_trigger": str(obj.get("next_trigger") or "")[:500],
    }


def review_active_trade(
    state: dict[str, Any],
    events: list[str],
    facts: dict[str, Any],
    ask_groq: Callable[..., Any],
) -> dict[str, Any]:
    if not events:
        return {
            "action": "HOLD", "confidence": 1.0,
            "reason": "No material management event",
            "protect_level": None, "management_target": None,
            "next_trigger": "next material event", "groq_called": False,
        }
    if "INVALIDATION_HIT" in events:
        return {
            "action": "EXIT", "confidence": 1.0,
            "reason": "Initial structural invalidation level was reached",
            "protect_level": None, "management_target": None,
            "next_trigger": "trade closed", "groq_called": False,
        }
    try:
        raw = ask_groq(_prompt(state, events, facts), max_tokens=300)
        review = _parse_review(raw, facts, state)
        review["groq_called"] = True
        return review
    except Exception as exc:
        return {
            "action": "WAIT_CONFIRMATION", "confidence": 0.0,
            "reason": f"Groq unavailable: {type(exc).__name__}",
            "protect_level": None, "management_target": None,
            "next_trigger": "next material event", "groq_called": False,
        }


def register_active_trade(
    signal: dict[str, Any],
    thesis: dict[str, Any] | None = None,
    db_path: str = DB_PATH,
) -> None:
    ensure_trade_manager_schema(db_path)
    strategy = normalize_strategy(
        signal.get("strategy") or signal.get("scan_type") or signal.get("grade") or signal.get("signal_type")
    )
    entry, sl = float(signal.get("entry") or 0), float(signal.get("sl") or 0)
    tp1 = float(signal.get("tp1", signal.get("tp")) or 0)
    if min(entry, sl, tp1) <= 0:
        return
    signal_id = int(signal.get("signal_id") or signal.get("id") or 0)
    if signal_id <= 0:
        return
    conn = _connect(db_path)
    conn.execute(
        """INSERT OR IGNORE INTO trade_manager_state
           (signal_id,symbol,strategy,direction,management_tf,initial_entry,initial_sl,
            initial_tp1,initial_tp2,initial_tp3,initial_rr,thesis_json)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            signal_id, str(signal.get("symbol") or "").upper(), strategy,
            str(signal.get("direction") or "").upper(), MANAGEMENT_TF[strategy], entry, sl, tp1,
            float(signal.get("tp2") or tp1),
            float(signal.get("tp3") or signal.get("tp2") or tp1),
            float(signal.get("rr") or 0),
            json.dumps(thesis or {}, ensure_ascii=False, default=str)[:20000],
        ),
    )
    conn.commit()
    conn.close()


def _load_setup_thesis(signal_id: int, signal: dict[str, Any], db_path: str) -> dict[str, Any]:
    """Load the final causal assessment, with a safe legacy fallback."""
    conn = _connect(db_path)
    row = None
    try:
        row = conn.execute(
            """SELECT assessment_json FROM setup_assessments WHERE signal_id=?
               ORDER BY CASE stage WHEN 'FINAL' THEN 0 ELSE 1 END,updated_at DESC LIMIT 1""",
            (int(signal_id),),
        ).fetchone()
        if row is None:
            row = conn.execute(
                """SELECT assessment_json FROM setup_assessments
                   WHERE symbol=? AND strategy=? AND direction=?
                   ORDER BY CASE stage WHEN 'FINAL' THEN 0 ELSE 1 END,updated_at DESC LIMIT 1""",
                (str(signal.get("symbol") or "").upper(),
                 normalize_strategy(signal.get("grade") or signal.get("signal_type")),
                 str(signal.get("direction") or "").upper()),
            ).fetchone()
    except sqlite3.Error:
        row = None
    conn.close()
    if row:
        try:
            assessment = json.loads(row["assessment_json"] or "{}")
            return {
                "source": "setup_evidence",
                "setup_class": assessment.get("state"),
                "thesis": assessment.get("thesis"),
                "CORE": (assessment.get("evidence_roles") or {}).get("CORE") or [],
                "TRIGGER": (assessment.get("evidence_roles") or {}).get("TRIGGER") or [],
                "TIER1": (assessment.get("evidence_roles") or {}).get("TIER1") or [],
                "conflicts": assessment.get("conflicts") or [],
                "dimensions": assessment.get("dimensions") or {},
            }
        except (TypeError, ValueError, json.JSONDecodeError):
            pass
    return {"source": "legacy_signal", "setup_class": "UNKNOWN", "CORE": [], "TRIGGER": [], "conflicts": []}


def register_pending_signals(db_path: str = DB_PATH) -> int:
    """Idempotently attach manager state to analytics signals that are already active.

    Waiting-entry rows are deliberately ignored when lifecycle state is available.
    This keeps management downstream of an actual entry touch.
    """
    ensure_trade_manager_schema(db_path)
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """SELECT s.id,s.symbol,s.direction,s.entry,s.sl,s.tp1,s.tp2,s.tp3,
                      s.timeframe,s.grade,s.signal_type,
                      COALESCE(x.status,'active') AS lifecycle_status
               FROM signals s
               LEFT JOIN signal_execution_state x ON x.signal_id=s.id
               WHERE s.result='pending'"""
        ).fetchall()
    except sqlite3.Error:
        rows = []
    conn.close()
    registered = 0
    for row in rows:
        data = dict(row)
        if str(data.pop("lifecycle_status", "active")).lower() != "active":
            continue
        before = load_state(int(data["id"]), db_path)
        thesis = _load_setup_thesis(int(data["id"]), data, db_path)
        register_active_trade(data, thesis=thesis, db_path=db_path)
        if before is not None and thesis.get("source") == "setup_evidence":
            try:
                current_thesis = json.loads(before.get("thesis_json") or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                current_thesis = {}
            if current_thesis.get("source") != "setup_evidence":
                conn = _connect(db_path)
                conn.execute(
                    "UPDATE trade_manager_state SET thesis_json=?,updated_at=CURRENT_TIMESTAMP WHERE signal_id=?",
                    (json.dumps(thesis, ensure_ascii=False, default=str)[:20000], int(data["id"])),
                )
                conn.commit(); conn.close()
        if before is None and load_state(int(data["id"]), db_path) is not None:
            registered += 1
    return registered


def load_state(signal_id: int, db_path: str = DB_PATH) -> dict[str, Any] | None:
    ensure_trade_manager_schema(db_path)
    conn = _connect(db_path)
    row = conn.execute("SELECT * FROM trade_manager_state WHERE signal_id=?", (int(signal_id),)).fetchone()
    conn.close()
    return dict(row) if row else None


def load_active_states(db_path: str = DB_PATH) -> list[dict[str, Any]]:
    ensure_trade_manager_schema(db_path)
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """SELECT m.* FROM trade_manager_state m
               JOIN signals s ON s.id=m.signal_id
               LEFT JOIN signal_execution_state x ON x.signal_id=s.id
               WHERE s.result='pending' AND COALESCE(x.status,'active')='active'
               ORDER BY m.signal_id"""
        ).fetchall()
    except sqlite3.Error:
        rows = []
    conn.close()
    return [dict(row) for row in rows]


def persist_review(
    state: dict[str, Any],
    price: float,
    events: list[str],
    facts: dict[str, Any],
    review: dict[str, Any],
    db_path: str = DB_PATH,
) -> None:
    ensure_trade_manager_schema(db_path)
    current_r = r_multiple(
        state["direction"], float(state["initial_entry"]), float(state["initial_sl"]), price
    )
    bullish = str(state["direction"]).upper() == "BULLISH"
    best = float(state.get("best_price") or price)
    best = max(best, price) if bullish else min(best, price)
    tp1_seen = int(state.get("tp1_seen") or 0) or int("TP1_HIT" in events)
    tp2_seen = int(state.get("tp2_seen") or 0) or int("TP2_HIT" in events)
    tp3_seen = int(state.get("tp3_seen") or 0) or int("TP3_HIT" in events)
    manager_target = review.get("management_target") or state.get("manager_target")
    protect_level = review.get("protect_level") or state.get("manager_protect_level")
    candle_id = facts.get("management_candle_id") or state.get("last_reviewed_candle")
    conn = _connect(db_path)
    conn.execute(
        """UPDATE trade_manager_state
           SET last_price=?,best_price=?,current_r=?,tp1_seen=?,tp2_seen=?,tp3_seen=?,manager_target=?,
               manager_protect_level=?,last_event=?,last_action=?,last_confidence=?,
               last_reviewed_candle=?,updated_at=CURRENT_TIMESTAMP WHERE signal_id=?""",
        (
            price, best, current_r, tp1_seen, tp2_seen, tp3_seen, manager_target, protect_level,
            ",".join(events), review.get("action"), review.get("confidence"),
            str(candle_id) if candle_id is not None else None, int(state["signal_id"]),
        ),
    )
    conn.execute(
        """INSERT INTO trade_manager_events
           (signal_id,event_type,action,confidence,price,r_multiple,manager_target,
            manager_protect_level,facts_json,reason) VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (
            int(state["signal_id"]), ",".join(events), review.get("action"),
            review.get("confidence"), price, current_r, manager_target, protect_level,
            json.dumps(facts or {}, ensure_ascii=False, default=str)[:20000],
            str(review.get("reason") or "")[:1500],
        ),
    )
    conn.commit()
    conn.close()
    try:
        from core.experience_memory import record_management_review
        observed_state = dict(state)
        observed_state["current_r"] = current_r
        record_management_review(observed_state, price, events, facts, review, db_path)
    except Exception:
        # Experience linkage is fail-safe and never blocks advisory management.
        pass


def should_notify(state: dict[str, Any], events: list[str], review: dict[str, Any]) -> bool:
    if not events:
        return False
    action = str(review.get("action") or "").upper()
    previous = str(state.get("last_action") or "").upper()
    important = {"TP1_HIT", "TP2_HIT", "TP3_HIT", "INVALIDATION_HIT", "BOS", "CHOCH", "EXTERNAL_CONFLICT"}
    return action != previous or bool(important.intersection(events))


def format_telegram_update(
    state: dict[str, Any], price: float, events: list[str], review: dict[str, Any]
) -> str:
    action = review.get("action", "WAIT_CONFIRMATION")
    icon = {
        "HOLD": "🟢", "LET_RUN": "🚀", "PROTECT": "🛡",
        "PARTIAL_EXIT": "🟡", "EXIT": "🔴", "WAIT_CONFIRMATION": "⏳",
    }.get(action, "🧠")
    r_now = r_multiple(
        state["direction"], float(state["initial_entry"]), float(state["initial_sl"]), price
    )
    protect = review.get("protect_level") or state.get("manager_protect_level")
    target = review.get("management_target") or state.get("manager_target")
    protection = f"\n🛡 Структурная защита: <code>{protect}</code>" if protect not in (None, "", 0) else ""
    target_text = f"\n🎯 Новая структурная цель: <code>{target}</code>" if target not in (None, "", 0) else "\n🎯 Новая цель: не подтверждена"
    symbol = html.escape(str(state.get("symbol") or "—"))
    strategy = html.escape(str(state.get("strategy") or "—"))
    direction = html.escape(str(state.get("direction") or "—"))
    event_text = html.escape(", ".join(str(event) for event in events))
    action_text = html.escape(str(action))
    reason = html.escape(str(review.get("reason") or "—"))
    next_trigger = html.escape(str(review.get("next_trigger") or "следующее значимое событие"))
    return (
        f"🛠 <b>APEX MANAGER — {symbol}</b>\n"
        f"Стратегия: <b>{strategy} {direction}</b>\n"
        f"Цена: <code>{price}</code> | результат: <b>{r_now:+.2f}R</b>\n"
        f"Событие: <code>{event_text}</code>\n\n"
        f"{icon} Решение: <b>{action_text}</b>\n"
        f"Уверенность: <b>{float(review.get('confidence') or 0)*100:.0f}%</b>\n"
        f"Причина: {reason}"
        f"{protection}{target_text}\n"
        f"Следующая проверка: {next_trigger}"
    )


def manager_cycle(
    get_prices: Callable[[], dict[str, Any]],
    get_candles: Callable[[str, str, int], list[Any]],
    ask_groq: Callable[..., Any],
    *,
    external_context: Callable[..., dict[str, Any]] | None = None,
    db_path: str = DB_PATH,
) -> list[dict[str, Any]]:
    """Run one non-blocking-by-design management pass over active analytics trades.

    Price polling is cheap; Groq is invoked only when detect_events reports a
    material event. Returned items are ready for Telegram delivery by bot.py.
    """
    ensure_trade_manager_schema(db_path)
    register_pending_signals(db_path)
    try:
        prices = get_prices() or {}
    except Exception:
        prices = {}
    output: list[dict[str, Any]] = []
    for state in load_active_states(db_path):
        symbol = state["symbol"]
        raw_price = prices.get(symbol)
        if isinstance(raw_price, dict):
            raw_price = raw_price.get("price")
        try:
            price = float(raw_price)
        except (TypeError, ValueError):
            continue
        try:
            candles = get_candles(symbol, state["management_tf"], 120) or []
        except Exception:
            candles = []
        facts = build_structure_facts(candles, state["direction"], state.get("last_reviewed_candle"))
        facts["current_price"] = price
        facts["management_matrix"] = management_matrix(state.get("strategy"))
        if external_context is not None:
            try:
                try:
                    extra = external_context(symbol, state.get("direction")) or {}
                except TypeError:
                    extra = external_context(symbol) or {}
                if isinstance(extra, dict):
                    compact = compact_external_context(extra, state.get("direction"))
                    facts["external"] = compact
                    facts["external_conflict"] = bool(compact.get("significant_conflict"))
            except Exception:
                facts["external_data_unavailable"] = True
        events = detect_events(state, price, facts)
        if not events:
            continue
        review = review_active_trade(state, events, facts, ask_groq)
        notify = should_notify(state, events, review)
        persist_review(state, price, events, facts, review, db_path)
        output.append({
            "signal_id": state["signal_id"],
            "symbol": symbol,
            "events": events,
            "review": review,
            "notify": notify,
            "telegram": format_telegram_update(state, price, events, review),
        })
    try:
        from core.experience_memory import resolve_management_reviews
        resolve_management_reviews(db_path)
    except Exception:
        pass
    return output
