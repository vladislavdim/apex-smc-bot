"""Event-driven advisory trade manager for active APEX signals.

The manager is deliberately downstream of entry strategies. It never creates a
signal and never changes the immutable initial entry/SL/TP/RR. It observes an
already-active thesis, detects meaningful management events, asks Groq for a
bounded advisory action, and persists the decision for learning/audit.
"""
from __future__ import annotations

import json
import os
import sqlite3
from typing import Any, Callable

DB_PATH = os.environ.get("APEX_DB_PATH", os.environ.get("APEX_BRAIN_DB_PATH", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "brain.db")))
ALLOWED_ACTIONS = {"HOLD", "PROTECT", "PARTIAL_EXIT", "LET_RUN", "EXIT", "WAIT_CONFIRMATION"}
MANAGEMENT_TF = {"FAST": "5m", "MTF": "15m", "ZONE": "15m", "SWING": "1h", "WYCKOFF": "1h"}


def _connect(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=20, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def ensure_trade_manager_schema(db_path: str = DB_PATH) -> None:
    conn = _connect(db_path)
    conn.executescript("""
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
        facts_json TEXT NOT NULL DEFAULT '{}',
        reason TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_trade_manager_events_signal ON trade_manager_events(signal_id, created_at);
    """)
    conn.commit(); conn.close()


def normalize_strategy(value: Any) -> str:
    raw = str(value or "MTF").upper()
    if "FAST" in raw: return "FAST"
    if "SWING" in raw: return "SWING"
    if "ZONE" in raw: return "ZONE"
    if "WYCKOFF" in raw: return "WYCKOFF"
    return "MTF"


def r_multiple(direction: str, entry: float, sl: float, price: float) -> float:
    risk = abs(entry - sl)
    if risk <= 0: return 0.0
    move = price - entry if str(direction).upper() == "BULLISH" else entry - price
    return round(move / risk, 4)


def detect_events(state: dict[str, Any], price: float, facts: dict[str, Any]) -> list[str]:
    events: list[str] = []
    bullish = str(state["direction"]).upper() == "BULLISH"
    tp1, tp2, sl = float(state["initial_tp1"]), float(state.get("initial_tp2") or state["initial_tp1"]), float(state["initial_sl"])
    if not int(state.get("tp1_seen") or 0) and ((bullish and price >= tp1) or (not bullish and price <= tp1)):
        events.append("TP1_HIT")
    if int(state.get("tp1_seen") or 0) and not int(state.get("tp2_seen") or 0) and ((bullish and price >= tp2) or (not bullish and price <= tp2)):
        events.append("TP2_HIT")
    if (bullish and price <= sl) or (not bullish and price >= sl):
        events.append("INVALIDATION_HIT")
    structure = str(facts.get("structure_event") or "").upper()
    if structure in {"BOS", "CHOCH", "CHoCH".upper()}:
        events.append(structure)
    if facts.get("closed_candle") and facts.get("new_management_candle"):
        events.append("MANAGEMENT_CANDLE_CLOSE")
    if facts.get("external_conflict"):
        events.append("EXTERNAL_CONFLICT")
    return list(dict.fromkeys(events))


def _prompt(state: dict[str, Any], events: list[str], facts: dict[str, Any]) -> str:
    payload = {
        "trade": {k: state.get(k) for k in ("signal_id", "symbol", "strategy", "direction", "management_tf", "initial_entry", "initial_sl", "initial_tp1", "initial_tp2", "initial_tp3", "initial_rr", "last_price", "best_price", "current_r", "tp1_seen", "tp2_seen")},
        "events": events,
        "facts": facts,
    }
    return """You are APEX Trade Manager. Manage an already-open trading thesis; do not create a new trade.\nInitial entry, initial SL, TP1/TP2/TP3 and initial RR are immutable historical facts. Never rewrite them.\nUse only supplied facts; never invent candles, structure, volume, OI, funding, news, levels or probabilities.\nChoose exactly one action: HOLD, PROTECT, PARTIAL_EXIT, LET_RUN, EXIT, WAIT_CONFIRMATION.\nPROTECT means recommend protection only when supplied facts contain a confirmed structural protection level. LET_RUN requires continuation evidence, not price alone. EXIT requires invalidation or strong confirmed reversal evidence. A wick alone is not a confirmed BOS/CHoCH.\nReturn strict JSON only: {\"action\":\"HOLD\",\"confidence\":0.0,\"reason\":\"...\",\"protect_level\":null,\"next_trigger\":\"...\"}.\nDATA:\n""" + json.dumps(payload, ensure_ascii=False, default=str)[:14000]


def _parse_review(raw: Any) -> dict[str, Any]:
    try:
        text = str(raw or "").strip().replace("```json", "").replace("```", "").strip()
        start, end = text.find("{"), text.rfind("}")
        obj = json.loads(text[start:end + 1]) if start >= 0 and end > start else {}
    except Exception:
        obj = {}
    action = str(obj.get("action") or "WAIT_CONFIRMATION").upper()
    if action not in ALLOWED_ACTIONS: action = "WAIT_CONFIRMATION"
    try: confidence = max(0.0, min(1.0, float(obj.get("confidence") or 0)))
    except (TypeError, ValueError): confidence = 0.0
    return {"action": action, "confidence": confidence, "reason": str(obj.get("reason") or "")[:1500], "protect_level": obj.get("protect_level"), "next_trigger": str(obj.get("next_trigger") or "")[:500]}


def review_active_trade(state: dict[str, Any], events: list[str], facts: dict[str, Any], ask_groq: Callable[..., Any]) -> dict[str, Any]:
    if not events:
        return {"action": "HOLD", "confidence": 1.0, "reason": "No material management event", "protect_level": None, "next_trigger": "next material event", "groq_called": False}
    try:
        raw = ask_groq(_prompt(state, events, facts), max_tokens=260)
        review = _parse_review(raw)
        review["groq_called"] = True
        return review
    except Exception as exc:
        return {"action": "WAIT_CONFIRMATION", "confidence": 0.0, "reason": f"Groq unavailable: {type(exc).__name__}", "protect_level": None, "next_trigger": "next material event", "groq_called": False}


def register_active_trade(signal: dict[str, Any], thesis: dict[str, Any] | None = None, db_path: str = DB_PATH) -> None:
    ensure_trade_manager_schema(db_path)
    strategy = normalize_strategy(signal.get("strategy") or signal.get("scan_type") or signal.get("grade") or signal.get("signal_type"))
    entry, sl = float(signal.get("entry") or 0), float(signal.get("sl") or 0)
    tp1 = float(signal.get("tp1", signal.get("tp")) or 0)
    if min(entry, sl, tp1) <= 0: return
    signal_id = int(signal.get("signal_id") or signal.get("id") or 0)
    if signal_id <= 0: return
    conn = _connect(db_path)
    conn.execute("""INSERT OR IGNORE INTO trade_manager_state
      (signal_id,symbol,strategy,direction,management_tf,initial_entry,initial_sl,initial_tp1,initial_tp2,initial_tp3,initial_rr,thesis_json)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (signal_id, str(signal.get("symbol") or "").upper(), strategy, str(signal.get("direction") or "").upper(), MANAGEMENT_TF[strategy], entry, sl, tp1, float(signal.get("tp2") or tp1), float(signal.get("tp3") or signal.get("tp2") or tp1), float(signal.get("rr") or 0), json.dumps(thesis or {}, ensure_ascii=False, default=str)[:20000]))
    conn.commit(); conn.close()


def persist_review(state: dict[str, Any], price: float, events: list[str], facts: dict[str, Any], review: dict[str, Any], db_path: str = DB_PATH) -> None:
    ensure_trade_manager_schema(db_path)
    current_r = r_multiple(state["direction"], float(state["initial_entry"]), float(state["initial_sl"]), price)
    bullish = str(state["direction"]).upper() == "BULLISH"
    best = float(state.get("best_price") or price)
    best = max(best, price) if bullish else min(best, price)
    tp1_seen = int(state.get("tp1_seen") or 0) or int("TP1_HIT" in events)
    tp2_seen = int(state.get("tp2_seen") or 0) or int("TP2_HIT" in events)
    conn = _connect(db_path)
    conn.execute("""UPDATE trade_manager_state SET last_price=?,best_price=?,current_r=?,tp1_seen=?,tp2_seen=?,last_event=?,last_action=?,last_confidence=?,updated_at=CURRENT_TIMESTAMP WHERE signal_id=?""", (price, best, current_r, tp1_seen, tp2_seen, ",".join(events), review.get("action"), review.get("confidence"), int(state["signal_id"])))
    conn.execute("""INSERT INTO trade_manager_events(signal_id,event_type,action,confidence,price,r_multiple,facts_json,reason) VALUES (?,?,?,?,?,?,?,?)""", (int(state["signal_id"]), ",".join(events), review.get("action"), review.get("confidence"), price, current_r, json.dumps(facts or {}, ensure_ascii=False, default=str)[:20000], str(review.get("reason") or "")[:1500]))
    conn.commit(); conn.close()


def format_telegram_update(state: dict[str, Any], price: float, events: list[str], review: dict[str, Any]) -> str:
    action = review.get("action", "WAIT_CONFIRMATION")
    icon = {"HOLD":"🟢", "LET_RUN":"🚀", "PROTECT":"🛡", "PARTIAL_EXIT":"🟡", "EXIT":"🔴", "WAIT_CONFIRMATION":"⏳"}.get(action, "🧠")
    r_now = r_multiple(state["direction"], float(state["initial_entry"]), float(state["initial_sl"]), price)
    protect = review.get("protect_level")
    protection = f"\n🛡 Уровень защиты: <code>{protect}</code>" if protect not in (None, "", 0) else ""
    return (f"🧠 <b>APEX MANAGER — {state['symbol']}</b>\n"
            f"Стратегия: <b>{state['strategy']} {state['direction']}</b>\n"
            f"Цена: <code>{price}</code> | результат: <b>{r_now:+.2f}R</b>\n"
            f"Событие: <code>{', '.join(events)}</code>\n\n"
            f"{icon} Решение: <b>{action}</b>\n"
            f"Уверенность: <b>{float(review.get('confidence') or 0)*100:.0f}%</b>\n"
            f"Причина: {review.get('reason') or '—'}{protection}\n"
            f"Следующая проверка: {review.get('next_trigger') or 'следующее значимое событие'}")
