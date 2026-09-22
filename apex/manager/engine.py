"""Canonical APEX V3 live Trade Manager.

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
import hashlib
import json
import sqlite3
from apex.db.connection import connect_compatibility as _connect_compatibility_db
from apex.db.repositories.manager_messages import ManagerMessageRepository
from apex.db.repositories.manager import ManagerRepository
from apex.db.repositories.executions import ExecutionRepository
from apex.db.repositories.signal_lifecycle import SignalLifecycleRepository
from apex.domain.ids import derived_id
from typing import Any, Callable
from apex.config.settings import ApexConfig

from core.market_structure import analyze_market_structure

DB_PATH = ApexConfig.from_env().database.compatibility_db_path
MANAGER_VERSION = 2
ALLOWED_ACTIONS = {
    "OPEN", "HOLD", "MOVE_STOP_TO_BREAKEVEN", "PROTECT", "PARTIAL_EXIT",
    "LET_RUN", "CLOSE",
}
MANAGER_STATES = {
    "OPENING", "PROTECTED", "MANAGING", "TP1_REACHED", "PROFIT_PROTECTED",
    "LET_RUN", "EXITING", "CLOSED", "DEGRADED", "RECONCILIATION_REQUIRED",
}
TRANSITION_MATRIX = {
    "OPENING": {"OPEN": "OPENING", "HOLD": "OPENING"},
    "PROTECTED": {"HOLD": "PROTECTED", "PROTECT": "MANAGING", "CLOSE": "EXITING"},
    "MANAGING": {"HOLD": "MANAGING", "PROTECT": "MANAGING", "CLOSE": "EXITING"},
    "TP1_REACHED": {"HOLD": "TP1_REACHED", "PARTIAL_EXIT": "TP1_REACHED", "MOVE_STOP_TO_BREAKEVEN": "PROFIT_PROTECTED", "PROTECT": "PROFIT_PROTECTED", "LET_RUN": "LET_RUN", "CLOSE": "EXITING"},
    "PROFIT_PROTECTED": {"HOLD": "PROFIT_PROTECTED", "PROTECT": "PROFIT_PROTECTED", "PARTIAL_EXIT": "PROFIT_PROTECTED", "LET_RUN": "LET_RUN", "CLOSE": "EXITING"},
    "LET_RUN": {"HOLD": "LET_RUN", "PROTECT": "PROFIT_PROTECTED", "CLOSE": "EXITING"},
    "EXITING": {"HOLD": "EXITING"},
    "DEGRADED": {"HOLD": "DEGRADED"},
    "RECONCILIATION_REQUIRED": {"HOLD": "RECONCILIATION_REQUIRED"},
    "CLOSED": {},
}
MANAGEMENT_TF = {
    "FAST": "5m",
    "MTF": "15m",
    "ZONE": "15m",
    "SWING": "1h",
    "WYCKOFF": "1h",
}
PROGRESS_TF = {"FAST": "15m", "MTF": "15m", "SWING": "1h", "ZONE": "1h", "WYCKOFF": "4h"}
NO_PROGRESS_BARS = {"FAST": 4, "MTF": 6, "SWING": 6, "ZONE": 4, "WYCKOFF": 3}
_MANAGED_EXECUTION_STATUSES = frozenset({
    "PROTECTED", "PROTECTED_NO_TP", "STOP_REPLACEMENT_PENDING",
})
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
    conn = _connect_compatibility_db(db_path, timeout=20, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


_MANAGER_MESSAGE_STATE_FACTORY = None
_MANAGER_STATE_FACTORY = None


def configure_manager_message_state(connection_factory=None) -> None:
    global _MANAGER_MESSAGE_STATE_FACTORY
    _MANAGER_MESSAGE_STATE_FACTORY = connection_factory


def configure_manager_state(connection_factory=None) -> None:
    global _MANAGER_STATE_FACTORY
    _MANAGER_STATE_FACTORY = connection_factory


def _manager_state_repository() -> ManagerRepository:
    if _MANAGER_STATE_FACTORY is None:
        raise RuntimeError("manager_state_not_configured")
    return ManagerRepository(_MANAGER_STATE_FACTORY)


def _manager_state_compatibility(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result["manager_protect_level"] = result.get("confirmed_protect_level")
    return result


def _manager_message_repository(db_path: str) -> ManagerMessageRepository:
    factory = _MANAGER_MESSAGE_STATE_FACTORY or (lambda: _connect(db_path))
    return ManagerMessageRepository(factory)


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
            proposed_protect_level REAL,
            last_event TEXT,
            last_action TEXT,
            last_confidence REAL,
            last_reviewed_candle TEXT,
            data_failure_count INTEGER NOT NULL DEFAULT 0,
            data_failure_notified INTEGER NOT NULL DEFAULT 0,
            last_data_error TEXT,
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
        CREATE TABLE IF NOT EXISTS trade_manager_messages (
            signal_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            thread_id INTEGER NOT NULL DEFAULT 0,
            message_id INTEGER NOT NULL,
            content_hash TEXT NOT NULL DEFAULT '',
            is_final INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(signal_id, chat_id, thread_id)
        );
        CREATE TABLE IF NOT EXISTS trade_manager_runtime (
            key TEXT PRIMARY KEY, value TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS trade_manager_entry_snapshots (
            signal_id INTEGER PRIMARY KEY, snapshot_json TEXT NOT NULL,
            snapshot_hash TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    # Safe additive migration for databases created by the first manager build.
    for table, column, typedef in (
        ("trade_manager_state", "manager_target", "REAL"),
        ("trade_manager_state", "manager_protect_level", "REAL"),
        ("trade_manager_state", "proposed_protect_level", "REAL"),
        ("trade_manager_events", "manager_target", "REAL"),
        ("trade_manager_events", "manager_protect_level", "REAL"),
        ("trade_manager_events", "manager_event_id", "TEXT"),
        ("trade_manager_state", "tp3_seen", "INTEGER NOT NULL DEFAULT 0"),
        ("trade_manager_state", "data_failure_count", "INTEGER NOT NULL DEFAULT 0"),
        ("trade_manager_state", "data_failure_notified", "INTEGER NOT NULL DEFAULT 0"),
        ("trade_manager_state", "last_data_error", "TEXT"),
        ("trade_manager_state", "status", "TEXT NOT NULL DEFAULT 'ACTIVE'"),
        ("trade_manager_state", "close_result", "TEXT"),
        ("trade_manager_state", "exit_price", "REAL"),
        ("trade_manager_state", "realized_pct", "REAL"),
        ("trade_manager_state", "realized_r", "REAL"),
        ("trade_manager_state", "closed_at", "TEXT"),
        ("trade_manager_state", "manager_version", "INTEGER NOT NULL DEFAULT 2"),
        ("trade_manager_state", "manager_state", "TEXT NOT NULL DEFAULT 'PROTECTED'"),
        ("trade_manager_state", "position_fraction", "REAL NOT NULL DEFAULT 1.0"),
        ("trade_manager_state", "partial_exit_done", "INTEGER NOT NULL DEFAULT 0"),
        ("trade_manager_state", "no_progress_bars", "INTEGER NOT NULL DEFAULT 0"),
        ("trade_manager_state", "progress_anchor_r", "REAL NOT NULL DEFAULT 0"),
        ("trade_manager_state", "last_progress_candle", "TEXT"),
        ("trade_manager_state", "reconciliation_reason", "TEXT"),
        ("trade_manager_state", "pre_degraded_state", "TEXT"),
    ):
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {typedef}")
        except sqlite3.OperationalError:
            pass
    event_rows = conn.execute(
        "SELECT id FROM trade_manager_events WHERE manager_event_id IS NULL OR manager_event_id=''"
    ).fetchall()
    for event_row in event_rows:
        conn.execute(
            "UPDATE trade_manager_events SET manager_event_id=? WHERE id=?",
            (derived_id("manager_event", "legacy", int(event_row[0])), int(event_row[0])),
        )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_trade_manager_events_identity "
        "ON trade_manager_events(manager_event_id)"
    )
    conn.execute(
        """INSERT INTO trade_manager_runtime(key,value) VALUES('active_manager_version','2')
           ON CONFLICT(key) DO UPDATE SET value='2',updated_at=CURRENT_TIMESTAMP"""
    )
    conn.execute(
        """INSERT INTO trade_manager_runtime(key,value) VALUES('opens_enabled','1')
           ON CONFLICT(key) DO NOTHING"""
    )
    marker = conn.execute(
        "SELECT value FROM trade_manager_runtime WHERE key='v2_cutover_complete'"
    ).fetchone()
    if not marker:
        # First V2 startup is the atomic cutover boundary. Existing live rows
        # remain fenced until trade_execution confirms reconciliation.
        conn.execute("UPDATE trade_manager_runtime SET value='0',updated_at=CURRENT_TIMESTAMP WHERE key='opens_enabled'")
        conn.execute(
            """UPDATE trade_manager_state SET manager_version=2,manager_state='PROTECTED',
                      reconciliation_reason=NULL WHERE COALESCE(status,'ACTIVE')!='CLOSED'"""
        )
        has_execution = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trade_executions'"
        ).fetchone()
        if has_execution:
            conn.execute(
                """UPDATE trade_manager_state SET manager_state='RECONCILIATION_REQUIRED',
                          reconciliation_reason='V1_TO_V2_LIVE_RECONCILIATION'
                   WHERE COALESCE(status,'ACTIVE')!='CLOSED' AND signal_id IN (
                     SELECT signal_id FROM trade_executions WHERE mode='live'
                      AND status IN ('ENTRY_PENDING','PROTECTED','PROTECTED_NO_TP','CLEANUP_PENDING'))"""
            )
        pending = int(conn.execute(
            "SELECT COUNT(*) FROM trade_manager_state WHERE manager_state='RECONCILIATION_REQUIRED'"
        ).fetchone()[0] or 0)
        if pending == 0:
            conn.execute("UPDATE trade_manager_runtime SET value='1',updated_at=CURRENT_TIMESTAMP WHERE key='opens_enabled'")
            conn.execute("INSERT INTO trade_manager_runtime(key,value) VALUES('v2_cutover_complete','1')")
    conn.commit()
    conn.close()


def telegram_content_hash(text: str) -> str:
    return hashlib.sha256(str(text or "").encode("utf-8")).hexdigest()


def validate_transition(state_name: Any, action: Any) -> tuple[bool, str]:
    """Return transition validity before any risk or exchange validation."""
    current = str(state_name or "PROTECTED").upper()
    requested = str(action or "HOLD").upper()
    if current not in MANAGER_STATES:
        return False, current
    next_state = TRANSITION_MATRIX.get(current, {}).get(requested)
    return (next_state is not None), (next_state or current)


def manager_runtime(db_path: str = DB_PATH) -> dict[str, str]:
    if _MANAGER_STATE_FACTORY is not None:
        return _manager_state_repository().runtime()
    ensure_trade_manager_schema(db_path)
    conn = _connect(db_path)
    rows = conn.execute("SELECT key,value FROM trade_manager_runtime").fetchall()
    conn.close()
    return {str(row[0]): str(row[1]) for row in rows}


def set_manager_runtime(key: str, value: Any, db_path: str = DB_PATH) -> None:
    if _MANAGER_STATE_FACTORY is not None:
        _manager_state_repository().set_runtime(key, value)
        return
    ensure_trade_manager_schema(db_path)
    conn = _connect(db_path)
    conn.execute(
        """INSERT INTO trade_manager_runtime(key,value) VALUES(?,?)
           ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=CURRENT_TIMESTAMP""",
        (str(key), str(value)),
    )
    conn.commit()
    conn.close()


def begin_v2_cutover(db_path: str = DB_PATH) -> int:
    """Atomically fence opens and mark active legacy rows for reconciliation."""
    ensure_trade_manager_schema(db_path)
    conn = _connect(db_path)
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("UPDATE trade_manager_runtime SET value='0',updated_at=CURRENT_TIMESTAMP WHERE key='opens_enabled'")
    cursor = conn.execute(
        """UPDATE trade_manager_state SET manager_version=2,
                  manager_state='RECONCILIATION_REQUIRED',
                  reconciliation_reason='V1_TO_V2_CUTOVER',updated_at=CURRENT_TIMESTAMP
           WHERE COALESCE(status,'ACTIVE')!='CLOSED' AND COALESCE(manager_version,1)<2"""
    )
    conn.execute(
        """INSERT INTO trade_manager_runtime(key,value) VALUES('active_manager_version','2')
           ON CONFLICT(key) DO UPDATE SET value='2',updated_at=CURRENT_TIMESTAMP"""
    )
    conn.commit()
    count = int(cursor.rowcount)
    conn.close()
    return count


def complete_v2_cutover(db_path: str = DB_PATH) -> bool:
    """Enable V2 opens only when no migrated row remains uncertain."""
    ensure_trade_manager_schema(db_path)
    conn = _connect(db_path)
    pending = conn.execute(
        "SELECT COUNT(*) FROM trade_manager_state WHERE manager_state='RECONCILIATION_REQUIRED'"
    ).fetchone()[0]
    if int(pending or 0) == 0:
        conn.execute("UPDATE trade_manager_runtime SET value='1',updated_at=CURRENT_TIMESTAMP WHERE key='opens_enabled'")
        conn.commit()
    conn.close()
    return int(pending or 0) == 0


def activate_v2_once(db_path: str = DB_PATH) -> dict[str, Any]:
    """One-time atomic V1 fence/migration; never runs two decision engines."""
    ensure_trade_manager_schema(db_path)
    runtime = manager_runtime(db_path)
    if runtime.get("v2_cutover_complete") == "1":
        return {"already_active": True, "reconciliation_required": 0}
    conn = _connect(db_path)
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("UPDATE trade_manager_runtime SET value='0',updated_at=CURRENT_TIMESTAMP WHERE key='opens_enabled'")
    conn.execute(
        """UPDATE trade_manager_state SET manager_version=2,manager_state='PROTECTED',
                  reconciliation_reason=NULL,updated_at=CURRENT_TIMESTAMP
           WHERE COALESCE(status,'ACTIVE')!='CLOSED'"""
    )
    has_execution = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trade_executions'"
    ).fetchone()
    if has_execution:
        conn.execute(
            """UPDATE trade_manager_state SET manager_state='RECONCILIATION_REQUIRED',
                      reconciliation_reason='V1_TO_V2_LIVE_RECONCILIATION',updated_at=CURRENT_TIMESTAMP
               WHERE COALESCE(status,'ACTIVE')!='CLOSED' AND signal_id IN (
                 SELECT signal_id FROM trade_executions WHERE mode='live'
                   AND status IN ('ENTRY_PENDING','PROTECTED','PROTECTED_NO_TP','CLEANUP_PENDING')
               )"""
        )
    pending = int(conn.execute(
        "SELECT COUNT(*) FROM trade_manager_state WHERE manager_state='RECONCILIATION_REQUIRED'"
    ).fetchone()[0] or 0)
    if pending == 0:
        conn.execute("UPDATE trade_manager_runtime SET value='1',updated_at=CURRENT_TIMESTAMP WHERE key='opens_enabled'")
        conn.execute(
            """INSERT INTO trade_manager_runtime(key,value) VALUES('v2_cutover_complete','1')
               ON CONFLICT(key) DO UPDATE SET value='1',updated_at=CURRENT_TIMESTAMP"""
        )
    conn.commit()
    conn.close()
    return {"already_active": False, "reconciliation_required": pending}


def confirm_v2_reconciliation(signal_id: int, exchange_status: str, db_path: str = DB_PATH) -> bool:
    """Resolve a cutover row only after bounded exchange reconciliation succeeds."""
    safe = str(exchange_status or "").upper() in {
        "PROTECTED", "PROTECTED_NO_TP", "ENTRY_CANCELLED", "EMERGENCY_CLOSED",
    } or str(exchange_status or "").upper().startswith("CLOSED_")
    if not safe:
        return False
    target = "CLOSED" if str(exchange_status).upper().startswith(("CLOSED_", "EMERGENCY_CLOSED", "ENTRY_CANCELLED")) else "PROTECTED"
    if _MANAGER_STATE_FACTORY is not None:
        return _manager_state_repository().confirm_reconciliation(int(signal_id), target)
    conn = _connect(db_path)
    conn.execute(
        """UPDATE trade_manager_state SET manager_state=?,reconciliation_reason=NULL,
                  updated_at=CURRENT_TIMESTAMP WHERE signal_id=?
           AND manager_state='RECONCILIATION_REQUIRED'""",
        (target, int(signal_id)),
    )
    remaining = int(conn.execute(
        "SELECT COUNT(*) FROM trade_manager_state WHERE manager_state='RECONCILIATION_REQUIRED'"
    ).fetchone()[0] or 0)
    if remaining == 0:
        conn.execute("UPDATE trade_manager_runtime SET value='1',updated_at=CURRENT_TIMESTAMP WHERE key='opens_enabled'")
        conn.execute(
            """INSERT INTO trade_manager_runtime(key,value) VALUES('v2_cutover_complete','1')
               ON CONFLICT(key) DO UPDATE SET value='1',updated_at=CURRENT_TIMESTAMP"""
        )
    conn.commit()
    conn.close()
    return True


def load_manager_message(
    signal_id: int, chat_id: int, thread_id: int = 0, db_path: str = DB_PATH,
) -> dict[str, Any] | None:
    if _MANAGER_MESSAGE_STATE_FACTORY is None:
        ensure_trade_manager_schema(db_path)
    return _manager_message_repository(db_path).load(signal_id, chat_id, thread_id)


def store_manager_message(
    signal_id: int, chat_id: int, message_id: int, text: str, *,
    thread_id: int = 0, is_final: bool = False, db_path: str = DB_PATH,
) -> None:
    if _MANAGER_MESSAGE_STATE_FACTORY is None:
        ensure_trade_manager_schema(db_path)
    _manager_message_repository(db_path).store(
        signal_id, chat_id, thread_id, message_id,
        telegram_content_hash(text), is_final,
    )


def finalize_manager_trade(
    signal_id: int, result: str, exit_price: float, *, closed_at: str | None = None,
    db_path: str = DB_PATH,
) -> dict[str, Any] | None:
    """Close a compatibility-only Manager row from analytical price data.

    Once the production State repository is configured, a candle-derived
    result is not exchange accounting.  A live execution is fenced for
    reconciliation and only confirmed Binance fills may close it.  This keeps
    the compatibility helper from becoming an accidental second close path.
    """
    state = load_state(signal_id, db_path)
    if not state:
        return None
    if str(state.get("status") or "ACTIVE").upper() == "CLOSED":
        return load_state(signal_id, db_path)
    if _MANAGER_STATE_FACTORY is not None:
        execution = ExecutionRepository(_MANAGER_STATE_FACTORY).get(int(signal_id))
        if execution and str(execution.get("mode") or "").lower() == "live":
            _manager_state_repository().await_exchange_close(int(signal_id))
        # State-backed Manager positions are production-only.  Missing or
        # non-live execution evidence must be handled by reconciliation, not
        # converted into a synthetic PnL here.
        return None
    entry = float(state["initial_entry"])
    sl = float(state["initial_sl"])
    exit_value = float(exit_price or state.get("last_price") or entry)
    direction = str(state.get("direction") or "").upper()
    signed_pct = ((exit_value - entry) / entry * 100.0) if direction == "BULLISH" else ((entry - exit_value) / entry * 100.0)
    realized_r = r_multiple(direction, entry, sl, exit_value)
    conn = _connect(db_path)
    conn.execute(
        """UPDATE trade_manager_state SET status='CLOSED',close_result=?,exit_price=?,
                  realized_pct=?,realized_r=?,closed_at=COALESCE(?,CURRENT_TIMESTAMP),
                  last_price=?,current_r=?,last_event='TRADE_CLOSED',last_action='CLOSE',
                  manager_state='CLOSED',manager_version=2,
                  updated_at=CURRENT_TIMESTAMP WHERE signal_id=? AND COALESCE(status,'ACTIVE')!='CLOSED'""",
        (str(result or "closed").lower(), exit_value, round(signed_pct, 4), realized_r,
         closed_at, exit_value, realized_r, int(signal_id)),
    )
    conn.execute(
        """INSERT INTO trade_manager_events
           (signal_id,event_type,action,confidence,price,r_multiple,facts_json,reason)
           VALUES (?,'TRADE_CLOSED','EXIT',1.0,?,?,?,?)""",
        (int(signal_id), exit_value, realized_r,
         json.dumps({"result": str(result or "closed").lower()}, ensure_ascii=False),
         f"Trade closed: {str(result or 'closed').upper()}"),
    )
    conn.commit()
    conn.close()
    try:
        from core.groq_calibration import resolve_signal
        resolve_signal(
            int(signal_id), reward_r=realized_r,
            outcome_label=1.0 if realized_r > 0 else 0.0,
            reason=str(result or "closed").upper(), db_path=db_path,
        )
    except Exception:
        pass
    return load_state(signal_id, db_path)


def reconcile_manager_states_from_signals(db_path: str = DB_PATH) -> int:
    """Close stale ACTUAL manager rows when the canonical signal is closed.

    This prevents a finished live signal from being managed as an active trade
    after a restart.
    """
    if _MANAGER_STATE_FACTORY is not None:
        positions = _manager_state_repository().active(limit=5000)
        signal_ids = [int(row["signal_id"]) for row in positions]
        lifecycles = SignalLifecycleRepository(_MANAGER_STATE_FACTORY).get_many(signal_ids)
        executions = ExecutionRepository(_MANAGER_STATE_FACTORY).get_many(signal_ids)
        live_execution_ids = {
            signal_id for signal_id, execution in executions.items()
            if str(execution.get("mode") or "") == "live"
        }
        rows = [
            {
                **position,
                "result": str(lifecycles.get(int(position["signal_id"]), {}).get("result") or "pending"),
            }
            for position in positions
            if int(position["signal_id"]) in live_execution_ids
            and str(lifecycles.get(int(position["signal_id"]), {}).get("result") or "pending") != "pending"
        ]
        not_opened_rows = []
        for position in positions:
            signal_id = int(position["signal_id"])
            execution = executions.get(signal_id)
            if execution is None or str(execution.get("mode") or "") != "live":
                not_opened_rows.append({
                    "signal_id": signal_id,
                    "execution_status": "NO_CONFIRMED_LIVE_EXECUTION",
                })
                continue
            status = str(execution.get("status") or "")
            if (
                float(execution.get("quantity") or 0) <= 0
                and not str(execution.get("entry_order_id") or "")
                and (status.startswith(("SKIPPED_", "BLOCKED_"))
                     or status in {"DISABLED", "LIVE_NOT_ARMED"})
            ):
                not_opened_rows.append({
                    "signal_id": signal_id, "execution_status": status,
                })
    else:
        ensure_trade_manager_schema(db_path)
        conn = _connect(db_path)
        try:
            signal_columns = {
                str(row[1]) for row in conn.execute("PRAGMA table_info(signals)").fetchall()
            }
            if not {"id", "result"}.issubset(signal_columns):
                return 0
            rows = [dict(row) for row in conn.execute(
                """SELECT m.signal_id,s.result,m.initial_sl,m.initial_tp1,
                          m.initial_tp2,m.initial_tp3,m.last_price,m.initial_entry
                     FROM trade_manager_state m JOIN signals s ON s.id=m.signal_id
                    WHERE COALESCE(m.status,'ACTIVE')!='CLOSED'
                      AND LOWER(COALESCE(s.result,'pending'))!='pending'"""
            ).fetchall()]
            has_executions = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trade_executions'"
            ).fetchone()
            not_opened_rows = [dict(row) for row in conn.execute(
                """SELECT m.signal_id,te.status AS execution_status
                     FROM trade_manager_state m JOIN trade_executions te ON te.signal_id=m.signal_id
                    WHERE COALESCE(m.status,'ACTIVE')!='CLOSED'
                      AND te.mode='live'
                      AND COALESCE(te.quantity,0)<=0
                      AND COALESCE(te.entry_order_id,'')=''
                      AND (te.status LIKE 'SKIPPED_%' OR te.status LIKE 'BLOCKED_%'
                           OR te.status IN ('DISABLED','LIVE_NOT_ARMED'))"""
            ).fetchall()] if has_executions else []
            live_execution_ids = {
                int(item[0]) for item in conn.execute(
                    "SELECT signal_id FROM trade_executions WHERE mode='live'"
                ).fetchall()
            } if has_executions else set()
        finally:
            conn.close()
    reconciled = 0
    for row in rows:
        signal_id = int(row["signal_id"])
        if signal_id in live_execution_ids:
            if _MANAGER_STATE_FACTORY is not None:
                if _manager_state_repository().await_exchange_close(signal_id):
                    reconciled += 1
                continue
            conn = _connect(db_path)
            conn.execute(
                """UPDATE trade_manager_state SET status='CLOSING',
                          manager_state='RECONCILIATION_REQUIRED',close_result=NULL,
                          exit_price=NULL,realized_pct=NULL,realized_r=NULL,closed_at=NULL,
                          last_event='AWAITING_CONFIRMED_BINANCE_CLOSE',
                          updated_at=CURRENT_TIMESTAMP WHERE signal_id=?""",
                (signal_id,),
            )
            conn.commit(); conn.close()
            reconciled += 1
            continue
        result = str(row["result"] or "closed").lower()
        if result == "sl" or "stop" in result:
            exit_price = row["initial_sl"]
        elif "tp3" in result:
            exit_price = row["initial_tp3"] or row["initial_tp2"] or row["initial_tp1"]
        elif "tp2" in result:
            exit_price = row["initial_tp2"] or row["initial_tp1"]
        elif "tp" in result:
            exit_price = row["initial_tp1"]
        else:
            exit_price = row["last_price"] or row["initial_entry"]
        if finalize_manager_trade(
            signal_id, result, float(exit_price or row["initial_entry"]), db_path=db_path
        ):
            reconciled += 1
    # A delivered Telegram candidate is not necessarily a Binance position.
    # When live execution proved that no order was submitted, close only the
    # Manager state without fabricating an exit/PnL.
    for row in not_opened_rows:
        signal_id = int(row["signal_id"])
        execution_status = str(row["execution_status"] or "NOT_OPENED")
        if _MANAGER_STATE_FACTORY is not None:
            if _manager_state_repository().close_not_opened(signal_id, execution_status):
                reconciled += 1
            continue
        conn = _connect(db_path)
        conn.execute(
            """UPDATE trade_manager_state
                  SET status='CLOSED',manager_state='CLOSED',close_result=?,
                      exit_price=NULL,realized_pct=NULL,realized_r=NULL,
                      closed_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
                WHERE signal_id=? AND COALESCE(status,'ACTIVE')!='CLOSED'""",
            (f"NOT_OPENED:{execution_status}", signal_id),
        )
        conn.execute(
            """INSERT INTO trade_manager_events
               (signal_id,event_type,action,confidence,facts_json,reason)
               VALUES (?,'EXECUTION_NOT_OPENED','HOLD',1.0,?,?)""",
            (signal_id, json.dumps({"execution_status": execution_status}),
             "Binance execution did not submit an entry order"),
        )
        conn.commit()
        conn.close()
        reconciled += 1
    return reconciled


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
        "latest_closed_open": float(closed[-1]["open"]),
        "latest_closed_volume": float(closed[-1].get("volume") or 0.0),
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
            "freshness_score": quality.get("freshness_score"),
            "provenance_score": quality.get("provenance_score"),
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
        "facts": {key: value for key, value in facts.items() if not str(key).startswith("_")},
    }
    return """You are APEX Trade Manager. Manage an already-open trading thesis; do not create a new trade.
Initial entry, initial SL, TP1/TP2/TP3 and initial RR are immutable historical facts. Never rewrite them.
Use the supplied original CORE, TRIGGER, setup class and conflicts as the immutable thesis context. Do not silently replace that thesis.
Use only supplied facts; never invent candles, structure, volume, OI, funding, news, levels or probabilities.
facts.execution is the cached Binance execution snapshot. Treat exchange protective orders as authoritative: if a reduce-only TP1 order exists, do not request a second partial exit for the same TP1 event. After TP1, prefer PROTECT with the supplied confirmed level or LET_RUN when continuation remains valid.
Choose exactly one action: OPEN, HOLD, MOVE_STOP_TO_BREAKEVEN, PROTECT, PARTIAL_EXIT, LET_RUN, CLOSE.
Compound actions are forbidden. A later action may be considered only after the previous action is persisted and confirmed.
On uncertainty return HOLD. Never use HOLD as an instruction to cancel or replace an exchange order.
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
    parsed_ok = bool(obj) and isinstance(obj.get("action"), str)
    action = str(obj.get("action") or "HOLD").upper()
    # Backward-compatible input normalization; V2 persists canonical names.
    action = {"EXIT": "CLOSE", "WAIT_CONFIRMATION": "HOLD"}.get(action, action)
    if action not in ALLOWED_ACTIONS:
        action = "HOLD"
        parsed_ok = False
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
        "valid_response": parsed_ok,
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
    if "AMBIGUOUS_BARRIERS" in events:
        return {
            "action": "HOLD", "confidence": 1.0,
            "reason": "Ambiguous OHLC barriers require confirmed Binance order/fill sequence",
            "protect_level": None, "management_target": None,
            "next_trigger": "exchange reconciliation", "groq_called": False,
        }
    legacy = "manager_version" not in state and "manager_state" not in state
    if legacy and "INVALIDATION_HIT" in events:
        return {
            "action": "EXIT", "confidence": 1.0,
            "reason": "Initial structural invalidation level was reached",
            "protect_level": None, "management_target": None,
            "next_trigger": "trade closed", "groq_called": False,
        }
    current_state = str(
        state.get("manager_state")
        or ("TP1_REACHED" if int(state.get("tp1_seen") or 0) else "PROTECTED")
    ).upper()
    if current_state in {"CLOSED", "RECONCILIATION_REQUIRED", "DEGRADED", "EXITING"}:
        return {
            "action": "HOLD", "confidence": 1.0,
            "reason": f"No-op safety hold in {current_state}",
            "protect_level": None, "management_target": None,
            "next_trigger": "successful reconciliation or recovery", "groq_called": False,
        }
    try:
        raw = ask_groq(_prompt(state, events, facts), max_tokens=300)
        review = _parse_review(raw, facts, state)
        if str(review.get("action") or "").upper() == "CLOSE":
            reverse_structure = bool(
                facts.get("structure_against_trade")
                and str(facts.get("structure_event") or "").upper() in {"BOS", "CHOCH"}
            )
            deterministic_close = (
                ("INVALIDATION_HIT" in events and "AMBIGUOUS_BARRIERS" not in events)
                or reverse_structure
            )
            if not deterministic_close:
                review.update({
                    "action": "HOLD", "protect_level": None,
                    "reason": "CLOSE rejected: no deterministic thesis invalidation or confirmed reversal",
                    "next_trigger": "initial SL or confirmed opposite BOS/CHoCH",
                })
        valid, next_state = validate_transition(current_state, review.get("action"))
        if not valid:
            review = {
                "action": "HOLD", "confidence": 0.0,
                "reason": f"Rejected unreachable transition {current_state}->{review.get('action')}",
                "protect_level": None, "management_target": None,
                "next_trigger": "next valid state event",
            }
            next_state = current_state
        review["next_state"] = next_state
        review["groq_called"] = True
        return review
    except Exception as exc:
        return {
            "action": "HOLD", "confidence": 0.0,
            "reason": f"Groq unavailable: {type(exc).__name__}",
            "protect_level": None, "management_target": None,
            "next_trigger": "next material event", "groq_called": False,
        }


def register_active_trade(
    signal: dict[str, Any],
    thesis: dict[str, Any] | None = None,
    db_path: str = DB_PATH,
) -> None:
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
    if _MANAGER_STATE_FACTORY is not None:
        # State owns both immutable geometry and thesis in V3.  Calling the
        # APEX V2 freezer here would recreate a compatibility write.
        frozen_thesis = thesis or {}
        _manager_state_repository().register({
            "signal_id": signal_id,
            "symbol": str(signal.get("symbol") or "").upper(),
            "strategy": strategy,
            "direction": str(signal.get("direction") or "").upper(),
            "management_tf": MANAGEMENT_TF[strategy],
            "initial_entry": entry,
            "initial_sl": sl,
            "initial_tp1": tp1,
            "initial_tp2": float(signal.get("tp2") or tp1),
            "initial_tp3": float(signal.get("tp3") or signal.get("tp2") or tp1),
            "initial_rr": float(signal.get("rr") or 0),
            "manager_version": MANAGER_VERSION,
            "thesis": frozen_thesis,
        })
        return
    ensure_trade_manager_schema(db_path)
    # Compatibility-only path retained for pre-cutover import and tests.
    try:
        from core.apex_v2 import freeze_trade_thesis
        frozen_thesis = freeze_trade_thesis(signal_id, signal, thesis, db_path)
    except Exception:
        frozen_thesis = thesis or {}
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
            json.dumps(frozen_thesis, ensure_ascii=False, default=str)[:20000],
        ),
    )
    snapshot = {
        "signal_id": signal_id, "symbol": str(signal.get("symbol") or "").upper(),
        "strategy": strategy, "direction": str(signal.get("direction") or "").upper(),
        "entry": entry, "initial_sl": sl, "tp1": tp1,
        "tp2": float(signal.get("tp2") or tp1),
        "terminal_tp": float(signal.get("tp3") or signal.get("tp2") or tp1),
        "initial_quantity_fraction": 1.0,
    }
    encoded = json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    conn.execute(
        """INSERT OR IGNORE INTO trade_manager_entry_snapshots(signal_id,snapshot_json,snapshot_hash)
           VALUES(?,?,?)""",
        (signal_id, encoded, hashlib.sha256(encoded.encode("utf-8")).hexdigest()),
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
    if _MANAGER_STATE_FACTORY is None:
        ensure_trade_manager_schema(db_path)
    conn = _connect(db_path)
    confirmed_live_ids: set[int] | None = None
    try:
        if _MANAGER_STATE_FACTORY is not None:
            rows = conn.execute(
                """SELECT id,symbol,direction,entry,sl,tp1,tp2,tp3,
                          timeframe,grade,signal_type
                   FROM signals WHERE result='pending'"""
            ).fetchall()
            signal_ids = [int(row["id"]) for row in rows]
            lifecycles = SignalLifecycleRepository(_MANAGER_STATE_FACTORY).get_many(signal_ids)
            executions = ExecutionRepository(_MANAGER_STATE_FACTORY).get_many(signal_ids)
            rows = [
                {
                    **dict(row),
                    # Missing State lifecycle is not proof of an activated entry.
                    "lifecycle_status": str(
                        lifecycles.get(int(row["id"]), {}).get("status") or "waiting_entry"
                    ),
                }
                for row in rows
            ]
            confirmed_live_ids = {
                signal_id for signal_id, execution in executions.items()
                if str(execution.get("mode") or "") == "live"
                and str(execution.get("status") or "").upper() in _MANAGED_EXECUTION_STATUSES
                and float(execution.get("quantity") or 0) > 0
                and bool(
                    str(execution.get("stop_order_id") or "")
                    or str(execution.get("pending_stop_order_id") or "")
                )
            }
            non_open_live_ids = set(signal_ids) - confirmed_live_ids
        else:
            rows = conn.execute(
                """SELECT s.id,s.symbol,s.direction,s.entry,s.sl,s.tp1,s.tp2,s.tp3,
                          s.timeframe,s.grade,s.signal_type,
                          COALESCE(x.status,'active') AS lifecycle_status
                   FROM signals s
                   LEFT JOIN signal_execution_state x ON x.signal_id=s.id
                   WHERE s.result='pending'"""
            ).fetchall()
            has_executions = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trade_executions'"
            ).fetchone()
            non_open_live_ids = {
                int(item[0]) for item in conn.execute(
                    """SELECT signal_id FROM trade_executions
                         WHERE mode='live' AND COALESCE(quantity,0)<=0
                           AND COALESCE(entry_order_id,'')=''
                           AND (status LIKE 'SKIPPED_%' OR status LIKE 'BLOCKED_%'
                                OR status IN ('DISABLED','LIVE_NOT_ARMED'))"""
                ).fetchall()
            } if has_executions else set()
    except sqlite3.Error:
        rows, non_open_live_ids = [], set()
    conn.close()
    registered = 0
    for row in rows:
        data = dict(row)
        if int(data["id"]) in non_open_live_ids:
            continue
        if confirmed_live_ids is not None and int(data["id"]) not in confirmed_live_ids:
            continue
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
                if _MANAGER_STATE_FACTORY is not None:
                    _manager_state_repository().update_thesis(int(data["id"]), thesis)
                else:
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
    if _MANAGER_STATE_FACTORY is not None:
        row = _manager_state_repository().get(int(signal_id))
        return _manager_state_compatibility(row) if row else None
    ensure_trade_manager_schema(db_path)
    conn = _connect(db_path)
    row = conn.execute("SELECT * FROM trade_manager_state WHERE signal_id=?", (int(signal_id),)).fetchone()
    conn.close()
    return dict(row) if row else None


def load_active_states(db_path: str = DB_PATH) -> list[dict[str, Any]]:
    if _MANAGER_STATE_FACTORY is not None:
        return [
            _manager_state_compatibility(row)
            for row in _manager_state_repository().active(limit=5000)
        ]
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


def no_progress_event_due(state: dict[str, Any], current_r: float, facts: dict[str, Any]) -> bool:
    if not facts.get("new_progress_candle", facts.get("new_management_candle")):
        return False
    strategy = normalize_strategy(state.get("strategy"))
    anchor = float(state.get("progress_anchor_r") or 0)
    # NO_PROGRESS is measured on the closed working-TF candle's favorable
    # excursion (MFE), not on a transient ticker price.  Callers may provide
    # ``progress_mfe_r``; the fallback keeps legacy/manual tests deterministic.
    observed_mfe = facts.get("progress_mfe_r")
    try:
        observed_mfe = float(observed_mfe) if observed_mfe is not None else float(current_r)
    except (TypeError, ValueError):
        observed_mfe = float(current_r)
    projected = 0 if observed_mfe >= anchor + 0.25 else int(state.get("no_progress_bars") or 0) + 1
    return projected >= NO_PROGRESS_BARS[strategy]


def persist_review(
    state: dict[str, Any],
    price: float,
    events: list[str],
    facts: dict[str, Any],
    review: dict[str, Any],
    db_path: str = DB_PATH,
) -> None:
    current_r = r_multiple(
        state["direction"], float(state["initial_entry"]), float(state["initial_sl"]), price
    )
    bullish = str(state["direction"]).upper() == "BULLISH"
    best = float(state.get("best_price") or price)
    best = max(best, price) if bullish else min(best, price)
    ambiguous = "AMBIGUOUS_BARRIERS" in events
    # A closed OHLC candle cannot reveal whether SL or TP executed first.
    # Binance order/fill reconciliation owns that fact in live production.
    tp1_seen = int(state.get("tp1_seen") or 0) or int(not ambiguous and "TP1_HIT" in events)
    tp2_seen = int(state.get("tp2_seen") or 0) or int(not ambiguous and "TP2_HIT" in events)
    tp3_seen = int(state.get("tp3_seen") or 0) or int(not ambiguous and "TP3_HIT" in events)
    manager_target = review.get("management_target") or state.get("manager_target")
    protect_level = state.get("manager_protect_level")
    proposed_protect_level = review.get("protect_level")
    candle_id = facts.get("management_candle_id") or state.get("last_reviewed_candle")
    current_state = str(state.get("manager_state") or "PROTECTED").upper()
    if not ambiguous and "TP1_HIT" in events and current_state in {"PROTECTED", "MANAGING"}:
        current_state = "TP1_REACHED"
    # An action is recorded here, but an exchange-affecting transition is not
    # committed until bot.py confirms execution. Event-driven TP1 is the only
    # immediate state change because it comes from the immutable Gate candle.
    next_state = current_state
    candle_is_new = bool(facts.get("new_progress_candle", facts.get("new_management_candle")))
    anchor = float(state.get("progress_anchor_r") or 0)
    no_progress = int(state.get("no_progress_bars") or 0)
    if candle_is_new:
        progress_mfe = facts.get("progress_mfe_r")
        try:
            progress_mfe = float(progress_mfe) if progress_mfe is not None else current_r
        except (TypeError, ValueError):
            progress_mfe = current_r
        if progress_mfe >= anchor + 0.25:
            anchor, no_progress = progress_mfe, 0
        else:
            no_progress += 1
    manager_event_id = derived_id(
        "manager_event", "live", int(state["signal_id"]), str(candle_id),
        str(review.get("action") or "HOLD"), ",".join(events),
    )
    if _MANAGER_STATE_FACTORY is not None:
        _manager_state_repository().record_review(
            int(state["signal_id"]),
            {
                "last_price": price, "best_price": best, "current_r": current_r,
                "tp1_seen": tp1_seen, "tp2_seen": tp2_seen, "tp3_seen": tp3_seen,
                "manager_target": manager_target,
                "proposed_protect_level": proposed_protect_level,
                "last_event": ",".join(events), "last_action": review.get("action"),
                "last_confidence": review.get("confidence"),
                "last_reviewed_candle": str(candle_id) if candle_id is not None else None,
                "no_progress_bars": no_progress, "progress_anchor_r": anchor,
                "last_progress_candle": (
                    str(facts.get("progress_candle_id"))
                    if facts.get("progress_candle_id") is not None
                    else state.get("last_progress_candle")
                ),
            },
            {
                "manager_event_id": manager_event_id,
                "signal_id": int(state["signal_id"]),
                "event_type": ",".join(events), "action": review.get("action"),
                "confidence": review.get("confidence"), "price": price,
                "r_multiple": current_r, "manager_target": manager_target,
                "confirmed_protect_level": protect_level, "facts": facts,
                "reason_codes": ("STATE_MANAGER_REVIEW",),
                "summary": str(review.get("reason") or ""),
            },
        )
        return
    ensure_trade_manager_schema(db_path)
    conn = _connect(db_path)
    conn.execute(
        """UPDATE trade_manager_state
           SET last_price=?,best_price=?,current_r=?,tp1_seen=?,tp2_seen=?,tp3_seen=?,manager_target=?,
               manager_protect_level=?,proposed_protect_level=?,last_event=?,last_action=?,last_confidence=?,
               last_reviewed_candle=?,manager_state=?,no_progress_bars=?,progress_anchor_r=?,
               last_progress_candle=?,
               updated_at=CURRENT_TIMESTAMP WHERE signal_id=?""",
        (
            price, best, current_r, tp1_seen, tp2_seen, tp3_seen, manager_target, protect_level,
            proposed_protect_level,
            ",".join(events), review.get("action"), review.get("confidence"),
            str(candle_id) if candle_id is not None else None, next_state, no_progress, anchor,
            str(facts.get("progress_candle_id")) if facts.get("progress_candle_id") is not None else state.get("last_progress_candle"),
            int(state["signal_id"]),
        ),
    )
    conn.execute(
        """INSERT OR IGNORE INTO trade_manager_events
           (manager_event_id,signal_id,event_type,action,confidence,price,r_multiple,manager_target,
            manager_protect_level,facts_json,reason) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (
            manager_event_id, int(state["signal_id"]), ",".join(events), review.get("action"),
            review.get("confidence"), price, current_r, manager_target, protect_level,
            json.dumps(facts or {}, ensure_ascii=False, default=str)[:20000],
            str(review.get("reason") or "")[:1500],
        ),
    )
    conn.commit()
    conn.close()
    try:
        from core.apex_v2 import record_decision
        action_id = hashlib.sha256(
            f"manager-v2:{int(state['signal_id'])}:{candle_id}:{review.get('action')}:{','.join(events)}".encode()
        ).hexdigest()
        record_decision(
            action_id=action_id,
            signal_id=int(state["signal_id"]),
            strategy=str(state.get("strategy") or ""),
            symbol=str(state.get("symbol") or ""),
            decision_source="GROQ_MANAGER" if review.get("groq_called") else "MANAGER_FALLBACK",
            action=str(review.get("action") or "HOLD"),
            confidence=review.get("confidence"),
            manager_state=current_state,
            outcome="PROPOSED",
            context=facts,
            payload={"events": events, "reason": review.get("reason"), "next_state": review.get("next_state")},
            db_path=db_path,
        )
        try:
            from core.groq_calibration import record_prediction
            record_prediction(
                action_id, str(review.get("action") or "HOLD"), review.get("confidence"),
                signal_id=int(state["signal_id"]), strategy=str(state.get("strategy") or ""),
                symbol=str(state.get("symbol") or ""),
                prediction_target="ACTION_INCREMENTAL_R_VS_HOLD", context_version="manager-v2",
                payload={"events": events, "reason": review.get("reason"), "next_state": review.get("next_state")},
                db_path=db_path,
            )
        except Exception:
            pass
    except Exception:
        pass


def confirm_manager_action(
    signal_id: int, action: str, execution_status: str, db_path: str = DB_PATH, *,
    confirmed_protect_level: float | None = None,
    remaining_fraction: float | None = None,
) -> bool:
    """Commit exactly one state transition after its action is confirmed."""
    canonical = {"EXIT": "CLOSE", "WAIT_CONFIRMATION": "HOLD"}.get(
        str(action or "HOLD").upper(), str(action or "HOLD").upper()
    )
    repository = _manager_state_repository() if _MANAGER_STATE_FACTORY is not None else None
    if repository is not None:
        row = repository.get(int(signal_id))
        if not row:
            return False
        current = str(row.get("manager_state") or "PROTECTED").upper()
    else:
        conn = _connect(db_path)
        row = conn.execute(
            "SELECT manager_state FROM trade_manager_state WHERE signal_id=?", (int(signal_id),)
        ).fetchone()
        conn.close()
        if not row:
            return False
        current = str(row[0] or "PROTECTED").upper()
    valid, next_state = validate_transition(current, canonical)
    internal_confirmed = canonical in {"HOLD", "LET_RUN"}
    exchange_confirmed = str(execution_status or "").upper() == "EXECUTED"
    if not valid or not (internal_confirmed or exchange_confirmed):
        return False
    confirmed_level = confirmed_protect_level if canonical in {"PROTECT", "MOVE_STOP_TO_BREAKEVEN"} else None
    fraction = None
    if canonical == "PARTIAL_EXIT" and remaining_fraction is not None:
        fraction = max(0.0, min(1.0, float(remaining_fraction)))
    if repository is not None:
        state_confirmed = repository.confirm_action(
            int(signal_id), action=canonical, next_state=next_state,
            execution_status=("EXECUTED" if exchange_confirmed else "INTERNAL_CONFIRMED"),
            confirmed_stop=confirmed_level, remaining_fraction=fraction,
        )
        return state_confirmed
    conn = _connect(db_path)
    conn.execute(
        """UPDATE trade_manager_state SET manager_state=?,
                  partial_exit_done=CASE WHEN ?='PARTIAL_EXIT' THEN 1 ELSE partial_exit_done END,
                  position_fraction=CASE WHEN ?='PARTIAL_EXIT' AND ? IS NOT NULL THEN ? ELSE position_fraction END,
                  manager_protect_level=CASE WHEN ? IN ('PROTECT','MOVE_STOP_TO_BREAKEVEN') AND ? IS NOT NULL THEN ? ELSE manager_protect_level END,
                  proposed_protect_level=CASE WHEN ? IN ('PROTECT','MOVE_STOP_TO_BREAKEVEN') THEN NULL ELSE proposed_protect_level END,
                  updated_at=CURRENT_TIMESTAMP WHERE signal_id=?""",
        (next_state, canonical, canonical, fraction, fraction,
         canonical, confirmed_level, confirmed_level, canonical, int(signal_id)),
    )
    conn.commit()
    conn.close()
    return True


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
    action = review.get("action", "HOLD")
    icon = {
        "HOLD": "🟢", "LET_RUN": "🚀", "PROTECT": "🛡",
        "PARTIAL_EXIT": "🟡", "CLOSE": "🔴", "MOVE_STOP_TO_BREAKEVEN": "🛡",
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


def _record_data_availability(
    state: dict[str, Any], available: bool, error: str, db_path: str
) -> tuple[int, bool]:
    """Persist consecutive manager-TF failures and return (count, alert_now)."""
    if _MANAGER_STATE_FACTORY is not None:
        return _manager_state_repository().record_data_availability(
            int(state["signal_id"]), available=available, error=error,
        )
    conn = _connect(db_path)
    try:
        if available:
            conn.execute(
                """UPDATE trade_manager_state SET data_failure_count=0,
                          data_failure_notified=0,last_data_error=NULL,
                          manager_state=CASE WHEN manager_state='DEGRADED'
                            THEN COALESCE(pre_degraded_state,'PROTECTED') ELSE manager_state END,
                          pre_degraded_state=NULL,
                          updated_at=CURRENT_TIMESTAMP WHERE signal_id=?""",
                (int(state["signal_id"]),),
            )
            conn.commit()
            return 0, False
        row = conn.execute(
            "SELECT data_failure_count,data_failure_notified FROM trade_manager_state WHERE signal_id=?",
            (int(state["signal_id"]),),
        ).fetchone()
        count = int((row[0] if row else 0) or 0) + 1
        already_notified = bool(int((row[1] if row else 0) or 0))
        alert_now = count >= 3 and not already_notified
        conn.execute(
            """UPDATE trade_manager_state SET data_failure_count=?,
                      data_failure_notified=?,last_data_error=?,updated_at=CURRENT_TIMESTAMP
               WHERE signal_id=?""",
            (count, int(already_notified or alert_now), str(error or "")[:500], int(state["signal_id"])),
        )
        if count >= 3:
            conn.execute(
                """UPDATE trade_manager_state SET
                      pre_degraded_state=CASE WHEN manager_state!='DEGRADED' THEN manager_state ELSE pre_degraded_state END,
                      manager_state='DEGRADED',updated_at=CURRENT_TIMESTAMP WHERE signal_id=?""",
                (int(state["signal_id"]),),
            )
        conn.commit()
        return count, alert_now
    finally:
        conn.close()


def format_data_degraded_update(state: dict[str, Any], cycles: int, error: str = "") -> str:
    symbol = html.escape(str(state.get("symbol") or "—"))
    strategy = html.escape(str(state.get("strategy") or "—"))
    timeframe = html.escape(str(state.get("management_tf") or "—"))
    detail = html.escape(str(error or "Gate candles unavailable")[:240])
    return (
        f"⚠️ <b>{symbol} {strategy}</b>: {timeframe} data unavailable "
        f"{int(cycles)} cycles — management degraded\n"
        f"Источник: Gate · {detail}"
    )


def manager_cycle(
    get_prices: Callable[[], dict[str, Any]],
    get_candles: Callable[[str, str, int], list[Any]],
    ask_groq: Callable[..., Any],
    *,
    external_context: Callable[..., dict[str, Any]] | None = None,
    execution_context: Callable[[int], dict[str, Any]] | None = None,
    db_path: str = DB_PATH,
) -> list[dict[str, Any]]:
    """Run one non-blocking-by-design management pass over active analytics trades.

    Price polling is cheap; Groq is invoked only when detect_events reports a
    material event. Returned items are ready for Telegram delivery by bot.py.
    """
    if _MANAGER_STATE_FACTORY is None:
        ensure_trade_manager_schema(db_path)
    reconcile_manager_states_from_signals(db_path)
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
            candle_error = ""
        except Exception as exc:
            candles = []
            candle_error = f"{type(exc).__name__}: {exc}"
        facts = build_structure_facts(candles, state["direction"], state.get("last_reviewed_candle"))
        data_available = bool(facts.get("closed_candle"))
        failure_reason = candle_error or (
            f"expected >=16 candles, received {len(candles)}" if not data_available else ""
        )
        failure_count, alert_now = _record_data_availability(
            state, data_available, failure_reason, db_path
        )
        if not data_available:
            if alert_now:
                output.append({
                    "signal_id": state["signal_id"], "symbol": symbol,
                    "strategy": normalize_strategy(state.get("strategy")),
                    "manager_state": "DEGRADED",
                    "events": ["MARKET_DATA_DEGRADED"],
                    "review": {"action": "HOLD", "confidence": 0.0},
                    "notify": True, "degraded": True,
                    "telegram": format_data_degraded_update(state, failure_count, failure_reason),
                })
            continue
        strategy = normalize_strategy(state.get("strategy"))
        progress_tf = PROGRESS_TF[strategy]
        if progress_tf == str(state["management_tf"]):
            progress_facts = facts
        else:
            try:
                progress_candles = get_candles(symbol, progress_tf, 120) or []
                progress_facts = build_structure_facts(
                    progress_candles, state["direction"], state.get("last_progress_candle")
                )
            except Exception:
                progress_facts = {"new_management_candle": False}
        facts["progress_tf"] = progress_tf
        facts["new_progress_candle"] = bool(progress_facts.get("new_management_candle"))
        facts["progress_candle_id"] = progress_facts.get("management_candle_id")
        if facts["new_progress_candle"]:
            try:
                favorable = progress_facts.get("latest_closed_high") if state["direction"] == "BULLISH" else progress_facts.get("latest_closed_low")
                facts["progress_mfe_r"] = r_multiple(
                    state["direction"], float(state["initial_entry"]), float(state["initial_sl"]), float(favorable)
                ) if favorable is not None else None
            except (TypeError, ValueError):
                facts["progress_mfe_r"] = None
        facts["current_price"] = price
        facts["manager_state_before"] = str(state.get("manager_state") or "PROTECTED")
        facts["management_matrix"] = management_matrix(state.get("strategy"))
        facts["similar_scenarios"] = []
        if str(state.get("status") or "ACTIVE").upper() == "CLOSED":
            continue
        if execution_context is not None:
            try:
                facts["execution"] = execution_context(int(state["signal_id"])) or {}
            except Exception:
                facts["execution"] = {"status": "UNAVAILABLE"}
        if external_context is not None:
            try:
                try:
                    extra = external_context(
                        symbol, state.get("direction"), state.get("strategy")
                    ) or {}
                except TypeError:
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
        current_r = r_multiple(
            state["direction"], float(state["initial_entry"]), float(state["initial_sl"]), price
        )
        if no_progress_event_due(state, current_r, facts):
            events.append("NO_PROGRESS")
        if not events:
            continue
        decision_state = dict(state)
        if "TP1_HIT" in events and str(state.get("manager_state") or "PROTECTED") in {"PROTECTED", "MANAGING"}:
            decision_state["manager_state"] = "TP1_REACHED"
        # An otherwise uneventful candle is not a reason to ask Groq and
        # over-manage the position.
        material_events = [event for event in events if event != "MANAGEMENT_CANDLE_CLOSE"]
        review = review_active_trade(decision_state, material_events, facts, ask_groq)
        notify = should_notify(state, events, review)
        persist_review(state, price, events, facts, review, db_path)
        if str(review.get("action") or "HOLD").upper() in {"HOLD", "LET_RUN"}:
            confirm_manager_action(
                int(state["signal_id"]), str(review.get("action") or "HOLD"),
                "INTERNAL_CONFIRMED", db_path,
            )
        output.append({
            "signal_id": state["signal_id"],
            "symbol": symbol,
            "strategy": strategy,
            "manager_state": str(state.get("manager_state") or "PROTECTED"),
            "events": events,
            "review": review,
            "facts": facts,
            "notify": notify,
            "telegram": format_telegram_update(state, price, events, review),
        })
    return output
