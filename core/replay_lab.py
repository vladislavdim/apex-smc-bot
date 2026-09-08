"""Deterministic, isolated replay tracks for Manager V2 learning.

The replay lab is downstream of the live manager.  It replays one immutable
stream of *closed Gate candles* through three independent states:

* ``ACTUAL`` — recorded Manager V2/Groq actions;
* ``NO_MANAGER`` — frozen Entry, initial SL, terminal TP and full quantity;
* ``PLAYBOOK_ONLY`` — frozen levels plus an optional book/playbook callback,
  without Groq.

No track shares mutable state and no replay function talks to an exchange.  An
intrabar candle touching both stop and target is resolved by an explicit policy
(conservative ``SL_FIRST`` by default), so results are reproducible rather than
silently optimistic.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from statistics import mean
from typing import Any, Callable, Iterable, Mapping


TRACKS = ("ACTUAL", "NO_MANAGER", "PLAYBOOK_ONLY")
TERMINAL_REASONS = {
    "SL", "TP", "CLOSE", "AMBIGUOUS_SL_FIRST", "AMBIGUOUS_TP_FIRST", "AMBIGUOUS",
}
DB_PATH = os.environ.get(
    "APEX_DB_PATH",
    os.environ.get(
        "APEX_BRAIN_DB_PATH",
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "brain.db"),
    ),
)


def _number(value: Any, default: float | None = None) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if result == result and abs(result) != float("inf") else default


def _direction(value: Any) -> str:
    raw = str(value or "").upper()
    if raw in {"LONG", "BUY", "BULL", "BULLISH"}:
        return "BULLISH"
    if raw in {"SHORT", "SELL", "BEAR", "BEARISH"}:
        return "BEARISH"
    raise ValueError("direction_must_be_bullish_or_bearish")


def _r(direction: str, entry: float, risk: float, price: float) -> float:
    if risk <= 0:
        return 0.0
    sign = 1.0 if direction == "BULLISH" else -1.0
    return sign * (price - entry) / risk


def _time(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        raw = float(value)
        return raw / 1000 if raw > 10_000_000_000 else raw
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(
            tzinfo=timezone.utc
        ).timestamp()
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class ReplayCandle:
    candle_id: str
    open: float
    high: float
    low: float
    close: float
    closed_at: str | None = None
    volume: float = 0.0

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any], index: int = 0) -> "ReplayCandle":
        close = _number(value.get("close"), 0.0) or 0.0
        return cls(
            candle_id=str(value.get("candle_id", value.get("id", value.get("timestamp", index)))),
            open=_number(value.get("open"), close) or close,
            high=_number(value.get("high"), close) or close,
            low=_number(value.get("low"), close) or close,
            close=close,
            closed_at=str(value.get("closed_at", value.get("timestamp"))) if value.get("closed_at", value.get("timestamp")) is not None else None,
            volume=max(0.0, _number(value.get("volume"), 0.0) or 0.0),
        )


@dataclass(frozen=True)
class FrozenEntry:
    signal_id: int
    symbol: str
    strategy: str
    direction: str
    entry: float
    initial_sl: float
    tp1: float
    tp2: float | None
    terminal_tp: float
    quantity: float = 1.0
    entry_at: str | None = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "FrozenEntry":
        entry = _number(value.get("entry", value.get("initial_entry")))
        sl = _number(value.get("initial_sl", value.get("sl")))
        tp1 = _number(value.get("tp1", value.get("initial_tp1")))
        tp2 = _number(value.get("tp2", value.get("initial_tp2")), tp1)
        terminal = _number(value.get("terminal_tp", value.get("tp3")), tp2)
        if not entry or not sl or not tp1 or terminal is None or entry == sl:
            raise ValueError("invalid_frozen_entry_levels")
        direction = _direction(value.get("direction"))
        return cls(
            signal_id=int(value.get("signal_id", 0)),
            symbol=str(value.get("symbol", "")).upper(),
            strategy=str(value.get("strategy", value.get("grade", "MTF"))).upper(),
            direction=direction, entry=entry, initial_sl=sl, tp1=tp1,
            tp2=tp2, terminal_tp=terminal,
            quantity=max(0.0, _number(value.get("quantity"), 1.0) or 1.0),
            entry_at=str(value.get("entry_at")) if value.get("entry_at") is not None else None,
        )


@dataclass
class _TrackState:
    track: str
    snapshot: FrozenEntry
    status: str = "OPEN"
    remaining_quantity: float = 1.0
    current_sl: float = 0.0
    gross_r: float = 0.0
    fee_r: float = 0.0
    slippage_r: float = 0.0
    mfe_r: float = 0.0
    mae_r: float = 0.0
    tp1_reached: bool = False
    tp2_reached: bool = False
    terminal_reached: bool = False
    targets_reached: list[str] = field(default_factory=list)
    events: list[dict[str, Any]] = field(default_factory=list)
    exit_reason: str | None = None
    exit_price: float | None = None
    exit_candle_id: str | None = None
    last_candle_id: str | None = None
    first_candle_at: str | None = None
    last_candle_at: str | None = None
    closed_index: int | None = None

    def __post_init__(self) -> None:
        self.remaining_quantity = self.snapshot.quantity
        self.current_sl = self.snapshot.initial_sl


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _event_action(value: Any) -> tuple[str, dict[str, Any]]:
    if isinstance(value, str):
        return value.upper(), {"action": value.upper()}
    if isinstance(value, Mapping):
        action = str(value.get("action", "HOLD")).upper().strip()
        return action, dict(value)
    return "HOLD", {"action": "HOLD"}


def _touch(direction: str, price: float, candle: ReplayCandle, favorable: bool) -> bool:
    if direction == "BULLISH":
        return candle.high >= price if favorable else candle.low <= price
    return candle.low <= price if favorable else candle.high >= price


def _add_target(state: _TrackState, name: str) -> None:
    if name not in state.targets_reached:
        state.targets_reached.append(name)
    if name == "TP1":
        state.tp1_reached = True
    elif name == "TP2":
        state.tp2_reached = True
    elif name == "TP":
        state.terminal_reached = True


def _mark_excursion(state: _TrackState, candle: ReplayCandle) -> None:
    snap = state.snapshot
    state.mfe_r = max(state.mfe_r, _r(snap.direction, snap.entry, abs(snap.entry - snap.initial_sl), candle.high if snap.direction == "BULLISH" else candle.low))
    state.mae_r = min(state.mae_r, _r(snap.direction, snap.entry, abs(snap.entry - snap.initial_sl), candle.low if snap.direction == "BULLISH" else candle.high))


def _record_fill(state: _TrackState, price: float, quantity: float, fee_r: float, slippage_bps: float) -> None:
    snap = state.snapshot
    risk = abs(snap.entry - snap.initial_sl)
    gross = _r(snap.direction, snap.entry, risk, price) * quantity / max(snap.quantity, 1e-12)
    slip = abs(price) * max(0.0, slippage_bps) / 10000 / max(risk, 1e-12) * quantity / max(snap.quantity, 1e-12)
    state.gross_r += gross
    state.fee_r += max(0.0, fee_r) * quantity / max(snap.quantity, 1e-12)
    state.slippage_r += slip
    state.remaining_quantity = max(0.0, state.remaining_quantity - quantity)


def _close(state: _TrackState, price: float, reason: str, candle: ReplayCandle, index: int, fee_r: float, slippage_bps: float) -> None:
    if state.status != "OPEN":
        return
    quantity = state.remaining_quantity
    _record_fill(state, price, quantity, fee_r, slippage_bps)
    state.status = "CLOSED"
    state.exit_reason = reason
    state.exit_price = price
    state.exit_candle_id = candle.candle_id
    state.closed_index = index
    state.events.append({"type": "CLOSE", "reason": reason, "price": price, "candle_id": candle.candle_id})


def _validate_action(state: _TrackState, action: str, payload: Mapping[str, Any]) -> tuple[bool, str]:
    if state.status != "OPEN":
        return False, "track_closed"
    if action not in {"HOLD", "MOVE_STOP_TO_BREAKEVEN", "PROTECT", "PARTIAL_EXIT", "LET_RUN", "CLOSE"}:
        return False, "unsupported_action"
    if action in {"MOVE_STOP_TO_BREAKEVEN", "PARTIAL_EXIT"} and not state.tp1_reached:
        return False, "tp1_not_reached"
    if action == "MOVE_STOP_TO_BREAKEVEN" and _r(
        state.snapshot.direction, state.snapshot.entry,
        abs(state.snapshot.entry - state.snapshot.initial_sl), state.current_sl,
    ) > 0:
        return False, "breakeven_would_loosen_protection"
    if action == "PARTIAL_EXIT":
        fraction = _number(payload.get("fraction", payload.get("quantity_fraction")), 0.5) or 0.0
        if not 0 < fraction <= 1:
            return False, "invalid_partial_fraction"
        if state.remaining_quantity <= 0:
            return False, "no_remaining_quantity"
    if action == "PROTECT":
        level = _number(payload.get("protect_level", payload.get("stop")))
        if level is None:
            return False, "missing_protect_level"
        risk = abs(state.snapshot.entry - state.snapshot.initial_sl)
        current_r = _r(state.snapshot.direction, state.snapshot.entry, risk, state.current_sl)
        new_r = _r(state.snapshot.direction, state.snapshot.entry, risk, level)
        if new_r < current_r:
            return False, "protect_would_increase_risk"
    return True, "ok"


def _apply_action(
    state: _TrackState, action_value: Any, candle: ReplayCandle, index: int,
    fee_r: float, slippage_bps: float,
) -> None:
    action, payload = _event_action(action_value)
    valid, reason = _validate_action(state, action, payload)
    if not valid:
        state.events.append({"type": "ACTION_REJECTED", "action": action, "reason": reason, "candle_id": candle.candle_id})
        return
    price = _number(payload.get("price"), candle.close) or candle.close
    state.events.append({"type": "ACTION", "action": action, "price": price, "candle_id": candle.candle_id})
    if action == "MOVE_STOP_TO_BREAKEVEN":
        state.current_sl = state.snapshot.entry
    elif action == "PROTECT":
        state.current_sl = _number(payload.get("protect_level", payload.get("stop")), state.current_sl) or state.current_sl
    elif action == "PARTIAL_EXIT":
        fraction = _number(payload.get("fraction", payload.get("quantity_fraction")), 0.5) or 0.0
        _record_fill(state, price, state.remaining_quantity * fraction, fee_r, slippage_bps)
    elif action == "CLOSE":
        _close(state, price, "MANAGER_CLOSE", candle, index, fee_r, slippage_bps)


def _terminal_touch(state: _TrackState, candle: ReplayCandle, index: int, policy: str, fee_r: float, slippage_bps: float) -> bool:
    direction = state.snapshot.direction
    hit_sl = _touch(direction, state.current_sl, candle, favorable=False)
    hit_tp = _touch(direction, state.snapshot.terminal_tp, candle, favorable=True)
    if not hit_sl and not hit_tp:
        return False
    if hit_sl and hit_tp:
        choice = str(policy or "SL_FIRST").upper()
        if choice == "TP_FIRST":
            _close(state, state.snapshot.terminal_tp, "AMBIGUOUS_TP_FIRST", candle, index, fee_r, slippage_bps)
        elif choice == "AMBIGUOUS":
            _close(state, candle.close, "AMBIGUOUS", candle, index, fee_r, slippage_bps)
        else:
            _close(state, state.current_sl, "AMBIGUOUS_SL_FIRST", candle, index, fee_r, slippage_bps)
        return True
    if hit_sl:
        _close(state, state.current_sl, "SL", candle, index, fee_r, slippage_bps)
    else:
        _add_target(state, "TP")
        _close(state, state.snapshot.terminal_tp, "TP", candle, index, fee_r, slippage_bps)
    return True


def _result(state: _TrackState, candles: list[ReplayCandle], fee_r: float, slippage_bps: float) -> dict[str, Any]:
    snap = state.snapshot
    net_r = state.gross_r - state.fee_r - state.slippage_r if state.status == "CLOSED" else None
    net_price_pct = None
    gross_pct = None
    if state.status == "CLOSED" and state.exit_price is not None:
        gross_pct = state.gross_r * abs(snap.entry - snap.initial_sl) / snap.entry * 100
        net_price_pct = gross_pct - ((state.fee_r + state.slippage_r) * abs(snap.entry - snap.initial_sl) / max(snap.entry, 1e-12) * 100)
    duration_bars = (state.closed_index + 1) if state.closed_index is not None else len(candles)
    duration_seconds = None
    start, end = _time(snap.entry_at), _time(state.last_candle_at)
    if start is not None and end is not None and end >= start:
        duration_seconds = round(end - start, 3)
    return {
        "track": state.track, "signal_id": snap.signal_id, "symbol": snap.symbol,
        "strategy": snap.strategy, "direction": snap.direction, "status": state.status,
        "quantity": snap.quantity, "remaining_quantity": state.remaining_quantity,
        "entry": snap.entry, "initial_sl": snap.initial_sl, "tp1": snap.tp1,
        "tp2": snap.tp2, "terminal_tp": snap.terminal_tp, "current_sl": state.current_sl,
        "tp1_reached": state.tp1_reached, "tp2_reached": state.tp2_reached,
        "exit_price": state.exit_price, "exit_reason": state.exit_reason,
        "gross_r": round(state.gross_r, 8) if state.status == "CLOSED" else None,
        "net_r": round(net_r, 8) if net_r is not None else None,
        "gross_pct": round(gross_pct, 8) if gross_pct is not None else None,
        "realized_pct": round(net_price_pct, 8) if net_price_pct is not None else None,
        "mfe_r": round(state.mfe_r, 8), "mae_r": round(state.mae_r, 8),
        "giveback_r": round(max(state.mfe_r - state.gross_r, 0.0), 8) if state.status == "CLOSED" else None,
        "fees_slippage_r": round(state.fee_r + state.slippage_r, 8),
        "fees_r": round(state.fee_r, 8), "slippage_r": round(state.slippage_r, 8),
        "duration_bars": duration_bars, "duration_seconds": duration_seconds,
        "targets_reached": list(state.targets_reached), "last_candle_id": state.last_candle_id,
        "exit_candle_id": state.exit_candle_id, "events": list(state.events),
        "ambiguous_policy": "SL_FIRST",
    }


def default_book_playbook(
    snapshot: FrozenEntry, state: Mapping[str, Any], candle: ReplayCandle,
) -> Any:
    """Conservative virtual interpretation of the selected book rules.

    The action is deliberately limited to the replay track.  Villahermosa /
    Holmes-derived features are evaluated only after 20 closed Gate candles;
    high relative effort without result after TP1 first suggests a 50% virtual
    reduction, and the following cycle may move the virtual stop to breakeven.
    No live strategy or Manager command calls this function.
    """
    history = state.get("_closed_candles") if isinstance(state, Mapping) else None
    if not isinstance(history, list):
        return None
    try:
        from core.manager_playbooks import shadow_features
        features = shadow_features(history, snapshot.direction)
    except Exception:
        return None
    if not features.get("eligible") or not state.get("tp1_reached"):
        return None
    if features.get("effort_without_result") and float(state.get("remaining_quantity") or 0) > 0:
        if not any(event.get("action") == "PARTIAL_EXIT" for event in state.get("events", []) if isinstance(event, Mapping)):
            return {"action": "PARTIAL_EXIT", "fraction": 0.5, "price": candle.close, "rule": "VILLAHERMOSA_EFFORT_RESULT"}
        if float(state.get("current_sl") or snapshot.initial_sl) != snapshot.entry:
            return {"action": "MOVE_STOP_TO_BREAKEVEN", "price": candle.close, "rule": "HOLMES_FOLLOW_THROUGH_PROTECT"}
    return None


def _run_track(
    track: str, snapshot: FrozenEntry, candles: list[ReplayCandle], actions: Mapping[str, Any] | None,
    playbook: Callable[[FrozenEntry, Mapping[str, Any], ReplayCandle], Any] | None,
    ambiguous_policy: str, fee_r: float, slippage_bps: float,
) -> dict[str, Any]:
    state = _TrackState(track=track, snapshot=replace(snapshot))
    state.first_candle_at = candles[0].closed_at if candles else None
    history: list[dict[str, Any]] = []
    for index, candle in enumerate(candles):
        if state.status != "OPEN":
            break
        state.last_candle_id, state.last_candle_at = candle.candle_id, candle.closed_at
        _mark_excursion(state, candle)
        if _touch(snapshot.direction, snapshot.tp1, candle, favorable=True):
            _add_target(state, "TP1")
        if snapshot.tp2 is not None and _touch(snapshot.direction, snapshot.tp2, candle, favorable=True):
            _add_target(state, "TP2")
        # Existing barriers were active during this candle. Close-boundary
        # decisions cannot retroactively change its execution.
        if _terminal_touch(state, candle, index, ambiguous_policy, fee_r, slippage_bps):
            break
        if track == "NO_MANAGER":
            continue
        # Actions are applied at the closed candle boundary; any new stop is
        # therefore effective from the next candle, while the immutable Gate
        # candle still records its MFE/MAE and TP1 reach.
        if track == "ACTUAL":
            action_value = (actions or {}).get(candle.candle_id)
        else:
            view = _result(state, candles[: index + 1], fee_r, slippage_bps)
            view["_closed_candles"] = list(history) + [asdict(candle)]
            action_value = playbook(snapshot, view, candle) if playbook else None
        if action_value is not None:
            _apply_action(state, action_value, candle, index, fee_r, slippage_bps)
        history.append(asdict(candle))
    result = _result(state, candles, fee_r, slippage_bps)
    result["ambiguous_policy"] = str(ambiguous_policy or "SL_FIRST").upper()
    return result


def replay_three_tracks(
    snapshot: FrozenEntry | Mapping[str, Any], candles: Iterable[ReplayCandle | Mapping[str, Any]],
    *, actual_actions: Mapping[str, Any] | Iterable[Mapping[str, Any]] | None = None,
    playbook: Callable[[FrozenEntry, Mapping[str, Any], ReplayCandle], Any] | None = None,
    ambiguous_policy: str = "SL_FIRST", fee_r: float = 0.02, slippage_bps: float = 0.0,
) -> dict[str, dict[str, Any]]:
    """Run isolated ACTUAL/NO_MANAGER/PLAYBOOK_ONLY tracks."""
    frozen = snapshot if isinstance(snapshot, FrozenEntry) else FrozenEntry.from_mapping(snapshot)
    stream = [c if isinstance(c, ReplayCandle) else ReplayCandle.from_mapping(c, i) for i, c in enumerate(candles)]
    action_map: dict[str, Any] = {}
    if isinstance(actual_actions, Mapping):
        action_map = {str(key): value for key, value in actual_actions.items()}
    elif actual_actions:
        for item in actual_actions:
            if isinstance(item, Mapping) and item.get("candle_id") is not None:
                action_map[str(item["candle_id"])] = item
    policy = str(ambiguous_policy or "SL_FIRST").upper()
    if policy not in {"SL_FIRST", "TP_FIRST", "AMBIGUOUS"}:
        raise ValueError("invalid_ambiguous_policy")
    return {
        "ACTUAL": _run_track("ACTUAL", frozen, stream, action_map, None, policy, fee_r, slippage_bps),
        "NO_MANAGER": _run_track("NO_MANAGER", frozen, stream, None, None, policy, fee_r, slippage_bps),
        "PLAYBOOK_ONLY": _run_track("PLAYBOOK_ONLY", frozen, stream, None, playbook or default_book_playbook, policy, fee_r, slippage_bps),
    }


def _connect(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=20, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def ensure_replay_schema(db_path: str = DB_PATH) -> None:
    conn = _connect(db_path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS apex_v2_replay_runs (
            run_hash TEXT PRIMARY KEY, signal_id INTEGER NOT NULL, stream_hash TEXT NOT NULL,
            snapshot_json TEXT NOT NULL, config_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_apex_v2_replay_runs_signal
          ON apex_v2_replay_runs(signal_id, created_at DESC);
        CREATE TABLE IF NOT EXISTS apex_v2_replay_track_results (
            run_hash TEXT NOT NULL, signal_id INTEGER NOT NULL, track TEXT NOT NULL,
            status TEXT NOT NULL, net_r REAL, gross_r REAL, realized_pct REAL,
            mfe_r REAL, mae_r REAL, giveback_r REAL, duration_bars INTEGER,
            exit_reason TEXT, targets_reached_json TEXT NOT NULL DEFAULT '[]',
            result_json TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(run_hash, track)
        );
        CREATE INDEX IF NOT EXISTS idx_apex_v2_replay_results_signal
          ON apex_v2_replay_track_results(signal_id, updated_at DESC);
        CREATE TABLE IF NOT EXISTS apex_v2_replay_candles (
            signal_id INTEGER NOT NULL, candle_id TEXT NOT NULL, open REAL NOT NULL,
            high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
            closed_at TEXT, volume REAL NOT NULL DEFAULT 0,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(signal_id,candle_id)
        );
        CREATE TABLE IF NOT EXISTS apex_v2_replay_actions (
            signal_id INTEGER NOT NULL, candle_id TEXT NOT NULL, action_hash TEXT NOT NULL,
            action TEXT NOT NULL, payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(signal_id,candle_id,action_hash)
        );
        """
    )
    try:
        conn.execute("ALTER TABLE apex_v2_replay_candles ADD COLUMN volume REAL NOT NULL DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    conn.close()


def persist_replay_candle(signal_id: int, candle: ReplayCandle | Mapping[str, Any], db_path: str = DB_PATH) -> bool:
    """Store one close-confirmed Gate candle exactly once for later replay."""
    ensure_replay_schema(db_path)
    item = candle if isinstance(candle, ReplayCandle) else ReplayCandle.from_mapping(candle)
    conn = _connect(db_path)
    changed = conn.execute(
        """INSERT OR IGNORE INTO apex_v2_replay_candles
           (signal_id,candle_id,open,high,low,close,closed_at,volume,payload_json) VALUES(?,?,?,?,?,?,?,?,?)""",
        (int(signal_id), item.candle_id, item.open, item.high, item.low, item.close,
         item.closed_at, item.volume, _canonical(asdict(item))),
    ).rowcount
    conn.commit(); conn.close()
    return bool(changed)


def persist_replay_action(signal_id: int, candle_id: str, action: Any, db_path: str = DB_PATH) -> bool:
    """Store one immutable Manager action event; duplicate cycles are ignored."""
    ensure_replay_schema(db_path)
    name, payload = _event_action(action)
    encoded = _canonical(payload)
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    conn = _connect(db_path)
    conn.execute("BEGIN IMMEDIATE")
    # One decision slot per signal/candle makes a repeated scheduler tick
    # idempotent even if a later retry would carry a different malformed
    # payload.  The authoritative Manager V2 event log retains the complete
    # audit trail separately.
    if conn.execute(
        "SELECT 1 FROM apex_v2_replay_actions WHERE signal_id=? AND candle_id=? LIMIT 1",
        (int(signal_id), str(candle_id)),
    ).fetchone():
        conn.close()
        return False
    changed = conn.execute(
        """INSERT OR IGNORE INTO apex_v2_replay_actions
           (signal_id,candle_id,action_hash,action,payload_json) VALUES(?,?,?,?,?)""",
        (int(signal_id), str(candle_id), digest, name, encoded),
    ).rowcount
    conn.commit(); conn.close()
    return bool(changed)


def load_replay_inputs(signal_id: int, db_path: str = DB_PATH) -> dict[str, Any]:
    """Load a frozen Gate stream and Manager actions for an offline replay job."""
    ensure_replay_schema(db_path)
    conn = _connect(db_path)
    candles = [dict(row) for row in conn.execute(
        "SELECT candle_id,open,high,low,close,closed_at,volume FROM apex_v2_replay_candles WHERE signal_id=? ORDER BY rowid",
        (int(signal_id),)
    ).fetchall()]
    actions = []
    for row in conn.execute(
        "SELECT candle_id,payload_json FROM apex_v2_replay_actions WHERE signal_id=? ORDER BY rowid",
        (int(signal_id),)
    ).fetchall():
        try:
            action = json.loads(row[1] or "{}")
        except (TypeError, json.JSONDecodeError):
            action = {"action": "HOLD"}
        action["candle_id"] = row[0]
        actions.append(action)
    conn.close()
    return {"signal_id": int(signal_id), "candles": candles, "actions": actions}


def persist_replay_bundle(
    snapshot: FrozenEntry | Mapping[str, Any], candles: Iterable[ReplayCandle | Mapping[str, Any]],
    results: Mapping[str, Mapping[str, Any]], *, config: Mapping[str, Any] | None = None,
    db_path: str = DB_PATH,
) -> str:
    """Persist a replay exactly once per immutable snapshot/stream/config hash."""
    frozen = snapshot if isinstance(snapshot, FrozenEntry) else FrozenEntry.from_mapping(snapshot)
    candle_rows = [asdict(c) if isinstance(c, ReplayCandle) else dict(c) for c in candles]
    payload = {"snapshot": asdict(frozen), "candles": candle_rows, "config": dict(config or {}), "results": dict(results), "engine_version": 2}
    encoded = _canonical(payload)
    run_hash = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    stream_hash = hashlib.sha256(_canonical(candle_rows).encode("utf-8")).hexdigest()
    ensure_replay_schema(db_path)
    conn = _connect(db_path)
    conn.execute(
        "INSERT OR IGNORE INTO apex_v2_replay_runs(run_hash,signal_id,stream_hash,snapshot_json,config_json) VALUES(?,?,?,?,?)",
        (run_hash, frozen.signal_id, stream_hash, _canonical(asdict(frozen)), _canonical(config or {})),
    )
    for track in TRACKS:
        row = dict(results.get(track) or {"track": track, "status": "MISSING"})
        conn.execute(
            """INSERT OR IGNORE INTO apex_v2_replay_track_results
               (run_hash,signal_id,track,status,net_r,gross_r,realized_pct,mfe_r,mae_r,giveback_r,
                duration_bars,exit_reason,targets_reached_json,result_json)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (run_hash, frozen.signal_id, track, str(row.get("status") or "UNKNOWN"),
             _number(row.get("net_r")), _number(row.get("gross_r")), _number(row.get("realized_pct")),
             _number(row.get("mfe_r"), 0.0), _number(row.get("mae_r"), 0.0), _number(row.get("giveback_r")),
             int(row.get("duration_bars") or 0), row.get("exit_reason"),
             _canonical(row.get("targets_reached") or []), _canonical(row)),
        )
    conn.commit()
    conn.close()
    return run_hash


def replay_dashboard_summary(db_path: str = DB_PATH, limit: int = 100) -> list[dict[str, Any]]:
    """Return latest complete replay bundles with the three edge measures."""
    ensure_replay_schema(db_path)
    conn = _connect(db_path)
    rows = conn.execute(
        """SELECT r.run_hash,r.signal_id,r.created_at,t.track,t.result_json,r.snapshot_json
           FROM apex_v2_replay_runs r JOIN apex_v2_replay_track_results t ON t.run_hash=r.run_hash
           WHERE r.rowid=(SELECT MAX(r2.rowid) FROM apex_v2_replay_runs r2 WHERE r2.signal_id=r.signal_id)
           ORDER BY r.created_at DESC LIMIT ?""", (max(1, int(limit)) * 3,)
    ).fetchall()
    conn.close()
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = grouped.setdefault(row[0], {"run_hash": row[0], "signal_id": row[1], "created_at": row[2], "tracks": {}})
        item["_snapshot"] = row[5]
        try:
            item["tracks"][row[3]] = json.loads(row[4] or "{}")
        except (TypeError, json.JSONDecodeError):
            item["tracks"][row[3]] = {}
    result = []
    for item in list(grouped.values())[: max(1, int(limit))]:
        tracks = item["tracks"]
        from core.execution_ledger import actual_result
        tracks["ACTUAL"] = actual_result(db_path, FrozenEntry.from_mapping(json.loads(item.pop("_snapshot"))))
        actual = _number((tracks.get("ACTUAL") or {}).get("net_r"))
        no_manager = _number((tracks.get("NO_MANAGER") or {}).get("net_r"))
        playbook = _number((tracks.get("PLAYBOOK_ONLY") or {}).get("net_r"))
        item.update({
            "groq_edge_r": round(actual - no_manager, 8) if actual is not None and no_manager is not None else None,
            "groq_vs_rules_r": round(actual - playbook, 8) if actual is not None and playbook is not None else None,
            "playbook_edge_r": round(playbook - no_manager, 8) if playbook is not None and no_manager is not None else None,
        })
        result.append(item)
    return result


def replay_persisted_trade(
    snapshot: FrozenEntry | Mapping[str, Any], *, db_path: str = DB_PATH,
    playbook: Callable[[FrozenEntry, Mapping[str, Any], ReplayCandle], Any] | None = None,
    ambiguous_policy: str = "SL_FIRST", fee_r: float = 0.02, slippage_bps: float = 0.0,
) -> str | None:
    """Replay the durable Gate stream captured by Manager V2 and persist it."""
    frozen = snapshot if isinstance(snapshot, FrozenEntry) else FrozenEntry.from_mapping(snapshot)
    inputs = load_replay_inputs(frozen.signal_id, db_path)
    if not inputs["candles"]:
        return None
    results = replay_three_tracks(
        frozen, inputs["candles"], actual_actions=inputs["actions"], playbook=playbook,
        ambiguous_policy=ambiguous_policy, fee_r=fee_r, slippage_bps=slippage_bps,
    )
    from core.execution_ledger import actual_result
    results["ACTUAL"] = actual_result(db_path, frozen)
    return persist_replay_bundle(
        frozen, inputs["candles"], results,
        config={"ambiguous_policy": ambiguous_policy, "fee_r": fee_r, "slippage_bps": slippage_bps},
        db_path=db_path,
    )


__all__ = [
    "FrozenEntry", "ReplayCandle", "TRACKS", "ensure_replay_schema", "load_replay_inputs",
    "persist_replay_action", "persist_replay_bundle", "persist_replay_candle",
    "replay_dashboard_summary", "replay_persisted_trade", "replay_three_tracks",
]
