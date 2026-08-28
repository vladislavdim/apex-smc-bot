"""Durable, factual market memory for APEX signal reviews.

This module records market structure around delivered signals and the observed
price path afterwards.  It never calculates or changes entry, stop-loss,
take-profit or risk/reward values.  Its compact summaries are evidence for the
Groq quality gate, not trading instructions.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any


DB_PATH = os.environ.get(
    "APEX_DB_PATH",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "brain.db"),
)
_MAX_CANDLES_PER_TIMEFRAME = 120
_MIN_PATH_INTERVAL_SECONDS = 300


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=20, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=20000")
    return conn


def init_market_memory() -> None:
    """Create isolated tables without altering existing strategy tables."""
    try:
        with _connect() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS market_memory_snapshots (
                signal_id INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL,
                strategy TEXT,
                direction TEXT,
                timeframe TEXT,
                entry REAL,
                sl REAL,
                tp1 REAL,
                tp2 REAL,
                tp3 REAL,
                confluence INTEGER DEFAULT 0,
                regime TEXT,
                snapshot_json TEXT NOT NULL,
                zones_json TEXT NOT NULL,
                outcome TEXT,
                outcome_label TEXT,
                max_favorable_pct REAL,
                max_adverse_pct REAL,
                captured_at TEXT DEFAULT CURRENT_TIMESTAMP,
                closed_at TEXT
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS market_memory_path (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER NOT NULL,
                observed_at TEXT NOT NULL,
                price REAL NOT NULL,
                UNIQUE(signal_id, observed_at)
            )""")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_memory_snapshot_lookup "
                "ON market_memory_snapshots(symbol, strategy, direction, timeframe, outcome)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_memory_path_signal "
                "ON market_memory_path(signal_id, observed_at)"
            )
    except Exception as exc:
        logging.warning("[MarketMemory] init failed: %s", exc)


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _compact_candles(candles: list[dict[str, Any]] | None) -> list[dict[str, float]]:
    compact: list[dict[str, float]] = []
    for candle in (candles or [])[-_MAX_CANDLES_PER_TIMEFRAME:]:
        row = {
            key: _number(candle.get(key))
            for key in ("open", "high", "low", "close", "volume")
        }
        if all(row[key] is not None for key in ("open", "high", "low", "close")):
            compact.append({key: value for key, value in row.items() if value is not None})
    return compact


def _pivot_zones(candles: list[dict[str, float]]) -> dict[str, Any]:
    """Extract repeatable price zones from local pivots; not trade levels."""
    if not candles:
        return {"supports": [], "resistances": [], "range_low": None, "range_high": None}

    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    supports: list[float] = []
    resistances: list[float] = []
    for index in range(2, len(candles) - 2):
        low = candles[index]["low"]
        high = candles[index]["high"]
        if low <= min(lows[index - 2:index + 3]):
            supports.append(low)
        if high >= max(highs[index - 2:index + 3]):
            resistances.append(high)

    def recent_unique(values: list[float], reverse: bool) -> list[float]:
        ordered = sorted(values, reverse=reverse)
        selected: list[float] = []
        for value in ordered:
            if not any(abs(value - item) / max(abs(item), 1e-12) < 0.003 for item in selected):
                selected.append(value)
            if len(selected) == 4:
                break
        return selected

    return {
        "supports": recent_unique(supports, reverse=True),
        "resistances": recent_unique(resistances, reverse=False),
        "range_low": min(lows),
        "range_high": max(highs),
        "last_close": candles[-1]["close"],
    }


def capture_snapshot(
    signal_id: int,
    symbol: str,
    strategy: str,
    direction: str,
    timeframe: str,
    entry: float,
    sl: float,
    tp1: float,
    tp2: float | None = None,
    tp3: float | None = None,
    confluence: int = 0,
    regime: str = "UNKNOWN",
    candles_by_timeframe: dict[str, list[dict[str, Any]]] | None = None,
) -> None:
    """Persist a delivered-signal snapshot. Duplicate captures are ignored."""
    if not signal_id:
        return
    try:
        compact = {
            str(tf): _compact_candles(candles)
            for tf, candles in (candles_by_timeframe or {}).items()
            if candles
        }
        primary = compact.get(timeframe) or next(iter(compact.values()), [])
        zones = _pivot_zones(primary)
        snapshot = {
            "version": 1,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "candles": compact,
            "primary_timeframe": timeframe,
        }
        with _connect() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO market_memory_snapshots
                   (signal_id,symbol,strategy,direction,timeframe,entry,sl,tp1,tp2,tp3,
                    confluence,regime,snapshot_json,zones_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    signal_id, symbol.upper(), strategy.upper(), direction.upper(), timeframe,
                    entry, sl, tp1, tp2, tp3, confluence or 0, regime or "UNKNOWN",
                    json.dumps(snapshot, separators=(",", ":"), ensure_ascii=False),
                    json.dumps(zones, separators=(",", ":"), ensure_ascii=False),
                ),
            )
    except Exception as exc:
        logging.warning("[MarketMemory] capture failed for %s: %s", symbol, exc)


def record_price(signal_id: int, price: float, now: datetime | None = None) -> None:
    """Store a throttled price path while the signal is active."""
    if not signal_id or _number(price) is None:
        return
    try:
        observed = now or datetime.now(timezone.utc)
        with _connect() as conn:
            previous = conn.execute(
                "SELECT observed_at FROM market_memory_path WHERE signal_id=? "
                "ORDER BY id DESC LIMIT 1", (signal_id,)
            ).fetchone()
            if previous:
                try:
                    last = datetime.fromisoformat(str(previous[0]).replace("Z", "+00:00"))
                    if (observed - last).total_seconds() < _MIN_PATH_INTERVAL_SECONDS:
                        return
                except (TypeError, ValueError):
                    pass
            conn.execute(
                "INSERT INTO market_memory_path(signal_id,observed_at,price) VALUES (?,?,?)",
                (signal_id, observed.isoformat(), float(price)),
            )
    except Exception as exc:
        logging.debug("[MarketMemory] path write failed for %s: %s", signal_id, exc)


def _outcome_label(result: str, favorable: float | None, adverse: float | None) -> str:
    result = (result or "").lower()
    if result in {"tp1", "tp2", "tp3"}:
        return "continuation"
    if result == "sl":
        if favorable is not None and adverse is not None and favorable > 0:
            return "reversal_after_initial_move"
        return "failed_setup_or_fakeout"
    if result in {"expired", "cancelled"}:
        return "no_follow_through"
    return "unknown"


def close_snapshot(signal_id: int, result: str, current_price: float | None = None) -> None:
    """Close a snapshot and derive objective MFE/MAE from the observed path."""
    if not signal_id:
        return
    try:
        if _number(current_price) is not None:
            record_price(signal_id, float(current_price))
        with _connect() as conn:
            row = conn.execute(
                "SELECT direction,entry FROM market_memory_snapshots WHERE signal_id=?",
                (signal_id,),
            ).fetchone()
            if not row:
                return
            direction, entry = row
            prices = [item[0] for item in conn.execute(
                "SELECT price FROM market_memory_path WHERE signal_id=? ORDER BY id",
                (signal_id,),
            ).fetchall()]
            if not prices or not entry:
                favorable = adverse = None
            elif str(direction).upper() == "BULLISH":
                favorable = (max(prices) - entry) / entry * 100
                adverse = (min(prices) - entry) / entry * 100
            else:
                favorable = (entry - min(prices)) / entry * 100
                adverse = (entry - max(prices)) / entry * 100
            conn.execute(
                """UPDATE market_memory_snapshots
                   SET outcome=?, outcome_label=?, max_favorable_pct=?, max_adverse_pct=?,
                       closed_at=CURRENT_TIMESTAMP
                   WHERE signal_id=?""",
                (result, _outcome_label(result, favorable, adverse), favorable, adverse, signal_id),
            )
    except Exception as exc:
        logging.warning("[MarketMemory] close failed for %s: %s", signal_id, exc)


def build_memory_context(
    symbol: str,
    strategy: str,
    direction: str,
    timeframe: str,
    limit: int = 12,
) -> dict[str, Any]:
    """Return compact historical evidence only from completed snapshots."""
    base = {
        "available": False,
        "symbol": symbol.upper(),
        "samples": 0,
        "wins": 0,
        "losses": 0,
        "win_rate": None,
        "labels": {},
        "important_supports": [],
        "important_resistances": [],
        "recent_cases": [],
    }
    try:
        with _connect() as conn:
            rows = conn.execute(
                """SELECT outcome,outcome_label,zones_json,max_favorable_pct,max_adverse_pct,captured_at
                   FROM market_memory_snapshots
                   WHERE symbol=? AND strategy=? AND direction=? AND timeframe=?
                     AND outcome IN ('tp1','tp2','tp3','sl','expired')
                   ORDER BY signal_id DESC LIMIT ?""",
                (symbol.upper(), strategy.upper(), direction.upper(), timeframe, limit),
            ).fetchall()
            if len(rows) < 3:
                rows = conn.execute(
                    """SELECT outcome,outcome_label,zones_json,max_favorable_pct,max_adverse_pct,captured_at
                       FROM market_memory_snapshots
                       WHERE symbol=? AND strategy=? AND direction=?
                         AND outcome IN ('tp1','tp2','tp3','sl','expired')
                       ORDER BY signal_id DESC LIMIT ?""",
                    (symbol.upper(), strategy.upper(), direction.upper(), limit),
                ).fetchall()
        if not rows:
            return base

        supports: list[float] = []
        resistances: list[float] = []
        labels: dict[str, int] = {}
        wins = 0
        cases = []
        for outcome, label, raw_zones, mfe, mae, captured in rows:
            if str(outcome).startswith("tp"):
                wins += 1
            label = label or "unknown"
            labels[label] = labels.get(label, 0) + 1
            try:
                zones = json.loads(raw_zones or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                zones = {}
            supports.extend(value for value in zones.get("supports", []) if _number(value) is not None)
            resistances.extend(value for value in zones.get("resistances", []) if _number(value) is not None)
            cases.append({
                "outcome": outcome, "label": label,
                "mfe_pct": round(float(mfe), 2) if mfe is not None else None,
                "mae_pct": round(float(mae), 2) if mae is not None else None,
                "captured_at": captured,
            })

        def levels(values: list[float]) -> list[float]:
            unique: list[float] = []
            for value in sorted(values):
                if not any(abs(value - prior) / max(abs(prior), 1e-12) < 0.005 for prior in unique):
                    unique.append(value)
                if len(unique) == 5:
                    break
            return unique

        total = len(rows)
        base.update({
            "available": True, "samples": total, "wins": wins, "losses": total - wins,
            "win_rate": round(wins / total * 100, 1),
            "labels": labels, "important_supports": levels(supports),
            "important_resistances": levels(resistances), "recent_cases": cases[:5],
        })
        return base
    except Exception as exc:
        logging.debug("[MarketMemory] context failed for %s: %s", symbol, exc)
        return base


def format_market_memory_context(memory: dict[str, Any]) -> str:
    """Format factual memory for Groq without exposing decision rights."""
    if not memory.get("available"):
        return (
            "MARKET MEMORY:\n"
            "No completed comparable snapshots yet. Do not invent historical behaviour."
        )
    labels = ", ".join(
        f"{label}: {count}" for label, count in sorted(memory.get("labels", {}).items())
    ) or "none"
    supports = ", ".join(str(round(value, 8)) for value in memory.get("important_supports", [])) or "none"
    resistances = ", ".join(str(round(value, 8)) for value in memory.get("important_resistances", [])) or "none"
    recent = "; ".join(
        f"{case['outcome']} ({case['label']}, MFE={case['mfe_pct']}%, MAE={case['mae_pct']}%)"
        for case in memory.get("recent_cases", [])
    ) or "none"
    return (
        "MARKET MEMORY (factual historical evidence):\n"
        f"- Comparable completed setups: {memory['samples']} | "
        f"wins: {memory['wins']} | losses: {memory['losses']} | "
        f"win rate: {memory['win_rate']}%\n"
        f"- Observed behaviours: {labels}\n"
        f"- Historical pivot supports: {supports}\n"
        f"- Historical pivot resistances: {resistances}\n"
        f"- Latest comparable outcomes: {recent}\n"
        "Memory is contextual evidence only. It must not create a trade, "
        "replace strategy levels, or override missing technical confirmation."
    )


init_market_memory()
