"""Confirmed fill accounting. No order submission or market-data requests.

Only registered execution orders may contribute fills. Missing pages, unknown
fee assets or incomplete quantities never produce a closed net-R estimate.
"""
from contextlib import closing
from decimal import Decimal, InvalidOperation
import json
import sqlite3
import time
from datetime import datetime, timezone


def number(value):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError("invalid_execution_number")
    if not result.is_finite():
        raise ValueError("invalid_execution_number")
    return result


def connect(path):
    conn = sqlite3.connect(path, timeout=20)
    conn.row_factory = sqlite3.Row
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS confirmed_execution_orders (
            signal_id INTEGER NOT NULL, symbol TEXT NOT NULL, kind TEXT NOT NULL,
            remote_id TEXT NOT NULL, is_algo INTEGER NOT NULL, expected_side TEXT NOT NULL,
            standard_id TEXT, complete INTEGER NOT NULL DEFAULT 0,
            checked_at REAL NOT NULL DEFAULT 0, error TEXT,
            PRIMARY KEY(signal_id,kind,remote_id));
        CREATE TABLE IF NOT EXISTS confirmed_execution_fills (
            symbol TEXT NOT NULL, trade_id TEXT NOT NULL, signal_id INTEGER NOT NULL,
            order_id TEXT NOT NULL, kind TEXT NOT NULL, qty TEXT NOT NULL,
            price TEXT NOT NULL, commission TEXT NOT NULL, commission_asset TEXT NOT NULL,
            time_ms INTEGER NOT NULL, payload TEXT NOT NULL,
            PRIMARY KEY(symbol,trade_id));
        CREATE TABLE IF NOT EXISTS confirmed_execution_poll (
            id INTEGER PRIMARY KEY CHECK(id=1), attempted_at REAL NOT NULL);
    ''')
    return conn


def register_order(path, signal_id, symbol, kind, remote_id, side, is_algo=False):
    if not remote_id or kind not in {"ENTRY", "SL", "TP1", "TP2", "PARTIAL_EXIT", "CLOSE"}:
        return
    with closing(connect(path)) as conn, conn:
        conn.execute('''INSERT OR IGNORE INTO confirmed_execution_orders
            (signal_id,symbol,kind,remote_id,is_algo,expected_side,standard_id)
            VALUES(?,?,?,?,?,?,?)''', (signal_id, symbol, kind, str(remote_id), int(is_algo), side,
                                      None if is_algo else str(remote_id)))


def save_fills(path, order, fills):
    validated = []
    for fill in fills:
        if (str(fill.get("orderId")) != str(order["standard_id"]) or
                fill.get("symbol") != order["symbol"] or fill.get("side") != order["expected_side"]):
            raise ValueError("execution_fill_identity_mismatch")
        qty, price, fee = number(fill["qty"]), number(fill["price"]), number(fill["commission"])
        if qty <= 0 or price <= 0 or fill.get("id") is None:
            raise ValueError("invalid_fill")
        validated.append((fill, qty, price, fee))
    with closing(connect(path)) as conn, conn:
        for fill, qty, price, fee in validated:
            existing = conn.execute("SELECT signal_id FROM confirmed_execution_fills WHERE symbol=? AND trade_id=?",
                                    (order["symbol"], str(fill["id"]))).fetchone()
            if existing and existing[0] != order["signal_id"]:
                raise ValueError("fill_owned_by_another_trade")
            conn.execute('''INSERT OR IGNORE INTO confirmed_execution_fills VALUES(?,?,?,?,?,?,?,?,?,?,?)''',
                         (order["symbol"], str(fill["id"]), order["signal_id"], str(order["standard_id"]),
                          order["kind"], str(qty), str(price), str(fee), str(fill["commissionAsset"]),
                          int(fill["time"]), json.dumps(fill, sort_keys=True)))


def reconcile_one(path, client, now=None):
    """At most one read request per call and per 60 seconds, across restarts."""
    now = time.time() if now is None else now
    with closing(connect(path)) as conn, conn:
        conn.execute("BEGIN IMMEDIATE")
        previous = conn.execute("SELECT attempted_at FROM confirmed_execution_poll WHERE id=1").fetchone()
        if previous and now - previous[0] < 60:
            return "DEFERRED"
        row = conn.execute("SELECT * FROM confirmed_execution_orders WHERE complete=0 AND checked_at<=? ORDER BY checked_at,signal_id LIMIT 1", (now-300,)).fetchone()
        if row is None:
            return "IDLE"
        conn.execute("INSERT OR REPLACE INTO confirmed_execution_poll VALUES(1,?)", (now,))
        conn.execute("UPDATE confirmed_execution_orders SET checked_at=? WHERE signal_id=? AND kind=? AND remote_id=?",
                     (now, row["signal_id"], row["kind"], row["remote_id"]))
    key = (row["signal_id"], row["kind"], row["remote_id"])
    try:
        client = client() if callable(client) else client
        if not row["standard_id"]:
            response = client.query_algo_order(algo_id=row["remote_id"])
            standard_id = response.get("actualOrderId")
            with closing(connect(path)) as conn, conn:
                if standard_id and str(standard_id) != "0":
                    conn.execute("UPDATE confirmed_execution_orders SET standard_id=? WHERE signal_id=? AND kind=? AND remote_id=?", (str(standard_id), *key))
                elif str(response.get("algoStatus")) in {"CANCELED", "REJECTED", "EXPIRED"}:
                    conn.execute("UPDATE confirmed_execution_orders SET complete=1 WHERE signal_id=? AND kind=? AND remote_id=?", key)
            return "ORDER_RESOLVED"
        fills = client.account_order_trades(row["symbol"], row["standard_id"])
        if not isinstance(fills, list) or len(fills) >= 1000:
            raise ValueError("incomplete_fill_page")
        save_fills(path, row, fills)
        with closing(connect(path)) as conn:
            table = conn.execute("SELECT 1 FROM sqlite_master WHERE name='trade_executions'").fetchone()
            execution = conn.execute("SELECT * FROM trade_executions WHERE signal_id=?", (row["signal_id"],)).fetchone() if table else None
        if execution:
            from core.replay_lab import FrozenEntry
            actual_result(path, FrozenEntry.from_mapping(dict(execution)))
        # Keep polling incomplete/partially filled orders: a nonempty page does
        # not prove that the order is terminal. Global admission bounds cost.
        return "FILLS_SAVED"
    except Exception as exc:
        with closing(connect(path)) as conn, conn:
            conn.execute("UPDATE confirmed_execution_orders SET error=? WHERE signal_id=? AND kind=? AND remote_id=?", (type(exc).__name__, *key))
        return "UNAVAILABLE"


def register_execution_orders(path):
    """Discover only order IDs owned by this bot; no exchange calls."""
    with closing(connect(path)) as conn:
        # A live execution can still be ``pending`` in the signal table while
        # its entry/protective orders are already on Binance.  Excluding those rows
        # loses the very fills needed to close ACTUAL.  Ownership is established
        # by the bot's execution row and order IDs, not by a scanner result.
        rows = conn.execute("""SELECT e.* FROM trade_executions e
            WHERE e.mode='live' AND e.entry_order_id IS NOT NULL""").fetchall()
        actions_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='manager_execution_actions'"
        ).fetchone()
        actions = (conn.execute("SELECT * FROM manager_execution_actions WHERE exchange_order_id IS NOT NULL").fetchall()
                   if actions_table else [])
    by_signal = {row["signal_id"]: row for row in rows}
    for row in rows:
        side = "BUY" if row["direction"] == "BULLISH" else "SELL"
        register_order(path, row["signal_id"], row["symbol"], "ENTRY", row["entry_order_id"], side)
        for field, kind in (("stop_order_id", "SL"), ("tp1_order_id", "TP1"), ("tp2_order_id", "TP2")):
            register_order(path, row["signal_id"], row["symbol"], kind, row[field], "SELL" if side == "BUY" else "BUY", True)
    for action in actions:
        row = by_signal.get(action["signal_id"])
        if row:
            kind = action["action"] if action["action"] in {"CLOSE", "PARTIAL_EXIT"} else "SL"
            register_order(path, row["signal_id"], row["symbol"], kind, action["exchange_order_id"],
                           "SELL" if row["direction"] == "BULLISH" else "BUY", kind == "SL")


def _candle_time_ms(candle):
    """Read a Gate candle event time without trusting insertion order."""
    value = candle.get("closed_at", candle.get("close_time", candle.get("timestamp")))
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        raw = float(value)
        return int(raw if raw > 10_000_000_000 else raw * 1000)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.astimezone(timezone.utc).timestamp() * 1000)
    except (TypeError, ValueError, OverflowError):
        return None


def _gate_excursion(snapshot, candles, entry_ms, exit_ms):
    """Return MFE/MAE/target touches only when an immutable Gate stream exists.

    This is descriptive context around confirmed fills, never evidence that an
    ACTUAL order filled merely because a candle touched a level.
    """
    if not isinstance(candles, (list, tuple)):
        return {"mfe_r": None, "mae_r": None, "gate_targets_touched": None,
                "excursion_status": "UNAVAILABLE_NO_GATE_STREAM"}
    risk = abs(float(snapshot.entry) - float(snapshot.initial_sl))
    if risk <= 0 or entry_ms is None or exit_ms is None:
        return {"mfe_r": None, "mae_r": None, "gate_targets_touched": None,
                "excursion_status": "UNAVAILABLE_INVALID_WINDOW"}
    selected = []
    for candle in candles:
        if not isinstance(candle, dict):
            continue
        ts = _candle_time_ms(candle)
        if ts is None or ts < entry_ms or ts > exit_ms:
            continue
        try:
            high = float(candle["high"]); low = float(candle["low"])
        except (KeyError, TypeError, ValueError):
            continue
        selected.append((ts, high, low))
    if not selected:
        return {"mfe_r": None, "mae_r": None, "gate_targets_touched": None,
                "excursion_status": "UNAVAILABLE_NO_CLOSED_GATE_CANDLES"}
    sign = 1.0 if snapshot.direction == "BULLISH" else -1.0
    mfe = 0.0; mae = 0.0
    target_names = []
    for _ts, high, low in sorted(selected):
        favorable = sign * ((high if sign > 0 else low) - float(snapshot.entry)) / risk
        adverse = sign * ((low if sign > 0 else high) - float(snapshot.entry)) / risk
        mfe = max(mfe, favorable, 0.0)
        mae = max(mae, -adverse, 0.0)
        for name, level in (("TP1", snapshot.tp1), ("TP2", snapshot.tp2), ("TP", snapshot.terminal_tp)):
            if level is None or name in target_names:
                continue
            reached = high >= float(level) if sign > 0 else low <= float(level)
            if reached:
                target_names.append(name)
    return {"mfe_r": round(mfe, 8), "mae_r": round(mae, 8),
            "gate_targets_touched": target_names, "excursion_status": "GATE_CLOSED_STREAM"}


def actual_result(path, snapshot, candles=None):
    signal_id = snapshot.signal_id
    result = {
        "track": "ACTUAL", "status": "UNVERIFIED_EXECUTION", "net_r": None,
        "gross_r": None, "entry_state": "UNFILLED", "entry_time": None,
        "exit_time": None, "mfe_r": None, "mae_r": None,
        "targets_reached": None, "gate_targets_touched": None,
        "excursion_status": "UNAVAILABLE",
        "funding_r": None, "slippage_r": None,
        "accounting_basis": "CONFIRMED_BOT_OWNED_FILLS_EXCLUDING_FUNDING",
    }
    with closing(connect(path)) as conn:
        fills = conn.execute("SELECT * FROM confirmed_execution_fills WHERE signal_id=? ORDER BY time_ms,trade_id", (signal_id,)).fetchall()
        table = conn.execute("SELECT 1 FROM sqlite_master WHERE name='trade_executions'").fetchone()
        execution = conn.execute("SELECT * FROM trade_executions WHERE signal_id=? AND mode='live'", (signal_id,)).fetchone() if table else None
    entries = [f for f in fills if f["kind"] == "ENTRY"]
    exits = [f for f in fills if f["kind"] != "ENTRY"]
    entry_qty = sum((number(f["qty"]) for f in entries), Decimal(0))
    exit_qty = sum((number(f["qty"]) for f in exits), Decimal(0))
    # Match the immutable original full quantity. Never infer an unobserved
    # entry or final fill from a price touching a Gate barrier.
    expected_quantity = number(execution["quantity"]) if execution else number(snapshot.quantity)
    result.update({
        "quantity": float(entry_qty) if entry_qty else float(expected_quantity),
        "remaining_quantity": float(max(expected_quantity - exit_qty, Decimal(0))),
        "entry_state": "FILLED" if entries else "UNFILLED",
        "fee_assets": sorted({str(f["commission_asset"]) for f in fills}),
    })
    if entries:
        result["entry_time"] = int(entries[0]["time_ms"])
    if exits:
        result["exit_time"] = int(exits[-1]["time_ms"])
    if not entries or not exits or entry_qty != expected_quantity or exit_qty != entry_qty:
        return result
    entry_value = sum(number(f["qty"])*number(f["price"]) for f in entries)
    exit_value = sum(number(f["qty"])*number(f["price"]) for f in exits)
    sign = Decimal(1) if snapshot.direction == "BULLISH" else Decimal(-1)
    gross = sign * (exit_value-entry_value)
    risk = abs(number(snapshot.entry)-number(snapshot.initial_sl))*entry_qty
    fees_known = all(str(f["commission_asset"]).upper() == "USDT" for f in fills)
    fee = sum(number(f["commission"]) for f in fills)
    entry_ms = int(entries[0]["time_ms"]); exit_ms = int(exits[-1]["time_ms"])
    excursion = _gate_excursion(snapshot, candles, entry_ms, exit_ms)
    target_fills = []
    for fill in exits:
        if fill["kind"] == "TP1" and "TP1" not in target_fills: target_fills.append("TP1")
        if fill["kind"] == "TP2" and "TP2" not in target_fills: target_fills.append("TP2")
    result.update(status="CLOSED" if fees_known else "FEES_UNRESOLVED", gross_r=float(gross/risk),
                  net_r=float((gross-fee)/risk) if fees_known else None,
                  gross_pct=float(gross/entry_value*100),
                  realized_pct=float((gross-fee)/entry_value*100) if fees_known else None,
                  entry=float(entry_value/entry_qty), exit_price=float(exit_value/exit_qty),
                  exit_reason=exits[-1]["kind"], duration_seconds=(exits[-1]["time_ms"]-entries[0]["time_ms"])/1000,
                  accounting_basis="confirmed_fills_after_commissions_excluding_funding",
                  fees_r=float(fee/risk) if risk and fees_known else None,
                  fee_assets=sorted({str(f["commission_asset"]) for f in fills}),
                  **excursion)
    # Confirmed TP/SL/CLOSE fills are authoritative for ACTUAL.  Gate touches
    # are retained only as descriptive context when no target fill was saved.
    result["targets_reached"] = target_fills or None
    # Only a fully closed, fully accounted position can close its reconciliation
    # admission.  Protective orders are not marked complete during partial
    # fills, so a restart still has a chance to observe their exchange state.
    if exit_qty == entry_qty:
        with closing(connect(path)) as conn, conn:
            conn.execute("UPDATE confirmed_execution_orders SET complete=1 WHERE signal_id=?", (signal_id,))
    return result
