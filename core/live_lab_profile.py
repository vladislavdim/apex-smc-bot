"""Asynchronous, execution-neutral parity run of Research profiles in live."""
from __future__ import annotations

import asyncio
import os
import queue
import threading
import time
from typing import Any, Mapping

PROFILE_ID = "research-v4-live-shadow"
_QUEUE: "queue.Queue[dict[str, Any]]" = queue.Queue(maxsize=1000)
_LOCK = threading.Lock()
_STARTED = False
_LAST: dict[tuple[str, str], float] = {}
_SNAPSHOTS: dict[str, tuple[float, dict[str, dict[str, Any]]]] = {}


def _enabled() -> bool:
    configured = os.environ.get("APEX_LAB_PROFILE_SHADOW")
    if configured is None:
        return os.environ.get("RENDER", "").strip().lower() in {"1", "true", "yes"}
    return configured.strip().lower() not in {"0", "false", "off", "no"}


def _normalise_closed(candles: list[Mapping[str, Any]], timeframe: str) -> list[dict[str, Any]]:
    from research.features import TIMEFRAME_SECONDS
    period = TIMEFRAME_SECONDS[timeframe]; rows = []
    for item in candles:
        opened = int(item.get("open_time", item.get("timestamp", 0)) or 0)
        if opened > 1_000_000_000_000: opened //= 1000
        if not opened: continue
        rows.append({"open_time": opened, "close_time": opened + period,
            "open": item.get("open"), "high": item.get("high"), "low": item.get("low"),
            "close": item.get("close"), "volume": item.get("volume", 0), "is_closed": True})
    return rows


def _live_derivatives(symbol: str) -> dict[str, Any]:
    """Use point-in-time live values; missing data stays absent, never zero."""
    output: dict[str, Any] = {}
    try:
        from external_sources import live_tape
        tape = live_tape.snapshot(symbol); gate = (tape.get("sources") or {}).get("gate") or {}
        buy, sell = gate.get("buy_usd_60s"), gate.get("sell_usd_60s")
        if buy is not None or sell is not None:
            buy, sell = float(buy or 0), float(sell or 0); total = buy + sell
            output["trade_cvd_real"] = {"source": "gate_ws", "buy_notional": buy,
                "sell_notional": sell, "delta_notional": buy - sell,
                "taker_imbalance": (buy - sell) / total if total else None,
                "availability": "RECENT_ONLY", "window_seconds": 60}
        if gate.get("oi") is not None: output["open_interest"] = {"source": "gate_ws", "usd": gate.get("oi")}
        if gate.get("funding") is not None: output["funding_rate"] = {"source": "gate_ws", "rate": gate.get("funding")}
        if gate.get("long_liq_usd_300s") or gate.get("short_liq_usd_300s"):
            output["liquidations"] = {"source": "gate_ws", "long_usd": gate.get("long_liq_usd_300s") or 0,
                "short_usd": gate.get("short_liq_usd_300s") or 0, "window_seconds": 300}
        book = live_tape.order_book_snapshot(symbol)
        if book.get("freshness_status") == "FRESH": output["order_book_liquidity"] = book
    except Exception:
        pass
    try:
        from core.external_market_context import collect_external_market_context
        context = asyncio.run(collect_external_market_context(symbol))
        gate = (context.get("providers") or {}).get("gateio") or {}
        if gate.get("long_short_account_ratio") is not None:
            output["long_short_ratio"] = {"source": "gateio_futures_public",
                "accounts": gate.get("long_short_account_ratio"), "takers": gate.get("long_short_taker_ratio")}
        if "open_interest" not in output and gate.get("open_interest_contracts") is not None:
            output["open_interest"] = {"source": "gateio_futures_public", "contracts": gate.get("open_interest_contracts"),
                "change_1h_pct": gate.get("open_interest_change_pct")}
        if "liquidations" not in output and (gate.get("long_liquidation_size") is not None or gate.get("short_liquidation_size") is not None):
            output["liquidations"] = {"source": "gateio_futures_public",
                "long_contracts": gate.get("long_liquidation_size"), "short_contracts": gate.get("short_liquidation_size")}
    except Exception:
        pass
    return output


def _snapshot_set(symbol: str) -> dict[str, dict[str, Any]]:
    cached = _SNAPSHOTS.get(symbol)
    if cached and time.monotonic() - cached[0] < 150: return cached[1]
    import market
    from research.features import compute_feature_snapshot
    external = _live_derivatives(symbol); snapshots = {}
    for timeframe in ("15m", "1h", "4h", "1d"):
        rows = _normalise_closed(market.get_confirmed_candles(market.get_candles(symbol, timeframe, 220)), timeframe)
        if len(rows) < 20: raise ValueError(f"{timeframe}: fewer than 20 closed candles")
        snapshots[timeframe] = compute_feature_snapshot(symbol, timeframe, rows,
            dataset_version="gate-live-shadow", external=external)
    _SNAPSHOTS[symbol] = (time.monotonic(), snapshots)
    return snapshots


def _evaluate(job: Mapping[str, Any]) -> None:
    from core.setup_audit import emit_event
    from research.features import FEATURE_VERSION
    from research.replay import WORKING_TF, _attempt_checks, _candidate
    strategy, symbol = str(job["strategy"]), str(job["symbol"])
    snapshots = _snapshot_set(symbol); candidate, stop = _candidate(strategy, snapshots, symbol)
    checks = _attempt_checks(strategy, snapshots, candidate, stop)
    hard = [x for x in checks if x.get("role") == "HARD_GATE"]
    reached = [x for x in hard if x.get("status") != "NOT_REACHED"]
    passed = [x for x in hard if x.get("status") == "PASS"]
    emit_event("lab_profile_shadow", strategy, symbol, {
        "attempt_key": job.get("attempt_key"), "profile_id": PROFILE_ID,
        "profile_kind": "REPLAY_PROFILE_SURROGATE", "feature_version": FEATURE_VERSION,
        "execution_authority": False, "point_in_time": True, "closed_candles_only": True,
        "working_timeframe": WORKING_TF[strategy], "live_outcome": job.get("live_outcome"),
        "lab_outcome": "CANDIDATE" if not stop else "FILTERED", "stop_code": stop or None,
        "hard_gate_match_pct": round(len(passed) / len(reached) * 100, 2) if reached else 0.0,
        "checks": checks, "candidate": {key: candidate.get(key) for key in
            ("direction", "entry", "sl", "tp1", "tp2", "terminal_tp", "rr", "technical_evidence")},
    })


def _worker() -> None:
    while True:
        job = _QUEUE.get()
        try: _evaluate(job)
        except Exception as exc:
            try:
                from core.setup_audit import emit_event
                emit_event("lab_profile_shadow", str(job.get("strategy") or ""), str(job.get("symbol") or ""),
                    {"attempt_key": job.get("attempt_key"), "profile_id": PROFILE_ID,
                     "execution_authority": False, "lab_outcome": "UNAVAILABLE",
                     "error": f"{type(exc).__name__}: {exc}"[:500]})
            except Exception: pass
        finally: _QUEUE.task_done()


def schedule(strategy: str, symbol: str, *, attempt_key: str = "", live_outcome: str = "") -> bool:
    global _STARTED
    strategy, symbol = str(strategy or "").upper(), str(symbol or "").upper()
    allowed_symbols = {item.strip().upper().replace("/", "") for item in
        os.environ.get("APEX_LAB_PROFILE_SHADOW_SYMBOLS", "BTCUSDT").split(",") if item.strip()}
    if (not _enabled() or strategy not in {"FAST", "MTF", "SWING", "ZONE", "WYCKOFF"}
            or not symbol or symbol not in allowed_symbols): return False
    now = time.monotonic(); key = (strategy, symbol)
    cadence = max(60, int(os.environ.get("APEX_LAB_PROFILE_SHADOW_INTERVAL_SECONDS", "180")))
    with _LOCK:
        last_run = _LAST.get(key)
        if last_run is not None and now - last_run < cadence: return False
        _LAST[key] = now
        if not _STARTED:
            threading.Thread(target=_worker, name="apex-live-lab-shadow", daemon=True).start(); _STARTED = True
    try:
        _QUEUE.put_nowait({"strategy": strategy, "symbol": symbol, "attempt_key": attempt_key,
            "live_outcome": live_outcome}); return True
    except queue.Full: return False


__all__ = ["PROFILE_ID", "schedule"]
