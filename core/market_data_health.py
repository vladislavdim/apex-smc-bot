"""Fail-open Gate market-data health telemetry.

The trading path calls :func:`record_market_data` after a candle read.  Events
are transition/throttle limited so Strategy Lab gets useful per-symbol/TF state
without turning normal cache traffic into telemetry spam.
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Any

from core.setup_audit import emit_event

_LOCK = threading.Lock()
_STATE: dict[tuple[str, str], dict[str, Any]] = {}
_EMIT_EVERY_SECONDS = 300.0


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def record_market_data(
    symbol: str,
    timeframe: str,
    ok: bool,
    *,
    source: str = "Gate",
    reason: str = "",
    candle_count: int = 0,
    cached: bool = False,
) -> None:
    """Record current candle availability without affecting the caller."""
    try:
        key = (str(symbol or "").upper(), str(timeframe or ""))
        if not key[0] or not key[1]:
            return
        now = time.monotonic()
        timestamp = _utc_now()
        with _LOCK:
            previous = _STATE.get(key, {})
            last_success = timestamp if ok else previous.get("last_success_at")
            changed = previous.get("ok") is not bool(ok)
            due = now - float(previous.get("emitted_at") or 0.0) >= _EMIT_EVERY_SECONDS
            state = {
                "ok": bool(ok),
                "last_success_at": last_success,
                "last_update_at": timestamp,
                "emitted_at": now,
            }
            if not (changed or due):
                state["emitted_at"] = previous.get("emitted_at", now)
                _STATE[key] = state
                return
            _STATE[key] = state
        emit_event(
            "market_data",
            "SYSTEM",
            key[0],
            {
                "provider": "Gate",
                "source": str(source or "Gate")[:80],
                "timeframe": key[1],
                "status": "OK" if ok else "FAILED",
                "reason": str(reason or "")[:500],
                "candle_count": max(0, int(candle_count or 0)),
                "cached": bool(cached),
                "last_success_at": last_success,
                "last_update_at": timestamp,
            },
        )
    except Exception:
        # Observability must never block market data or a trading decision.
        return


def reset_market_data_state_for_tests() -> None:
    with _LOCK:
        _STATE.clear()
