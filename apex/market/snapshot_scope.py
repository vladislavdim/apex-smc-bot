"""Thread/task-local candle boundary for snapshot-native strategy evaluation."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator

from apex.domain.models import MarketSnapshot


_ACTIVE_SNAPSHOT: ContextVar[MarketSnapshot | None] = ContextVar(
    "apex_active_market_snapshot", default=None,
)


@contextmanager
def use_market_snapshot(snapshot: MarketSnapshot) -> Iterator[MarketSnapshot]:
    """Make one immutable snapshot the only candle source in this context."""
    token = _ACTIVE_SNAPSHOT.set(snapshot)
    try:
        yield snapshot
    finally:
        _ACTIVE_SNAPSHOT.reset(token)


def snapshot_candle_override(
    symbol: str, timeframe: str, limit: int,
) -> list[dict] | None:
    """Return ``None`` outside a scope, otherwise bounded snapshot candles.

    An empty list inside an active scope is deliberate and fail-closed: the
    router must not fetch a missing symbol or timeframe from the live market.
    """
    snapshot = _ACTIVE_SNAPSHOT.get()
    if snapshot is None:
        return None
    if str(symbol).upper() != snapshot.symbol.upper():
        return []
    rows = snapshot.candles.get(str(timeframe))
    if not rows:
        return []
    requested = max(1, int(limit or 1))
    return [dict(row) for row in rows[-requested:]]


def snapshot_scope_active() -> bool:
    """Return whether strategy data must be isolated to one snapshot."""
    return _ACTIVE_SNAPSHOT.get() is not None


__all__ = [
    "snapshot_candle_override", "snapshot_scope_active", "use_market_snapshot",
]
