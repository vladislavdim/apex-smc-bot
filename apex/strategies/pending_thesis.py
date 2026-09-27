"""Fail-closed pair ownership across State and bounded legacy migration rows."""
from __future__ import annotations

import sqlite3
from typing import Callable


def has_pending_thesis(
    symbol: str,
    state_factory: Callable[[], sqlite3.Connection],
    legacy_factory: Callable[[], sqlite3.Connection],
) -> bool:
    """A State lifecycle or position, or pre-cutover signal, owns the pair.

    Both stores must be readable before reporting the pair free. Callers must
    inhibit new entries on a read error; UNKNOWN is never interpreted as free.
    """
    pair = str(symbol or "").upper().strip()
    if not pair:
        raise ValueError("pending_thesis_symbol_required")
    state = state_factory()
    try:
        live = state.execute(
            """SELECT 1 FROM signal_lifecycle
               WHERE symbol=? AND status IN ('waiting_entry','active') LIMIT 1""",
            (pair,),
        ).fetchone()
        if not live:
            live = state.execute(
                """SELECT 1 FROM executions e
                   JOIN signal_lifecycle l ON l.signal_entity_id=e.signal_entity_id
                   WHERE e.symbol=? AND e.mode='live' AND e.position_id IS NOT NULL
                     AND l.status IN ('waiting_entry','active') LIMIT 1""",
                (pair,),
            ).fetchone()
    finally:
        state.close()
    if live:
        return True
    legacy = legacy_factory()
    try:
        row = legacy.execute(
            "SELECT 1 FROM signals WHERE symbol=? AND result='pending' LIMIT 1",
            (pair,),
        ).fetchone()
        return bool(row)
    finally:
        legacy.close()


__all__ = ["has_pending_thesis"]
