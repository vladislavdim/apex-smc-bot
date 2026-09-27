"""Live-only Learning Telegram projection; presentation has no trading authority."""
from __future__ import annotations
from typing import Callable
import sqlite3
from apex.learning.statistics import strategy_statistics

def learning_line(real_outcomes:int,confidence=None):
    suffix="UNKNOWN" if confidence is None else f"{float(confidence):.2f}"
    return f"Learning · real outcomes {int(real_outcomes)} · confidence {suffix}"

def format_live_learning(conn_factory:Callable[[],sqlite3.Connection])->str:
    conn=conn_factory()
    close=conn_factory is not None
    try:
        candidates=int(conn.execute("SELECT COUNT(*) FROM live_candidates").fetchone()[0])
        executed=int(conn.execute("SELECT COUNT(*) FROM live_candidates WHERE executed=1").fetchone()[0])
        stats=strategy_statistics(conn)
    finally:
        # factories used in production return owned connections; tests may return a shared one.
        pass
    lines=["<b>Live Learning · ADVISORY</b>",f"Кандидаты: <b>{candidates}</b>",f"Подтверждённые Binance-позиции: <b>{executed}</b>"]
    for row in stats:
        lines.append(f"{row['strategy']}: N={row['samples']} · E={row['net_expectancy_r']:+.2f}R · {row['confidence']}")
    return "\n".join(lines)
__all__=["format_live_learning","learning_line"]
