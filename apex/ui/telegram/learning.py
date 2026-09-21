"""Read-only Telegram view of confirmed live learning evidence."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable

from apex.learning.statistics import strategy_statistics


def _count(conn: sqlite3.Connection, sql: str) -> int:
    row = conn.execute(sql).fetchone()
    return int(row[0] or 0) if row else 0


def format_live_learning(conn_factory: Callable[[], sqlite3.Connection]) -> str:
    conn = conn_factory()
    try:
        candidates = _count(conn, "SELECT COUNT(*) FROM live_candidates")
        executed = _count(conn, "SELECT COUNT(*) FROM live_candidates WHERE executed=1")
        outcomes = _count(conn, "SELECT COUNT(*) FROM live_trade_outcomes")
        stats = strategy_statistics(conn)
    finally:
        conn.close()

    lines = [
        "📚 <b>LIVE LEARNING</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        f"Кандидаты: <b>{candidates}</b>",
        f"Подтверждённые Binance-позиции: <b>{executed}</b>",
        f"Закрытые реальные сделки: <b>{outcomes}</b>",
        "",
        "<b>По стратегиям:</b>",
    ]
    if not stats:
        lines.append("Пока нет закрытых live-сделок.")
    for item in stats:
        pf = item["profit_factor"]
        pf_text = "—" if pf is None else f"{pf:.2f}"
        lines.append(
            f"• {item['strategy']}: N={item['samples']} · "
            f"WR={item['win_rate'] * 100:.1f}% · "
            f"E={item['net_expectancy_r']:+.2f}R · "
            f"PF={pf_text} · DD={item['max_drawdown_r']:.2f}R · "
            f"{item['confidence']}"
        )
    lines.extend([
        "",
        "ℹ️ Только фактические fills, комиссии и funding Binance.",
        "Learning имеет статус <b>ADVISORY</b>: не меняет стратегии, риск или исполнение.",
    ])
    return "\n".join(lines)


__all__ = ["format_live_learning"]
