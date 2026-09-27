"""Read-only Telegram views for active and completed APEX signals."""

from __future__ import annotations

import html
import sqlite3
from typing import Any, Callable


def fetch_live_trades(
    category: str, limit: int, connection_factory: Callable[[], sqlite3.Connection],
) -> list[dict[str, Any]]:
    """Project confirmed live positions from State without a legacy fallback."""
    category = str(category).lower()
    active = "m.status='ACTIVE' AND l.status='active'"
    result = "COALESCE(NULLIF(l.result,'pending'),m.close_result,'')"
    where = {
        "active": active,
        "take": f"NOT ({active}) AND {result} IN ('tp1','tp2','tp3')",
        "stop": f"NOT ({active}) AND {result}='sl'",
    }.get(category)
    if where is None:
        raise ValueError(f"unsupported trade category: {category}")
    conn = connection_factory()
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""SELECT m.signal_id AS id, m.symbol, m.direction,
                      m.strategy AS signal_type, m.strategy AS grade,
                      m.management_tf AS timeframe, m.initial_entry AS entry,
                      m.initial_sl AS sl, m.initial_tp1 AS tp1,
                      m.initial_tp2 AS tp2, m.initial_tp3 AS tp3,
                      m.tp1_seen AS tp1_hit,
                      m.confirmed_protect_level AS trailing_sl,
                      m.status AS manager_status,
                      m.close_result, m.created_at, m.closed_at,
                      l.status AS lifecycle_status, l.result AS lifecycle_result,
                      e.status AS execution_status
                 FROM manager_positions m
                 JOIN executions e ON e.signal_entity_id=m.signal_entity_id
                 JOIN signal_lifecycle l ON l.signal_entity_id=m.signal_entity_id
                WHERE e.mode='live' AND e.position_id IS NOT NULL
                  AND ({where})
                ORDER BY COALESCE(m.closed_at,m.updated_at) DESC,m.signal_id DESC
                LIMIT ?""",
            (max(1, min(int(limit), 30)),),
        ).fetchall()
        selected: list[dict[str, Any]] = []
        for item in rows:
            row = dict(item)
            result = str(row["lifecycle_result"] or "").lower()
            if result == "pending":
                result = str(row["close_result"] or "").lower()
            row["result"] = "pending" if category == "active" else result
            if category != "active":
                row["lifecycle_status"] = "closed"
            selected.append(row)
        return selected
    finally:
        conn.close()


def _fmt_price(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "—"
    if number >= 1000:
        return f"{number:,.2f}"
    if number >= 1:
        return f"{number:.4f}".rstrip("0").rstrip(".")
    return f"{number:.8f}".rstrip("0").rstrip(".")


def format_trade_view(category: str, rows: list[dict[str, Any]]) -> str:
    category = str(category).lower()
    headers = {
        "active": "📍 <b>Активные сделки</b>",
        "take": "✅ <b>Закрытые по тейку</b>",
        "stop": "🛑 <b>Закрытые по стопу</b>",
    }
    empty = {
        "active": "Сейчас нет открытых подтверждённых позиций.",
        "take": "Пока нет сделок, закрытых по тейку.",
        "stop": "Пока нет сделок, закрытых по стопу.",
    }
    if category not in headers:
        raise ValueError(f"unsupported trade category: {category}")
    if not rows:
        return f"{headers[category]}\n\n{empty[category]}"

    state_labels = {
        "waiting_entry": "⏳ ждёт входа",
        "active": "🟢 в позиции",
        "closed": "закрыта",
        "cancelled": "отменена",
    }
    lines = [headers[category], f"\nПоказано: <b>{len(rows)}</b>"]
    for row in rows:
        symbol = html.escape(str(row.get("symbol") or "?"))
        strategy = html.escape(str(row.get("grade") or row.get("signal_type") or "?"))
        timeframe = html.escape(str(row.get("timeframe") or "?"))
        bullish = str(row.get("direction")).upper() == "BULLISH"
        direction = "LONG" if bullish else "SHORT"
        direction_icon = "🟢" if bullish else "🔴"
        date_value = row.get("closed_at") or row.get("created_at") or ""
        date_text = html.escape(str(date_value)[:16].replace("T", " "))
        status = state_labels.get(str(row.get("lifecycle_status")), str(row.get("lifecycle_status") or ""))

        if category == "active":
            current_sl = row.get("trailing_sl") or row.get("sl")
            tp2 = row.get("tp2")
            target_text = f"TP1 <code>{_fmt_price(row.get('tp1'))}</code>"
            if tp2 and abs(float(tp2) - float(row.get("tp1") or 0)) > 1e-12:
                target_text += f" · TP2 <code>{_fmt_price(tp2)}</code>"
            execution = row.get("execution_status")
            execution_text = f"\n   Автоисполнение: <code>{html.escape(str(execution))}</code>" if execution else ""
            lines.append(
                f"\n{direction_icon} <b>{symbol}</b> {direction} · {strategy}/{timeframe}\n"
                f"   {html.escape(status)} · вход <code>{_fmt_price(row.get('entry'))}</code>\n"
                f"   SL <code>{_fmt_price(current_sl)}</code> · {target_text}{execution_text}"
            )
        else:
            result = html.escape(str(row.get("result") or "").upper())
            lines.append(
                f"\n{direction_icon} <b>{symbol}</b> {direction} · {strategy}/{timeframe}\n"
                f"   Результат: <b>{result}</b> · вход <code>{_fmt_price(row.get('entry'))}</code>\n"
                f"   SL <code>{_fmt_price(row.get('sl'))}</code> · TP1 <code>{_fmt_price(row.get('tp1'))}</code> · {date_text}"
            )
    return "\n".join(lines)[:3900]



def trade_line(symbol,direction,status):
    return f"{symbol} · {direction} · {status}"

__all__=["fetch_live_trades","format_trade_view","trade_line"]
