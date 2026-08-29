"""Read-only Telegram views for active and completed APEX signals."""

from __future__ import annotations

import html
import sqlite3
from typing import Any


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def fetch_trades(db_path: str, category: str, limit: int = 12) -> list[dict[str, Any]]:
    """Return existing signal rows; never creates or migrates a table."""
    category = str(category).lower()
    where = {
        "active": "s.result='pending'",
        "take": "s.result IN ('tp1','tp2','tp3')",
        "stop": "s.result='sl'",
    }.get(category)
    if not where:
        raise ValueError(f"unsupported trade category: {category}")
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            f"""SELECT s.id, s.symbol, s.direction, s.signal_type, s.entry, s.sl,
                       s.tp1, s.tp2, s.tp3, s.timeframe, s.grade, s.result,
                       s.created_at, s.closed_at, s.tp1_hit, s.trailing_sl,
                       COALESCE(es.status, CASE WHEN s.result='pending' THEN 'active' ELSE 'closed' END) AS lifecycle_status,
                       te.status AS execution_status
                FROM signals s
                LEFT JOIN signal_execution_state es ON es.signal_id=s.id
                LEFT JOIN trade_executions te ON te.signal_id=s.id
                WHERE {where}
                ORDER BY COALESCE(s.closed_at, s.created_at) DESC LIMIT ?""",
            (max(1, min(int(limit), 30)),),
        ).fetchall()
        return [dict(row) for row in rows]
    except sqlite3.OperationalError as exc:
        # Older databases can predate either isolated status table.  Preserve
        # the view with only the stable legacy signals schema.
        if "no such table" not in str(exc).lower() and "no such column" not in str(exc).lower():
            raise
        rows = conn.execute(
            f"""SELECT s.id, s.symbol, s.direction, s.signal_type, s.entry, s.sl,
                       s.tp1, s.tp2, s.tp3, s.timeframe, s.grade, s.result,
                       s.created_at, s.closed_at, 0 AS tp1_hit, 0 AS trailing_sl,
                       CASE WHEN s.result='pending' THEN 'active' ELSE 'closed' END AS lifecycle_status,
                       NULL AS execution_status
                FROM signals s WHERE {where}
                ORDER BY COALESCE(s.closed_at, s.created_at) DESC LIMIT ?""",
            (max(1, min(int(limit), 30)),),
        ).fetchall()
        return [dict(row) for row in rows]
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
        "active": "Сейчас нет сигналов, ожидающих вход или находящихся в позиции.",
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

