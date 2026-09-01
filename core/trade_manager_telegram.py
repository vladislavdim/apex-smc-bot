"""Telegram read-only views for the APEX Trade Manager."""
from __future__ import annotations

import html
import sqlite3
from typing import Any

from core.trade_manager import ensure_trade_manager_schema


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=20, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def _fmt_price(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "—"
    if number == 0:
        return "—"
    if abs(number) < 0.0001:
        return f"{number:.8f}"
    if abs(number) < 1:
        return f"{number:.6f}".rstrip("0").rstrip(".")
    return f"{number:,.4f}".rstrip("0").rstrip(".")


def _direction_label(value: Any) -> str:
    direction = str(value or "").upper()
    if direction == "BULLISH":
        return "🟢 LONG"
    if direction == "BEARISH":
        return "🔴 SHORT"
    return html.escape(direction or "—")


def fetch_manager_trades(db_path: str, limit: int = 12) -> list[dict[str, Any]]:
    ensure_trade_manager_schema(db_path)
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT m.signal_id, m.symbol, m.strategy, m.direction, m.management_tf,
                   m.initial_entry, m.initial_sl, m.initial_tp1, m.initial_tp2, m.initial_tp3,
                   m.initial_rr, m.last_price, m.current_r, m.tp1_seen, m.tp2_seen,
                   m.manager_target, m.manager_protect_level, m.last_event, m.last_action,
                   m.last_confidence, m.updated_at,
                   COALESCE(s.result, 'pending') AS signal_result
              FROM trade_manager_state m
              LEFT JOIN signals s ON s.id = m.signal_id
             WHERE COALESCE(s.result, 'pending') = 'pending'
             ORDER BY m.updated_at DESC, m.signal_id DESC
             LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def fetch_manager_trade(db_path: str, signal_id: int, event_limit: int = 12) -> dict[str, Any] | None:
    ensure_trade_manager_schema(db_path)
    conn = _connect(db_path)
    try:
        state = conn.execute(
            "SELECT * FROM trade_manager_state WHERE signal_id=?",
            (int(signal_id),),
        ).fetchone()
        if not state:
            return None
        events = conn.execute(
            """
            SELECT event_type, action, confidence, price, r_multiple,
                   manager_target, manager_protect_level, reason, created_at
              FROM trade_manager_events
             WHERE signal_id=?
             ORDER BY id DESC
             LIMIT ?
            """,
            (int(signal_id), max(1, int(event_limit))),
        ).fetchall()
        return {"state": dict(state), "events": [dict(row) for row in events]}
    finally:
        conn.close()


def format_manager_dashboard(items: list[dict[str, Any]]) -> str:
    lines = ["🧠 <b>Менеджер сделок APEX</b>", "━━━━━━━━━━━━━━━━", ""]
    if not items:
        lines += [
            "Сейчас менеджер не сопровождает активные сделки.",
            "",
            "<i>Здесь появляются только сделки, зарегистрированные Trade Manager.</i>",
        ]
        return "\n".join(lines)

    lines.append(f"Активно сопровождается: <b>{len(items)}</b>\n")
    for item in items:
        action = html.escape(str(item.get("last_action") or "HOLD"))
        event = html.escape(str(item.get("last_event") or "—"))
        confidence = item.get("last_confidence")
        confidence_text = f"{float(confidence) * 100:.0f}%" if confidence is not None else "—"
        tp_state = "TP1 ✅" if item.get("tp1_seen") else "TP1 ⏳"
        if item.get("tp2_seen"):
            tp_state += " · TP2 ✅"
        elif item.get("tp1_seen"):
            tp_state += " · TP2 ⏳"
        lines += [
            f"<b>#{item['signal_id']} {html.escape(str(item.get('symbol') or ''))}</b> · "
            f"{html.escape(str(item.get('strategy') or 'MTF'))} · {_direction_label(item.get('direction'))}",
            f"Entry <code>{_fmt_price(item.get('initial_entry'))}</code> · "
            f"Цена <code>{_fmt_price(item.get('last_price'))}</code> · "
            f"R <b>{float(item.get('current_r') or 0):+.2f}</b>",
            f"{tp_state} · Событие: <b>{event}</b>",
            f"Решение: <b>{action}</b> · уверенность {confidence_text}",
            f"Цель менеджера: <code>{_fmt_price(item.get('manager_target'))}</code> · "
            f"защита: <code>{_fmt_price(item.get('manager_protect_level'))}</code>",
            "",
        ]
    lines.append("<i>Решения менеджера — рекомендации; сами по себе они не означают исполненный ордер.</i>")
    return "\n".join(lines)[:4000]


def manager_trade_buttons(items: list[dict[str, Any]]) -> list[tuple[str, str]]:
    buttons: list[tuple[str, str]] = []
    for item in items:
        label = f"{str(item.get('symbol') or '').replace('USDT', '')} · {item.get('strategy') or 'MTF'} · {float(item.get('current_r') or 0):+.1f}R"
        buttons.append((label[:48], f"manager_trade_{int(item['signal_id'])}"))
    return buttons


def format_manager_trade_detail(payload: dict[str, Any]) -> str:
    state = payload["state"]
    events = payload.get("events") or []
    confidence = state.get("last_confidence")
    confidence_text = f"{float(confidence) * 100:.0f}%" if confidence is not None else "—"
    lines = [
        f"🧠 <b>APEX MANAGER · #{state['signal_id']}</b>",
        "━━━━━━━━━━━━━━━━",
        f"<b>{html.escape(str(state.get('symbol') or ''))}</b> · {html.escape(str(state.get('strategy') or 'MTF'))} · {_direction_label(state.get('direction'))}",
        f"Management TF: <b>{html.escape(str(state.get('management_tf') or '—'))}</b>",
        "",
        f"Entry: <code>{_fmt_price(state.get('initial_entry'))}</code>",
        f"Initial SL: <code>{_fmt_price(state.get('initial_sl'))}</code>",
        f"TP1/TP2/TP3: <code>{_fmt_price(state.get('initial_tp1'))}</code> / <code>{_fmt_price(state.get('initial_tp2'))}</code> / <code>{_fmt_price(state.get('initial_tp3'))}</code>",
        f"Initial RR: <b>{float(state.get('initial_rr') or 0):.2f}</b>",
        f"Current price: <code>{_fmt_price(state.get('last_price'))}</code> · R <b>{float(state.get('current_r') or 0):+.2f}</b>",
        f"TP1: {'✅' if state.get('tp1_seen') else '⏳'} · TP2: {'✅' if state.get('tp2_seen') else '⏳'}",
        "",
        f"Последнее событие: <b>{html.escape(str(state.get('last_event') or '—'))}</b>",
        f"Рекомендация: <b>{html.escape(str(state.get('last_action') or 'HOLD'))}</b> · {confidence_text}",
        f"Структурная цель: <code>{_fmt_price(state.get('manager_target'))}</code>",
        f"Уровень защиты: <code>{_fmt_price(state.get('manager_protect_level'))}</code>",
        "",
        "<b>Последние события:</b>",
    ]
    if not events:
        lines.append("Пока нет записанных событий.")
    else:
        for event in events[:10]:
            reason = html.escape(str(event.get("reason") or "").strip())[:180]
            action = html.escape(str(event.get("action") or "—"))
            event_type = html.escape(str(event.get("event_type") or "—"))
            ts = html.escape(str(event.get("created_at") or ""))[:16]
            r_value = float(event.get("r_multiple") or 0)
            conf = event.get("confidence")
            conf_text = f"{float(conf) * 100:.0f}%" if conf is not None else "—"
            lines.append(f"• <code>{ts}</code> {event_type} → <b>{action}</b> · {r_value:+.2f}R · {conf_text}")
            if reason:
                lines.append(f"  {reason}")
    lines += ["", "<i>История read-only: исходные Entry/SL/TP/RR не переписываются.</i>"]
    return "\n".join(lines)[:4000]
