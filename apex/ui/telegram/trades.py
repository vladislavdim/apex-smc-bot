"""Read-only Telegram views for active and completed APEX signals."""

from __future__ import annotations

import html
import math
import sqlite3
from typing import Any, Callable
from datetime import datetime, timezone


def _history_order(row: dict[str, Any]) -> tuple[float, int]:
    value = row.get("closed_at") or row.get("updated_at") or row.get("created_at")
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        stamp = (parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).timestamp()
    except (ValueError, TypeError, OverflowError):
        stamp = 0.0
    return stamp, int(row["id"])


def fetch_live_trades(
    category: str, limit: int, connection_factory: Callable[[], sqlite3.Connection],
) -> list[dict[str, Any]]:
    """Read all delivered-signal history, preserving verified Binance authority."""
    category = str(category).lower()
    cap = max(1, min(int(limit), 30))
    # Manager owns actual position outcomes; analytical lifecycle is separate.
    real_result = "COALESCE(NULLIF(NULLIF(m.close_result,''),'pending'),NULLIF(l.result,'pending'),'')"
    live_where = {
        "active": "m.status='ACTIVE'",
        "closed": "m.status='CLOSED'",
        "take": f"m.status='CLOSED' AND {real_result} IN ('tp1','tp2','tp3')",
        "stop": f"m.status='CLOSED' AND {real_result}='sl'",
    }
    analytical_where = {
        "active": "l.status IN ('waiting_entry','active')",
        "closed": "l.status IN ('closed','cancelled')",
        "take": "l.status='closed' AND l.result IN ('tp1','tp2','tp3')",
        "stop": "l.status='closed' AND l.result='sl'",
        "signals": "1=1",
    }
    if category not in analytical_where:
        raise ValueError(f"unsupported trade category: {category}")
    conn = connection_factory()
    try:
        conn.row_factory = sqlite3.Row
        # Both sources must be read in the same SQLite snapshot.
        if not conn.in_transaction:
            conn.execute("BEGIN")
        analytical = conn.execute(
            f"""SELECT l.signal_id AS id,l.symbol,l.direction,l.signal_type,l.grade,
                      l.timeframe,l.entry,l.sl,l.tp1,l.tp2,l.tp3,
                      l.status AS lifecycle_status,l.result,l.created_at,l.updated_at,
                      l.closed_at,l.last_checked_at,l.cancel_reason,
                      e.status AS execution_status,e.mode AS execution_mode,
                      e.position_id AS execution_position_id
                 FROM signal_lifecycle l
                 LEFT JOIN executions e ON e.signal_entity_id=l.signal_entity_id
                WHERE l.symbol IS NOT NULL AND l.entry IS NOT NULL
                  AND ({analytical_where[category]})
                  AND NOT EXISTS (
                      SELECT 1 FROM manager_positions m
                      JOIN executions actual ON actual.signal_entity_id=m.signal_entity_id
                       WHERE m.signal_entity_id=l.signal_entity_id
                         AND actual.mode='live' AND actual.position_id IS NOT NULL
                         AND m.status IN ('ACTIVE','CLOSED'))
                ORDER BY COALESCE(l.closed_at,l.updated_at,l.created_at) DESC,l.signal_id DESC
                LIMIT ?""", (cap,),
        ).fetchall()
        selected = [dict(row, _view_kind="analytical") for row in analytical]
        if category != "signals":
            live = conn.execute(
                f"""SELECT m.signal_id AS id,m.symbol,m.direction,
                          m.strategy AS signal_type,m.strategy AS grade,
                          m.management_tf AS timeframe,m.initial_entry AS entry,
                          m.initial_sl AS sl,m.initial_tp1 AS tp1,
                          m.initial_tp2 AS tp2,m.initial_tp3 AS tp3,m.initial_rr,
                          m.tp1_seen AS tp1_hit,m.confirmed_protect_level AS trailing_sl,
                          m.status AS manager_status,m.close_result,m.created_at,
                          m.updated_at,m.closed_at,m.last_price,m.current_r,
                          m.last_reviewed_candle,m.price_observed_at,m.exit_price,m.realized_pct,m.realized_r,
                          l.status AS lifecycle_status,l.result AS lifecycle_result,
                          e.status AS execution_status,{real_result} AS resolved_result
                     FROM manager_positions m
                     JOIN executions e ON e.signal_entity_id=m.signal_entity_id
                     LEFT JOIN signal_lifecycle l ON l.signal_entity_id=m.signal_entity_id
                    WHERE e.mode='live' AND e.position_id IS NOT NULL
                      AND ({live_where[category]})
                    ORDER BY COALESCE(m.closed_at,m.updated_at) DESC,m.signal_id DESC
                    LIMIT ?""", (cap,),
            ).fetchall()
            for item in live:
                row = dict(item)
                row["result"] = "pending" if category == "active" else row.pop("resolved_result")
                row["lifecycle_status"] = "active" if category == "active" else "closed"
                row["_view_kind"] = "live"
                selected.append(row)
        selected.sort(key=_history_order, reverse=True)
        return selected[:cap]
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


def _finite_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def enrich_active_analytical_progress(
    rows: list[dict[str, Any]],
    prices: dict[str, Any],
    *,
    observed_at: datetime | None = None,
) -> list[dict[str, Any]]:
    """Attach a fresh Gate mark to active analytical signals without mutating State."""
    stamp = (observed_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    enriched: list[dict[str, Any]] = []
    for original in rows:
        row = dict(original)
        is_analytical_active = (
            row.get("_view_kind") == "analytical"
            and str(row.get("lifecycle_status") or "").lower() == "active"
        )
        quote = prices.get(str(row.get("symbol") or "").upper()) if is_analytical_active else None
        quote = quote if isinstance(quote, dict) else {}
        price = _finite_number(quote.get("price"))
        entry = _finite_number(row.get("entry"))
        stop = _finite_number(row.get("sl"))
        direction = str(row.get("direction") or "").upper()
        sign = {"BULLISH": 1.0, "BEARISH": -1.0}.get(direction)
        if (
            price is not None and price > 0 and entry is not None and entry > 0
            and stop is not None and stop > 0 and sign is not None
            and sign * (entry - stop) > 0
        ):
            move = sign * (price - entry)
            row["display_price"] = price
            row["display_r"] = move / abs(entry - stop)
            row["display_pct"] = move / entry * 100
            row["display_price_observed_at"] = stamp.isoformat(timespec="seconds")
            row["display_price_source"] = "Gate"
        enriched.append(row)
    return enriched


def fetch_trade_view_rows(
    category: str,
    limit: int,
    connection_factory: Callable[[], sqlite3.Connection],
    price_provider: Callable[[], dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Fetch State rows, then enrich active analytical signals with one fresh market read."""
    rows = fetch_live_trades(category, limit, connection_factory)
    if str(category).lower() != "active" or price_provider is None:
        return rows
    try:
        prices = price_provider()
    except Exception:
        return rows
    if not isinstance(prices, dict) or not prices:
        return rows
    return enrich_active_analytical_progress(rows, prices)


def _analytical_return(row: dict[str, Any]) -> str:
    """Nominal barrier distance; never imply a fill, trailing exit or account PnL."""
    if str(row.get("lifecycle_status") or "").lower() != "closed":
        return ""
    outcome = str(row.get("result") or "").lower()
    if outcome not in {"sl", "tp1", "tp2", "tp3"}:
        return "\n   Расчёт результата: недостаточно данных"
    entry, stop, target = (_finite_number(row.get(k)) for k in ("entry", "sl", outcome))
    direction = str(row.get("direction") or "").upper()
    sign = {"BULLISH": 1, "BEARISH": -1}.get(direction)
    if sign is None or entry is None or stop is None or target is None:
        return "\n   Расчёт результата: недостаточно данных"
    if entry <= 0 or stop <= 0 or target <= 0 or sign * (entry - stop) <= 0:
        return "\n   Расчёт результата: недостаточно данных"
    if outcome != "sl" and sign * (target - entry) <= 0:
        return "\n   Расчёт результата: недостаточно данных"
    change = sign * (target - entry)
    pct, r = change / entry * 100, change / abs(entry - stop)
    if not math.isfinite(pct) or not math.isfinite(r):
        return "\n   Расчёт результата: недостаточно данных"
    return (
        f"\n   Расчёт до уровня {outcome.upper()}: <b>{pct:+.2f}% · {r:+.2f}R</b>"
    )


def _analytical_live_return(row: dict[str, Any]) -> str:
    """Render an on-refresh analytical mark without claiming a Binance fill or cash PnL."""
    price, pct, r = (
        _finite_number(row.get(key))
        for key in ("display_price", "display_pct", "display_r")
    )
    if price is None or pct is None or r is None:
        return "\n   Текущий результат: цена временно недоступна"
    source = html.escape(str(row.get("display_price_source") or "Gate"))
    checked = html.escape(
        str(row.get("display_price_observed_at") or "")[:19].replace("T", " ")
    )
    return (
        f"\n   📈 Цена {source} <code>{_fmt_price(price)}</code> · "
        f"текущий результат <b>{r:+.2f}R · {pct:+.2f}%</b>"
        + (f" · обновлено {checked} UTC" if checked else "")
    )


def _manager_return(row: dict[str, Any]) -> str:
    """Show only recorded Manager return; do not guess net account PnL."""
    pct, r = (_finite_number(row.get(k)) for k in ("realized_pct", "realized_r"))
    values = []
    if pct is not None:
        values.append(f"{pct:+.2f}%")
    if r is not None:
        values.append(f"{r:+.2f}R")
    if not values:
        return "\n   Результат Manager: нет подтверждённого расчёта"
    return "\n   Результат Manager: <b>" + " · ".join(values) + "</b>"


def format_trade_view(category: str, rows: list[dict[str, Any]]) -> str:
    category = str(category).lower()
    headers = {
        "active": "📍 <b>Активные сигналы и позиции</b>",
        "closed": "📁 <b>Закрытые сигналы и сделки</b>",
        "take": "✅ <b>Закрытые по тейку</b>",
        "stop": "🛑 <b>Закрытые по стопу</b>",
        "signals": "📨 <b>Аналитические сигналы</b>",
    }
    empty = {
        "active": "Сейчас нет активных сигналов или позиций.",
        "closed": "Пока нет закрытых сигналов или сделок.",
        "take": "Пока нет сделок или сигналов, закрытых по тейку.",
        "stop": "Пока нет сделок или сигналов, закрытых по стопу.",
        "signals": "Аналитических сигналов для отображения пока нет.",
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
    analytical_shown = False
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

        if category == "signals" or row.get("_view_kind") == "analytical":
            analytical_shown = True
            lifecycle = str(row.get("lifecycle_status") or "").lower()
            state = state_labels.get(lifecycle, lifecycle or "—")
            outcome = str(row.get("result") or "pending").lower()
            if lifecycle == "closed":
                state = f"аналитически закрыт: {outcome.upper()}"
            elif lifecycle == "active":
                state = "аналитически активен"
            execution = str(row.get("execution_status") or "").upper()
            status = (
                "Позиция Binance подтверждена; результат ниже аналитический"
                if row.get("execution_mode") == "live" and row.get("execution_position_id")
                else "Автовход выключен" if execution == "DISABLED"
                else "Биржевой вход не подтверждён"
            )
            lines.append(
                f"\n{direction_icon} <b>#{int(row['id'])} {symbol}</b> {direction} · {strategy}/{timeframe}\n"
                "   📨 Аналитический сигнал\n"
                f"   {html.escape(state)} · {status}\n"
                f"   Вход <code>{_fmt_price(row.get('entry'))}</code> · SL <code>{_fmt_price(row.get('sl'))}</code> "
                f"· TP1 <code>{_fmt_price(row.get('tp1'))}</code> · {date_text}"
                f"{_analytical_live_return(row) if category == 'active' and lifecycle == 'active' else ''}"
                f"{_analytical_return(row)}"
            )
        elif category == "active":
            live_label = "Binance · " if row.get("_view_kind") == "live" else ""
            current_sl = row.get("trailing_sl") or row.get("sl")
            tp2 = row.get("tp2")
            target_text = f"TP1 <code>{_fmt_price(row.get('tp1'))}</code>"
            if tp2 and abs(float(tp2) - float(row.get("tp1") or 0)) > 1e-12:
                target_text += f" · TP2 <code>{_fmt_price(tp2)}</code>"
            plan_text = f" · план {float(row['initial_rr']):.2f}R" if row.get("initial_rr") is not None else ""
            execution = row.get("execution_status")
            execution_text = f"\n   Автоисполнение: <code>{html.escape(str(execution))}</code>" if execution else ""
            price = row.get("last_price")
            checked = row.get("price_observed_at") or row.get("updated_at")
            progress = (
                f"\n   Цена проверки <code>{_fmt_price(price)}</code> · текущий R "
                f"<b>{float(row['current_r']):+.2f}R</b> · обновлено {html.escape(str(checked or '')[:16].replace('T', ' '))} UTC"
                if price is not None and row.get("current_r") is not None
                else "\n   Текущий R: нет подтверждённой проверки"
            )
            lines.append(
                f"\n{direction_icon} <b>#{int(row['id'])} {symbol}</b> {direction} · {strategy}/{timeframe}\n"
                f"   {live_label}{html.escape(status)} · вход <code>{_fmt_price(row.get('entry'))}</code>\n"
                f"   SL <code>{_fmt_price(current_sl)}</code> · {target_text}{plan_text}{progress}{execution_text}"
            )
        else:
            live_label = "Binance · " if row.get("_view_kind") == "live" else ""
            result = html.escape(str(row.get("result") or "").upper())
            lines.append(
                f"\n{direction_icon} <b>#{int(row['id'])} {symbol}</b> {direction} · {strategy}/{timeframe}\n"
                f"   {live_label}Результат: <b>{result}</b> · вход <code>{_fmt_price(row.get('entry'))}</code>\n"
                f"   SL <code>{_fmt_price(row.get('sl'))}</code> · TP1 <code>{_fmt_price(row.get('tp1'))}</code> · {date_text}"
                f"{_manager_return(row)}"
            )
        if len("\n".join(lines)) > 3600:
            lines.pop()
            break
    if analytical_shown:
        lines.append("\n<i>Это история сигналов. Аналитический результат не означает исполненную сделку на Binance. Текущий результат обновляется по цене Gate при нажатии «Обновить»; расчёты — без плеча, комиссий, funding и проскальзывания; при трейлинге это не цена выхода и не итоговый PnL. Денежный PnL счёта не рассчитан.</i>")
    elif category in {"closed", "take", "stop"}:
        lines.append("\n<i>Результат Manager не подтверждает чистый PnL счёта с комиссиями, funding и частичными выходами.</i>")
    elif category == "active":
        lines.append("\n<i>R обновляется после проверки Manager; нажмите «Обновить» для новых данных.</i>")
    return "\n".join(lines)



def trade_line(symbol,direction,status):
    return f"{symbol} · {direction} · {status}"

__all__=["enrich_active_analytical_progress","fetch_live_trades","fetch_trade_view_rows","format_trade_view","trade_line"]
