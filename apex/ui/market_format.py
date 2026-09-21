"""Presentation-only formatting for market context cards."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from .price_format import smart_price_fmt


def format_market_prices(market: Mapping[str, Mapping[str, Any]]) -> str:
    """Format a compact live-price list without owning market-data access."""
    if not market:
        return "Данные недоступны"
    lines = []
    for pair, data in market.items():
        emoji = "🟢" if data["change"] >= 0 else "🔴"
        price = data["price"]
        price_text = (
            f"${price:,.2f}" if price >= 1000
            else f"${price:.3f}" if price >= 1
            else f"${price:.6f}"
        )
        lines.append(
            f"{emoji} {pair.replace('USDT', '')}: {price_text} "
            f"({data['change']:+.2f}%)"
        )
    return "\n".join(lines)


def format_accumulation(acc: Mapping[str, Any]) -> str:
    score = acc["score"]
    if score >= 80:
        grade, grade_note = "🔥🔥🔥 МЕГА НАКОПЛЕНИЕ", "Высокая вероятность памп"
    elif score >= 60:
        grade, grade_note = "🔥🔥 СИЛЬНОЕ НАКОПЛЕНИЕ", "Следи внимательно"
    else:
        grade, grade_note = "🔥 НАКОПЛЕНИЕ", "Ранняя стадия"
    signals_text = "\n".join(acc["signals"])
    price = acc["price"]
    price_text = (
        f"${price:,.4f}" if price < 1
        else f"${price:,.3f}" if price < 100
        else f"${price:,.2f}"
    )
    separator = "━" * 26
    low = smart_price_fmt(acc.get("low_min", price))
    high = smart_price_fmt(acc.get("high_max", price))
    target = ""
    if acc.get("pump_target"):
        target = (
            f"\n🎯 <b>Цель памп:</b> <code>{smart_price_fmt(acc['pump_target'])}</code> "
            f"(+{acc.get('pump_target_pct', 0):.1f}%)\n"
            f"💡 <i>{acc.get('pump_logic', '')}</i>\n"
        )
    return (
        f"{separator}\n{grade}\n📦 <b>{acc['symbol']}</b> | {grade_note}\n"
        f"{separator}\n\n💰 Цена: <code>{price_text}</code>\n"
        f"📐 Диапазон: <code>{low}</code> — <code>{high}</code>\n"
        f"📊 Скор накопления: <b>{score}/100</b>\n{target}\n"
        f"<b>Признаки:</b>\n{signals_text}\n\n"
        f"💡 <i>Войти при пробое <code>{high}</code> с объёмом</i>\n{separator}"
    )


def format_news(news_items: Sequence[Mapping[str, Any]]) -> str:
    if not news_items:
        return "Новости временно недоступны"
    lines = []
    for item in news_items:
        date = f"[{item['date']}] " if item["date"] else ""
        lines.append(f"📰 {date}<b>{item['title']}</b> — {item['source']}")
    return "\n\n".join(lines)


__all__ = ["format_accumulation", "format_market_prices", "format_news"]
