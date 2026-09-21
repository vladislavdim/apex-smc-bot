"""Presentation-only formatting for market prices."""

from __future__ import annotations


def smart_price_fmt(price) -> str:
    if price is None or price == 0:
        return "нет данных"
    if price >= 10000:
        return f"{price:,.2f}"
    if price >= 1000:
        return f"{price:,.2f}"
    if price >= 100:
        return f"{price:,.3f}"
    if price >= 10:
        return f"{price:,.4f}"
    if price >= 1:
        return f"{price:,.4f}"
    if price >= 0.1:
        return f"{price:.5f}"
    if price >= 0.01:
        return f"{price:.6f}"
    if price >= 0.001:
        return f"{price:.7f}"
    if price >= 0.0001:
        return f"{price:.8f}"
    return f"{price:.10f}"


__all__ = ["smart_price_fmt"]
