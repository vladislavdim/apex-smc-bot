"""Execution-quality calculations."""
from __future__ import annotations


def slippage_bps(expected_price: float, fill_price: float) -> float:
    expected = float(expected_price)
    if expected <= 0:
        raise ValueError("expected_price_must_be_positive")
    return (float(fill_price) - expected) / expected * 10_000.0


__all__ = ["slippage_bps"]
