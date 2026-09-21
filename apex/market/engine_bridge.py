"""Lazy exports from the canonical SMC engine."""

from __future__ import annotations


def calculate_vwap(candles: list) -> dict:
    from core.smc_engine import calculate_vwap as implementation
    return implementation(candles)


def get_liquidity_heatmap(candles: list) -> dict:
    from core.smc_engine import get_liquidity_heatmap as implementation
    return implementation(candles)


__all__ = ["calculate_vwap", "get_liquidity_heatmap"]
