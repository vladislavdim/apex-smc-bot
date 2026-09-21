"""Frozen legacy OB/FVG lookup helpers used during the V3 migration."""

from __future__ import annotations


def find_ob(candles: list, direction: str) -> dict | None:
    for index in range(len(candles) - 2, max(0, len(candles) - 25), -1):
        candle = candles[index]
        if direction == "BULLISH" and candle["close"] < candle["open"]:
            return {
                "top": max(candle["open"], candle["close"]),
                "bottom": min(candle["open"], candle["close"]),
                "index": index,
            }
        if direction == "BEARISH" and candle["close"] > candle["open"]:
            return {
                "top": max(candle["open"], candle["close"]),
                "bottom": min(candle["open"], candle["close"]),
                "index": index,
            }
    return None


def find_fvg(candles: list, direction: str) -> dict | None:
    for index in range(len(candles) - 3, max(1, len(candles) - 20), -1):
        if direction == "BULLISH" and candles[index + 1]["low"] > candles[index - 1]["high"]:
            return {
                "top": candles[index + 1]["low"],
                "bottom": candles[index - 1]["high"],
                "index": index,
            }
        if direction == "BEARISH" and candles[index + 1]["high"] < candles[index - 1]["low"]:
            return {
                "top": candles[index - 1]["low"],
                "bottom": candles[index + 1]["high"],
                "index": index,
            }
    return None


__all__ = ["find_fvg", "find_ob"]
