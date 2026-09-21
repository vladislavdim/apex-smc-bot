"""Point-in-time relative performance against BTC/ETH benchmarks."""

from __future__ import annotations

from typing import Any, Iterable, Mapping


def _returns(candles: Iterable[Mapping[str, Any]]) -> dict[float, float]:
    rows = sorted(
        (row for row in candles if row.get("is_closed") is True and row.get("close_time") is not None),
        key=lambda row: float(row["close_time"]),
    )
    result: dict[float, float] = {}
    for previous, current in zip(rows, rows[1:]):
        start = float(previous["close"])
        if start:
            result[float(current["close_time"])] = float(current["close"]) / start - 1
    return result


def relative_strength(
    symbol_candles: Iterable[Mapping[str, Any]],
    benchmark_candles: Iterable[Mapping[str, Any]],
    *,
    benchmark: str,
) -> dict[str, Any]:
    symbol_returns, benchmark_returns = _returns(symbol_candles), _returns(benchmark_candles)
    common = sorted(set(symbol_returns) & set(benchmark_returns))
    if not common:
        return {"available": False, "benchmark": benchmark, "alignment": "closed_candle_time"}
    symbol_total = 1.0
    benchmark_total = 1.0
    for timestamp in common:
        symbol_total *= 1 + symbol_returns[timestamp]
        benchmark_total *= 1 + benchmark_returns[timestamp]
    return {
        "available": True, "source": "gate", "mode": "LIVE_CONTEXT",
        "benchmark": benchmark, "samples": len(common),
        "symbol_return": symbol_total - 1,
        "benchmark_return": benchmark_total - 1,
        "excess_return": symbol_total - benchmark_total,
        "alignment": "closed_candle_time",
    }


__all__ = ["relative_strength"]
