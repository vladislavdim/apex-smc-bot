"""BTC directional context used by legacy strategy adapters."""

from __future__ import annotations

from collections.abc import Callable


class BtcDirectionFilter:
    def __init__(self, get_candles: Callable[[str, str, int], list]):
        self._get_candles = get_candles

    def one_hour_change(self) -> float:
        return self._average_change("1h")

    def four_hour_change(self) -> float:
        return self._average_change("4h")

    def allows_signal(
        self,
        direction: str,
        use_4h: bool = False,
    ) -> tuple[bool, str]:
        """Apply the legacy ±0.8% BTC directional guard."""
        change = self.four_hour_change() if use_4h else self.one_hour_change()
        timeframe = "4h" if use_4h else "1h"
        if direction == "BULLISH" and change < -0.8:
            return False, f"BTC падает {change:.1f}%/{timeframe} — лонги опасны"
        if direction == "BEARISH" and change > 0.8:
            return False, f"BTC растёт {change:.1f}%/{timeframe} — шорты опасны"
        return True, ""

    def _average_change(self, timeframe: str) -> float:
        try:
            candles = self._get_candles("BTCUSDT", timeframe, 5)
            if not candles or len(candles) < 4:
                return 0.0
            changes = []
            for index in range(-3, 0):
                previous, current = candles[index - 1], candles[index]
                changes.append(
                    (current["close"] - previous["close"])
                    / previous["close"] * 100
                )
            return round(sum(changes) / len(changes), 3)
        except Exception:
            return 0.0


__all__ = ["BtcDirectionFilter"]
