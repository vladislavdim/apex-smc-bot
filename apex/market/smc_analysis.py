"""Legacy SMC timeframe orchestration with injected market dependencies."""

from __future__ import annotations

from collections.abc import Callable, Mapping


def _canonical_smc_tf(symbol: str, interval: str):
    from core.smc_engine import smc_tf
    return smc_tf(symbol, interval)


class LegacySmcAnalysis:
    def __init__(
        self,
        *,
        engine_available: Callable[[], bool],
        smart_multi_tf: Callable[[str, list | None], object],
        get_candles: Callable[[str, str, int], list],
        get_confirmed_candles: Callable[[list], list],
        find_swings: Callable[[list], tuple],
        classify_swings: Callable[[list, list], object],
        detect_events: Callable[[list, object], list],
        timeframe_labels: Mapping[str, str],
        smc_tf: Callable[[str, str], object] = _canonical_smc_tf,
    ):
        self._engine_available = engine_available
        self._smart_multi_tf = smart_multi_tf
        self._get_candles = get_candles
        self._get_confirmed_candles = get_confirmed_candles
        self._find_swings = find_swings
        self._classify_swings = classify_swings
        self._detect_events = detect_events
        self._timeframe_labels = timeframe_labels
        self._smc_tf = smc_tf

    def smc_on_tf(self, symbol: str, interval: str):
        if self._engine_available():
            try:
                result = self._smc_tf(symbol, interval)
                if result and result.get("direction"):
                    return result["direction"]
            except Exception:
                pass
        candles = self._get_confirmed_candles(
            self._get_candles(symbol, interval, 150),
        )
        if len(candles) < 20:
            return None
        highs, lows = self._find_swings(candles)
        classified = self._classify_swings(highs, lows)
        events = self._detect_events(candles, classified)
        return events[0]["direction"] if events else None

    def multi_tf_analysis(self, symbol: str, timeframes: list | None = None):
        if self._engine_available():
            return self._smart_multi_tf(symbol, timeframes)
        selected = timeframes or ["15m", "1h", "4h", "1d"]
        results = {
            timeframe: self.smc_on_tf(symbol, timeframe)
            for timeframe in selected
        }
        bullish = [timeframe for timeframe, value in results.items() if value == "BULLISH"]
        bearish = [timeframe for timeframe, value in results.items() if value == "BEARISH"]
        total = len(selected)
        if len(bullish) > len(bearish):
            direction, matched = "BULLISH", bullish
        elif len(bearish) > len(bullish):
            direction, matched = "BEARISH", bearish
        else:
            return None
        match_count = len(matched)
        if match_count == total and total >= 3:
            grade, emoji, stars = "МЕГА ТОП", "🔥🔥🔥", "⭐⭐⭐⭐⭐"
        elif match_count >= 3:
            grade, emoji, stars = "ТОП СДЕЛКА", "🔥🔥", "⭐⭐⭐⭐"
        elif match_count == 2:
            grade, emoji, stars = "ХОРОШАЯ", "✅", "⭐⭐⭐"
        else:
            grade, emoji, stars = "СЛАБАЯ", "⚠️", "⭐⭐"
        status = ""
        for timeframe in selected:
            value = results.get(timeframe)
            icon = "🟢" if value == "BULLISH" else "🔴" if value == "BEARISH" else "⚪️"
            status += (
                f"{icon} {self._timeframe_labels.get(timeframe, timeframe)}: "
                f"{value or 'нет сигнала'}\n"
            )
        return {
            "direction": direction, "matched": matched,
            "match_count": match_count, "total": total, "grade": grade,
            "grade_emoji": emoji, "stars": stars, "tf_status": status,
            "results": results,
        }


__all__ = ["LegacySmcAnalysis"]
