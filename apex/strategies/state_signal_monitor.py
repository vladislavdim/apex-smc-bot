"""State-owned analytical signal monitor; never authorizes exchange actions."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from apex.db.repositories.signal_lifecycle import SignalLifecycleRepository
from core.signal_lifecycle import barrier_hits, entry_touched


_EXPIRY = {"FAST": 4, "MTF": 72, "SWING": 96, "WYCKOFF": 504}
_TRAIL = {"FAST": .3, "MTF": .4, "SWING": .5, "WYCKOFF": .5, "ZONE": .4}


class StateSignalMonitor:
    def __init__(
        self, repository: SignalLifecycleRepository,
        get_prices: Callable[[], dict[str, Any]],
        get_candles: Callable[[str, str, int], list[dict[str, Any]]],
        emit_stats: Callable[..., Any],
    ) -> None:
        self.repository = repository
        self.get_prices = get_prices
        self.get_candles = get_candles
        self.emit_stats = emit_stats

    def check(self, *, now: datetime | None = None) -> list[dict[str, Any]]:
        observed_at = now or datetime.now()
        prices = self.get_prices()
        closed: list[dict[str, Any]] = []
        for row in self.repository.pending_for_monitor():
            signal_id, symbol = int(row["signal_id"]), str(row["symbol"])
            raw = prices.get(symbol)
            current = raw.get("price") if isinstance(raw, dict) else raw
            if current is None:
                continue
            current = float(current)
            direction = str(row["direction"])
            strategy = str(row["signal_type"]).upper()
            entry, sl, tp1, tp2, tp3 = (
                float(row[field]) for field in ("entry", "sl", "tp1", "tp2", "tp3")
            )
            created = datetime.fromisoformat(str(row["created_at"]))
            hours = (observed_at - created).total_seconds() / 3600
            expiry = _EXPIRY.get(strategy, row["estimated_hours"] or 72)
            status = str(row["status"])
            interval_low = interval_high = current
            use_interval = status == "active"
            if row["activated_at"]:
                try:
                    activated = datetime.fromisoformat(str(row["activated_at"]))
                    use_interval = (observed_at - activated).total_seconds() >= 300
                except ValueError:
                    use_interval = False
            if use_interval:
                try:
                    candles = self.get_candles(symbol, "5m", 3)
                    if candles:
                        interval_low = min(float(candles[-1]["low"]), current)
                        interval_high = max(float(candles[-1]["high"]), current)
                except Exception:
                    pass

            if status == "waiting_entry":
                invalidated = current <= sl if direction == "BULLISH" else current >= sl
                target_passed = current >= tp1 if direction == "BULLISH" else current <= tp1
                if invalidated or target_passed or hours > expiry:
                    reason = (
                        "stop_reached_before_confirmed_entry" if invalidated else
                        "target_reached_without_entry" if target_passed else
                        "entry_not_filled_before_expiry"
                    )
                    if self.repository.advance_monitor(
                        signal_id, expected_status=status, transition="cancel",
                        result="cancelled", reason=reason,
                    ):
                        closed.append({"signal_id": signal_id, "symbol": symbol,
                                       "result": "cancelled", "hours": round(hours, 1),
                                       "grade": row["grade"], "is_win": False, "reason": reason})
                    continue
                if entry_touched(direction, entry, current=current):
                    if self.repository.advance_monitor(
                        signal_id, expected_status=status, transition="activate",
                    ):
                        self.emit_stats(
                            "OPEN", signal_id, symbol, strategy, direction,
                            entry, sl, tp1, tp2, tp3, hours=hours,
                        )
                continue

            tp1_hit = bool(row["tp1_hit"])
            trailing_sl = row["trailing_sl"]
            active_sl = float(trailing_sl) if trailing_sl else sl
            result: str | None = None
            if tp1_hit:
                best = float(row["best_price"] or entry)
                best = max(best, interval_high) if direction == "BULLISH" else min(best, interval_low)
                hits = barrier_hits(direction, active_sl, tp1, tp2, interval_low, interval_high)
                if hits["sl"] and hits["tp2"]:
                    result = "tp1"
                elif hits["tp2"]:
                    result = "tp2"
                elif hits["sl"]:
                    result = "tp1"
                offset = abs(tp1 - entry) * _TRAIL.get(strategy, .4)
                candidate = best - offset if direction == "BULLISH" else best + offset
                if not trailing_sl or (candidate > trailing_sl if direction == "BULLISH" else candidate < trailing_sl):
                    trailing_sl = round(candidate, 8)
                if not self.repository.advance_monitor(
                    signal_id, expected_status=status, transition="progress",
                    trailing_sl=trailing_sl, best_price=best,
                ):
                    continue
            else:
                hits = barrier_hits(direction, sl, tp1, tp2, interval_low, interval_high)
                if hits["sl"]:
                    result = "sl"
                elif strategy == "FAST":
                    result = "tp2" if hits["tp2"] else "tp1" if hits["tp1"] else None
                elif hits["tp2"]:
                    result = "tp2"
                elif hits["tp1"]:
                    offset = abs(tp1 - entry) * _TRAIL.get(strategy, .4)
                    new_trail = entry + offset if direction == "BULLISH" else entry - offset
                    if self.repository.advance_monitor(
                        signal_id, expected_status=status, transition="progress",
                        tp1_hit=True, trailing_sl=round(new_trail, 8),
                        best_price=interval_high if direction == "BULLISH" else interval_low,
                    ):
                        closed.append({"signal_id": signal_id, "symbol": symbol,
                                       "result": "tp1_hit", "hours": round(hours, 1),
                                       "grade": row["grade"], "is_win": False,
                                       "trailing_sl": round(new_trail, 8), "tp2": tp2,
                                       "entry": entry, "direction": direction})
                    continue

            if not result and hours > expiry:
                result = "expired"
            if result and self.repository.advance_monitor(
                signal_id, expected_status=status, transition="close", result=result,
            ):
                if result in {"sl", "tp1", "tp2", "tp3"}:
                    self.emit_stats(
                        "CLOSE", signal_id, symbol, strategy, direction,
                        entry, sl, tp1, tp2, tp3, result=result,
                        exit_price=active_sl if result == "sl" else
                        tp1 if result == "tp1" else tp2 if result == "tp2" else tp3,
                        hours=hours,
                    )
                closed.append({
                    "signal_id": signal_id, "symbol": symbol, "result": result,
                    "hours": round(hours, 1), "grade": row["grade"],
                    "is_win": result in {"tp1", "tp2", "tp3"},
                    "direction": direction, "entry": entry, "sl": sl,
                    "tp1": tp1, "tp2": tp2, "tp3": tp3,
                    "exit_price": active_sl if result == "sl" else
                    tp1 if result == "tp1" else tp2 if result == "tp2" else
                    tp3 if result == "tp3" else current,
                })
        return closed


__all__ = ["StateSignalMonitor"]
