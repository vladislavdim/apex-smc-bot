"""State-owned analytical monitor over every unprocessed closed candle."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from apex.db.repositories.signal_lifecycle import SignalLifecycleRepository
from apex.market.candles import candle_close_time, candle_open_time
from core.signal_lifecycle import barrier_hits, entry_touched

_EXPIRY = {"FAST": 4, "MTF": 72, "SWING": 96, "WYCKOFF": 504}
_TRAIL = {"FAST": .3, "MTF": .4, "SWING": .5, "WYCKOFF": .5, "ZONE": .4}


def _epoch(value: str | datetime) -> float:
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


class StateSignalMonitor:
    def __init__(self, repository: SignalLifecycleRepository,
                 get_prices: Callable[[], dict[str, Any]],
                 get_candles: Callable[[str, str, int], list[dict[str, Any]]],
                 emit_stats: Callable[..., Any]) -> None:
        self.repository = repository
        self.get_prices = get_prices
        self.get_candles = get_candles
        self.emit_stats = emit_stats

    def check(self, *, now: datetime | None = None) -> list[dict[str, Any]]:
        observed_at = now or datetime.now(timezone.utc)
        now_epoch = _epoch(observed_at)
        prices = self.get_prices()
        closed: list[dict[str, Any]] = []
        for row in self.repository.pending_for_monitor():
            symbol = str(row["symbol"])
            raw = prices.get(symbol)
            current = raw.get("price") if isinstance(raw, dict) else raw
            if current is None:
                continue
            current = float(current)
            signal_id = int(row["signal_id"])
            direction = str(row["direction"])
            strategy = str(row["signal_type"]).upper()
            entry, sl, tp1, tp2, tp3 = (float(row[field]) for field in
                                         ("entry", "sl", "tp1", "tp2", "tp3"))
            hours = (now_epoch - _epoch(str(row["created_at"]))) / 3600
            expiry = _EXPIRY.get(strategy, row["estimated_hours"] or 72)
            status = str(row["status"])
            if status == "waiting_entry":
                invalidated = current <= sl if direction == "BULLISH" else current >= sl
                target_passed = current >= tp1 if direction == "BULLISH" else current <= tp1
                if invalidated or target_passed or hours > expiry:
                    reason = ("stop_reached_before_confirmed_entry" if invalidated else
                              "target_reached_without_entry" if target_passed else
                              "entry_not_filled_before_expiry")
                    if self.repository.advance_monitor(signal_id, expected_status=status,
                                                       transition="cancel", result="cancelled", reason=reason):
                        closed.append({"signal_id": signal_id, "symbol": symbol,
                                       "result": "cancelled", "hours": round(hours, 1),
                                       "grade": row["grade"], "is_win": False, "reason": reason})
                elif entry_touched(direction, entry, current=current):
                    if self.repository.advance_monitor(signal_id, expected_status=status,
                                                       transition="activate"):
                        self.emit_stats("OPEN", signal_id, symbol, strategy, direction,
                                        entry, sl, tp1, tp2, tp3, hours=hours)
                continue

            activated_at = _epoch(str(row["activated_at"])) if row["activated_at"] else now_epoch
            cursor = row.get("monitor_bar_close")
            since = float(cursor) if cursor is not None else activated_at
            elapsed = max(0.0, now_epoch - since)
            # Five-minute bars; request enough history to bridge a normal outage.
            needed = min(1000, max(3, int(elapsed / 300) + 3))
            try:
                candles = self.get_candles(symbol, "5m", needed)
            except Exception:
                candles = []
            ordered = []
            for candle in candles or []:
                if not isinstance(candle, dict):
                    continue
                opened = candle_open_time(candle)
                bar_close = candle_close_time(candle, "5m") if opened is not None else None
                if (bar_close is not None and opened >= activated_at and
                        since < bar_close <= now_epoch):
                    ordered.append((bar_close, candle))
            ordered.sort(key=lambda item: item[0])
            # If the requested history is truncated, never skip an unknown bar.
            if ordered and ordered[0][0] > since + 600:
                continue
            # Legacy tests/data without timestamps use the current interval only.
            if not ordered and candles and all(candle_open_time(c) is None for c in candles):
                ordered = [(None, candles[-1])]
            if not ordered:
                # A point quote cannot prove a barrier was missed during an outage.
                continue
            for bar_close, candle in ordered:
                low = min(float(candle["low"]), current) if bar_close is None else float(candle["low"])
                high = max(float(candle["high"]), current) if bar_close is None else float(candle["high"])
                tp1_hit = bool(row["tp1_hit"])
                trailing_sl = row["trailing_sl"]
                active_sl = float(trailing_sl) if trailing_sl else sl
                best = float(row["best_price"] or entry)
                best = max(best, high) if direction == "BULLISH" else min(best, low)
                result = None
                progress = {}
                if tp1_hit:
                    hits = barrier_hits(direction, active_sl, tp1, tp2, low, high)
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
                    progress = {"trailing_sl": trailing_sl, "best_price": best}
                else:
                    hits = barrier_hits(direction, sl, tp1, tp2, low, high)
                    if hits["sl"]:
                        result = "sl"
                    elif strategy == "FAST":
                        result = "tp2" if hits["tp2"] else "tp1" if hits["tp1"] else None
                    elif hits["tp2"]:
                        result = "tp2"
                    elif hits["tp1"]:
                        offset = abs(tp1 - entry) * _TRAIL.get(strategy, .4)
                        new_trail = round(entry + offset if direction == "BULLISH" else entry - offset, 8)
                        if self.repository.advance_monitor(
                                signal_id, expected_status=status, transition="progress",
                                tp1_hit=True, trailing_sl=new_trail,
                                best_price=high if direction == "BULLISH" else low,
                                monitor_bar_close=bar_close, expected_bar_close=cursor):
                            row.update(tp1_hit=True, trailing_sl=new_trail,
                                       best_price=high if direction == "BULLISH" else low)
                            cursor = bar_close
                            closed.append({"signal_id": signal_id, "symbol": symbol,
                                           "result": "tp1_hit", "hours": round(hours, 1),
                                           "grade": row["grade"], "is_win": False,
                                           "trailing_sl": new_trail, "tp2": tp2,
                                           "entry": entry, "direction": direction})
                            continue
                        break
                if result:
                    if not self.repository.advance_monitor(
                            signal_id, expected_status=status, transition="close", result=result,
                            monitor_bar_close=bar_close, expected_bar_close=cursor):
                        break
                    if result in {"sl", "tp1", "tp2", "tp3"}:
                        self.emit_stats("CLOSE", signal_id, symbol, strategy, direction,
                                        entry, sl, tp1, tp2, tp3, result=result,
                                        exit_price=active_sl if result == "sl" else
                                        tp1 if result == "tp1" else tp2 if result == "tp2" else tp3,
                                        hours=hours)
                    closed.append({"signal_id": signal_id, "symbol": symbol,
                                   "result": result, "hours": round(hours, 1),
                                   "grade": row["grade"], "is_win": result in {"tp1", "tp2", "tp3"},
                                   "direction": direction, "entry": entry, "sl": sl,
                                   "tp1": tp1, "tp2": tp2, "tp3": tp3,
                                   "exit_price": active_sl if result == "sl" else
                                   tp1 if result == "tp1" else tp2 if result == "tp2" else current})
                    break
                if not self.repository.advance_monitor(
                        signal_id, expected_status=status, transition="progress",
                        monitor_bar_close=bar_close, expected_bar_close=cursor, **progress):
                    break
                cursor = bar_close
                row.update(progress)
        return closed


__all__ = ["StateSignalMonitor"]
