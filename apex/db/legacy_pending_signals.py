"""Injected compatibility monitor for delivered pending signals."""

from __future__ import annotations

import logging
from datetime import datetime


def configure_pending_signal_monitor(
    *, get_db_conn_fn, get_live_prices_fn, get_candles_fn, connector,
    database_path, lifecycle_available, lifecycle_active, lifecycle_cancelled,
    lifecycle_waiting, lifecycle_state_for, lifecycle_activated_at_for,
    lifecycle_touch, lifecycle_entry_touched, lifecycle_mark_active,
    lifecycle_barrier_hits, lifecycle_mark_finished, emit_trade_stats_event,
) -> None:
    """Inject every runtime dependency without granting execution authority."""
    global get_db_conn, get_live_prices, get_candles
    global _connect_compatibility_db, DB_PATH, _SIGNAL_LIFECYCLE_OK
    global _LIFECYCLE_ACTIVE, _LIFECYCLE_CANCELLED, _LIFECYCLE_WAITING
    global _lifecycle_state_for, _lifecycle_activated_at_for, _lifecycle_touch
    global _lifecycle_entry_touched, _lifecycle_mark_active
    global _lifecycle_barrier_hits, _lifecycle_mark_finished
    global _emit_trade_stats_event
    get_db_conn = get_db_conn_fn
    get_live_prices = get_live_prices_fn
    get_candles = get_candles_fn
    _connect_compatibility_db = connector
    DB_PATH = database_path
    _SIGNAL_LIFECYCLE_OK = lifecycle_available
    _LIFECYCLE_ACTIVE = lifecycle_active
    _LIFECYCLE_CANCELLED = lifecycle_cancelled
    _LIFECYCLE_WAITING = lifecycle_waiting
    _lifecycle_state_for = lifecycle_state_for
    _lifecycle_activated_at_for = lifecycle_activated_at_for
    _lifecycle_touch = lifecycle_touch
    _lifecycle_entry_touched = lifecycle_entry_touched
    _lifecycle_mark_active = lifecycle_mark_active
    _lifecycle_barrier_hits = lifecycle_barrier_hits
    _lifecycle_mark_finished = lifecycle_mark_finished
    _emit_trade_stats_event = emit_trade_stats_event


def check_pending_signals():
    """Проверяем открытые сигналы — сработал ли TP/SL"""
    try:
        conn = get_db_conn()
        pending = conn.execute(
            """SELECT id, symbol, direction, entry, tp1, tp2, tp3, sl,
                      timeframe, grade, created_at, signal_type, estimated_hours,
                      tp1_hit, trailing_sl, best_price, confluence, regime, learning_id
               FROM signals WHERE result='pending'"""
        ).fetchall()
        conn.close()

        # Expiry по стратегии (часы)
        _STRATEGY_EXPIRY = {"FAST": 4, "MTF": 72, "SWING": 96, "WYCKOFF": 504}
        # Trailing коэффициенты: после TP1 переносим SL на entry + X% от (tp1-entry)
        _TRAIL_COEFF = {"FAST": 0.3, "MTF": 0.4, "SWING": 0.5, "WYCKOFF": 0.5, "ZONE": 0.4}

        closed = []
        prices = get_live_prices()
        for row in pending:
            (sig_id, symbol, direction, entry, tp1, tp2, tp3, sl, timeframe,
             grade, created_at, signal_type, estimated_hours, tp1_hit_flag,
             trailing_sl, best_price, confluence, regime, learning_id_value) = row
            tp1_hit_flag = tp1_hit_flag or 0
            current = None
            if symbol in prices:
                current = prices[symbol]["price"]
            if current is None:
                continue
            created = datetime.fromisoformat(created_at)
            hours_elapsed = (datetime.now() - created).total_seconds() / 3600

            lifecycle_state = _LIFECYCLE_ACTIVE
            lifecycle_activated_at = None
            if _SIGNAL_LIFECYCLE_OK:
                try:
                    _lc = get_db_conn(timeout=10)
                    lifecycle_state = _lifecycle_state_for(_lc, sig_id)
                    lifecycle_activated_at = _lifecycle_activated_at_for(_lc, sig_id)
                    _lifecycle_touch(_lc, sig_id)
                    _lc.commit()
                    _lc.close()
                except Exception as _lc_error:
                    logging.warning("[SignalLifecycle] state read %s: %s", sig_id, _lc_error)

            # The current 5m candle can include price action from before the
            # signal was delivered or activated.  Candle high/low becomes safe
            # only after one full five-minute boundary since activation.
            interval_low = interval_high = current
            use_interval = lifecycle_state == _LIFECYCLE_ACTIVE
            if lifecycle_activated_at:
                try:
                    activated = datetime.fromisoformat(lifecycle_activated_at)
                    use_interval = (datetime.now() - activated).total_seconds() >= 300
                except (TypeError, ValueError):
                    use_interval = False
            if use_interval:
                try:
                    _obs = get_candles(symbol, "5m", 3)
                    if _obs:
                        interval_low = min(float(_obs[-1]["low"]), current)
                        interval_high = max(float(_obs[-1]["high"]), current)
                except Exception:
                    pass

            _sig_type_check = (signal_type or "").upper()
            _expiry_h = _STRATEGY_EXPIRY.get(_sig_type_check, estimated_hours or 72)

            if lifecycle_state == _LIFECYCLE_WAITING:
                invalidated = (
                    direction == "BULLISH" and current <= sl
                ) or (
                    direction == "BEARISH" and current >= sl
                )
                target_passed = (
                    direction == "BULLISH" and current >= tp1
                ) or (
                    direction == "BEARISH" and current <= tp1
                )
                if invalidated or target_passed or hours_elapsed > _expiry_h:
                    reason = (
                        "stop_reached_before_confirmed_entry" if invalidated else
                        "target_reached_without_entry" if target_passed else
                        "entry_not_filled_before_expiry"
                    )
                    _cc = get_db_conn(timeout=10)
                    _cc.execute(
                        "UPDATE signals SET result='cancelled', closed_at=CURRENT_TIMESTAMP WHERE id=?",
                        (sig_id,),
                    )
                    if _SIGNAL_LIFECYCLE_OK:
                        _lifecycle_mark_finished(_cc, sig_id, _LIFECYCLE_CANCELLED, reason)
                    _cc.commit(); _cc.close()
                    logging.info("[SignalLifecycle] %s cancelled unfilled: %s", symbol, reason)
                    closed.append({
                        "signal_id": sig_id, "symbol": symbol, "result": "cancelled",
                        "hours": round(hours_elapsed, 1), "grade": grade,
                        "is_win": False, "reason": reason,
                    })
                    continue

                if not _lifecycle_entry_touched(
                    direction, entry, current=current
                ):
                    continue

                _ac = get_db_conn(timeout=10)
                if _SIGNAL_LIFECYCLE_OK:
                    _lifecycle_mark_active(_ac, sig_id)
                _ac.commit(); _ac.close()
                logging.info("[SignalLifecycle] %s entry activated at %s", symbol, entry)
                _emit_trade_stats_event(
                    "OPEN", sig_id, symbol, _sig_type_check, direction, entry, sl, tp1, tp2, tp3,
                    hours=hours_elapsed,
                )
                # Never infer entry→TP/SL ordering from the activation bar.
                continue

            result = None
            hit_tp = None
            _active_sl = trailing_sl if trailing_sl else sl

            # ── Trailing Stop Logic ──
            if tp1_hit_flag:
                # TP1 уже достигнут — отслеживаем best_price и trailing SL → TP2.
                # Используем high/low интерва, а не только текущий snapshot.
                _bp = best_price or entry
                if direction == "BULLISH":
                    _bp = max(_bp, interval_high)
                else:
                    _bp = min(_bp, interval_low)

                _hits = _lifecycle_barrier_hits(
                    direction, _active_sl, tp1, tp2, interval_low, interval_high
                )
                if _hits["sl"] and _hits["tp2"]:
                    # Порядок внутри 5m свечи неизвестен: не завышаем WR.
                    result, hit_tp = "tp1", 1
                elif _hits["tp2"]:
                    result, hit_tp = "tp2", 2
                elif _hits["sl"]:
                    # TP1 уже зафиксирован, trailing-выход не является SL.
                    result, hit_tp = "tp1", 1

                # Обновляем best_price и trailing_sl
                _trail_c = _TRAIL_COEFF.get(_sig_type_check, 0.4)
                if direction == "BULLISH":
                    _new_trail = _bp - abs(tp1 - entry) * _trail_c
                    if not trailing_sl or _new_trail > trailing_sl:
                        trailing_sl = round(_new_trail, 8)
                else:
                    _new_trail = _bp + abs(entry - tp1) * _trail_c
                    if not trailing_sl or _new_trail < trailing_sl:
                        trailing_sl = round(_new_trail, 8)

                # Сохраняем trailing state
                try:
                    _tc = get_db_conn(timeout=10)
                    _tc.execute("UPDATE signals SET best_price=?, trailing_sl=? WHERE id=?", (_bp, trailing_sl, sig_id))
                    _tc.commit()
                    _tc.close()
                except Exception:
                    pass
            else:
                # TP1 ещё не достигнут. При одновременном касании SL и TP
                # внутри одной 5m свечи засчитываем SL: иначе WR будет завышен.
                _hits = _lifecycle_barrier_hits(
                    direction, sl, tp1, tp2, interval_low, interval_high
                )
                if _hits["sl"]:
                    result = "sl"
                elif _sig_type_check == "FAST":
                    if _hits["tp2"]:
                        result, hit_tp = "tp2", 2
                    elif _hits["tp1"]:
                        result, hit_tp = "tp1", 1
                elif _hits["tp2"]:
                    # Сигнал был активен до этого интерва, поэтому TP2
                    # невозможен без предварительного прохода TP1.
                    result, hit_tp = "tp2", 2
                elif _hits["tp1"]:
                    # TP1 hit — НЕ закрываем, включаем trailing.
                    hit_tp = 1
                    _trail_c = _TRAIL_COEFF.get(_sig_type_check, 0.4)
                    if direction == "BULLISH":
                        _new_trail_sl = round(entry + abs(tp1 - entry) * _trail_c, 8)
                        _new_best = interval_high
                    else:
                        _new_trail_sl = round(entry - abs(entry - tp1) * _trail_c, 8)
                        _new_best = interval_low
                    try:
                        _tc = get_db_conn(timeout=10)
                        _tc.execute(
                            "UPDATE signals SET tp1_hit=1, trailing_sl=?, best_price=? WHERE id=?",
                            (_new_trail_sl, _new_best, sig_id),
                        )
                        _tc.commit()
                        _tc.close()
                        logging.info(f"[Trailing] {symbol} TP1 hit! Trail SL → {_new_trail_sl}")
                    except Exception:
                        pass
                    closed.append({
                        "signal_id": sig_id, "symbol": symbol,
                        "result": "tp1_hit", "hours": round(hours_elapsed, 1),
                        "grade": grade, "is_win": False,
                        "trailing_sl": _new_trail_sl, "tp2": tp2,
                        "entry": entry, "direction": direction,
                    })
                    # Не даём expiry в том же цикле отменить trailing.
                    continue

            # Expiry: используем estimated_hours или стратегию, fallback 72ч
            _sig_type = (signal_type or "").upper()
            _expiry_h = _STRATEGY_EXPIRY.get(_sig_type, estimated_hours or 72)
            if not result and hours_elapsed > _expiry_h:
                result = "expired"

            if result:
                conn2 = _connect_compatibility_db(DB_PATH, timeout=30, check_same_thread=False)
                conn2.execute(
                    "UPDATE signals SET result=?, closed_at=CURRENT_TIMESTAMP WHERE id=?",
                    (result, sig_id)
                )
                if _SIGNAL_LIFECYCLE_OK:
                    _lifecycle_mark_finished(conn2, sig_id, "closed", result)
                conn2.commit()
                conn2.close()

                if result in ("sl", "tp1", "tp2", "tp3"):
                    _exit_for_stats = (
                        _active_sl if result == "sl" else
                        tp1 if result == "tp1" else
                        tp2 if result == "tp2" else tp3
                    )
                    _emit_trade_stats_event(
                        "CLOSE", sig_id, symbol, _sig_type_check, direction, entry, sl, tp1, tp2, tp3,
                        result=result, exit_price=_exit_for_stats, hours=hours_elapsed,
                    )

                is_win = result in ("tp1", "tp2", "tp3")
                # Получаем confluence и regime из БД для этого сигнала
                try:
                    _row_extra = _connect_compatibility_db(DB_PATH, timeout=30, check_same_thread=False).execute(
                        "SELECT confluence, regime FROM signals WHERE id=?", (sig_id,)
                    ).fetchone()
                    _confluence_val = _row_extra[0] if _row_extra and _row_extra[0] else 0
                    _regime_val = _row_extra[1] if _row_extra and _row_extra[1] else "UNKNOWN"
                except Exception:
                    _confluence_val, _regime_val = 0, "UNKNOWN"

                closed.append({
                    "signal_id": sig_id,
                    "symbol": symbol,
                    "result": result,
                    "hours": round(hours_elapsed, 1),
                    "grade": grade,
                    "is_win": is_win,
                    "direction": direction,
                    "entry": entry,
                    "sl": sl,
                    "tp1": tp1,
                    "tp2": tp2,
                    "tp3": tp3,
                    "exit_price": (
                        _active_sl if result == "sl" else
                        tp1 if result == "tp1" else
                        tp2 if result == "tp2" else
                        tp3 if result == "tp3" else current
                    ),
                })

        return closed
    except Exception as e:
        logging.error(f"Check signals error: {e}")
        return []

# ===== ЖИВОЙ АНАЛИЗ — ГДЕ МЫ СЕЙЧАС =====



__all__ = ["check_pending_signals", "configure_pending_signal_monitor"]
