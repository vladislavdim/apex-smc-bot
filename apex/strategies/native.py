"""Snapshot-only strategy calculations migrated without changing trading gates.

The initial arithmetic and ordered audit gates originate at deployed 0f8c9edd.
Every market dependency belongs to this evaluation's SnapshotContext; this
module does not call symbol detectors, databases, live APIs, or model review.
Legacy detectors remain a separate offline comparison oracle during cutover.
"""

from __future__ import annotations
import logging
from apex.telemetry.event_log import (
    audit_strategy as _audit_strategy,
    audit_test as _audit_test,
    audit_fail as _audit_fail,
    audit_observe as _audit_observe,
)
from apex.domain.models import MarketSnapshot
from apex.market.runtime_cache import get_confirmed_candles
from apex.market.structure_bridge import (
    find_swings,
    find_equal_highs_lows,
    get_bos_choch_event,
)
from apex.market.structure import analyze_market_structure
from apex.market.legacy_zones import find_ob, find_fvg
from apex.market.indicators import average_true_range, ema_value
from apex.market.structural_levels import (
    smart_round,
    select_structural_targets,
    calc_smart_levels,
)
from apex.market.adaptive_indicators import LegacyAdaptiveIndicators
from apex.market.derived_context import LegacyDerivedContext
from apex.market.btc_direction_filter import BtcDirectionFilter
from apex.market.entry_timing import check_entry_timing
from apex.market.time_estimate import get_estimated_time
from apex.strategies.common import fast_session
from apex.ui.price_format import smart_price_fmt
from apex.compatibility.market_constants import TF_LABELS
from core.smc_engine import (
    get_premium_discount,
    calculate_cvd,
    detect_rsi_macd_divergence,
    calculate_vwap,
    detect_breaker_block,
    detect_mm_accumulation,
    get_liquidity_heatmap,
)

_SMC_ENGINE_OK = True


def legacy_strategy_groq_enabled():
    # Model review runs once in the central candidate pipeline.
    return False


def ask_groq(*args, **kwargs):
    raise RuntimeError("detector_model_review_forbidden")


class SnapshotContext:
    def __init__(self, snapshot: MarketSnapshot):
        self.snapshot = snapshot
        self._adaptive = LegacyAdaptiveIndicators(self.get_candles, ema_value)
        self._derived = LegacyDerivedContext(self.get_candles)
        self._btc = BtcDirectionFilter(self.get_candles)

    def get_candles(self, symbol, timeframe, limit=100):
        data = (
            self.snapshot.candles
            if symbol.upper() == self.snapshot.symbol.upper()
            else self.snapshot.market_context.get("reference_candles", {}).get(
                symbol.upper(), {}
            )
        )
        return [dict(row) for row in data.get(timeframe, ())[-max(1, int(limit)) :]]

    def smc_tf(self, symbol, interval):
        candles = get_confirmed_candles(self.get_candles(symbol, interval, 150))
        structure = (
            analyze_market_structure(candles, swing_lookback=5, max_break_age=1)
            if len(candles) >= 20
            else {"direction": None, "event": None}
        )
        return {
            "direction": structure["direction"],
            "source": "APEX_V3_SNAPSHOT",
            "quality": "high" if candles else "none",
            "is_synthetic": False,
            "candles": candles,
            "candles_count": len(candles),
            "structure_event": structure["event"],
        }

    def smc_on_tf(self, symbol, interval):
        result = self.smc_tf(symbol, interval)
        if result.get("direction"):
            return result["direction"]
        # Same secondary structure calculation as the legacy orchestration.
        from apex.market.structure_bridge import classify_swings, detect_events

        candles = self.get_candles(symbol, interval, 150)
        if len(candles) < 20:
            return None
        highs, lows = find_swings(candles)
        events = detect_events(candles, classify_swings(highs, lows))
        return events[0]["direction"] if events else None

    def get_adaptive_params(self, symbol, candles=None):
        return self._adaptive.get_adaptive_params(symbol, candles)

    def get_market_regime(self, symbol):
        return self._derived.get_market_regime(symbol)

    def btc_allows_signal(self, direction, **kwargs):
        timeframe = "4h" if kwargs.get("use_4h") else "1h"
        if len(self.get_candles("BTCUSDT", timeframe, 5)) < 4:
            return False, "BTC snapshot context unavailable"
        return self._btc.allows_signal(direction, **kwargs)

    def get_funding_rate(self, symbol):
        observation = self.snapshot.derivatives_context.get("funding", {})
        value = observation.get("value") if observation.get("available") else None
        if isinstance(value, dict):
            value = value.get("rate")
        return float(value) * 100 if value is not None else None

    def get_fear_greed(self):
        return None

    def get_orderbook(self, symbol):
        return self.snapshot.microstructure_context.get("strategy_orderbook")

    def _fast_session(self):
        return fast_session(self.snapshot.as_of)

    def check_opposing_ob(self, candles, direction, entry, tp):
        """Проверяет нет ли противоположного OB или FVG между entry и TP.
        Возвращает скорректированный TP или None если блокирует."""
        opposing_dir = "BEARISH" if direction == "BULLISH" else "BULLISH"
        opp_ob = find_ob(candles, opposing_dir)
        opp_fvg = find_fvg(candles, opposing_dir)
        blockers = []
        if opp_ob:
            blockers.append(("OB", opp_ob["bottom"], opp_ob["top"]))
        if opp_fvg:
            blockers.append(("FVG", opp_fvg["bottom"], opp_fvg["top"]))
        if not blockers:
            return tp
        for _btype, b_bottom, b_top in blockers:
            if direction == "BULLISH":
                if entry < b_bottom < tp:
                    new_tp = smart_round(b_bottom * 0.998)
                    if new_tp > entry * 1.003:
                        tp = new_tp
                    else:
                        return None
            elif tp < b_top < entry:
                new_tp = smart_round(b_top * 1.002)
                if new_tp < entry * 0.997:
                    tp = new_tp
                else:
                    return None
        return tp

    def _swing_build_ltf_entry(self, symbol: str, direction: str, tp: float) -> dict:
        """Refine a valid 4h SWING thesis into a fresh 1h/15m executable entry.

        The 4h layer defines direction and structural target. Entry timing is delegated
        to closed 1h/15m candles so a thesis can remain alive while the bot waits for
        a fresh BOS/CHoCH, OB/FVG retest, displacement and volume confirmation.
        """
        out = {
            "data_ok": False,
            "structure_ok": False,
            "zone_ok": False,
            "retest_ok": False,
            "displacement_ok": False,
            "volume_ok": False,
            "chase_ok": False,
            "target_ok": False,
            "ready": False,
            "entry": None,
            "sl": None,
            "structure_event": None,
            "zone_type": None,
            "zone": None,
        }
        try:
            c1h = get_confirmed_candles(self.get_candles(symbol, "1h", 61))
            c15 = get_confirmed_candles(self.get_candles(symbol, "15m", 81))
            if not c1h or len(c1h) < 30 or (not c15) or (len(c15) < 30):
                return out
            out["data_ok"] = True
            event = get_bos_choch_event(c1h, direction, lookback=8, max_break_age=2)
            if not event:
                return out
            _swing_bos_age = max(
                1, len(c1h) - int(event.get("candle_index", len(c1h) - 1))
            )
            _audit_observe(
                "bos_event",
                {
                    "role": "SWING_ENTRY_STRUCTURE",
                    "timeframe": "1h",
                    "age_bars": _swing_bos_age,
                    "event_type": event.get("type"),
                    "direction": event.get("direction"),
                },
            )
            _audit_observe("bos_progress", {"structure_confirmed": True})
            out["structure_event"] = event
            out["structure_ok"] = True
            atr1h = average_true_range(c1h, 14)
            if atr1h <= 0:
                return out
            zones = []
            for zone_type, zone in (
                ("OB", find_ob(c1h, direction)),
                ("FVG", find_fvg(c1h, direction)),
            ):
                if not isinstance(zone, dict):
                    continue
                try:
                    bottom, top = (float(zone["bottom"]), float(zone["top"]))
                except (KeyError, TypeError, ValueError):
                    continue
                if top <= bottom:
                    continue
                zones.append((zone_type, bottom, top))
            if not zones:
                return out
            out["zone_ok"] = True
            _audit_observe(
                "bos_progress", {"zone_reached": True, "zone_confirmed": True}
            )
            latest = c15[-1]
            current = float(latest["close"])
            tolerance = atr1h * 0.15
            touched = []
            for zone_type, bottom, top in zones:
                recent_touch = any(
                    (
                        float(c["low"]) <= top + tolerance
                        and float(c["high"]) >= bottom - tolerance
                        for c in c15[-4:]
                    )
                )
                if not recent_touch:
                    continue
                distance = (
                    0.0
                    if bottom <= current <= top
                    else min(abs(current - bottom), abs(current - top))
                )
                touched.append((distance, zone_type, bottom, top))
            if not touched:
                return out
            touched.sort(key=lambda x: x[0])
            distance, zone_type, bottom, top = touched[0]
            out["retest_ok"] = True
            _audit_observe(
                "bos_progress", {"retest_reached": True, "retest_confirmed": True}
            )
            out["zone_type"] = zone_type
            out["zone"] = {"bottom": bottom, "top": top}
            candle_range = float(latest["high"]) - float(latest["low"])
            candle_body = abs(float(latest["close"]) - float(latest["open"]))
            direction_ok = (
                direction == "BULLISH"
                and float(latest["close"]) > float(latest["open"])
                or (
                    direction == "BEARISH"
                    and float(latest["close"]) < float(latest["open"])
                )
            )
            displacement_ok = (
                candle_range > 0 and candle_body / candle_range >= 0.5 and direction_ok
            )
            out["displacement_ok"] = bool(displacement_ok)
            vol_window = c15[-21:-1] if len(c15) >= 21 else c15[:-1]
            avg_vol = (
                sum((float(c.get("volume", 0) or 0) for c in vol_window))
                / len(vol_window)
                if vol_window
                else 0.0
            )
            last_vol = float(latest.get("volume", 0) or 0)
            out["volume_ok"] = bool(avg_vol > 0 and last_vol >= avg_vol * 1.2)
            out["chase_ok"] = bool(distance <= atr1h * 0.75)
            _audit_observe(
                "swing_numeric",
                {
                    "displacement_body_ratio": round(candle_body / candle_range, 6)
                    if candle_range > 0
                    else None,
                    "direction_ok": bool(direction_ok),
                    "directional_displacement_ratio": round(
                        candle_body / candle_range, 6
                    )
                    if candle_range > 0 and direction_ok
                    else 0.0,
                    "displacement_gate_pass": bool(out["displacement_ok"]),
                    "volume_ratio": round(last_vol / avg_vol, 6)
                    if avg_vol > 0
                    else None,
                    "volume_pass_1_2": bool(avg_vol > 0 and last_vol >= avg_vol * 1.2),
                    "volume_pass_1_1_observed": bool(
                        avg_vol > 0 and last_vol >= avg_vol * 1.1
                    ),
                    "retest_distance_atr": round(distance / atr1h, 6)
                    if atr1h > 0
                    else None,
                },
            )
            _audit_observe(
                "bos_progress",
                {
                    "displacement_reached": True,
                    "displacement_confirmed": bool(out["displacement_ok"]),
                    "volume_reached": True,
                    "volume_confirmed": bool(out["volume_ok"]),
                },
            )
            if (
                not out["displacement_ok"]
                or not out["volume_ok"]
                or (not out["chase_ok"])
            ):
                return out
            entry = current
            sw_highs, sw_lows = find_swings(c15, lookback=2)
            if direction == "BULLISH":
                lows_below = [
                    float(level) for _, level in sw_lows if float(level) < entry
                ]
                nearest_swing = max(lows_below) if lows_below else bottom
                anchor = min(bottom, nearest_swing)
                sl = anchor - atr1h * 0.1
                target_ok = float(tp) > entry
            else:
                highs_above = [
                    float(level) for _, level in sw_highs if float(level) > entry
                ]
                nearest_swing = min(highs_above) if highs_above else top
                anchor = max(top, nearest_swing)
                sl = anchor + atr1h * 0.1
                target_ok = float(tp) < entry
            out["target_ok"] = bool(target_ok)
            if not target_ok:
                return out
            if direction == "BULLISH" and sl >= entry:
                return out
            if direction == "BEARISH" and sl <= entry:
                return out
            out["entry"] = smart_round(entry)
            out["sl"] = smart_round(sl)
            out["ready"] = True
            _audit_observe("bos_progress", {"ltf_ready": True})
            return out
        except Exception as exc:
            logging.debug("[SWING LTF] %s refinement failed: %s", symbol, exc)
            return out

    def detect_swing_setup(self, symbol: str, timeframe: str = "4h") -> dict | None:
        """
        Ловит swing сетапы: sweep экстремума → CHoCH → вход.
        Логика: лоу пробит и закрылся выше (бычий sweep) → лонг
                хай пробит и закрылся ниже (медвежий sweep) → шорт
        """
        try:
            raw_candles = self.get_candles(symbol, timeframe, 101)
            candles = get_confirmed_candles(raw_candles)
            if _audit_test(
                "SWING_DETECT_SWING_SETUP_G7123",
                not candles or len(candles) < 20,
                "not candles or len(candles) < 20",
                "not candles or len(candles) < 20",
                7123,
            ):
                return _audit_fail(
                    "SWING_DETECT_SWING_SETUP_R7124",
                    "not candles or len(candles) < 20",
                    locals(),
                    "not candles or len(candles) < 20",
                    7124,
                )
            live_price = raw_candles[-1]["close"]
            closes = [c["close"] for c in candles]
            highs = [c["high"] for c in candles]
            lows = [c["low"] for c in candles]
            price = closes[-1]
            last = candles[-1]
            prev = candles[-2]
            atr = average_true_range(candles, 14)
            _ap_sw = self.get_adaptive_params(symbol, candles)
            _vf_sw = _ap_sw["volatility_factor"]
            swing_highs, swing_lows = find_swings(candles, lookback=7)
            if _audit_test(
                "SWING_DETECT_SWING_SETUP_G7142",
                len(swing_highs) < 2 or len(swing_lows) < 2,
                "Свинг-структура найдена (lookback=7)",
                "len(swing_highs) < 2 or len(swing_lows) < 2",
                7142,
            ):
                return _audit_fail(
                    "SWING_DETECT_SWING_SETUP_R7143",
                    "Свинг-структура найдена (lookback=7)",
                    locals(),
                    "len(swing_highs) < 2 or len(swing_lows) < 2",
                    7143,
                )
            recent_highs = sorted(swing_highs[-3:], key=lambda x: x[0])
            recent_lows = sorted(swing_lows[-3:], key=lambda x: x[0])
            last_swing_high = recent_highs[-1][1]
            last_swing_low = recent_lows[-1][1]
            prev_swing_high = recent_highs[-2][1]
            prev_swing_low = recent_lows[-2][1]
            direction = None
            entry = sl = tp = None
            logic = ""
            trigger_candle = None
            trigger_lookback = 1
            for lookback_i in range(1, 7):
                check = candles[-lookback_i]
                check_prev = candles[-lookback_i - 1]
                base_candles = candles[:-lookback_i] if lookback_i > 0 else candles
                if len(base_candles) < 20:
                    continue
                sh, sl_list = find_swings(base_candles, lookback=5)
                if len(sh) < 2 or len(sl_list) < 2:
                    continue
                rec_h = sorted(sh[-3:], key=lambda x: x[0])
                rec_l = sorted(sl_list[-3:], key=lambda x: x[0])
                chk_high = rec_h[-1][1]
                chk_low = rec_l[-1][1]
                prv_high = rec_h[-2][1]
                prv_low = rec_l[-2][1]
                if (
                    check["low"] < chk_low
                    and check["close"] > chk_low
                    and (chk_low - check["low"] > atr * 0.1)
                    and (check_prev["close"] < check_prev["open"])
                ):
                    direction = "BULLISH"
                    entry = smart_round(check["close"])
                    sl = smart_round(check["low"] - atr * _vf_sw)
                    tp = smart_round(prv_high)
                    logic = "свип лоу ↓ + возврат в диапазон + импульс вверх"
                    trigger_candle = check
                    trigger_lookback = lookback_i
                    break
                if (
                    check["high"] > chk_high
                    and check["close"] < chk_high
                    and (check["high"] - chk_high > atr * 0.1)
                    and (check_prev["close"] > check_prev["open"])
                ):
                    direction = "BEARISH"
                    entry = smart_round(check["close"])
                    sl = smart_round(check["high"] + atr * _vf_sw)
                    tp = smart_round(prv_low)
                    logic = "свип хая ↑ + отклонение + импульс вниз"
                    trigger_candle = check
                    trigger_lookback = lookback_i
                    break
            if not direction:
                try:
                    eqh_level, eql_level = find_equal_highs_lows(candles, lookback=30)
                    last_c = candles[-1]
                    prev_c = candles[-2]
                    if (
                        eql_level
                        and last_c["low"] < eql_level
                        and (last_c["close"] > eql_level)
                    ):
                        wick = (last_c["close"] - last_c["low"]) / (
                            last_c["high"] - last_c["low"] + 1e-06
                        )
                        if wick > 0.4:
                            direction = "BULLISH"
                            entry = smart_round(last_c["close"])
                            sl = smart_round(last_c["low"] - atr * _vf_sw)
                            tp = smart_round(last_swing_high)
                            logic = f"EQL sweep — двойной лоу ${eql_level:.4f} выбит → разворот"
                            trigger_candle = last_c
                            trigger_lookback = 1
                    elif (
                        eqh_level
                        and last_c["high"] > eqh_level
                        and (last_c["close"] < eqh_level)
                    ):
                        wick = (last_c["high"] - last_c["close"]) / (
                            last_c["high"] - last_c["low"] + 1e-06
                        )
                        if wick > 0.4:
                            direction = "BEARISH"
                            entry = smart_round(last_c["close"])
                            sl = smart_round(last_c["high"] + atr * _vf_sw)
                            tp = smart_round(last_swing_low)
                            logic = f"EQH sweep — двойной хай ${eqh_level:.4f} выбит → разворот"
                            trigger_candle = last_c
                            trigger_lookback = 1
                except Exception:
                    pass
            if not direction:
                try:
                    _ob_sw = find_ob(candles, "BULLISH")
                    _fvg_sw = find_fvg(candles, "BULLISH")
                    _atr_sw = atr
                    _price = candles[-1]["close"]
                    if _ob_sw:
                        _in_ob = (
                            _ob_sw["bottom"] - _atr_sw * 0.3
                            <= _price
                            <= _ob_sw["top"] + _atr_sw * 0.3
                        )
                        _bull_candle = candles[-1]["close"] > candles[-1]["open"]
                        _bull_body = abs(candles[-1]["close"] - candles[-1]["open"])
                        _bull_range = candles[-1]["high"] - candles[-1]["low"]
                        _bull_disp = (
                            _bull_body / _bull_range > 0.5 if _bull_range > 0 else False
                        )
                        if _in_ob and _bull_candle and _bull_disp:
                            direction = "BULLISH"
                            entry = smart_round(_price)
                            sl = smart_round(_ob_sw["bottom"] - _atr_sw * 0.5)
                            tp = smart_round(last_swing_high)
                            logic = f"Реакция от OB {smart_price_fmt(_ob_sw['bottom'])}–{smart_price_fmt(_ob_sw['top'])}"
                            trigger_candle = candles[-1]
                            trigger_lookback = 1
                    _ob_bear = find_ob(candles, "BEARISH")
                    if not direction and _ob_bear:
                        _in_ob_b = (
                            _ob_bear["bottom"] - _atr_sw * 0.3
                            <= _price
                            <= _ob_bear["top"] + _atr_sw * 0.3
                        )
                        _bear_candle = candles[-1]["close"] < candles[-1]["open"]
                        _bear_body = abs(candles[-1]["close"] - candles[-1]["open"])
                        _bear_range = candles[-1]["high"] - candles[-1]["low"]
                        _bear_disp = (
                            _bear_body / _bear_range > 0.5 if _bear_range > 0 else False
                        )
                        if _in_ob_b and _bear_candle and _bear_disp:
                            direction = "BEARISH"
                            entry = smart_round(_price)
                            sl = smart_round(_ob_bear["top"] + _atr_sw * 0.5)
                            tp = smart_round(last_swing_low)
                            logic = f"Реакция от OB {smart_price_fmt(_ob_bear['bottom'])}–{smart_price_fmt(_ob_bear['top'])}"
                            trigger_candle = candles[-1]
                            trigger_lookback = 1
                except Exception:
                    pass
            if direction:
                try:
                    _reaction_candles = 0
                    _react_ob = None
                    if direction == "BULLISH":
                        _react_ob = find_ob(candles, "BULLISH")
                    else:
                        _react_ob = find_ob(candles, "BEARISH")
                    if _react_ob:
                        for _rc in candles[-5:]:
                            if direction == "BULLISH":
                                _in_zone_rc = (
                                    _react_ob["bottom"]
                                    <= _rc["low"]
                                    <= _react_ob["top"] * 1.01
                                )
                            else:
                                _in_zone_rc = (
                                    _react_ob["bottom"] * 0.99
                                    <= _rc["high"]
                                    <= _react_ob["top"]
                                )
                            if _in_zone_rc:
                                _reaction_candles += 1
                        if _reaction_candles >= 4:
                            logging.debug(
                                f"[SWING] {symbol}: цена тупит у зоны {_reaction_candles} свечей — слабый сетап"
                            )
                            direction = None
                            entry = None
                except Exception:
                    pass
            if direction:
                try:
                    eqh_level, eql_level = find_equal_highs_lows(candles, lookback=30)
                    if direction == "BULLISH" and eqh_level and (eqh_level > entry):
                        if eqh_level < tp:
                            tp = smart_round(eqh_level)
                        logic = (
                            logic + f" → ликвидность EQH {smart_price_fmt(eqh_level)}"
                        )
                    elif direction == "BEARISH" and eql_level and (eql_level < entry):
                        if eql_level > tp:
                            tp = smart_round(eql_level)
                        logic = (
                            logic + f" ��� ликвидность EQL {smart_price_fmt(eql_level)}"
                        )
                except Exception:
                    pass
            if _audit_test(
                "SWING_DETECT_SWING_SETUP_G7336",
                not direction,
                "not direction",
                "not direction",
                7336,
            ):
                return _audit_fail(
                    "SWING_DETECT_SWING_SETUP_R7337",
                    "not direction",
                    locals(),
                    "not direction",
                    7337,
                )
            if symbol != "BTCUSDT":
                btc_ok, btc_reason = self.btc_allows_signal(direction)
                if _audit_test(
                    "SWING_DETECT_SWING_SETUP_G7350",
                    not btc_ok,
                    "BTC фильтр для SWING",
                    "not btc_ok",
                    7350,
                ):
                    logging.info(
                        f"[SWING BTC Filter] {symbol} {direction} пропущен: {btc_reason}"
                    )
                    return _audit_fail(
                        "SWING_DETECT_SWING_SETUP_R7352",
                        "BTC фильтр для SWING",
                        locals(),
                        "not btc_ok",
                        7352,
                    )
            _swing_4h_volume_ok = False
            _swing_4h_displacement_ok = False
            try:
                from datetime import datetime as _dt_vol

                _vol_hour = self.snapshot.as_of.hour
                _vol_mult = 1.3 if 8 <= _vol_hour <= 21 else 1.2
                sweep_candle = trigger_candle or candles[-1]
                avg_vol = (
                    sum((c["volume"] for c in candles[-20:-1])) / 19
                    if len(candles) >= 20
                    else 0
                )
                sweep_vol = sweep_candle.get("volume", 0)
                _swing_4h_volume_ok = bool(
                    avg_vol > 0 and sweep_vol >= avg_vol * _vol_mult
                )
            except Exception:
                pass
            try:
                _disp_candle = (
                    candles[-trigger_lookback + 1]
                    if trigger_lookback >= 2
                    else trigger_candle or candles[-1]
                )
                _disp_range = _disp_candle["high"] - _disp_candle["low"]
                _disp_body = abs(_disp_candle["close"] - _disp_candle["open"])
                _disp_ratio = _disp_body / _disp_range if _disp_range > 0 else 0.0
                _disp_direction_ok = (
                    direction == "BULLISH"
                    and _disp_candle["close"] > _disp_candle["open"]
                    or (
                        direction == "BEARISH"
                        and _disp_candle["close"] < _disp_candle["open"]
                    )
                )
                _swing_4h_displacement_ok = bool(
                    _disp_ratio >= 0.5 and _disp_direction_ok
                )
            except Exception:
                pass
            _swing_structure_event = get_bos_choch_event(
                candles, direction, lookback=30, max_break_age=max(1, trigger_lookback)
            )
            _audit_test(
                "SWING_4H_STRUCTURE_CONTEXT",
                not _swing_structure_event,
                "4h BOS/CHoCH context after trigger (non-blocking)",
                "not _swing_structure_event",
                7403,
            )
            if not _swing_structure_event:
                logging.info(
                    f"[SWING] {symbol}: 4h BOS/CHoCH не подтверждён — context weak; обязательный 1h trigger остаётся"
                )
            try:
                if trigger_lookback <= 2:
                    pass
                elif _audit_test(
                    "SWING_DETECT_SWING_SETUP_G7411",
                    trigger_lookback > 6,
                    "Reaction speed: sweep recovery within 1-2 candles",
                    "trigger_lookback > 6",
                    7411,
                ):
                    return _audit_fail(
                        "SWING_DETECT_SWING_SETUP_R7412",
                        "Reaction speed: sweep recovery within 1-2 candles",
                        locals(),
                        "trigger_lookback > 6",
                        7412,
                    )
            except Exception:
                pass
            _swing_rsi_bonus = False
            try:
                if _SMC_ENGINE_OK:
                    _sw_rsi_div = detect_rsi_macd_divergence(candles, direction)
                    if _sw_rsi_div.get("found"):
                        _swing_rsi_bonus = True
            except Exception:
                pass
            _swing_cvd_ok = False
            try:
                _sw_cvd = calculate_cvd(candles)
                if _sw_cvd["signal"] == direction[:4] or _sw_cvd["signal"] == direction:
                    _swing_cvd_ok = True
            except Exception:
                pass
            _swing_fvg_ok = False
            _sw_dir_fvg = None
            try:
                _sw_dir_fvg = find_fvg(candles, direction)
                if _sw_dir_fvg:
                    if direction == "BULLISH" and entry <= _sw_dir_fvg["bottom"] <= tp:
                        _swing_fvg_ok = True
                    elif direction == "BEARISH" and tp <= _sw_dir_fvg["top"] <= entry:
                        _swing_fvg_ok = True
            except Exception:
                pass
            _swing_1h_choch = False
            _swing_1h_structure_event = None
            _swing_pd_ok = False
            try:
                if _SMC_ENGINE_OK:
                    _sw_pd = get_premium_discount(candles)
                    if (
                        direction == "BULLISH"
                        and _sw_pd.get("zone") == "DISCOUNT"
                        or (direction == "BEARISH" and _sw_pd.get("zone") == "PREMIUM")
                    ):
                        _swing_pd_ok = True
            except Exception:
                pass
            _swing_thesis_entry = entry
            _swing_thesis_sl = sl
            _swing_thesis_tp = tp
            entry = sl = None
            _ltf = self._swing_build_ltf_entry(symbol, direction, tp)
            if _audit_test(
                "SWING_LTF_DATA",
                not _ltf.get("data_ok"),
                "LTF data: 1h + 15m history",
                "not _ltf.data_ok",
                7460,
            ):
                return _audit_fail(
                    "SWING_LTF_R_DATA",
                    "LTF data: 1h + 15m history",
                    locals(),
                    "not _ltf.data_ok",
                    7460,
                )
            if _audit_test(
                "SWING_LTF_STRUCTURE",
                not _ltf.get("structure_ok"),
                "LTF: fresh 1h BOS/CHoCH",
                "not _ltf.structure_ok",
                7461,
            ):
                return _audit_fail(
                    "SWING_LTF_R_STRUCTURE",
                    "LTF: fresh 1h BOS/CHoCH",
                    locals(),
                    "not _ltf.structure_ok",
                    7461,
                )
            if _audit_test(
                "SWING_LTF_ZONE",
                not _ltf.get("zone_ok"),
                "LTF: 1h OB/FVG zone",
                "not _ltf.zone_ok",
                7462,
            ):
                return _audit_fail(
                    "SWING_LTF_R_ZONE",
                    "LTF: 1h OB/FVG zone",
                    locals(),
                    "not _ltf.zone_ok",
                    7462,
                )
            if _audit_test(
                "SWING_LTF_RETEST",
                not _ltf.get("retest_ok"),
                "LTF: recent 15m retest of 1h OB/FVG",
                "not _ltf.retest_ok",
                7463,
            ):
                return _audit_fail(
                    "SWING_LTF_R_RETEST",
                    "LTF: recent 15m retest of 1h OB/FVG",
                    locals(),
                    "not _ltf.retest_ok",
                    7463,
                )
            if _audit_test(
                "SWING_LTF_DISPLACEMENT",
                not _ltf.get("displacement_ok"),
                "LTF: 15m displacement >= 50% in direction",
                "not _ltf.displacement_ok",
                7464,
            ):
                return _audit_fail(
                    "SWING_LTF_R_DISPLACEMENT",
                    "LTF: 15m displacement >= 50% in direction",
                    locals(),
                    "not _ltf.displacement_ok",
                    7464,
                )
            if _audit_test(
                "SWING_LTF_VOLUME",
                not _ltf.get("volume_ok"),
                "LTF: 15m volume >= 1.2x average",
                "not _ltf.volume_ok",
                7465,
            ):
                return _audit_fail(
                    "SWING_LTF_R_VOLUME",
                    "LTF: 15m volume >= 1.2x average",
                    locals(),
                    "not _ltf.volume_ok",
                    7465,
                )
            if _audit_test(
                "SWING_LTF_NO_CHASE",
                not _ltf.get("chase_ok"),
                "LTF: entry still near retest zone",
                "not _ltf.chase_ok",
                7466,
            ):
                return _audit_fail(
                    "SWING_LTF_R_NO_CHASE",
                    "LTF: entry still near retest zone",
                    locals(),
                    "not _ltf.chase_ok",
                    7466,
                )
            if _audit_test(
                "SWING_LTF_TARGET",
                not _ltf.get("target_ok"),
                "LTF: 4h structural target remains ahead of entry",
                "not _ltf.target_ok",
                7467,
            ):
                return _audit_fail(
                    "SWING_LTF_R_TARGET",
                    "LTF: 4h structural target remains ahead of entry",
                    locals(),
                    "not _ltf.target_ok",
                    7467,
                )
            if _audit_test(
                "SWING_LTF_READY",
                not _ltf.get("ready"),
                "LTF executable entry ready",
                "not _ltf.ready",
                7468,
            ):
                return _audit_fail(
                    "SWING_LTF_R_READY",
                    "LTF executable entry ready",
                    locals(),
                    "not _ltf.ready",
                    7468,
                )
            entry = _ltf["entry"]
            sl = _ltf["sl"]
            _swing_1h_structure_event = _ltf.get("structure_event")
            _swing_1h_choch = bool(_swing_1h_structure_event)
            logic = (
                logic
                + f" -> LTF {_ltf.get('zone_type') or 'zone'} retest + 15m trigger"
            )
            current_price = live_price
            if _audit_test(
                "SWING_DETECT_SWING_SETUP_G7490",
                abs(current_price - entry) > atr * 4,
                "Если sweep был давно — цена могла уйти далеко от входа",
                "abs(current_price - entry) > atr * 4",
                7490,
            ):
                return _audit_fail(
                    "SWING_DETECT_SWING_SETUP_R7491",
                    "Если sweep был давно — цена могла уйти далеко от входа",
                    locals(),
                    "abs(current_price - entry) > atr * 4",
                    7491,
                )
            _adj_tp = self.check_opposing_ob(candles, direction, entry, tp)
            if _audit_test(
                "SWING_DETECT_SWING_SETUP_G7495",
                _adj_tp is None,
                "Проверка противоположного OB между entry и TP",
                "_adj_tp is None",
                7495,
            ):
                return _audit_fail(
                    "SWING_DETECT_SWING_SETUP_R7496",
                    "Проверка противоположного OB между entry и TP",
                    locals(),
                    "_adj_tp is None",
                    7496,
                )
            tp = _adj_tp
            _sl_max_pct = 0.04
            if _audit_test(
                "SWING_DETECT_SWING_SETUP_G7502",
                abs(entry - sl) / max(abs(entry), 1e-12) > _sl_max_pct,
                "candidate instead of pulling SL inside market noise.",
                "abs(entry - sl) / max(abs(entry), 1e-12) > _sl_max_pct",
                7502,
            ):
                return _audit_fail(
                    "SWING_DETECT_SWING_SETUP_R7503",
                    "candidate instead of pulling SL inside market noise.",
                    locals(),
                    "abs(entry - sl) / max(abs(entry), 1e-12) > _sl_max_pct",
                    7503,
                )
            risk = abs(entry - sl)
            reward = abs(tp - entry)
            if _audit_test(
                "SWING_DETECT_SWING_SETUP_G7508",
                risk == 0,
                "Фильтр RR",
                "risk == 0",
                7508,
            ):
                return _audit_fail(
                    "SWING_DETECT_SWING_SETUP_R7509",
                    "Фильтр RR",
                    locals(),
                    "risk == 0",
                    7509,
                )
            rr_check = reward / risk
            _audit_observe(
                "bos_progress", {"rr_reached": True, "rr_passed": bool(rr_check >= 2.0)}
            )
            if _audit_test(
                "SWING_DETECT_SWING_SETUP_G7511",
                rr_check < 2.0,
                "rr_check < 2.0",
                "rr_check < 2.0",
                7511,
            ):
                return _audit_fail(
                    "SWING_DETECT_SWING_SETUP_R7512",
                    "rr_check < 2.0",
                    locals(),
                    "rr_check < 2.0",
                    7512,
                )
            if _audit_test(
                "SWING_DETECT_SWING_SETUP_G7515",
                direction == "BULLISH" and tp <= entry,
                "Фильтр — цель должна быть реальной",
                'direction == "BULLISH" and tp <= entry',
                7515,
            ):
                return _audit_fail(
                    "SWING_DETECT_SWING_SETUP_R7516",
                    "Фильтр — цель должна быть реальной",
                    locals(),
                    'direction == "BULLISH" and tp <= entry',
                    7516,
                )
            if _audit_test(
                "SWING_DETECT_SWING_SETUP_G7517",
                direction == "BEARISH" and tp >= entry,
                "Фильтр — цель должна быть реальной",
                'direction == "BEARISH" and tp >= entry',
                7517,
            ):
                return _audit_fail(
                    "SWING_DETECT_SWING_SETUP_R7518",
                    "Фильтр — цель должна быть реальной",
                    locals(),
                    'direction == "BEARISH" and tp >= entry',
                    7518,
                )
            htf_4h_sw = self.smc_on_tf(symbol, "4h")
            htf_1d_sw = self.smc_on_tf(symbol, "1d")
            htf_dir = htf_1d_sw
            if direction == "BULLISH":
                _4h_against = htf_4h_sw and "BEARISH" in str(htf_4h_sw).upper()
                _1d_against = htf_1d_sw and "BEARISH" in str(htf_1d_sw).upper()
                if _audit_test(
                    "SWING_DETECT_SWING_SETUP_G7528",
                    _4h_against and _1d_against,
                    "_4h_against and _1d_against",
                    "_4h_against and _1d_against",
                    7528,
                ):
                    return _audit_fail(
                        "SWING_DETECT_SWING_SETUP_R7529",
                        "_4h_against and _1d_against",
                        locals(),
                        "_4h_against and _1d_against",
                        7529,
                    )
            elif direction == "BEARISH":
                _4h_against = htf_4h_sw and "BULLISH" in str(htf_4h_sw).upper()
                _1d_against = htf_1d_sw and "BULLISH" in str(htf_1d_sw).upper()
                if _audit_test(
                    "SWING_DETECT_SWING_SETUP_G7533",
                    _4h_against and _1d_against,
                    "_4h_against and _1d_against",
                    "_4h_against and _1d_against",
                    7533,
                ):
                    return _audit_fail(
                        "SWING_DETECT_SWING_SETUP_R7534",
                        "_4h_against and _1d_against",
                        locals(),
                        "_4h_against and _1d_against",
                        7534,
                    )
            htf_1w_swing = self.smc_on_tf(symbol, "1w")
            weekly_warning = ""
            if htf_1w_swing:
                _1w_str = str(htf_1w_swing).upper()
                if direction == "BULLISH" and "BEARISH" in _1w_str:
                    weekly_warning = "⚠️ 1w BEARISH — осторожно с лонгом"
                elif direction == "BEARISH" and "BULLISH" in _1w_str:
                    weekly_warning = "⚠️ 1w BULLISH — осторожно с шортом"
            _swing_15m_confirms = False
            try:
                candles_15m = get_confirmed_candles(self.get_candles(symbol, "15m", 21))
                if candles_15m and len(candles_15m) >= 5:
                    last_15m = candles_15m[-1]
                    body_15m = abs(last_15m["close"] - last_15m["open"])
                    range_15m = (
                        last_15m["high"] - last_15m["low"]
                        if last_15m["high"] != last_15m["low"]
                        else 0.001
                    )
                    is_impulse_15m = body_15m / range_15m > 0.6
                    if (
                        direction == "BULLISH"
                        and last_15m["close"] > last_15m["open"]
                        and is_impulse_15m
                    ):
                        _swing_15m_confirms = True
                    elif (
                        direction == "BEARISH"
                        and last_15m["close"] < last_15m["open"]
                        and is_impulse_15m
                    ):
                        _swing_15m_confirms = True
            except Exception:
                pass
            rr = round(reward / risk, 2)
            sl_pct = round(abs(entry - sl) / entry * 100, 2)
            tp_pct = round(abs(tp - entry) / entry * 100, 2)
            _swing_funding_warning = ""
            try:
                _sw_funding = self.get_funding_rate(symbol)
                if _sw_funding is not None and abs(_sw_funding) > 0.2:
                    if (
                        direction == "BULLISH"
                        and _sw_funding > 0.2
                        or (direction == "BEARISH" and _sw_funding < -0.2)
                    ):
                        _swing_funding_warning = (
                            f"extreme crowded funding {_sw_funding:+.4f}%"
                        )
                        logging.info(
                            "[SWING Funding Warning] %s: %s",
                            symbol,
                            _swing_funding_warning,
                        )
            except Exception:
                pass
            from datetime import datetime as _dt_sw

            _sw_hour = self.snapshot.as_of.hour
            _is_dead_hours = 22 <= _sw_hour or _sw_hour <= 5
            _rr_min = 1.8 if _is_dead_hours else 1.5
            if _audit_test(
                "SWING_DETECT_SWING_SETUP_G7583",
                rr_check < _rr_min,
                "Dead hours penalty для SWING (22:00-06:00 UTC)",
                "rr_check < _rr_min",
                7583,
            ):
                logging.info(
                    f"[SWING] {symbol}: RR {rr_check} < {_rr_min} {('(dead hours)' if _is_dead_hours else '')} — пропускаем"
                )
                return _audit_fail(
                    "SWING_DETECT_SWING_SETUP_R7585",
                    "rr_check < _rr_min",
                    locals(),
                    "rr_check < _rr_min",
                    7585,
                )
            _swing_groq_ok = False
            _swing_ob = None
            try:
                last_candles_summary = []
                for c in candles[-5:]:
                    body = abs(c["close"] - c["open"])
                    wick_up = c["high"] - max(c["open"], c["close"])
                    wick_dn = min(c["open"], c["close"]) - c["low"]
                    color = "🟢" if c["close"] > c["open"] else "🔴"
                    last_candles_summary.append(
                        f"{color} O={smart_round(c['open'])} H={smart_round(c['high'])} L={smart_round(c['low'])} C={smart_round(c['close'])}"
                    )
                tf_hours = {"1h": 1, "4h": 4, "1d": 24, "1w": 168}
                candle_hours = tf_hours.get(timeframe, 4)
                distance_to_tp = abs(tp - entry)
                est_candles = round(distance_to_tp / atr, 1) if atr > 0 else 3
                est_hours = int(round(est_candles * candle_hours, 0))
                est_hours = max(12, min(est_hours, 96))
                candles_str = " | ".join(last_candles_summary)
                _swing_ob = find_ob(candles, direction)
                _sw_fvg = find_fvg(candles, direction)
                _ob_desc = (
                    f"OB: {_swing_ob['bottom']:.6f}–{_swing_ob['top']:.6f}"
                    if _swing_ob
                    else "OB: нет"
                )
                _fvg_desc = (
                    f"FVG: {_sw_fvg['bottom']:.6f}–{_sw_fvg['top']:.6f}"
                    if _sw_fvg
                    else "FVG: нет"
                )
                _vol_10 = [c.get("volume", 0) for c in candles[-10:]]
                _avg_vol_10 = sum(_vol_10) / len(_vol_10) if _vol_10 else 0
                _vol_desc = (
                    f"Vol avg10: {_avg_vol_10:.0f}, last: {_vol_10[-1]:.0f}"
                    if _vol_10
                    else ""
                )
                _sw_funding = self.get_funding_rate(symbol)
                _sw_fg = self.get_fear_greed()
                _sw_regime = self.get_market_regime(symbol)
                _sw_fund_str = (
                    f"{_sw_funding:+.4f}%" if _sw_funding is not None else "N/A"
                )
                _sw_fg_str = (
                    f"{_sw_fg['value']} ({_sw_fg['label']})" if _sw_fg else "N/A"
                )
                _sw_regime_str = (
                    _sw_regime.get("mode", "?")
                    if isinstance(_sw_regime, dict)
                    else str(_sw_regime)
                )
                _sw_pat_str = ""
                _sw_sl_pct = round(abs(entry - sl) / entry * 100, 1) if entry > 0 else 0
                groq_prompt = f'Ты опытный SMC трейдер специализирующийся на swing торговле. Оцени качество sweep сетапа. Ответь СТРОГО JSON: {{"logic": "макс 15 слов", "hours": число, "valid": true/false}}\n\nБЛОКИРУЙ (valid: false) если:\n- RR < 2.5 или стоп > 4% (RR={rr}, стоп={_sw_sl_pct}%)\n- Sweep был слабым (нет объёма, нет импульсной свечи обратно)\n- Нет CHoCH после sweep — структура не сменилась\n- 1d тренд против направления сигнала\n- Цена уже далеко от зоны sweep (> 2% от входа)\n- Между входом и TP есть сильный OB или FVG против направления\n- SL выставлен математически (entry ± X%), а не за структуру\n\nПОДТВЕРЖДАЙ (valid: true) если:\n- Sweep чёткий — пробой swing low/high с быстрым возвратом за 1-2 свечи\n- Объём на sweep выше среднего\n- CHoCH или BOS подтверждает разворот\n- TP на реальном структурном уровне\n- RR ≥ 2.5 — это swing, нужен запас\n\nПРАВИЛА ВЫСТАВЛЕНИЯ УРОВНЕЙ:\n- SL ТОЛЬКО за структурный уровень (swing low/high, OB edge, FVG edge)\n- ЗАПРЕЩЕНО: SL = entry ± X% (математические стопы не работают)\n- TP ТОЛЬКО на структурный уровень (EQH/EQL, OB, FVG, swing point)\n- Если нет структуры для SL — НЕ ВХОДИТЬ\n\nДанные: Пара: {symbol} ТФ: {timeframe} Направление: {direction}\nВход: {entry} SL: {sl} TP: {tp} HTF: {htf_dir} 1w: {htf_1w_swing}\nRR: {rr} | Стоп: {_sw_sl_pct}% | ATR: {smart_round(atr)} | До TP: {smart_round(distance_to_tp)}ч\nFunding: {_sw_fund_str} | Fear&Greed: {_sw_fg_str} | Режим: {_sw_regime_str}\n{_ob_desc} | {_fvg_desc} | {_vol_desc}\nСвечи: {candles_str}{_sw_pat_str}'
                groq_response = (
                    ask_groq(groq_prompt, max_tokens=100)
                    if legacy_strategy_groq_enabled()
                    else None
                )
                if groq_response and len(groq_response) > 5:
                    try:
                        import json as _json, re as _re

                        clean = (
                            groq_response.strip()
                            .replace("```json", "")
                            .replace("```", "")
                            .strip()
                        )
                        json_match = _re.search("\\{[^}]+\\}", clean, _re.DOTALL)
                        if json_match:
                            clean = json_match.group()
                        parsed = _json.loads(clean)
                        if parsed.get("valid", True):
                            _swing_groq_ok = True
                        else:
                            logging.info(f"[SWING Groq] {symbol}: Groq отклонил сигнал")
                            return _audit_fail(
                                "SWING_DETECT_SWING_SETUP_R7692",
                                "не должен проходить через один случайный Q-бонус.",
                                locals(),
                                'parsed.get("valid", True)',
                                7692,
                            )
                        if parsed.get("logic") and len(str(parsed["logic"])) > 5:
                            logic = str(parsed["logic"]).strip()
                        if parsed.get("hours") and str(parsed["hours"]).isdigit():
                            est_hours = max(12, min(int(parsed["hours"]), 96))
                    except Exception:
                        clean_text = groq_response.strip().replace("\n", " ")
                        if len(clean_text) > 10:
                            logic = clean_text[:80]
                else:
                    logging.debug(f"[SWING Groq] {symbol}: Groq не ответил — fallback")
            except Exception as ge:
                logging.debug(f"[SwingGroq] {symbol}: {ge}")
                tf_hours = {"1h": 1, "4h": 4, "1d": 24}
                est_hours = (
                    int(round(abs(tp - entry) / atr * tf_hours.get(timeframe, 4), 0))
                    if atr > 0
                    else 12
                )
                est_hours = max(12, min(est_hours, 96))
            _sw_vol_ok = False
            try:
                _sw_avg_vol = sum((c["volume"] for c in candles[-20:-1])) / 19
                _sw_vol_ok = (trigger_candle or candles[-1])[
                    "volume"
                ] >= _sw_avg_vol * 1.2
            except Exception:
                pass
            _sw_disp_ok = False
            try:
                _sw_last = (
                    _disp_candle
                    if "_disp_candle" in locals()
                    else trigger_candle or candles[-1]
                )
                _sw_body = abs(_sw_last["close"] - _sw_last["open"])
                _sw_range = _sw_last["high"] - _sw_last["low"]
                _sw_disp_ok = _sw_body / _sw_range >= 0.45 if _sw_range > 0 else False
            except Exception:
                pass
            if _audit_test(
                "SWING_DETECT_SWING_SETUP_G7729",
                not _swing_1h_choch,
                "Fresh 1h structure is part of the SWING thesis, not a bonus vote.",
                "not _swing_1h_choch",
                7729,
            ):
                logging.info(
                    f"[SWING Structure] {symbol}: no fresh 1h BOS/CHoCH after sweep — пропуск"
                )
                return _audit_fail(
                    "SWING_DETECT_SWING_SETUP_R7731",
                    "Fresh 1h structure is part of the SWING thesis, not a bonus vote.",
                    locals(),
                    "not _swing_1h_choch",
                    7731,
                )
            _sw_confirms = sum(
                [
                    _sw_vol_ok,
                    _sw_disp_ok,
                    _swing_pd_ok,
                    _swing_fvg_ok,
                    _swing_15m_confirms,
                ]
            )
            _sw_quality = f" [Q:{_sw_confirms}/5]"
            if _audit_test(
                "SWING_DETECT_SWING_SETUP_G7741",
                _sw_confirms < 2,
                "_sw_confirms < 2",
                "_sw_confirms < 2",
                7741,
            ):
                logging.info(
                    f"[SWING Quality] {symbol}: confirms={_sw_confirms}/5 < 2 — пропуск"
                )
                return _audit_fail(
                    "SWING_DETECT_SWING_SETUP_R7743",
                    "_sw_confirms < 2",
                    locals(),
                    "_sw_confirms < 2",
                    7743,
                )
            if direction == "BULLISH":
                _sw_tp2_candidates = [
                    level for _, level in swing_highs if level > tp * 1.005
                ]
                _sw_tp2 = (
                    smart_round(min(_sw_tp2_candidates)) if _sw_tp2_candidates else None
                )
            else:
                _sw_tp2_candidates = [
                    level for _, level in swing_lows if level < tp * 0.995
                ]
                _sw_tp2 = (
                    smart_round(max(_sw_tp2_candidates)) if _sw_tp2_candidates else None
                )
            return {
                "symbol": symbol,
                "direction": direction,
                "timeframe": timeframe,
                "entry": entry,
                "sl": sl,
                "tp": tp,
                "tp2": _sw_tp2,
                "sl_pct": sl_pct,
                "tp_pct": tp_pct,
                "rr": rr,
                "logic": logic + _sw_quality,
                "htf_dir": htf_dir,
                "htf_1w": htf_1w_swing,
                "weekly_warning": weekly_warning,
                "est_hours": est_hours,
                "ob": _swing_ob,
                "fvg": _sw_dir_fvg if _swing_fvg_ok else None,
                "confirms": _sw_confirms,
                "funding_warning": _swing_funding_warning,
                "structure_event": _swing_structure_event,
                "structure_event_1h": _swing_1h_structure_event,
                "scan_type": "swing",
            }
        except Exception as e:
            logging.debug(f"detect_swing_setup {symbol}: {e}")
            return _audit_fail(
                "SWING_DETECT_SWING_SETUP_R7781",
                "detector returned None",
                locals(),
                "",
                7781,
            )

    def detect_zone_setup(
        self, symbol: str, timeframe: str = "4h", passive_watch: bool = False
    ) -> dict | None:
        """
        ZONE стратегия: вход из Discount/Premium зоны с OB/FVG подтверждением.
        Не требует sweep — опирается на зону интереса и отбой от неё.
        """
        try:
            raw_candles = self.get_candles(symbol, timeframe, 101)
            candles = get_confirmed_candles(raw_candles)
            if _audit_test(
                "ZONE_DETECT_ZONE_SETUP_G7796",
                not candles or len(candles) < 40,
                "Достаточно 4H истории (≥40 закрытых свечей)",
                "not candles or len(candles) < 40",
                7796,
            ):
                return _audit_fail(
                    "ZONE_DETECT_ZONE_SETUP_R7797",
                    "Достаточно 4H истории (≥40 закрытых свечей)",
                    locals(),
                    "not candles or len(candles) < 40",
                    7797,
                )
            price = raw_candles[-1]["close"]
            atr = average_true_range(candles, 14)
            _ap_zone = self.get_adaptive_params(symbol, candles)
            _vf_zone = _ap_zone["volatility_factor"]
            range_candles = candles[-50:]
            range_high = max((c["high"] for c in range_candles))
            range_low = min((c["low"] for c in range_candles))
            range_mid = (range_high + range_low) / 2
            range_size = range_high - range_low
            if _audit_test(
                "ZONE_DETECT_ZONE_SETUP_G7811",
                range_size < atr * 2,
                "ZONE: range size >= 2 ATR",
                "range_size < atr * 2",
                7811,
            ):
                return _audit_fail(
                    "ZONE_DETECT_ZONE_SETUP_R7812",
                    "ZONE: range size >= 2 ATR",
                    locals(),
                    "range_size < atr * 2",
                    7812,
                )
            in_discount = price <= range_low + range_size * 0.3
            in_premium = price >= range_high - range_size * 0.3
            _audit_observe(
                "zone_numeric",
                {
                    "range_position_pct": round(
                        (price - range_low) / range_size * 100, 6
                    )
                    if range_size > 0
                    else None,
                    "range_atr": round(range_size / atr, 6) if atr > 0 else None,
                },
            )
            if _audit_test(
                "ZONE_DETECT_ZONE_SETUP_G7818",
                not in_discount and (not in_premium),
                "Require a real range extreme and leave the middle 40% neutral.",
                "not in_discount and not in_premium",
                7818,
            ):
                return _audit_fail(
                    "ZONE_DETECT_ZONE_SETUP_R7819",
                    "Require a real range extreme and leave the middle 40% neutral.",
                    locals(),
                    "not in_discount and not in_premium",
                    7819,
                )
            direction = "BULLISH" if in_discount else "BEARISH"
            ob = find_ob(candles, direction)
            fvg = find_fvg(candles, direction)
            zone_level = None
            zone_type = None
            if ob:
                if direction == "BULLISH" and ob["top"] <= range_mid:
                    if ob["bottom"] <= price <= ob["top"] + atr * 0.5:
                        zone_level = ob["bottom"]
                        zone_type = "OB"
                elif direction == "BEARISH" and ob["bottom"] >= range_mid:
                    if ob["bottom"] - atr * 0.5 <= price <= ob["top"]:
                        zone_level = ob["top"]
                        zone_type = "OB"
            if not zone_level and fvg:
                if direction == "BULLISH" and fvg["top"] <= range_mid:
                    if fvg["bottom"] <= price <= fvg["top"] + atr * 0.5:
                        zone_level = fvg["bottom"]
                        zone_type = "FVG"
                elif direction == "BEARISH" and fvg["bottom"] >= range_mid:
                    if fvg["bottom"] - atr * 0.5 <= price <= fvg["top"]:
                        zone_level = fvg["top"]
                        zone_type = "FVG"
            if _audit_test(
                "ZONE_DETECT_ZONE_SETUP_G7852",
                not zone_level,
                "not zone_level",
                "not zone_level",
                7852,
            ):
                return _audit_fail(
                    "ZONE_DETECT_ZONE_SETUP_R7853",
                    "not zone_level",
                    locals(),
                    "not zone_level",
                    7853,
                )
            _audit_observe(
                "zone_numeric",
                {
                    "zone_distance_atr": round(abs(price - zone_level) / atr, 6)
                    if atr > 0
                    else None
                },
            )
            if zone_level and zone_type:
                try:
                    _test_count = 0
                    _zone_top = (
                        ob["top"]
                        if zone_type == "OB" and ob
                        else fvg["top"]
                        if fvg
                        else zone_level * 1.01
                    )
                    _zone_bot = (
                        ob["bottom"]
                        if zone_type == "OB" and ob
                        else fvg["bottom"]
                        if fvg
                        else zone_level * 0.99
                    )
                    _zone_origin = ob if zone_type == "OB" else fvg
                    # An OB is observable at its origin close. A three-bar
                    # FVG exists only when its right-hand candle has closed.
                    _zone_birth_idx = int(_zone_origin["index"]) + (
                        1 if zone_type == "FVG" else 0
                    )
                    # Preserve the existing forty-bar window and exclusion
                    # of the latest three setup bars; exclude formation too.
                    for _zone_test_idx in range(
                        max(0, len(candles) - 40, _zone_birth_idx + 1),
                        max(0, len(candles) - 3),
                    ):
                        c = candles[_zone_test_idx]
                        if c["low"] <= _zone_top and c["high"] >= _zone_bot:
                            _test_count += 1
                    _audit_observe("zone_numeric", {"test_count": _test_count})
                    if _audit_test(
                        "ZONE_DETECT_ZONE_SETUP_G7866",
                        _test_count > 2,
                        "ZONE: fresh zone has at most 2 prior tests",
                        "_test_count > 2",
                        7866,
                    ):
                        logging.debug(
                            f"[ZONE] {symbol}: зона протестирована {_test_count} раз — mitigated"
                        )
                        return _audit_fail(
                            "ZONE_DETECT_ZONE_SETUP_R7868",
                            "ZONE: fresh zone has at most 2 prior tests",
                            locals(),
                            "_test_count > 2",
                            7868,
                        )
                    _strong_move = False
                    _zone_best_displacement = 0.0
                    _zone_best_body_atr = 0.0
                    for i in range(max(-len(candles), -35), -3):
                        c = candles[i]
                        c_body = abs(c["close"] - c["open"])
                        c_range = c["high"] - c["low"]
                        _zone_directional = (
                            direction == "BULLISH"
                            and c["close"] > c["open"]
                            or (direction == "BEARISH" and c["close"] < c["open"])
                        )
                        if _zone_directional and c_range > 0:
                            _zone_best_displacement = max(
                                _zone_best_displacement, c_body / c_range
                            )
                            if atr > 0:
                                _zone_best_body_atr = max(
                                    _zone_best_body_atr, c_body / atr
                                )
                        if (
                            c_range > 0
                            and c_body / c_range >= 0.5
                            and (c_body > atr * _vf_zone * 0.8)
                        ):
                            if direction == "BULLISH" and c["close"] > c["open"]:
                                _strong_move = True
                                break
                            elif direction == "BEARISH" and c["close"] < c["open"]:
                                _strong_move = True
                                break
                    _audit_observe(
                        "zone_numeric",
                        {
                            "best_directional_displacement_ratio": round(
                                _zone_best_displacement, 6
                            ),
                            "best_directional_body_atr": round(_zone_best_body_atr, 6),
                        },
                    )
                    if _audit_test(
                        "ZONE_DETECT_ZONE_SETUP_G7884",
                        not _strong_move,
                        "not _strong_move",
                        "not _strong_move",
                        7884,
                    ):
                        logging.debug(
                            f"[ZONE] {symbol}: нет сильного импульса (displacement < 0.5)"
                        )
                        return _audit_fail(
                            "ZONE_DETECT_ZONE_SETUP_R7886",
                            "not _strong_move",
                            locals(),
                            "not _strong_move",
                            7886,
                        )
                except Exception as _zone_freshness_error:
                    logging.debug(
                        "[ZONE] %s: zone freshness unavailable: %s",
                        symbol,
                        _zone_freshness_error,
                    )
                    return _audit_fail(
                        "ZONE_DETECT_ZONE_SETUP_R7890",
                        "zone_level and zone_type",
                        locals(),
                        "zone_level and zone_type",
                        7890,
                    )
            last = candles[-1]
            rebound_bull = (
                last["close"] > last["open"] and last["low"] <= zone_level + atr * 0.3
            )
            rebound_bear = (
                last["close"] < last["open"] and last["high"] >= zone_level - atr * 0.3
            )
            if _audit_test(
                "ZONE_DETECT_ZONE_SETUP_G7899",
                direction == "BULLISH" and (not rebound_bull),
                'direction == "BULLISH" and not rebound_bull',
                'direction == "BULLISH" and not rebound_bull',
                7899,
            ):
                return _audit_fail(
                    "ZONE_DETECT_ZONE_SETUP_R7900",
                    'direction == "BULLISH" and not rebound_bull',
                    locals(),
                    'direction == "BULLISH" and not rebound_bull',
                    7900,
                )
            if _audit_test(
                "ZONE_DETECT_ZONE_SETUP_G7901",
                direction == "BEARISH" and (not rebound_bear),
                'direction == "BEARISH" and not rebound_bear',
                'direction == "BEARISH" and not rebound_bear',
                7901,
            ):
                return _audit_fail(
                    "ZONE_DETECT_ZONE_SETUP_R7902",
                    'direction == "BEARISH" and not rebound_bear',
                    locals(),
                    'direction == "BEARISH" and not rebound_bear',
                    7902,
                )
            htf_1d = self.smc_on_tf(symbol, "1d")
            if htf_1d:
                if _audit_test(
                    "ZONE_DETECT_ZONE_SETUP_G7907",
                    direction == "BULLISH" and "BEARISH" in str(htf_1d).upper(),
                    "4. HTF фильтры",
                    'direction == "BULLISH" and "BEARISH" in str(htf_1d).upper()',
                    7907,
                ):
                    return _audit_fail(
                        "ZONE_DETECT_ZONE_SETUP_R7908",
                        "4. HTF фильтры",
                        locals(),
                        'direction == "BULLISH" and "BEARISH" in str(htf_1d).upper()',
                        7908,
                    )
                if _audit_test(
                    "ZONE_DETECT_ZONE_SETUP_G7909",
                    direction == "BEARISH" and "BULLISH" in str(htf_1d).upper(),
                    "4. HTF фильтры",
                    'direction == "BEARISH" and "BULLISH" in str(htf_1d).upper()',
                    7909,
                ):
                    return _audit_fail(
                        "ZONE_DETECT_ZONE_SETUP_R7910",
                        'direction == "BEARISH" and "BULLISH" in str(htf_1d).upper()',
                        locals(),
                        'direction == "BEARISH" and "BULLISH" in str(htf_1d).upper()',
                        7910,
                    )
            if symbol != "BTCUSDT":
                btc_ok, btc_reason = self.btc_allows_signal(direction)
                if _audit_test(
                    "ZONE_DETECT_ZONE_SETUP_G7915",
                    not btc_ok,
                    "5. BTC фильтр",
                    "not btc_ok",
                    7915,
                ):
                    return _audit_fail(
                        "ZONE_DETECT_ZONE_SETUP_R7916",
                        "5. BTC фильтр",
                        locals(),
                        "not btc_ok",
                        7916,
                    )
            try:
                fr = self.get_funding_rate(symbol)
                if fr is not None and abs(fr) > 0.2:
                    logging.info(
                        "[ZONE] %s: extreme funding %.4f%% — risk warning", symbol, fr
                    )
            except Exception:
                pass
            q_score = 0
            _zone_quality_components = {
                "wick_rejection": {"role": "QUALITY", "passed": False},
                "closed_1h_bos_choch": {"role": "HARD_GATE", "passed": False},
                "rsi_30_70": {"role": "QUALITY", "passed": False, "value": None},
                "directional_fvg": {"role": "QUALITY", "passed": False},
                "btc_4h_alignment": {"role": "QUALITY", "passed": False},
                "funding_neutral_or_contrarian": {
                    "role": "QUALITY",
                    "passed": False,
                    "value": None,
                },
                "rejection_volume_1_3x": {
                    "role": "QUALITY",
                    "passed": False,
                    "value": None,
                },
                "recent_imbalance": {"role": "QUALITY_PENALTY", "passed": None},
            }
            try:
                if direction == "BULLISH":
                    _wick_z = candles[-1]["close"] - candles[-1]["low"]
                    _body_z = abs(candles[-1]["close"] - candles[-1]["open"])
                    if _wick_z > _body_z:
                        q_score += 1
                        _zone_quality_components["wick_rejection"]["passed"] = True
                else:
                    _wick_z = candles[-1]["high"] - candles[-1]["close"]
                    _body_z = abs(candles[-1]["close"] - candles[-1]["open"])
                    if _wick_z > _body_z:
                        q_score += 1
                        _zone_quality_components["wick_rejection"]["passed"] = True
            except Exception:
                pass
            _zone_ltf_structure = False
            _zone_structure_event = None
            try:
                _c1h_zone = get_confirmed_candles(self.get_candles(symbol, "1h", 31))
                _zone_structure_event = (
                    get_bos_choch_event(
                        _c1h_zone, direction, lookback=8, max_break_age=1
                    )
                    if _c1h_zone
                    else None
                )
                if _zone_structure_event:
                    _zone_ltf_structure = True
                    _zone_quality_components["closed_1h_bos_choch"]["passed"] = True
            except Exception:
                pass
            if passive_watch and (not _zone_ltf_structure):
                return {
                    "_pending_ltf": True,
                    "symbol": symbol,
                    "strategy": "ZONE",
                    "direction": direction,
                    "required_timeframe": "1h",
                    "reason": "зона подтверждена; ожидается свежий 1h BOS/CHoCH",
                }
            if _audit_test(
                "ZONE_DETECT_ZONE_SETUP_G7966",
                not _zone_ltf_structure,
                "not _zone_ltf_structure",
                "not _zone_ltf_structure",
                7966,
            ):
                return _audit_fail(
                    "ZONE_DETECT_ZONE_SETUP_R7967",
                    "not _zone_ltf_structure",
                    locals(),
                    "not _zone_ltf_structure",
                    7967,
                )
            try:
                rmd = detect_rsi_macd_divergence(candles, direction)
                rsi_val = rmd.get("rsi") if rmd else None
                _zone_quality_components["rsi_30_70"]["value"] = rsi_val
                if rsi_val is not None and 30 <= rsi_val <= 70:
                    q_score += 1
                    _zone_quality_components["rsi_30_70"]["passed"] = True
            except Exception:
                pass
            try:
                if fvg and direction == "BULLISH" and (fvg["bottom"] > price):
                    q_score += 1
                    _zone_quality_components["directional_fvg"]["passed"] = True
                elif fvg and direction == "BEARISH" and (fvg["top"] < price):
                    q_score += 1
                    _zone_quality_components["directional_fvg"]["passed"] = True
            except Exception:
                pass
            try:
                btc_4h = self.smc_on_tf("BTCUSDT", "4h")
                if btc_4h and direction in str(btc_4h).upper():
                    q_score += 1
                    _zone_quality_components["btc_4h_alignment"]["passed"] = True
            except Exception:
                pass
            _zone_funding_warning = ""
            try:
                fr = self.get_funding_rate(symbol)
                _zone_quality_components["funding_neutral_or_contrarian"]["value"] = fr
                if fr is not None:
                    if direction == "BULLISH" and fr < 0:
                        q_score += 1
                        _zone_quality_components["funding_neutral_or_contrarian"][
                            "passed"
                        ] = True
                    elif direction == "BEARISH" and fr > 0:
                        q_score += 1
                        _zone_quality_components["funding_neutral_or_contrarian"][
                            "passed"
                        ] = True
                    elif abs(fr) < 0.05:
                        q_score += 1
                        _zone_quality_components["funding_neutral_or_contrarian"][
                            "passed"
                        ] = True
                    elif (
                        direction == "BULLISH"
                        and fr > 0.2
                        or (direction == "BEARISH" and fr < -0.2)
                    ):
                        _zone_funding_warning = f"extreme crowded funding {fr:+.4f}%"
            except Exception:
                pass
            try:
                avg_vol = sum((c["volume"] for c in candles[-20:-1])) / 19
                _zone_quality_components["rejection_volume_1_3x"]["value"] = (
                    last["volume"] / avg_vol if avg_vol else None
                )
                if last["volume"] > avg_vol * 1.3:
                    q_score += 1
                    _zone_quality_components["rejection_volume_1_3x"]["passed"] = True
            except Exception:
                pass
            try:
                _imbalance_found = False
                for i in range(1, min(10, len(candles) - 1)):
                    _c1 = candles[-i - 1]
                    _c2 = candles[-i]
                    _c3 = candles[-i + 1] if i > 1 else candles[-1]
                    if direction == "BULLISH":
                        if _c3["low"] > _c1["high"]:
                            _imbalance_found = True
                            break
                    elif _c3["high"] < _c1["low"]:
                        _imbalance_found = True
                        break
                if not _imbalance_found:
                    q_score = max(0, q_score - 1)
                    logging.debug(
                        f"[ZONE] {symbol}: нет FVG дисбаланса — q_score снижен до {q_score}"
                    )
                _zone_quality_components["recent_imbalance"]["passed"] = (
                    _imbalance_found
                )
            except Exception:
                pass
            _zone_ap = self.get_adaptive_params(symbol, candles)
            _zone_vf = _zone_ap.get("volatility_factor", 1.0) if _zone_ap else 1.0
            _q_min = 3
            _audit_observe(
                "zone_numeric", {"quality_score": q_score, "quality_required": _q_min}
            )
            if _audit_test(
                "ZONE_DETECT_ZONE_SETUP_G8050",
                q_score < _q_min,
                "wick/RSI/FVG/BTC/funding/rejection-volume, without double-counting.",
                "q_score < _q_min",
                8050,
            ):
                return _audit_fail(
                    "ZONE_DETECT_ZONE_SETUP_R8051",
                    "wick/RSI/FVG/BTC/funding/rejection-volume, without double-counting.",
                    locals(),
                    "q_score < _q_min",
                    8051,
                )
            if direction == "BULLISH":
                entry = smart_round(price)
                sl = smart_round(zone_level - atr * 0.5)
                swing_highs, _ = find_swings(candles, lookback=5)
                tp_candidates = [sh[1] for sh in swing_highs if sh[1] > entry * 1.005]
                if _audit_test(
                    "ZONE_DETECT_ZONE_SETUP_G8060",
                    not tp_candidates,
                    "TP = ближайший swing high",
                    "not tp_candidates",
                    8060,
                ):
                    return _audit_fail(
                        "ZONE_DETECT_ZONE_SETUP_R8061",
                        "TP = ближайший swing high",
                        locals(),
                        "not tp_candidates",
                        8061,
                    )
                tp = smart_round(min(tp_candidates))
            else:
                entry = smart_round(price)
                sl = smart_round(zone_level + atr * 0.5)
                _, swing_lows = find_swings(candles, lookback=5)
                tp_candidates = [sw[1] for sw in swing_lows if sw[1] < entry * 0.995]
                if _audit_test(
                    "ZONE_DETECT_ZONE_SETUP_G8068",
                    not tp_candidates,
                    "not tp_candidates",
                    "not tp_candidates",
                    8068,
                ):
                    return _audit_fail(
                        "ZONE_DETECT_ZONE_SETUP_R8069",
                        "not tp_candidates",
                        locals(),
                        "not tp_candidates",
                        8069,
                    )
                tp = smart_round(max(tp_candidates))
            risk = abs(entry - sl)
            reward = abs(tp - entry)
            if _audit_test(
                "ZONE_DETECT_ZONE_SETUP_G8075",
                risk == 0,
                "9. RR фильтр",
                "risk == 0",
                8075,
            ):
                return _audit_fail(
                    "ZONE_DETECT_ZONE_SETUP_R8076",
                    "9. RR фильтр",
                    locals(),
                    "risk == 0",
                    8076,
                )
            rr = round(reward / risk, 2)
            if _audit_test(
                "ZONE_DETECT_ZONE_SETUP_G8078", rr < 2.0, "rr < 2.0", "rr < 2.0", 8078
            ):
                return _audit_fail(
                    "ZONE_DETECT_ZONE_SETUP_R8079",
                    "rr < 2.0",
                    locals(),
                    "rr < 2.0",
                    8079,
                )
            logic = f"Вход из {('Discount' if direction == 'BULLISH' else 'Premium')} зоны ({zone_type})"
            try:
                _zone_sl_pct = (
                    round(abs(entry - sl) / entry * 100, 1) if entry > 0 else 0
                )
                _zone_prompt = f"""Ты SMC трейдер специализирующийся на зонах интереса. Оцени вход из Discount/Premium зоны. Ответь СТРОГО JSON: {{"logic": "макс 15 слов", "valid": true/false}}\n\nБЛОКИРУЙ (valid: false) если:\n- RR < 2.0 или стоп > 3% (RR={rr}, стоп={_zone_sl_pct}%)\n- Цена не чётко в OB или FVG зоне\n- Нет отбоя от зоны (нет бычьей/медвежьей свечи от уровня)\n- 1d тренд против направления\n- OB/FVG уже был протестирован несколько раз (mitigated)\n- Нет FVG между входом и TP для притяжения цены\n- SL выставлен математически (entry ± X%), а не за структуру\n\nПОДТВЕРЖДАЙ (valid: true) если:\n- Цена чётко внутри OB или касается FVG\n- Зона нетронутая (первый или второй тест)\n- Есть хотя бы одна свеча отбоя от зоны\n- 1d и 4h тренд совпадают с направлением\n- RR ≥ 2.0, TP на реальном swing уровне\n\nПРАВИЛА ВЫСТАВЛЕНИЯ УРОВНЕЙ:\n- SL ТОЛЬКО за структурный уровень (OB edge, FVG edge, swing low/high)\n- ЗАПРЕЩЕНО: SL = entry ± X% (математические стопы не работают)\n- TP ТОЛЬКО на структурный уровень (EQH/EQL, OB, FVG, swing point)\n- Если нет структуры для SL — НЕ ВХОДИТЬ\n\nДанные: Пара: {symbol} ТФ: {timeframe} Направление: {direction}\nЗона: {("Discount" if direction == "BULLISH" else "Premium")} | Тип: {zone_type}\nДиапазон: {smart_price_fmt(range_low)}–{smart_price_fmt(range_high)} | Mid: {smart_price_fmt(range_mid)}\nЦена: {smart_price_fmt(price)} | OB: {(smart_price_fmt(ob["bottom"]) + "–" + smart_price_fmt(ob["top"]) if ob else "нет")}\nFVG: {(smart_price_fmt(fvg["bottom"]) + "–" + smart_price_fmt(fvg["top"]) if fvg else "нет")}\n1d тренд: {htf_1d} | Quality score: {q_score}/6\nEntry: {smart_price_fmt(entry)} SL: {smart_price_fmt(sl)} TP: {smart_price_fmt(tp)} RR: {rr} Стоп: {_zone_sl_pct}%"""
                _zone_resp = (
                    ask_groq(_zone_prompt, max_tokens=80)
                    if legacy_strategy_groq_enabled()
                    else None
                )
                if _zone_resp:
                    import json as _j, re as _re

                    _clean = _re.sub("```json|```", "", _zone_resp).strip()
                    _m = _re.search("\\{[^}]+\\}", _clean, _re.DOTALL)
                    if _m:
                        _parsed = _j.loads(_m.group())
                        if _audit_test(
                            "ZONE_DETECT_ZONE_SETUP_G8127",
                            not _parsed.get("valid", True),
                            'not _parsed.get("valid", True)',
                            'not _parsed.get("valid", True)',
                            8127,
                        ):
                            return _audit_fail(
                                "ZONE_DETECT_ZONE_SETUP_R8128",
                                'not _parsed.get("valid", True)',
                                locals(),
                                'not _parsed.get("valid", True)',
                                8128,
                            )
                        if _parsed.get("logic"):
                            logic = str(_parsed["logic"]).strip()
            except Exception:
                pass
            try:
                _z_sh, _z_sl = find_swings(candles, lookback=12)
                if direction == "BULLISH":
                    _tp2_cands = [s[1] for s in _z_sh if s[1] > tp * 1.005]
                    _z_tp2 = smart_round(min(_tp2_cands)) if _tp2_cands else None
                else:
                    _tp2_cands = [s[1] for s in _z_sl if s[1] < tp * 0.995]
                    _z_tp2 = smart_round(max(_tp2_cands)) if _tp2_cands else None
            except Exception:
                _z_tp2 = None
            _zone_est_hours = max(
                12, min(96, int(round(abs(tp - entry) / max(atr, 1e-12) * 4)))
            )
            return {
                "symbol": symbol,
                "direction": direction,
                "entry": entry,
                "sl": sl,
                "tp": tp,
                "tp2": _z_tp2,
                "rr": rr,
                "zone_type": zone_type,
                "zone": "Discount" if direction == "BULLISH" else "Premium",
                "q_score": q_score,
                "quality_components": _zone_quality_components,
                "htf_dir": htf_1d,
                "funding_warning": _zone_funding_warning,
                "logic": logic,
                "est_hours": _zone_est_hours,
                "structure_event": _zone_structure_event,
            }
        except Exception as e:
            logging.warning(f"detect_zone_setup {symbol}: {e}")
            return _audit_fail(
                "ZONE_DETECT_ZONE_SETUP_R8168",
                "detector returned None",
                locals(),
                "",
                8168,
            )

    def _find_wyckoff_phases_accumulation(self, candles_1d, candles_4h):
        """
        Определяет фазы Wyckoff Accumulation:
        PS  — Preliminary Support (первая поддержка на падении)
        SC  — Selling Climax (паническая свеча с огромным объёмом = дно)
        AR  — Automatic Rally (отскок от SC — кит выкупает)
        ST  — Secondary Test (тест лоу SC с меньшим объёмом)
        Spring — ложный пробой ниже ST/SC с возвратом
        SOS — Sign of Strength (пробой AR с объёмом = подтверждение)
        """
        if len(candles_1d) < 40:
            return {}
        phases = {}
        vols = [c["volume"] for c in candles_1d]
        avg_vol = sum(vols) / len(vols) if vols else 1
        sc_idx = None
        sc_vol_max = 0
        for i in range(10, len(candles_1d) - 5):
            c = candles_1d[i]
            body = c["open"] - c["close"]
            if body > 0 and c["volume"] > avg_vol * 2.5:
                if c["volume"] > sc_vol_max:
                    sc_vol_max = c["volume"]
                    sc_idx = i
        if sc_idx is None:
            return {}
        sc_candle = candles_1d[sc_idx]
        phases["SC"] = {"idx": sc_idx, "price": sc_candle["low"], "vol": sc_vol_max}
        ar_idx = None
        ar_high = 0
        for i in range(sc_idx + 1, min(sc_idx + 15, len(candles_1d))):
            c = candles_1d[i]
            if c["close"] > c["open"] and c["high"] > ar_high:
                ar_high = c["high"]
                ar_idx = i
        if ar_idx is None:
            return {}
        phases["AR"] = {"idx": ar_idx, "price": ar_high}
        st_idx = None
        for i in range(ar_idx + 1, min(ar_idx + 20, len(candles_1d))):
            c = candles_1d[i]
            near_sc_low = abs(c["low"] - sc_candle["low"]) / sc_candle["low"] < 0.05
            lower_vol = c["volume"] < sc_vol_max * 0.6
            if near_sc_low and lower_vol:
                st_idx = i
                break
        if st_idx:
            phases["ST"] = {"idx": st_idx, "price": candles_1d[st_idx]["low"]}
        spring_level = phases.get("ST", phases["SC"])["price"]
        spring_found = False
        spring_price = None
        for c in candles_4h[-30:]:
            if c["low"] < spring_level and c["close"] > spring_level:
                wick = (c["close"] - c["low"]) / (c["high"] - c["low"] + 1e-06)
                if wick > 0.4:
                    spring_found = True
                    spring_price = c["low"]
                    phases["Spring"] = {"found": True, "price": spring_price}
                    break
        if not spring_found:
            phases["Spring"] = {"found": False}
        sos_found = False
        for c in candles_4h[-10:]:
            if c["close"] > ar_high:
                vol_4h_avg = (
                    sum((x["volume"] for x in candles_4h[-20:-10])) / 10
                    if len(candles_4h) >= 20
                    else 1
                )
                if c["volume"] > vol_4h_avg * 1.5:
                    sos_found = True
                    phases["SOS"] = {"found": True, "price": c["close"]}
                    break
        if not sos_found:
            phases["SOS"] = {"found": False}
        return phases

    def _find_wyckoff_phases_distribution(self, candles_1d, candles_4h):
        """
        Определяет фазы Wyckoff Distribution (для шортов):
        PSY — Preliminary Supply (первое сопротивление на росте)
        BC  — Buying Climax (эйфорийная свеча с объёмом = вершина)
        AR  — Automatic Reaction (откат от BC)
        ST  — Secondary Test (тест хая BC с меньшим объёмом)
        UTAD — UpThrust After Distribution (ложный пробой вверх = финальная ловушка)
        LPSY — Last Point of Supply (последний отскок перед падением)
        """
        if len(candles_1d) < 40:
            return {}
        phases = {}
        vols = [c["volume"] for c in candles_1d]
        avg_vol = sum(vols) / len(vols) if vols else 1
        bc_idx = None
        bc_vol_max = 0
        for i in range(10, len(candles_1d) - 5):
            c = candles_1d[i]
            body = c["close"] - c["open"]
            if body > 0 and c["volume"] > avg_vol * 2.5:
                if c["volume"] > bc_vol_max:
                    bc_vol_max = c["volume"]
                    bc_idx = i
        if bc_idx is None:
            return {}
        bc_candle = candles_1d[bc_idx]
        phases["BC"] = {"idx": bc_idx, "price": bc_candle["high"], "vol": bc_vol_max}
        ar_idx = None
        ar_low = float("inf")
        for i in range(bc_idx + 1, min(bc_idx + 15, len(candles_1d))):
            c = candles_1d[i]
            if c["close"] < c["open"] and c["low"] < ar_low:
                ar_low = c["low"]
                ar_idx = i
        if ar_idx is None:
            return {}
        phases["AR"] = {"idx": ar_idx, "price": ar_low}
        st_idx = None
        for i in range(ar_idx + 1, min(ar_idx + 20, len(candles_1d))):
            c = candles_1d[i]
            near_bc_high = abs(c["high"] - bc_candle["high"]) / bc_candle["high"] < 0.05
            lower_vol = c["volume"] < bc_vol_max * 0.6
            if near_bc_high and lower_vol:
                st_idx = i
                break
        if st_idx:
            phases["ST"] = {"idx": st_idx, "price": candles_1d[st_idx]["high"]}
        utad_level = phases.get("ST", phases["BC"])["price"]
        utad_found = False
        for c in candles_4h[-30:]:
            if c["high"] > utad_level and c["close"] < utad_level:
                wick = (c["high"] - c["close"]) / (c["high"] - c["low"] + 1e-06)
                if wick > 0.4:
                    utad_found = True
                    phases["UTAD"] = {"found": True, "price": c["high"]}
                    break
        if not utad_found:
            phases["UTAD"] = {"found": False}
        sow_found = False
        for c in candles_4h[-10:]:
            if c["close"] < ar_low:
                vol_4h_avg = (
                    sum((x["volume"] for x in candles_4h[-20:-10])) / 10
                    if len(candles_4h) >= 20
                    else 1
                )
                if c["volume"] > vol_4h_avg * 1.5:
                    sow_found = True
                    phases["SOW"] = {"found": True, "price": c["close"]}
                    break
        if not sow_found:
            phases["SOW"] = {"found": False}
        return phases

    def detect_wyckoff_spring(self, symbol: str) -> dict | None:
        """
        Wyckoff Accumulation Spring — LONG сигнал.
        Полный анализ фаз: SC → AR → ST → Spring → SOS
        Редкий сигнал +30-200%
        """
        try:
            raw_candles_1d = self.get_candles(symbol, "1d", 61)
            raw_candles_4h = self.get_candles(symbol, "4h", 121)
            candles_1d = get_confirmed_candles(raw_candles_1d)
            candles_4h = get_confirmed_candles(raw_candles_4h)
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_SPRING_G8375",
                not candles_1d or len(candles_1d) < 40,
                "not candles_1d or len(candles_1d) < 40",
                "not candles_1d or len(candles_1d) < 40",
                8375,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_SPRING_R8376",
                    "not candles_1d or len(candles_1d) < 40",
                    locals(),
                    "not candles_1d or len(candles_1d) < 40",
                    8376,
                )
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_SPRING_G8377",
                not candles_4h or len(candles_4h) < 40,
                "not candles_4h or len(candles_4h) < 40",
                "not candles_4h or len(candles_4h) < 40",
                8377,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_SPRING_R8378",
                    "not candles_4h or len(candles_4h) < 40",
                    locals(),
                    "not candles_4h or len(candles_4h) < 40",
                    8378,
                )
            price_now = raw_candles_1d[-1]["close"]
            score = 0
            signals = []
            if symbol != "BTCUSDT":
                btc_ok, btc_reason = self.btc_allows_signal("BULLISH", use_4h=True)
                if _audit_test(
                    "WYCKOFF_DETECT_WYCKOFF_SPRING_G8387",
                    not btc_ok,
                    "BTC фильтр для WYCKOFF (4h)",
                    "not btc_ok",
                    8387,
                ):
                    logging.info(
                        f"[WYCKOFF BTC Filter] {symbol} LONG пропущен: {btc_reason}"
                    )
                    return _audit_fail(
                        "WYCKOFF_DETECT_WYCKOFF_SPRING_R8389",
                        "BTC фильтр для WYCKOFF (4h)",
                        locals(),
                        "not btc_ok",
                        8389,
                    )
            price_peak = max((c["high"] for c in candles_1d[-50:-15]))
            drawdown_pct = (
                (price_peak - price_now) / price_peak * 100 if price_peak > 0 else 0
            )
            _wyckoff_min_drawdown = 7 if symbol == "BTCUSDT" else 12
            if drawdown_pct >= 35:
                score += 30
                signals.append(f"✅ Глубокий даунтренд -{drawdown_pct:.0f}% от пика")
            elif drawdown_pct >= _wyckoff_min_drawdown:
                score += 15
                signals.append(f"⚡️ Коррекция -{drawdown_pct:.0f}% от пика")
            else:
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_SPRING_R8404",
                    "drawdown_pct >= _wyckoff_min_drawdown",
                    locals(),
                    "drawdown_pct >= _wyckoff_min_drawdown",
                    8404,
                )
            accumulation_candles = candles_1d[-30:]
            acc_high = max((c["high"] for c in accumulation_candles))
            acc_low = min((c["low"] for c in accumulation_candles))
            acc_range_pct = (acc_high - acc_low) / acc_low * 100 if acc_low > 0 else 0
            _audit_observe(
                "wyckoff_accumulation",
                {
                    "acc_range_pct": round(acc_range_pct, 6),
                    "old_range_under_25": bool(acc_range_pct < 25),
                },
            )
            try:
                _telemetry_phases_acc = self._find_wyckoff_phases_accumulation(
                    candles_1d, candles_4h
                )
                _telemetry_points_acc = []
                for _telemetry_name_acc in ("SC", "AR", "ST"):
                    _telemetry_phase_acc = (
                        _telemetry_phases_acc.get(_telemetry_name_acc)
                        if isinstance(_telemetry_phases_acc, dict)
                        else None
                    )
                    if (
                        isinstance(_telemetry_phase_acc, dict)
                        and _telemetry_phase_acc.get("price") is not None
                    ):
                        _telemetry_points_acc.append(
                            (_telemetry_name_acc, float(_telemetry_phase_acc["price"]))
                        )
                if len(_telemetry_points_acc) >= 2:
                    _telemetry_prices_acc = [p for _, p in _telemetry_points_acc]
                    _telemetry_box_low_acc = min(_telemetry_prices_acc)
                    _telemetry_box_high_acc = max(_telemetry_prices_acc)
                    _telemetry_box_width_acc = (
                        (_telemetry_box_high_acc - _telemetry_box_low_acc)
                        / _telemetry_box_low_acc
                        * 100
                        if _telemetry_box_low_acc > 0
                        else None
                    )
                    _telemetry_structural_acc = bool(
                        _telemetry_box_width_acc is not None
                        and _telemetry_box_width_acc < 25
                    )
                    _telemetry_spring_acc = bool(
                        (_telemetry_phases_acc.get("Spring") or {}).get("found")
                    )
                    _telemetry_sos_acc = bool(
                        (_telemetry_phases_acc.get("SOS") or {}).get("found")
                    )
                    _audit_observe(
                        "wyckoff_accumulation",
                        {
                            "structural_box_width_pct": round(
                                _telemetry_box_width_acc, 6
                            )
                            if _telemetry_box_width_acc is not None
                            else None,
                            "structural_box_under_25": _telemetry_structural_acc,
                            "observed_phase_ready": bool(
                                _telemetry_structural_acc
                                and _telemetry_spring_acc
                                and _telemetry_sos_acc
                            ),
                            "spring_found": _telemetry_spring_acc,
                            "sos_found": _telemetry_sos_acc,
                            "structure_points": {
                                name: price for name, price in _telemetry_points_acc
                            },
                        },
                    )
            except Exception:
                pass
            if acc_range_pct < 15:
                score += 20
                signals.append(f"✅ Боковик {acc_range_pct:.1f}% за 20 дней")
            elif acc_range_pct < 25:
                score += 10
                signals.append(f"⚡️ Диапазон {acc_range_pct:.1f}% за 20 дней")
            else:
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_SPRING_R8419",
                    "acc_range_pct < 25",
                    locals(),
                    "acc_range_pct < 25",
                    8419,
                )
            phases = self._find_wyckoff_phases_accumulation(candles_1d, candles_4h)
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_SPRING_G8424",
                not phases,
                "3. ФАЗЫ WYCKOFF",
                "not phases",
                8424,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_SPRING_R8425",
                    "3. ФАЗЫ WYCKOFF",
                    locals(),
                    "not phases",
                    8425,
                )
            if "SC" in phases:
                score += 15
                signals.append(
                    f"✅ SC (Selling Climax) — паника с объёмом x{phases['SC']['vol'] / (sum((c['volume'] for c in candles_1d)) / len(candles_1d)):.1f}"
                )
            if "AR" in phases:
                score += 10
                signals.append("✅ AR (Automatic Rally) — кит выкупает")
            if "ST" in phases:
                score += 10
                signals.append("✅ ST (Secondary Test) — объём падает на тесте")
            spring_found = phases.get("Spring", {}).get("found", False)
            if spring_found:
                score += 25
                signals.append("🎯 SPRING! Ложный пробой лоу с возвратом")
            sos_found = phases.get("SOS", {}).get("found", False)
            if sos_found:
                score += 20
                signals.append("💪 SOS (Sign of Strength) — пробой AR с объёмом!")
            all_vols = [c["volume"] for c in candles_1d[-50:-20]]
            avg_vol_trend = sum(all_vols) / len(all_vols) if all_vols else 1
            acc_vols = [c["volume"] for c in accumulation_candles]
            avg_vol_acc = sum(acc_vols) / len(acc_vols) if acc_vols else 1
            vol_compression = avg_vol_acc / avg_vol_trend if avg_vol_trend > 0 else 1
            if vol_compression < 0.7:
                score += 15
                signals.append(
                    f"✅ Объём сжался {vol_compression:.0%} (тихое накопление)"
                )
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_SPRING_G8466",
                score < 50,
                "Минимальный порог",
                "score < 50",
                8466,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_SPRING_R8467",
                    "Минимальный порог",
                    locals(),
                    "score < 50",
                    8467,
                )
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_SPRING_G8469",
                not spring_found or not sos_found,
                "Требуем Spring И SOS одновременно (AND, не OR)",
                "not spring_found or not sos_found",
                8469,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_SPRING_R8470",
                    "Требуем Spring И SOS одновременно (AND, не OR)",
                    locals(),
                    "not spring_found or not sos_found",
                    8470,
                )
            try:
                _last_7 = candles_1d[-7:]
                _prev_13 = candles_1d[-20:-7]
                _avg_range_prev = (
                    sum((c["high"] - c["low"] for c in _prev_13)) / len(_prev_13)
                    if _prev_13
                    else 1
                )
                _avg_range_last = sum((c["high"] - c["low"] for c in _last_7)) / len(
                    _last_7
                )
                _avg_vol_prev = (
                    sum((c["volume"] for c in _prev_13)) / len(_prev_13)
                    if _prev_13
                    else 1
                )
                _avg_vol_last = sum((c["volume"] for c in _last_7)) / len(_last_7)
                _range_compress = (
                    _avg_range_last / _avg_range_prev if _avg_range_prev > 0 else 1
                )
                _vol_compress = (
                    _avg_vol_last / _avg_vol_prev if _avg_vol_prev > 0 else 1
                )
                if _audit_test(
                    "WYCKOFF_DETECT_WYCKOFF_SPRING_G8483",
                    _range_compress > 0.85 and _vol_compress > 0.85,
                    "_range_compress > 0.85 and _vol_compress > 0.85",
                    "_range_compress > 0.85 and _vol_compress > 0.85",
                    8483,
                ):
                    logging.info(
                        f"[WYCKOFF] {symbol}: нет сжатия (range {_range_compress:.2f}, vol {_vol_compress:.2f}) — ждём"
                    )
                    return _audit_fail(
                        "WYCKOFF_DETECT_WYCKOFF_SPRING_R8486",
                        "Нет сжатия — ещё рано входить",
                        locals(),
                        "_range_compress > 0.85 and _vol_compress > 0.85",
                        8486,
                    )
                if _range_compress < 0.7:
                    score += 10
                    signals.append(f"✅ Диапазон сжат {_range_compress:.0%}")
            except Exception:
                pass
            try:
                _wy_timing = check_entry_timing(candles_4h, "BULLISH", price_now, "4h")
                if _audit_test(
                    "WYCKOFF_DETECT_WYCKOFF_SPRING_G8496",
                    not _wy_timing.get("valid", True),
                    "check_entry_timing() — валидация тайминга входа",
                    'not _wy_timing.get("valid", True)',
                    8496,
                ):
                    logging.info(f"[WYCKOFF] {symbol}: тайминг входа не подтверждён")
                    return _audit_fail(
                        "WYCKOFF_DETECT_WYCKOFF_SPRING_R8498",
                        "check_entry_timing() — валидация тайминга входа",
                        locals(),
                        'not _wy_timing.get("valid", True)',
                        8498,
                    )
            except Exception:
                pass
            creek = acc_high
            creek_tolerance = (acc_high - acc_low) * 0.15
            if abs(price_now - creek) <= creek_tolerance:
                entry = price_now
            elif price_now < creek:
                entry = price_now
            else:
                if _audit_test(
                    "WYCKOFF_DETECT_WYCKOFF_SPRING_G8513",
                    price_now > creek * 1.05,
                    "Цена далеко выше Creek — пропускаем, поезд ушёл",
                    "price_now > creek * 1.05",
                    8513,
                ):
                    return _audit_fail(
                        "WYCKOFF_DETECT_WYCKOFF_SPRING_R8514",
                        "Цена далеко выше Creek — пропускаем, поезд ушёл",
                        locals(),
                        "price_now > creek * 1.05",
                        8514,
                    )
                entry = price_now
            atr_1d = average_true_range(candles_1d)
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_SPRING_G8518",
                not atr_1d,
                "Цена далеко выше Creek — пропускаем, поезд ушёл",
                "not atr_1d",
                8518,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_SPRING_R8519",
                    "not atr_1d",
                    locals(),
                    "not atr_1d",
                    8519,
                )
            spring_low = phases.get("Spring", {}).get("price")
            sc_low = phases.get("SC", {}).get("price")
            structural_low = min(
                (level for level in (spring_low, sc_low, acc_low) if level)
            )
            sl = smart_round(structural_low - atr_1d * 0.25)
            ar_price = phases.get("AR", {}).get("price")
            _wyk_ob = find_ob(candles_1d, "BULLISH")
            _wyk_fvg = find_fvg(candles_1d, "BULLISH")
            acc_range = acc_high - acc_low
            fib_1272 = acc_low + acc_range * 1.272
            fib_1618 = acc_low + acc_range * 1.618
            tp, tp2 = select_structural_targets(
                entry,
                sl,
                [ar_price, fib_1272, fib_1618, price_peak],
                "BULLISH",
                2.0,
                None,
            )
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_SPRING_G8540",
                tp is None,
                "tp is None",
                "tp is None",
                8540,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_SPRING_R8541",
                    "tp is None",
                    locals(),
                    "tp is None",
                    8541,
                )
            logic = ""
            try:
                phase_summary = []
                for ph in ["SC", "AR", "ST", "Spring", "SOS"]:
                    if ph in phases:
                        phase_summary.append(ph)
                _phase_vols = []
                if "SC" in phases:
                    _phase_vols.append(f"SC vol: {phases['SC'].get('vol', 0):.0f}")
                if "AR" in phases:
                    _phase_vols.append(f"AR idx: {phases['AR'].get('idx', '?')}")
                if "ST" in phases:
                    _phase_vols.append(f"ST price: {phases['ST'].get('price', 0):.6f}")
                _avg_vol_1d = (
                    sum((c["volume"] for c in candles_1d[-30:])) / 30
                    if candles_1d
                    else 0
                )
                _phase_vols.append(f"avg_vol_1d: {_avg_vol_1d:.0f}")
                _phase_vols.append(f"vol_compression: {vol_compression:.2f}")
                _wy_htf_1d = self.smc_on_tf(symbol, "1d")
                _wy_htf_1w = self.smc_on_tf(symbol, "1w")
                _wy_funding = self.get_funding_rate(symbol)
                _wy_fg = self.get_fear_greed()
                _wy_fund_str = (
                    f"{_wy_funding:+.4f}%" if _wy_funding is not None else "N/A"
                )
                _wy_fg_str = (
                    f"{_wy_fg['value']} ({_wy_fg['label']})" if _wy_fg else "N/A"
                )
                _wy_ob_str = (
                    f"OB: {_wyk_ob['bottom']:.6f}–{_wyk_ob['top']:.6f}"
                    if _wyk_ob
                    else "OB: нет"
                )
                _wy_fvg_str = (
                    f"FVG: {_wyk_fvg['bottom']:.6f}–{_wyk_fvg['top']:.6f}"
                    if _wyk_fvg
                    else "FVG: нет"
                )
                _wy_pat_str = ""
                _wy_sl_pct = round(abs(entry - sl) / entry * 100, 1) if entry else 0
                _wy_rr = (
                    round(abs(tp - entry) / abs(entry - sl), 2)
                    if abs(entry - sl) > 0
                    else 0
                )
                groq_prompt = f"""Ты SMC трейдер специализирующийся на методе Вайкоффа и накоплении/дистрибуции. Оцени качество Wyckoff Spring сетапа. Ответь СТРОГО JSON: {{"logic": "макс 15 слов", "valid": true/false}}\n\nБЛОКИРУЙ (valid: false) если:\n- Spring или SOS отсутствуют или слабые\n- Объём на Spring не выше среднего\n- Нет compression (сжатие диапазона и объёма)\n- RR < 2.5 от текущей цены до целевой\n- Цена уже выше Creek линии (пропустили вход)\n- BTC в нисходящем тренде на 4h\n- SL выставлен математически (entry ± X%), а не за структуру\n\nПОДТВЕРЖДАЙ (valid: true) если:\n- Spring пробил поддержку и вернулся — ликвидность собрана\n- SOS показал силу покупателей\n- Объём снижается в боковике (накопление завершается)\n- Цена у или ниже Creek — идеальный вход\n- TP = уровень AR (автоматический ралли) или выше\n\nУРОВНИ УЖЕ РАССЧИТАНЫ СТРАТЕГИЕЙ. НИКОГДА НЕ МЕНЯЙ entry, SL или TP.\n\nДанные: Пара: {symbol} Цена: {price_now}\nSC лоу: {phases["SC"]["price"]:.6f} | AR хай: {ar_price:.6f}\nПик до падения: {price_peak:.6f} | Даунтренд: -{drawdown_pct:.0f}%\nБоковик: {acc_low:.6f}—{acc_high:.6f}\nФазы: {", ".join(phase_summary)}\nОбъёмы фаз: {", ".join(_phase_vols)}\nEntry: {entry} SL: {sl} TP: {tp} RR: {_wy_rr} | Стоп: {_wy_sl_pct}%\n1d: {_wy_htf_1d} | 1w: {_wy_htf_1w}\nFunding: {_wy_fund_str} | Fear&Greed: {_wy_fg_str}\n{_wy_ob_str} | {_wy_fvg_str}{_wy_pat_str}"""
                groq_resp = (
                    ask_groq(groq_prompt, max_tokens=120)
                    if legacy_strategy_groq_enabled()
                    else None
                )
                if groq_resp:
                    import json as _j, re as _re

                    clean = (
                        groq_resp.strip()
                        .replace("```json", "")
                        .replace("```", "")
                        .strip()
                    )
                    m = _re.search("\\{[^}]+\\}", clean, _re.DOTALL)
                    if m:
                        parsed = _j.loads(m.group())
                        if _audit_test(
                            "WYCKOFF_DETECT_WYCKOFF_SPRING_G8626",
                            not parsed.get("valid", True),
                            "Groq как фильтр — если valid=false, блокируем",
                            'not parsed.get("valid", True)',
                            8626,
                        ):
                            logging.info(
                                f"[WYCKOFF Groq] {symbol} LONG: Groq отклонил сигнал"
                            )
                            return _audit_fail(
                                "WYCKOFF_DETECT_WYCKOFF_SPRING_R8628",
                                "Groq как фильтр — если valid=false, блокируем",
                                locals(),
                                'not parsed.get("valid", True)',
                                8628,
                            )
                        if parsed.get("logic"):
                            logic = str(parsed["logic"]).strip()
            except Exception:
                pass
            if not logic:
                logic = "Spring после SC+AR+ST — разворот Wyckoff"
            risk = abs(entry - sl)
            reward = abs(tp - entry)
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_SPRING_G8639",
                risk == 0 or reward / risk < 2.0,
                "risk == 0 or reward / risk < 2.0",
                "risk == 0 or reward / risk < 2.0",
                8639,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_SPRING_R8640",
                    "risk == 0 or reward / risk < 2.0",
                    locals(),
                    "risk == 0 or reward / risk < 2.0",
                    8640,
                )
            rr = round(reward / risk, 2)
            tp_pct = round((tp - entry) / entry * 100, 1)
            sl_pct = round((entry - sl) / entry * 100, 1)
            phase_names = [
                p
                for p in ["SC", "AR", "ST", "Spring", "SOS"]
                if p in phases
                and (p not in ["Spring", "SOS"] or phases[p].get("found"))
            ]
            return {
                "symbol": symbol,
                "direction": "BULLISH",
                "timeframe": "1d",
                "entry": entry,
                "sl": sl,
                "tp": tp,
                "tp2": tp2,
                "sl_pct": sl_pct,
                "tp_pct": tp_pct,
                "rr": rr,
                "logic": logic,
                "score": min(score, 100),
                "drawdown_pct": drawdown_pct,
                "acc_range": acc_range_pct,
                "spring": spring_found,
                "sos": sos_found,
                "phases": " → ".join(phase_names),
                "acc_low": acc_low,
                "acc_high": acc_high,
                "ob": _wyk_ob,
                "fvg": _wyk_fvg,
                "scan_type": "wyckoff",
            }
        except Exception as e:
            logging.debug(f"detect_wyckoff_spring {symbol}: {e}")
            return _audit_fail(
                "WYCKOFF_DETECT_WYCKOFF_SPRING_R8664",
                "detector returned None",
                locals(),
                "",
                8664,
            )

    def detect_wyckoff_distribution(self, symbol: str) -> dict | None:
        """
        Wyckoff Distribution UTAD — SHORT сигнал.
        Полный анализ фаз: BC → AR → ST → UTAD → SOW
        Редкий сигнал -30-200%
        """
        try:
            raw_candles_1d = self.get_candles(symbol, "1d", 61)
            raw_candles_4h = self.get_candles(symbol, "4h", 121)
            candles_1d = get_confirmed_candles(raw_candles_1d)
            candles_4h = get_confirmed_candles(raw_candles_4h)
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8683",
                not candles_1d or len(candles_1d) < 40,
                "not candles_1d or len(candles_1d) < 40",
                "not candles_1d or len(candles_1d) < 40",
                8683,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8684",
                    "not candles_1d or len(candles_1d) < 40",
                    locals(),
                    "not candles_1d or len(candles_1d) < 40",
                    8684,
                )
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8685",
                not candles_4h or len(candles_4h) < 40,
                "not candles_4h or len(candles_4h) < 40",
                "not candles_4h or len(candles_4h) < 40",
                8685,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8686",
                    "not candles_4h or len(candles_4h) < 40",
                    locals(),
                    "not candles_4h or len(candles_4h) < 40",
                    8686,
                )
            price_now = raw_candles_1d[-1]["close"]
            score = 0
            signals = []
            if symbol != "BTCUSDT":
                btc_ok, btc_reason = self.btc_allows_signal("BEARISH", use_4h=True)
                if _audit_test(
                    "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8695",
                    not btc_ok,
                    "BTC фильтр для WYCKOFF DISTRIBUTION (4h)",
                    "not btc_ok",
                    8695,
                ):
                    logging.info(
                        f"[WYCKOFF BTC Filter] {symbol} SHORT пропущен: {btc_reason}"
                    )
                    return _audit_fail(
                        "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8697",
                        "BTC фильтр для WYCKOFF DISTRIBUTION (4h)",
                        locals(),
                        "not btc_ok",
                        8697,
                    )
            price_bottom = min((c["low"] for c in candles_1d[-50:-15]))
            pump_pct = (
                (price_now - price_bottom) / price_bottom * 100
                if price_bottom > 0
                else 0
            )
            if pump_pct >= 50:
                score += 30
                signals.append(f"✅ Аптренд +{pump_pct:.0f}% от основания")
            elif pump_pct >= 30:
                score += 15
                signals.append(f"⚡️ Рост +{pump_pct:.0f}% от основания")
            else:
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8710",
                    "pump_pct >= 30",
                    locals(),
                    "pump_pct >= 30",
                    8710,
                )
            distribution_candles = candles_1d[-30:]
            dist_high = max((c["high"] for c in distribution_candles))
            dist_low = min((c["low"] for c in distribution_candles))
            dist_range_pct = (
                (dist_high - dist_low) / dist_low * 100 if dist_low > 0 else 0
            )
            _audit_observe(
                "wyckoff_distribution",
                {
                    "dist_range_pct": round(dist_range_pct, 6),
                    "old_range_under_25": bool(dist_range_pct < 25),
                },
            )
            try:
                _telemetry_phases = self._find_wyckoff_phases_distribution(
                    candles_1d, candles_4h
                )
                _telemetry_points = []
                for _telemetry_name in ("BC", "AR", "ST"):
                    _telemetry_phase = (
                        _telemetry_phases.get(_telemetry_name)
                        if isinstance(_telemetry_phases, dict)
                        else None
                    )
                    if (
                        isinstance(_telemetry_phase, dict)
                        and _telemetry_phase.get("price") is not None
                    ):
                        _telemetry_points.append(
                            (_telemetry_name, float(_telemetry_phase["price"]))
                        )
                if len(_telemetry_points) >= 2:
                    _telemetry_prices = [p for _, p in _telemetry_points]
                    _telemetry_box_low = min(_telemetry_prices)
                    _telemetry_box_high = max(_telemetry_prices)
                    _telemetry_box_width_pct = (
                        (_telemetry_box_high - _telemetry_box_low)
                        / _telemetry_box_low
                        * 100
                        if _telemetry_box_low > 0
                        else None
                    )
                    _audit_observe(
                        "wyckoff_distribution",
                        {
                            "distribution_box_width_pct": round(
                                _telemetry_box_width_pct, 6
                            )
                            if _telemetry_box_width_pct is not None
                            else None,
                            "structural_box_under_25": bool(
                                _telemetry_box_width_pct < 25
                            )
                            if _telemetry_box_width_pct is not None
                            else None,
                            "observed_phase_ready": bool(
                                _telemetry_box_width_pct is not None
                                and _telemetry_box_width_pct < 25
                                and bool(
                                    (_telemetry_phases.get("UTAD") or {}).get("found")
                                )
                                and bool(
                                    (_telemetry_phases.get("SOW") or {}).get("found")
                                )
                            ),
                            "utad_found": bool(
                                (_telemetry_phases.get("UTAD") or {}).get("found")
                            ),
                            "sow_found": bool(
                                (_telemetry_phases.get("SOW") or {}).get("found")
                            ),
                            "structure_points": {
                                name: price for name, price in _telemetry_points
                            },
                        },
                    )
            except Exception:
                pass
            _dist_range_too_wide = dist_range_pct >= 25
            if _audit_test(
                "WYCKOFF_DIST_RANGE",
                _dist_range_too_wide,
                "WYCKOFF Distribution: 30d range < 25%",
                "dist_range_pct >= 25",
                8720,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8725",
                    "WYCKOFF Distribution: 30d range < 25%",
                    locals(),
                    "dist_range_pct >= 25",
                    8725,
                )
            if dist_range_pct < 15:
                score += 20
                signals.append(f"✅ Боковик {dist_range_pct:.1f}% у вершины")
            else:
                score += 10
                signals.append(f"⚡️ Диапазон {dist_range_pct:.1f}% у вершины")
            phases = self._find_wyckoff_phases_distribution(candles_1d, candles_4h)
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8730",
                not phases,
                "3. ФАЗЫ WYCKOFF DISTRIBUTION",
                "not phases",
                8730,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8731",
                    "3. ФАЗЫ WYCKOFF DISTRIBUTION",
                    locals(),
                    "not phases",
                    8731,
                )
            if "BC" in phases:
                score += 15
                signals.append("✅ BC (Buying Climax) — эйфория с объёмом")
            if "AR" in phases:
                score += 10
                signals.append("✅ AR (Automatic Reaction) — первый откат")
            if "ST" in phases:
                score += 10
                signals.append("✅ ST (Secondary Test) — объём падает на тесте вершины")
            utad_found = phases.get("UTAD", {}).get("found", False)
            if utad_found:
                score += 25
                signals.append("🎯 UTAD! Ложный пробой хая — ловушка для покупателей")
            sow_found = phases.get("SOW", {}).get("found", False)
            if sow_found:
                score += 20
                signals.append("💪 SOW (Sign of Weakness) — пробой AR вниз с объёмом!")
            all_vols = [c["volume"] for c in candles_1d[-50:-20]]
            avg_vol_trend = sum(all_vols) / len(all_vols) if all_vols else 1
            dist_vols = [c["volume"] for c in distribution_candles]
            avg_vol_dist = sum(dist_vols) / len(dist_vols) if dist_vols else 1
            vol_compression = avg_vol_dist / avg_vol_trend if avg_vol_trend > 0 else 1
            if vol_compression < 0.7:
                score += 15
                signals.append(
                    f"✅ Объём сжался {vol_compression:.0%} (тихое распределение)"
                )
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8766",
                score < 50,
                "score < 50",
                "score < 50",
                8766,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8767",
                    "score < 50",
                    locals(),
                    "score < 50",
                    8767,
                )
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8769",
                not utad_found or not sow_found,
                "Требуем UTAD И SOW одновременно (AND, не OR)",
                "not utad_found or not sow_found",
                8769,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8770",
                    "Требуем UTAD И SOW одновременно (AND, не OR)",
                    locals(),
                    "not utad_found or not sow_found",
                    8770,
                )
            try:
                _last_7d = candles_1d[-7:]
                _prev_13d = candles_1d[-20:-7]
                _avg_range_prev_d = (
                    sum((c["high"] - c["low"] for c in _prev_13d)) / len(_prev_13d)
                    if _prev_13d
                    else 1
                )
                _avg_range_last_d = sum((c["high"] - c["low"] for c in _last_7d)) / len(
                    _last_7d
                )
                _avg_vol_prev_d = (
                    sum((c["volume"] for c in _prev_13d)) / len(_prev_13d)
                    if _prev_13d
                    else 1
                )
                _avg_vol_last_d = sum((c["volume"] for c in _last_7d)) / len(_last_7d)
                _range_comp_d = (
                    _avg_range_last_d / _avg_range_prev_d
                    if _avg_range_prev_d > 0
                    else 1
                )
                _vol_comp_d = (
                    _avg_vol_last_d / _avg_vol_prev_d if _avg_vol_prev_d > 0 else 1
                )
                if _audit_test(
                    "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8782",
                    _range_comp_d > 0.85 and _vol_comp_d > 0.85,
                    "_range_comp_d > 0.85 and _vol_comp_d > 0.85",
                    "_range_comp_d > 0.85 and _vol_comp_d > 0.85",
                    8782,
                ):
                    return _audit_fail(
                        "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8783",
                        "_range_comp_d > 0.85 and _vol_comp_d > 0.85",
                        locals(),
                        "_range_comp_d > 0.85 and _vol_comp_d > 0.85",
                        8783,
                    )
            except Exception:
                pass
            try:
                _wy_timing_d = check_entry_timing(
                    candles_4h, "BEARISH", price_now, "4h"
                )
                if _audit_test(
                    "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8790",
                    not _wy_timing_d.get("valid", True),
                    "check_entry_timing()",
                    'not _wy_timing_d.get("valid", True)',
                    8790,
                ):
                    return _audit_fail(
                        "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8791",
                        "check_entry_timing()",
                        locals(),
                        'not _wy_timing_d.get("valid", True)',
                        8791,
                    )
            except Exception:
                pass
            creek_d = dist_low
            creek_tolerance_d = (dist_high - dist_low) * 0.15
            if abs(price_now - creek_d) <= creek_tolerance_d:
                entry = price_now
            elif price_now > creek_d:
                entry = price_now
            else:
                if _audit_test(
                    "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8805",
                    price_now < creek_d * 0.95,
                    "Цена далеко ниже Creek — поезд ушёл",
                    "price_now < creek_d * 0.95",
                    8805,
                ):
                    return _audit_fail(
                        "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8806",
                        "Цена далеко ниже Creek — поезд ушёл",
                        locals(),
                        "price_now < creek_d * 0.95",
                        8806,
                    )
                entry = price_now
            atr_1d = average_true_range(candles_1d)
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8810",
                not atr_1d,
                "Цена далеко ниже Creek — поезд ушёл",
                "not atr_1d",
                8810,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8811",
                    "not atr_1d",
                    locals(),
                    "not atr_1d",
                    8811,
                )
            utad_high = phases.get("UTAD", {}).get("price")
            bc_high = phases.get("BC", {}).get("price")
            structural_high = max(
                (level for level in (utad_high, bc_high, dist_high) if level)
            )
            sl = smart_round(structural_high + atr_1d * 0.25)
            ar_price = phases.get("AR", {}).get("price")
            _wyk_ob = find_ob(candles_1d, "BEARISH")
            _wyk_fvg = find_fvg(candles_1d, "BEARISH")
            dist_range = dist_high - dist_low
            fib_1272 = dist_high - dist_range * 1.272
            fib_1618 = dist_high - dist_range * 1.618
            tp, tp2 = select_structural_targets(
                entry,
                sl,
                [ar_price, fib_1272, fib_1618, price_bottom],
                "BEARISH",
                2.0,
                None,
            )
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8832",
                tp is None,
                "tp is None",
                "tp is None",
                8832,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8833",
                    "tp is None",
                    locals(),
                    "tp is None",
                    8833,
                )
            logic = ""
            try:
                phase_summary = []
                for ph in ["BC", "AR", "ST", "UTAD", "SOW"]:
                    if ph in phases:
                        phase_summary.append(ph)
                _d_phase_vols = []
                if "BC" in phases:
                    _d_phase_vols.append(f"BC vol: {phases['BC'].get('vol', 0):.0f}")
                _avg_vol_1d_d = (
                    sum((c["volume"] for c in candles_1d[-30:])) / 30
                    if candles_1d
                    else 0
                )
                _d_phase_vols.append(f"avg_vol_1d: {_avg_vol_1d_d:.0f}")
                _d_phase_vols.append(f"vol_compression: {vol_compression:.2f}")
                _wyd_htf_1d = self.smc_on_tf(symbol, "1d")
                _wyd_htf_1w = self.smc_on_tf(symbol, "1w")
                _wyd_funding = self.get_funding_rate(symbol)
                _wyd_fg = self.get_fear_greed()
                _wyd_fund_str = (
                    f"{_wyd_funding:+.4f}%" if _wyd_funding is not None else "N/A"
                )
                _wyd_fg_str = (
                    f"{_wyd_fg['value']} ({_wyd_fg['label']})" if _wyd_fg else "N/A"
                )
                _wyd_ob_str = (
                    f"OB: {_wyk_ob['bottom']:.6f}–{_wyk_ob['top']:.6f}"
                    if _wyk_ob
                    else "OB: нет"
                )
                _wyd_fvg_str = (
                    f"FVG: {_wyk_fvg['bottom']:.6f}–{_wyk_fvg['top']:.6f}"
                    if _wyk_fvg
                    else "FVG: нет"
                )
                _wyd_pat_str = ""
                _wyd_sl_pct = round(abs(entry - sl) / entry * 100, 1) if entry else 0
                _wyd_rr = (
                    round(abs(tp - entry) / abs(entry - sl), 2)
                    if abs(entry - sl) > 0
                    else 0
                )
                groq_prompt = f"""Ты SMC трейдер специализирующийся на методе Вайкоффа Distribution (дистрибуция). Оцени качество Wyckoff Distribution сетапа для SHORT. Ответь СТРОГО JSON: {{"logic": "макс 15 слов", "valid": true/false}}\n\nБЛОКИРУЙ (valid: false) если:\n- UTAD или SOW отсутствуют или слабые\n- Объём на UTAD не выше среднего\n- RR < 2.5 от текущей цены до целевой\n- Цена уже ниже AR лоу (пропустили вход)\n- BTC в восходящем тренде на 4h\n- SL выставлен математически (entry ± X%), а не за структуру\n\nПОДТВЕРЖДАЙ (valid: true) если:\n- UTAD пробил вершину и вернулся — ликвидность собрана\n- SOW показал слабость покупателей\n- Объём снижается у вершины (дистрибуция завершается)\n- Цена у или выше Ice Line — идеальный SHORT\n- TP = уровень AR лоу или ниже\n\nУРОВНИ УЖЕ РАССЧИТАНЫ СТРАТЕГИЕЙ. НИКОГДА НЕ МЕНЯЙ entry, SL или TP.\n\nДанные: Пара: {symbol} Цена: {price_now}\nBC хай: {phases["BC"]["price"]:.6f} | AR лоу: {ar_price:.6f}\nОснование до роста: {price_bottom:.6f} | Рост: +{pump_pct:.0f}%\nБоковик: {dist_low:.6f}—{dist_high:.6f}\nФазы: {", ".join(phase_summary)}\nОбъёмы фаз: {", ".join(_d_phase_vols)}\nEntry: {entry} SL: {sl} TP: {tp} RR: {_wyd_rr} | Стоп: {_wyd_sl_pct}%\n1d: {_wyd_htf_1d} | 1w: {_wyd_htf_1w}\nFunding: {_wyd_fund_str} | Fear&Greed: {_wyd_fg_str}\n{_wyd_ob_str} | {_wyd_fvg_str}{_wyd_pat_str}"""
                groq_resp = (
                    ask_groq(groq_prompt, max_tokens=120)
                    if legacy_strategy_groq_enabled()
                    else None
                )
                if groq_resp:
                    import json as _j, re as _re

                    clean = (
                        groq_resp.strip()
                        .replace("```json", "")
                        .replace("```", "")
                        .strip()
                    )
                    m = _re.search("\\{[^}]+\\}", clean, _re.DOTALL)
                    if m:
                        parsed = _j.loads(m.group())
                        if _audit_test(
                            "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8913",
                            not parsed.get("valid", True),
                            "Groq как фильтр — если valid=false, блокируем",
                            'not parsed.get("valid", True)',
                            8913,
                        ):
                            logging.info(
                                f"[WYCKOFF Groq] {symbol} SHORT: Groq отклонил сигнал"
                            )
                            return _audit_fail(
                                "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8915",
                                "Groq как фильтр — если valid=false, блокируем",
                                locals(),
                                'not parsed.get("valid", True)',
                                8915,
                            )
                        if parsed.get("logic"):
                            logic = str(parsed["logic"]).strip()
            except Exception:
                pass
            if not logic:
                logic = "UTAD после BC+AR+ST — дистрибуция Wyckoff"
            risk = abs(sl - entry)
            reward = abs(entry - tp)
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8926",
                risk == 0 or reward / risk < 2.0,
                "risk == 0 or reward / risk < 2.0",
                "risk == 0 or reward / risk < 2.0",
                8926,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8927",
                    "risk == 0 or reward / risk < 2.0",
                    locals(),
                    "risk == 0 or reward / risk < 2.0",
                    8927,
                )
            rr = round(reward / risk, 2)
            tp_pct = round((entry - tp) / entry * 100, 1)
            sl_pct = round((sl - entry) / entry * 100, 1)
            phase_names = [
                p
                for p in ["BC", "AR", "ST", "UTAD", "SOW"]
                if p in phases and (p not in ["UTAD", "SOW"] or phases[p].get("found"))
            ]
            return {
                "symbol": symbol,
                "direction": "BEARISH",
                "timeframe": "1d",
                "entry": entry,
                "sl": sl,
                "tp": tp,
                "tp2": tp2,
                "sl_pct": sl_pct,
                "tp_pct": tp_pct,
                "rr": rr,
                "logic": logic,
                "score": min(score, 100),
                "pump_pct": pump_pct,
                "dist_range": dist_range_pct,
                "utad": utad_found,
                "sow": sow_found,
                "phases": " → ".join(phase_names),
                "dist_low": dist_low,
                "dist_high": dist_high,
                "ob": _wyk_ob,
                "fvg": _wyk_fvg,
                "scan_type": "wyckoff",
            }
        except Exception as e:
            logging.debug(f"detect_wyckoff_distribution {symbol}: {e}")
            return _audit_fail(
                "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8951",
                "detector returned None",
                locals(),
                "",
                8951,
            )

    def detect_wyckoff_reaccumulation(self, symbol: str) -> dict | None:
        """
        Re-accumulation: боковик после коррекции + higher lows + ликвидность выше
        Работает чаще чем классический Wyckoff (раз в неделю vs раз в полгода)
        """
        try:
            raw_candles_1d = self.get_candles(symbol, "1d", 61)
            raw_candles_4h = self.get_candles(symbol, "4h", 101)
            candles_1d = get_confirmed_candles(raw_candles_1d)
            candles_4h = get_confirmed_candles(raw_candles_4h)
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G8968",
                not candles_1d or len(candles_1d) < 30,
                "not candles_1d or len(candles_1d) < 30",
                "not candles_1d or len(candles_1d) < 30",
                8968,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R8968",
                    "not candles_1d or len(candles_1d) < 30",
                    locals(),
                    "not candles_1d or len(candles_1d) < 30",
                    8968,
                )
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G8969",
                not candles_4h or len(candles_4h) < 50,
                "not candles_4h or len(candles_4h) < 50",
                "not candles_4h or len(candles_4h) < 50",
                8969,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R8969",
                    "not candles_4h or len(candles_4h) < 50",
                    locals(),
                    "not candles_4h or len(candles_4h) < 50",
                    8969,
                )
            price_now = raw_candles_1d[-1]["close"]
            price_peak = max((c["high"] for c in candles_1d[-40:-10]))
            drawdown_pct = (price_peak - price_now) / price_peak * 100
            _min_drawdown = 3 if symbol in ["BTCUSDT", "ETHUSDT", "BNBUSDT"] else 5
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G8977",
                drawdown_pct < _min_drawdown,
                "1. Коррекция от пика (5% для BTC/ETH/BNB, 8% для остальных)",
                "drawdown_pct < _min_drawdown",
                8977,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R8978",
                    "1. Коррекция от пика (5% для BTC/ETH/BNB, 8% для остальных)",
                    locals(),
                    "drawdown_pct < _min_drawdown",
                    8978,
                )
            acc_candles = candles_1d[-30:]
            acc_high = max((c["high"] for c in acc_candles))
            acc_low = min((c["low"] for c in acc_candles))
            acc_range_pct = (acc_high - acc_low) / acc_low * 100
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G8985",
                acc_range_pct > 15,
                "2. Боковик последние 10-30 дней (range < 15%)",
                "acc_range_pct > 15",
                8985,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R8986",
                    "acc_range_pct > 15",
                    locals(),
                    "acc_range_pct > 15",
                    8986,
                )
            lows_20 = [c["low"] for c in acc_candles]
            local_lows = []
            for i in range(1, len(lows_20) - 1):
                if lows_20[i] < lows_20[i - 1] and lows_20[i] < lows_20[i + 1]:
                    local_lows.append(lows_20[i])
            higher_lows = len(local_lows) >= 2 and local_lows[-1] > local_lows[-2]
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G8995",
                not higher_lows,
                "not higher_lows",
                "not higher_lows",
                8995,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R8996",
                    "not higher_lows",
                    locals(),
                    "not higher_lows",
                    8996,
                )
            avg_vol_before = sum((c["volume"] for c in candles_1d[-40:-20])) / 20
            avg_vol_acc = sum((c["volume"] for c in acc_candles)) / len(acc_candles)
            vol_compressed = avg_vol_acc < avg_vol_before * 0.8
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G9002",
                not vol_compressed,
                "4. Volume compression — объём снижается в боковике",
                "not vol_compressed",
                9002,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R9003",
                    "4. Volume compression — объём снижается в боковике",
                    locals(),
                    "not vol_compressed",
                    9003,
                )
            last_vol = candles_1d[-1]["volume"]
            avg_vol_acc_last = sum((c["volume"] for c in candles_1d[-10:-1])) / 9
            vol_expanding = last_vol > avg_vol_acc_last * 1.5
            highs_acc = [c["high"] for c in acc_candles]
            eqh_levels = [h for h in highs_acc if abs(h - acc_high) / acc_high < 0.005]
            liquidity_target = acc_high if len(eqh_levels) >= 2 else price_peak
            if symbol != "BTCUSDT":
                btc_ok, _ = self.btc_allows_signal("BULLISH")
                if _audit_test(
                    "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G9018",
                    not btc_ok,
                    "7. BTC фильтр",
                    "not btc_ok",
                    9018,
                ):
                    return _audit_fail(
                        "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R9018",
                        "7. BTC фильтр",
                        locals(),
                        "not btc_ok",
                        9018,
                    )
            entry = smart_round(price_now)
            atr_1d = average_true_range(candles_1d)
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G9023",
                not atr_1d,
                "8. Расчёт уровней",
                "not atr_1d",
                9023,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R9024",
                    "8. Расчёт уровней",
                    locals(),
                    "not atr_1d",
                    9024,
                )
            sl = smart_round(acc_low - atr_1d * 0.25)
            acc_range = acc_high - acc_low
            fib_1272 = acc_low + acc_range * 1.272
            fib_1618 = acc_low + acc_range * 1.618
            tp, tp2 = select_structural_targets(
                entry,
                sl,
                [liquidity_target, fib_1272, fib_1618, price_peak],
                "BULLISH",
                2.0,
                None,
            )
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G9034",
                tp is None,
                "tp is None",
                "tp is None",
                9034,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R9035",
                    "tp is None",
                    locals(),
                    "tp is None",
                    9035,
                )
            risk = abs(entry - sl)
            reward = abs(tp - entry)
            rr = round(reward / risk, 2)
            signals = ["Higher Lows", "Vol Compression", "Liquidity Above"]
            if vol_expanding:
                signals.append("Vol Expansion")
            _ranges_wy = [c["high"] - c["low"] for c in candles_1d[-10:]]
            _avg_range_early = sum(_ranges_wy[:5]) / 5 if len(_ranges_wy) >= 5 else 1
            _avg_range_late = (
                sum(_ranges_wy[5:]) / 5 if len(_ranges_wy) >= 10 else _avg_range_early
            )
            if _avg_range_late < _avg_range_early * 0.8:
                signals.append("Range Tightening")
            try:
                _wyk_prompt = f'Ты SMC трейдер эксперт по накоплению Вайкоффа.\nОтвечай СТРОГО JSON: {{"logic": "макс 15 слов", "valid": true/false}}\n\nКАК ДУМАТЬ:\n1. Higher lows = покупатели накапливают позиции\n2. Volume compression = умные деньги поглощают продажи тихо\n3. Ликвидность выше (EQH) = цель для выноса стопов\n4. Стоп ЗА acc_low — ниже всей зоны накопления\n5. TP на ликвидности (EQH/swing high)\n\nБЛОКИРУЙ если:\n- Higher lows слабые или нет compression\n- RR={rr} < 2.0\n- BTC в нисходящем тренде\n- Нет чёткой ликвидности выше для TP\n- SL выставлен математически (entry ± X%), а не за структуру\n\nПОДТВЕРЖДАЙ если:\n- Чёткие higher lows + volume compression\n- Коррекция 8%+ от пика завершена\n- Ликвидность (EQH) чётко видна выше\n\nУРОВНИ УЖЕ РАССЧИТАНЫ СТРАТЕГИЕЙ. НИКОГДА НЕ МЕНЯЙ entry, SL или TP.\n\nДанные: drawdown={round(drawdown_pct, 1)}% range={round(acc_range_pct, 1)}% higher_lows={higher_lows} vol_compressed={vol_compressed} vol_expanding={vol_expanding} (объём растёт = выход начался)\nentry={smart_price_fmt(entry)} sl={smart_price_fmt(sl)} tp={smart_price_fmt(tp)} RR={rr}'
                _resp = (
                    ask_groq(_wyk_prompt, max_tokens=100)
                    if legacy_strategy_groq_enabled()
                    else None
                )
                if _resp:
                    import json as _j, re as _re

                    _m = _re.search("\\{[^}]+\\}", _resp, _re.DOTALL)
                    if _m:
                        _p = _j.loads(_m.group())
                        if _audit_test(
                            "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G9089",
                            not _p.get("valid", True),
                            'not _p.get("valid", True)',
                            'not _p.get("valid", True)',
                            9089,
                        ):
                            return _audit_fail(
                                "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R9089",
                                'not _p.get("valid", True)',
                                locals(),
                                'not _p.get("valid", True)',
                                9089,
                            )
            except Exception:
                pass
            risk = abs(entry - sl)
            reward = abs(tp - entry)
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G9096",
                risk == 0,
                "TP remains the structural liquidity target calculated above.",
                "risk == 0",
                9096,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R9097",
                    "TP remains the structural liquidity target calculated above.",
                    locals(),
                    "risk == 0",
                    9097,
                )
            rr = round(reward / risk, 2)
            if _audit_test(
                "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G9099",
                rr < 2.0,
                "rr < 2.0",
                "rr < 2.0",
                9099,
            ):
                return _audit_fail(
                    "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R9100",
                    "rr < 2.0",
                    locals(),
                    "rr < 2.0",
                    9100,
                )
            tp_pct = round((tp - entry) / entry * 100, 1)
            sl_pct = round((entry - sl) / entry * 100, 1)
            return {
                "symbol": symbol,
                "direction": "BULLISH",
                "timeframe": "1d",
                "entry": entry,
                "sl": sl,
                "tp": tp,
                "tp2": tp2,
                "sl_pct": sl_pct,
                "tp_pct": tp_pct,
                "rr": rr,
                "score": 75,
                "signals": signals,
                "logic": f"Re-accumulation: higher lows + liquidity {smart_price_fmt(liquidity_target)}",
                "drawdown_pct": drawdown_pct,
                "acc_range": acc_range_pct,
                "phases": "Re-accumulation",
            }
        except Exception as e:
            logging.warning(f"detect_wyckoff_reaccumulation {symbol}: {e}")
            return _audit_fail(
                "WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R9114",
                "detector returned None",
                locals(),
                "",
                9114,
            )

    def detect_fast_deal(self, symbol: str) -> dict | None:
        """
        SMC FAST: 4h context, confirmed 15m setup and optional 5m control:
        1. BTC направление — синхронизация с рынком
        2. 4h/1h trend — торгуем только по тренду
        3. 4h OB/FVG — цена в зоне интереса
        4. 15m displacement/engulfing с объёмом — подтверждение
        5. SL и TP — только за структурой/ликвидностью, без фиксированных %.
        """
        try:
            if _audit_test(
                "FAST_DETECT_FAST_DEAL_G9138",
                not self._fast_session(),
                "manual FAST scans make the same decision in summer and winter.",
                "not _fast_session()",
                9138,
            ):
                return _audit_fail(
                    "FAST_DETECT_FAST_DEAL_R9139",
                    "manual FAST scans make the same decision in summer and winter.",
                    locals(),
                    "not _fast_session()",
                    9139,
                )
            _fast_context_15m = get_confirmed_candles(
                self.get_candles(symbol, "15m", 61)
            )
            if _audit_test(
                "FAST_LTF_CONTEXT_DATA",
                not _fast_context_15m or len(_fast_context_15m) < 30,
                "FAST: enough closed 15m context",
                "not _fast_context_15m or len(_fast_context_15m) < 30",
                9142,
            ):
                return _audit_fail(
                    "FAST_LTF_CONTEXT_R_DATA",
                    "FAST: enough closed 15m context",
                    locals(),
                    "not _fast_context_15m or len(_fast_context_15m) < 30",
                    9142,
                )
            _fast_bull_event = get_bos_choch_event(
                _fast_context_15m, "BULLISH", lookback=15, max_break_age=4
            )
            _fast_bear_event = get_bos_choch_event(
                _fast_context_15m, "BEARISH", lookback=15, max_break_age=4
            )
            if _audit_test(
                "FAST_LTF_CONTEXT_STRUCTURE",
                bool(_fast_bull_event) == bool(_fast_bear_event),
                "FAST: one fresh 15m BOS/CHoCH direction",
                "bool(_fast_bull_event) == bool(_fast_bear_event)",
                9145,
            ):
                return _audit_fail(
                    "FAST_LTF_CONTEXT_R_STRUCTURE",
                    "FAST: one fresh 15m BOS/CHoCH direction",
                    locals(),
                    "bool(_fast_bull_event) == bool(_fast_bear_event)",
                    9145,
                )
            direction = "BULLISH" if _fast_bull_event else "BEARISH"
            _fast_thesis_event = _fast_bull_event or _fast_bear_event
            _fast_bos_age = max(
                1,
                len(_fast_context_15m)
                - int(
                    _fast_thesis_event.get("candle_index", len(_fast_context_15m) - 1)
                ),
            )
            _audit_observe(
                "bos_event",
                {
                    "role": "FAST_THESIS",
                    "timeframe": "15m",
                    "age_bars": _fast_bos_age,
                    "event_type": _fast_thesis_event.get("type"),
                    "direction": _fast_thesis_event.get("direction"),
                },
            )
            _audit_observe("bos_progress", {"structure_confirmed": True})
            direction_4h = self.smc_on_tf(symbol, "4h")
            direction_1h = self.smc_on_tf(symbol, "1h")
            _fast_htf_support = direction_1h == direction or direction_4h == direction
            if _audit_test(
                "FAST_HTF_SUPPORT",
                not _fast_htf_support,
                "FAST: 1h or 4h supports 15m direction",
                "not _fast_htf_support",
                9150,
            ):
                return _audit_fail(
                    "FAST_HTF_R_SUPPORT",
                    "FAST: 1h or 4h supports 15m direction",
                    locals(),
                    "not _fast_htf_support",
                    9150,
                )
            btc_direction_1h = self.smc_on_tf("BTCUSDT", "1h")
            btc_direction_4h = self.smc_on_tf("BTCUSDT", "4h")
            btc_trend = btc_direction_1h or btc_direction_4h or "NEUTRAL"
            if symbol != "BTCUSDT":
                _fast_btc_hard_conflict = (
                    btc_direction_1h is not None
                    and btc_direction_4h is not None
                    and (btc_direction_1h == btc_direction_4h)
                    and (btc_direction_1h != direction)
                )
                if _audit_test(
                    "FAST_BTC_HARD_CONFLICT",
                    _fast_btc_hard_conflict,
                    "FAST: BTC 1h+4h both oppose 15m thesis",
                    "_fast_btc_hard_conflict",
                    9158,
                ):
                    return _audit_fail(
                        "FAST_BTC_R_HARD_CONFLICT",
                        "FAST: BTC 1h+4h both oppose 15m thesis",
                        locals(),
                        "_fast_btc_hard_conflict",
                        9158,
                    )
            direction_1d = direction_1h or direction_4h or direction
            _fast_funding_warning = ""
            try:
                _fast_funding = self.get_funding_rate(symbol)
                if _fast_funding is not None and abs(_fast_funding) > 0.2:
                    if (
                        direction == "BULLISH"
                        and _fast_funding > 0.2
                        or (direction == "BEARISH" and _fast_funding < -0.2)
                    ):
                        _fast_funding_warning = (
                            f"extreme crowded funding {_fast_funding:+.4f}%"
                        )
                        logging.info(
                            "[FAST Funding Warning] %s: %s",
                            symbol,
                            _fast_funding_warning,
                        )
            except Exception:
                pass
            raw_candles_4h = self.get_candles(symbol, "4h", 51)
            candles_4h = get_confirmed_candles(raw_candles_4h)
            if _audit_test(
                "FAST_DETECT_FAST_DEAL_G9192",
                not candles_4h or len(candles_4h) < 20,
                "3. 4h OB/FVG зона",
                "not candles_4h or len(candles_4h) < 20",
                9192,
            ):
                return _audit_fail(
                    "FAST_DETECT_FAST_DEAL_R9193",
                    "3. 4h OB/FVG зона",
                    locals(),
                    "not candles_4h or len(candles_4h) < 20",
                    9193,
                )
            price_now = raw_candles_4h[-1]["close"]
            ob_4h = find_ob(candles_4h, direction)
            fvg_4h = find_fvg(candles_4h, direction)
            in_zone = False
            zone_desc = ""
            atr_4h = average_true_range(candles_4h, 14)
            _ap_fast = self.get_adaptive_params(symbol, candles_4h)
            _zone_tol = atr_4h * _ap_fast["volatility_factor"] * 0.5
            if ob_4h:
                zone_bottom = ob_4h["bottom"]
                zone_top = ob_4h["top"]
                if (
                    direction == "BULLISH"
                    and zone_bottom - _zone_tol <= price_now <= zone_top + _zone_tol
                ):
                    in_zone = True
                    zone_desc = f"4h OB ${zone_bottom:.4f}–${zone_top:.4f}"
                elif (
                    direction == "BEARISH"
                    and zone_bottom - _zone_tol <= price_now <= zone_top + _zone_tol
                ):
                    in_zone = True
                    zone_desc = f"4h OB ${zone_bottom:.4f}–${zone_top:.4f}"
            if not in_zone and fvg_4h:
                zone_bottom = fvg_4h["bottom"]
                zone_top = fvg_4h["top"]
                if zone_bottom - _zone_tol <= price_now <= zone_top + _zone_tol:
                    in_zone = True
                    zone_desc = f"4h FVG ${zone_bottom:.4f}–${zone_top:.4f}"
            _fast_4h_zone_context = bool(in_zone)
            _range_high = max((c["high"] for c in candles_4h[-20:]))
            _range_low = min((c["low"] for c in candles_4h[-20:]))
            _range_mid = (_range_high + _range_low) / 2
            _range_size = _range_high - _range_low
            _in_premium = price_now > _range_mid + _range_size * 0.1
            _in_discount = price_now < _range_mid - _range_size * 0.1
            _no_middle_ok = (
                direction == "BULLISH"
                and _in_discount
                or (direction == "BEARISH" and _in_premium)
            )
            _fast_pd_context = bool(_no_middle_ok)
            candles_15m_imp = get_confirmed_candles(self.get_candles(symbol, "15m", 21))
            if _audit_test(
                "FAST_DETECT_FAST_DEAL_G9242",
                not candles_15m_imp or len(candles_15m_imp) < 3,
                "4. 15m импульсная свеча (подтверждение на младшем ТФ)",
                "not candles_15m_imp or len(candles_15m_imp) < 3",
                9242,
            ):
                return _audit_fail(
                    "FAST_DETECT_FAST_DEAL_R9243",
                    "4. 15m импульсная свеча (подтверждение на младшем ТФ)",
                    locals(),
                    "not candles_15m_imp or len(candles_15m_imp) < 3",
                    9243,
                )
            last_15m = candles_15m_imp[-1]
            _avg_vol_15m_imp = sum(
                (c.get("volume", 0) for c in candles_15m_imp[:-1])
            ) / max(len(candles_15m_imp) - 1, 1)
            _fast_impulse_volume_context_weak = bool(
                _avg_vol_15m_imp > 0
                and last_15m.get("volume", 0) < _avg_vol_15m_imp * 1.1
            )
            _audit_test(
                "FAST_IMPULSE_VOLUME_CONTEXT",
                _fast_impulse_volume_context_weak,
                "15m preliminary impulse volume >= 1.1x average (non-blocking)",
                "_fast_impulse_volume_context_weak",
                9249,
            )
            candles_15m = get_confirmed_candles(self.get_candles(symbol, "15m", 31))
            if _audit_test(
                "FAST_DETECT_FAST_DEAL_G9254",
                not candles_15m or len(candles_15m) < 10,
                "FAST: enough closed 15m trigger candles",
                "not candles_15m or len(candles_15m) < 10",
                9254,
            ):
                return _audit_fail(
                    "FAST_DETECT_FAST_DEAL_R9255",
                    "FAST: enough closed 15m trigger candles",
                    locals(),
                    "not candles_15m or len(candles_15m) < 10",
                    9255,
                )
            atr_15m = average_true_range(candles_15m, 14)
            _fast_ob_15m = find_ob(candles_15m, direction)
            _fast_fvg_15m = find_fvg(candles_15m, direction)
            _fast_zone_15m = _fast_ob_15m or _fast_fvg_15m
            if _audit_test(
                "FAST_LTF_ZONE",
                not _fast_zone_15m,
                "FAST: 15m OB/FVG zone exists",
                "not _fast_zone_15m",
                9260,
            ):
                return _audit_fail(
                    "FAST_LTF_R_ZONE",
                    "FAST: 15m OB/FVG zone exists",
                    locals(),
                    "not _fast_zone_15m",
                    9260,
                )
            _fast_zone_bottom = float(_fast_zone_15m["bottom"])
            _fast_zone_top = float(_fast_zone_15m["top"])
            _fast_zone_tol = atr_15m * 0.2
            _fast_ltf_retest = any(
                (
                    float(c["low"]) <= _fast_zone_top + _fast_zone_tol
                    and float(c["high"]) >= _fast_zone_bottom - _fast_zone_tol
                    for c in candles_15m[-8:]
                )
            )
            _audit_observe(
                "bos_progress",
                {"retest_reached": True, "retest_confirmed": bool(_fast_ltf_retest)},
            )
            if _audit_test(
                "FAST_LTF_RETEST",
                not _fast_ltf_retest,
                "FAST: recent 15m OB/FVG retest",
                "not _fast_ltf_retest",
                9261,
            ):
                return _audit_fail(
                    "FAST_LTF_R_RETEST",
                    "FAST: recent 15m OB/FVG retest",
                    locals(),
                    "not _fast_ltf_retest",
                    9261,
                )
            _fast_ltf_zone_type = "OB" if _fast_ob_15m else "FVG"
            zone_desc = f"15m {_fast_ltf_zone_type} ${_fast_zone_bottom:.4f}–${_fast_zone_top:.4f}"
            engulfing_found = False
            entry = None
            sl = None
            _fast_telem_displacement_seen = False
            _fast_telem_engulfing_seen = False
            _fast_telem_volume_confirmed = False
            for i in range(1, 11):
                if i >= len(candles_15m):
                    break
                curr = candles_15m[-i]
                prev = candles_15m[-i - 1]
                curr_body = abs(curr["close"] - curr["open"])
                curr_range = curr["high"] - curr["low"]
                prev_body = abs(prev["close"] - prev["open"])
                if curr_range > 0 and curr_body / curr_range < 0.65:
                    continue
                _fast_telem_displacement_seen = True
                if direction == "BULLISH":
                    bull_eng = (
                        curr["close"] > curr["open"]
                        and curr["open"] <= prev["close"]
                        and (curr["close"] >= prev["open"])
                        and (curr_body > prev_body * 1.1)
                    )
                    if not bull_eng:
                        continue
                    entry = smart_round(curr["close"])
                    sl = smart_round(curr["low"] - atr_15m * 0.5)
                else:
                    bear_eng = (
                        curr["close"] < curr["open"]
                        and curr["open"] >= prev["close"]
                        and (curr["close"] <= prev["open"])
                        and (curr_body > prev_body * 1.1)
                    )
                    if not bear_eng:
                        continue
                    entry = smart_round(curr["close"])
                    sl = smart_round(curr["high"] + atr_15m * 0.5)
                _fast_telem_engulfing_seen = True
                _vol_threshold = 1.6
                avg_vol_15m = sum((c["volume"] for c in candles_15m[-20:-1])) / 19
                if avg_vol_15m > 0 and curr["volume"] < avg_vol_15m * _vol_threshold:
                    continue
                _fast_telem_volume_confirmed = True
                engulfing_found = True
                _sweep_candles_ago = i
                break
            _audit_observe(
                "bos_progress",
                {
                    "displacement_reached": True,
                    "displacement_confirmed": bool(_fast_telem_displacement_seen),
                    "volume_reached": bool(_fast_telem_engulfing_seen),
                    "volume_confirmed": bool(_fast_telem_volume_confirmed),
                },
            )
            _audit_observe(
                "fast_trigger",
                {
                    "displacement_seen": bool(_fast_telem_displacement_seen),
                    "engulfing_seen": bool(_fast_telem_engulfing_seen),
                    "volume_1_6_confirmed": bool(_fast_telem_volume_confirmed),
                },
            )
            if _audit_test(
                "FAST_DETECT_FAST_DEAL_G9303",
                not engulfing_found or entry is None,
                "FAST: displacement + engulfing + volume >= 1.6x",
                "not engulfing_found or entry is None",
                9303,
            ):
                return _audit_fail(
                    "FAST_DETECT_FAST_DEAL_R9304",
                    "FAST: displacement + engulfing + volume >= 1.6x",
                    locals(),
                    "not engulfing_found or entry is None",
                    9304,
                )
            _eng_idx = _sweep_candles_ago if "_sweep_candles_ago" in dir() else 1
            _eng_candle = (
                candles_15m[-_eng_idx]
                if _eng_idx < len(candles_15m)
                else candles_15m[-1]
            )
            _acceptance = bool(_fast_ltf_retest and engulfing_found)
            _fast_structure_event = get_bos_choch_event(
                candles_15m,
                direction,
                lookback=15,
                max_break_age=min(3, max(1, _sweep_candles_ago)),
            )
            if _audit_test(
                "FAST_DETECT_FAST_DEAL_G9332",
                not _fast_structure_event,
                "not _fast_structure_event",
                "not _fast_structure_event",
                9332,
            ):
                logging.debug(f"[FAST] {symbol}: нет свежего 15m BOS/CHoCH")
                return _audit_fail(
                    "FAST_DETECT_FAST_DEAL_R9334",
                    "not _fast_structure_event",
                    locals(),
                    "not _fast_structure_event",
                    9334,
                )
            _fast_exec_bos_age = max(
                1,
                len(candles_15m)
                - int(_fast_structure_event.get("candle_index", len(candles_15m) - 1)),
            )
            _audit_observe(
                "bos_execution_event",
                {
                    "role": "FAST_EXECUTION",
                    "timeframe": "15m",
                    "age_bars": _fast_exec_bos_age,
                    "event_type": _fast_structure_event.get("type"),
                    "direction": _fast_structure_event.get("direction"),
                },
            )
            _fast_highs, _fast_lows = find_swings(candles_15m, lookback=3)
            if direction == "BULLISH":
                _fast_targets = sorted(
                    {level for _, level in _fast_highs if level > entry * 1.001}
                )
            else:
                _fast_targets = sorted(
                    {level for _, level in _fast_lows if level < entry * 0.999},
                    reverse=True,
                )
            _audit_observe(
                "fast_target_geometry",
                {
                    "swing_high_count": len(_fast_highs),
                    "swing_low_count": len(_fast_lows),
                    "targets_ahead_count": len(_fast_targets),
                    "entry": entry,
                    "direction": direction,
                    "reason": "confirmed_15m_swing_ahead"
                    if _fast_targets
                    else "no_confirmed_15m_swing_ahead",
                },
            )
            if _audit_test(
                "FAST_DETECT_FAST_DEAL_G9344",
                not _fast_targets,
                "FAST: confirmed 15m swing target ahead of entry",
                "not _fast_targets",
                9344,
            ):
                return _audit_fail(
                    "FAST_DETECT_FAST_DEAL_R9345",
                    "FAST: confirmed 15m swing target ahead of entry",
                    locals(),
                    "not _fast_targets",
                    9345,
                )
            risk = abs(entry - sl)
            if _audit_test(
                "FAST_DETECT_FAST_DEAL_G9353",
                risk == 0,
                "RR проверка",
                "risk == 0",
                9353,
            ):
                return _audit_fail(
                    "FAST_DETECT_FAST_DEAL_R9354",
                    "RR проверка",
                    locals(),
                    "risk == 0",
                    9354,
                )
            _fast_target_prices = []
            for _fast_target_raw in _fast_targets:
                _fast_target_price = smart_round(_fast_target_raw)
                if _fast_target_price not in _fast_target_prices:
                    _fast_target_prices.append(_fast_target_price)
            _fast_target_geometry = [
                {"price": target, "rr": round(abs(target - entry) / risk, 4)}
                for target in _fast_target_prices
            ]
            _fast_qualifying_targets = [
                item for item in _fast_target_geometry if item["rr"] >= 2.0
            ]
            _fast_intermediate_targets = [
                item for item in _fast_target_geometry if item["rr"] < 2.0
            ]
            _fast_selected_targets = (
                _fast_qualifying_targets or _fast_target_geometry[:1]
            )
            tp1 = _fast_selected_targets[0]["price"]
            tp2 = (
                _fast_qualifying_targets[1]["price"]
                if len(_fast_qualifying_targets) > 1
                else tp1
            )
            tp = tp1
            reward = abs(tp1 - entry)
            rr = round(reward / risk, 2)
            _audit_observe(
                "fast_rr_geometry",
                {
                    "target_count": len(_fast_target_geometry),
                    "nearest_structural_target": _fast_target_geometry[0]["price"]
                    if _fast_target_geometry
                    else None,
                    "nearest_structural_rr": _fast_target_geometry[0]["rr"]
                    if _fast_target_geometry
                    else None,
                    "best_structural_rr": max(
                        (item["rr"] for item in _fast_target_geometry), default=None
                    ),
                    "qualifying_target_count": len(_fast_qualifying_targets),
                    "intermediate_target_count": len(_fast_intermediate_targets),
                    "selected_tp1": tp1,
                    "selected_tp1_rr": rr,
                    "targets": _fast_target_geometry[:8],
                },
            )
            _audit_observe(
                "bos_progress", {"rr_reached": True, "rr_passed": bool(rr >= 2.0)}
            )
            if _audit_test(
                "FAST_DETECT_FAST_DEAL_G9356", rr < 2.0, "rr < 2.0", "rr < 2.0", 9356
            ):
                return _audit_fail(
                    "FAST_DETECT_FAST_DEAL_R9357",
                    "rr < 2.0",
                    locals(),
                    "rr < 2.0",
                    9357,
                )
            sl_pct = round(abs(entry - sl) / entry * 100, 2)
            tp_pct = round(abs(tp1 - entry) / entry * 100, 2)
            tp2_pct = round(abs(tp2 - entry) / entry * 100, 2)
            entry_drift_pct = round(max(0.15, min(0.6, atr_15m / entry * 50)), 2)
            logic = ""
            try:
                _ob_4h_desc = (
                    f"OB: {ob_4h['bottom']:.4f}–{ob_4h['top']:.4f}"
                    if ob_4h
                    else "OB: нет"
                )
                _fvg_4h_desc = (
                    f"FVG: {fvg_4h['bottom']:.4f}–{fvg_4h['top']:.4f}"
                    if fvg_4h
                    else "FVG: нет"
                )
                _eng_c = candles_15m[-_sweep_candles_ago]
                _avg_vol_15m_g = (
                    sum((c.get("volume", 0) for c in candles_15m[-20:-1])) / 19
                    if len(candles_15m) >= 20
                    else 0
                )
                _eng_vol_desc = (
                    f"Vol engulfing: {_eng_c.get('volume', 0):.0f}, avg: {_avg_vol_15m_g:.0f}"
                    if _avg_vol_15m_g > 0
                    else ""
                )
                _fast_fg = self.get_fear_greed()
                _fast_funding = self.get_funding_rate(symbol)
                _fast_regime = self.get_market_regime(symbol)
                _fast_fg_str = (
                    f"{_fast_fg['value']} ({_fast_fg['label']})" if _fast_fg else "N/A"
                )
                _fast_fund_str = (
                    f"{_fast_funding:+.4f}%" if _fast_funding is not None else "N/A"
                )
                _fast_regime_str = (
                    _fast_regime.get("mode", "?")
                    if isinstance(_fast_regime, dict)
                    else str(_fast_regime)
                )
                _fast_pat_str = ""
                _fast_sl_pct = (
                    round(abs(entry - sl) / entry * 100, 2) if entry > 0 else 0
                )
                groq_prompt = f"""Ты Kill Zone скальпер — торгуешь только в подтверждённой London/NY сессии.\nОтвечай СТРОГО JSON: {{"logic": "макс 10 слов", "valid": true/false}}\n\nКАК ДУМАТЬ:\n1. 15m engulfing + displacement — тело > 65% range, поглощение предыдущей свечи\n2. 15m OB/FVG retest задаёт реальную LTF зону входа\n3. Volume spike 1.6x+ — реальный интерес на trigger-свече\n4. 4h/1h используются как контекст; хотя бы один HTF поддерживает 15m\n5. Закрытая 15m свеча подтверждает настоящий BOS/CHoCH\n6. 1h/4h подтверждают контекст; BTC блокирует только при согласованном 1h+4h конфликте\n\nБЛОКИРУЙ если:\n- RR={rr} < 2.0\n- Стоп {_fast_sl_pct}% > 1.5% от входа (скальп = узкий стоп)\n- Нет свежего 15m OB/FVG retest — вход без LTF локации\n- Нет свежего подтверждённого BOS/CHoCH на закрытой 15m свече\n- Ни 1h, ни 4h не поддерживает 15m направление\n- BTC 1h и 4h одновременно направлены против сделки\n- Вне переданной приложением London/NY Kill Zone\n- SL выставлен математически (entry ± X%), а не за структуру\n\nПОДТВЕРЖДАЙ если:\n- Engulfing/displacement чёткий с объёмом 1.6x+\n- Есть свежий BOS/CHoCH в направлении сделки\n- Есть свежий 15m OB/FVG retest\n- RR={rr} >= 2.0\n- 1h или 4h поддерживает 15m, без двойного BTC-конфликта\n- Сейчас Kill Zone\n\nПРАВИЛА ВЫСТАВЛЕНИЯ УРОВНЕЙ:\n- SL ТОЛЬКО за структурный уровень (OB edge, FVG edge, engulfing low/high)\n- ЗАПРЕЩЕНО: SL = entry ± X% (математические стопы не работают)\n- TP ТОЛЬКО на структурный уровень (OB, FVG, swing point)\n- Если нет структуры для SL — НЕ ВХОДИТЬ\n\nДАННЫЕ СЕТАПА:\nПара: {symbol} Направление: {direction}\n15m engulfing ({_sweep_candles_ago} свечей назад) | Acceptance: {_acceptance}\nСтруктура: {_fast_structure_event["type"]} @ {_fast_structure_event["level"]} | closed=true\n4h зона: {zone_desc} | {_ob_4h_desc} | {_fvg_4h_desc}\nТренд: {direction_1d} | BTC: {btc_trend}\nFunding: {_fast_fund_str} | Fear&Greed: {_fast_fg_str} | Режим: {_fast_regime_str}\n{_eng_vol_desc}\nВход: {entry} SL: {sl} TP1: {tp1} TP2: {tp2}\nRR: {rr} | Стоп: {_fast_sl_pct}%{_fast_pat_str}"""
                groq_resp = (
                    ask_groq(groq_prompt, max_tokens=80)
                    if legacy_strategy_groq_enabled()
                    else None
                )
                if groq_resp:
                    import json as _j, re as _re

                    clean = (
                        groq_resp.strip()
                        .replace("```json", "")
                        .replace("```", "")
                        .strip()
                    )
                    m = _re.search("\\{[^}]+\\}", clean, _re.DOTALL)
                    if m:
                        parsed = _j.loads(m.group())
                        if _audit_test(
                            "FAST_DETECT_FAST_DEAL_G9449",
                            not parsed.get("valid", True),
                            "Groq как фильтр — блокируем только если явно valid=false",
                            'not parsed.get("valid", True)',
                            9449,
                        ):
                            return _audit_fail(
                                "FAST_DETECT_FAST_DEAL_R9450",
                                "Groq как фильтр — блокируем только если явно valid=false",
                                locals(),
                                'not parsed.get("valid", True)',
                                9450,
                            )
                        if parsed.get("logic"):
                            logic = str(parsed["logic"]).strip()
                else:
                    logging.debug(f"[FAST Groq] {symbol}: Groq не ответил — fallback")
            except Exception as _fast_ge:
                logging.debug(f"[FAST Groq] {symbol}: {_fast_ge}")
            if not logic:
                logic = f"Engulfing 15m в зоне {zone_desc[:20]}"
            return {
                "symbol": symbol,
                "direction": direction,
                "timeframe": "15m",
                "entry": entry,
                "sl": sl,
                "tp": tp,
                "tp1": tp1,
                "tp2": tp2,
                "sl_pct": sl_pct,
                "tp_pct": tp_pct,
                "tp2_pct": tp2_pct,
                "entry_drift_pct": entry_drift_pct,
                "rr": rr,
                "logic": logic,
                "zone": zone_desc,
                "direction_1d": direction_1d,
                "funding_warning": _fast_funding_warning,
                "ob": _fast_ob_15m,
                "fvg": _fast_fvg_15m,
                "htf_ob": ob_4h,
                "htf_fvg": fvg_4h,
                "htf_1h": direction_1h,
                "htf_4h": direction_4h,
                "btc_1h": btc_direction_1h,
                "btc_4h": btc_direction_4h,
                "fast_score": 0,
                "scan_type": "fast",
                "structure_event": _fast_structure_event,
            }
        except Exception as e:
            logging.debug(f"detect_fast_deal {symbol}: {e}")
            return _audit_fail(
                "FAST_DETECT_FAST_DEAL_R9488",
                "detector returned None",
                locals(),
                "",
                9488,
            )

    def full_scan_raw(self, symbol, timeframe="1h", auto=False, passive_watch=False):
        """Возвращает dict с текстом и grade для фильтрации"""
        try:
            _row = self.snapshot.market_context.get("pending_signal", False)
            if _audit_test("MTF_FULL_SCAN_RAW_G4558", _row, "_row", "_row", 4558):
                return _audit_fail(
                    "MTF_FULL_SCAN_RAW_R4559", "_row", locals(), "_row", 4559
                )
            mtf = self.multi_tf_analysis(symbol, ["15m", "1h", "4h", "1d"])
            if _audit_test(
                "MTF_FULL_SCAN_RAW_G4564", not mtf, "not mtf", "not mtf", 4564
            ):
                return _audit_fail(
                    "MTF_FULL_SCAN_RAW_R4565", "not mtf", locals(), "not mtf", 4565
                )
            direction = mtf["direction"]
            if symbol != "BTCUSDT":
                try:
                    btc_mtf = self.multi_tf_analysis("BTCUSDT", ["1h", "4h"])
                    if btc_mtf:
                        btc_dir = btc_mtf.get("direction")
                        if _audit_test(
                            "MTF_FULL_SCAN_RAW_G4585",
                            direction == "BEARISH"
                            and btc_dir == "BULLISH"
                            and (timeframe in ("1h",)),
                            "Фильтр BTC тренда — не шортим если BTC растёт, не лонгуем если BTC падает",
                            'direction == "BEARISH" and btc_dir == "BULLISH" and timeframe in ("1h",)',
                            4585,
                        ):
                            return _audit_fail(
                                "MTF_FULL_SCAN_RAW_R4586",
                                "Фильтр BTC тренда — не шортим если BTC растёт, не лонгуем если BTC падает",
                                locals(),
                                'direction == "BEARISH" and btc_dir == "BULLISH" and timeframe in ("1h",)',
                                4586,
                            )
                        if _audit_test(
                            "MTF_FULL_SCAN_RAW_G4587",
                            direction == "BULLISH"
                            and btc_dir == "BEARISH"
                            and (timeframe in ("1h",)),
                            'direction == "BULLISH" and btc_dir == "BEARISH" and timeframe in ("1h",)',
                            'direction == "BULLISH" and btc_dir == "BEARISH" and timeframe in ("1h",)',
                            4587,
                        ):
                            return _audit_fail(
                                "MTF_FULL_SCAN_RAW_R4588",
                                'direction == "BULLISH" and btc_dir == "BEARISH" and timeframe in ("1h",)',
                                locals(),
                                'direction == "BULLISH" and btc_dir == "BEARISH" and timeframe in ("1h",)',
                                4588,
                            )
                except Exception:
                    pass
            raw_candles = self.get_candles(symbol, timeframe, 101)
            if _audit_test(
                "MTF_FULL_SCAN_RAW_G4592",
                len(raw_candles) < 21,
                "len(raw_candles) < 21",
                "len(raw_candles) < 21",
                4592,
            ):
                return _audit_fail(
                    "MTF_FULL_SCAN_RAW_R4593",
                    "len(raw_candles) < 21",
                    locals(),
                    "len(raw_candles) < 21",
                    4593,
                )
            live_price = raw_candles[-1]["close"]
            candles = get_confirmed_candles(raw_candles)
            try:
                _ob_check = find_ob(candles, direction)
                _fvg_check = find_fvg(candles, direction)
                _atr_check = average_true_range(candles, 14)
                _ap_mtf = self.get_adaptive_params(symbol, candles)
                _vf_mtf = _ap_mtf["volatility_factor"]
                _in_ob = (
                    _ob_check
                    and abs(live_price - (_ob_check["top"] + _ob_check["bottom"]) / 2)
                    <= _atr_check * _vf_mtf
                )
                _in_fvg = (
                    _fvg_check
                    and abs(live_price - (_fvg_check["top"] + _fvg_check["bottom"]) / 2)
                    <= _atr_check * _vf_mtf
                )
                if _audit_test(
                    "MTF_FULL_SCAN_RAW_G4610",
                    not _in_ob and (not _in_fvg),
                    "not _in_ob and not _in_fvg",
                    "not _in_ob and not _in_fvg",
                    4610,
                ):
                    logging.debug(f"[MTF] {symbol}: цена не у OB/FVG зоны — блок")
                    return _audit_fail(
                        "MTF_FULL_SCAN_RAW_R4612",
                        "not _in_ob and not _in_fvg",
                        locals(),
                        "not _in_ob and not _in_fvg",
                        4612,
                    )
            except Exception as _mtf_zone_error:
                logging.warning(
                    "[MTF] %s: обязательная проверка OB/FVG недоступна: %s",
                    symbol,
                    _mtf_zone_error,
                )
                return _audit_fail(
                    "MTF_FULL_SCAN_RAW_R4615",
                    "detector returned None",
                    locals(),
                    "",
                    4615,
                )
            try:
                _ema_candles = get_confirmed_candles(self.get_candles(symbol, "4h", 61))
                if _ema_candles and len(_ema_candles) >= 50:
                    _closes = [c["close"] for c in _ema_candles]
                    _ema50 = ema_value(_closes, 50)
                    _ema20 = ema_value(_closes, 20)
                    if _audit_test(
                        "MTF_FULL_SCAN_RAW_G4624",
                        _ema20 is None or _ema50 is None,
                        "_ema20 is None or _ema50 is None",
                        "_ema20 is None or _ema50 is None",
                        4624,
                    ):
                        return _audit_fail(
                            "MTF_FULL_SCAN_RAW_R4625",
                            "_ema20 is None or _ema50 is None",
                            locals(),
                            "_ema20 is None or _ema50 is None",
                            4625,
                        )
                    _price_4h = _closes[-1]
                    _mtf_highs, _mtf_lows = find_swings(_ema_candles, lookback=5)
                    _hh_hl = (
                        len(_mtf_highs) >= 2
                        and len(_mtf_lows) >= 2
                        and (_mtf_highs[-1][1] > _mtf_highs[-2][1])
                        and (_mtf_lows[-1][1] > _mtf_lows[-2][1])
                    )
                    _ll_lh = (
                        len(_mtf_highs) >= 2
                        and len(_mtf_lows) >= 2
                        and (_mtf_highs[-1][1] < _mtf_highs[-2][1])
                        and (_mtf_lows[-1][1] < _mtf_lows[-2][1])
                    )
                    _mtf_adx_weak = _ap_mtf.get("adx_weak", False)
                    if direction == "BULLISH":
                        if _mtf_adx_weak:
                            _trend_ok = _hh_hl or _ema20 > _ema50
                        else:
                            _trend_ok = _price_4h > _ema50 and _ema20 > _ema50 or _hh_hl
                        if _audit_test(
                            "MTF_FULL_SCAN_RAW_G4645",
                            not _trend_ok,
                            "not _trend_ok",
                            "not _trend_ok",
                            4645,
                        ):
                            logging.debug(
                                f"[MTF] {symbol}: тренд не подтверждён для LONG — блок"
                            )
                            return _audit_fail(
                                "MTF_FULL_SCAN_RAW_R4647",
                                "not _trend_ok",
                                locals(),
                                "not _trend_ok",
                                4647,
                            )
                    else:
                        if _mtf_adx_weak:
                            _trend_ok = _ll_lh or _ema20 < _ema50
                        else:
                            _trend_ok = _price_4h < _ema50 and _ema20 < _ema50 or _ll_lh
                        if _audit_test(
                            "MTF_FULL_SCAN_RAW_G4653",
                            not _trend_ok,
                            "not _trend_ok",
                            "not _trend_ok",
                            4653,
                        ):
                            logging.debug(
                                f"[MTF] {symbol}: тренд не подтверждён для SHORT — блок"
                            )
                            return _audit_fail(
                                "MTF_FULL_SCAN_RAW_R4655",
                                "not _trend_ok",
                                locals(),
                                "not _trend_ok",
                                4655,
                            )
                else:
                    return _audit_fail(
                        "MTF_FULL_SCAN_RAW_R4657",
                        "_ema_candles and len(_ema_candles) >= 50",
                        locals(),
                        "_ema_candles and len(_ema_candles) >= 50",
                        4657,
                    )
            except Exception as _mtf_trend_error:
                logging.warning(
                    "[MTF] %s: обязательная проверка тренда недоступна: %s",
                    symbol,
                    _mtf_trend_error,
                )
                return _audit_fail(
                    "MTF_FULL_SCAN_RAW_R4660",
                    "detector returned None",
                    locals(),
                    "",
                    4660,
                )
            try:
                _pd_raw = self.get_candles(symbol, "4h", 51)
                _pd_candles = get_confirmed_candles(_pd_raw)
                if _pd_candles and len(_pd_candles) >= 20:
                    _pd_high = max((c["high"] for c in _pd_candles[-20:]))
                    _pd_low = min((c["low"] for c in _pd_candles[-20:]))
                    _pd_mid = (_pd_high + _pd_low) / 2
                    _pd_price = _pd_raw[-1]["close"]
                    _pd_span = _pd_high - _pd_low
                    _audit_observe(
                        "mtf_numeric",
                        {
                            "pd_position_pct": round(
                                (_pd_price - _pd_low) / _pd_span * 100, 6
                            )
                            if _pd_span > 0
                            else None,
                            "pd_mid_distance_pct": round(
                                (_pd_price - _pd_mid) / _pd_span * 100, 6
                            )
                            if _pd_span > 0
                            else None,
                            "pd_price": _pd_price,
                            "pd_low": _pd_low,
                            "pd_mid": _pd_mid,
                            "pd_high": _pd_high,
                        },
                    )
                    if _audit_test(
                        "MTF_FULL_SCAN_RAW_G4672",
                        direction == "BULLISH" and _pd_price > _pd_mid,
                        'direction == "BULLISH" and _pd_price > _pd_mid',
                        'direction == "BULLISH" and _pd_price > _pd_mid',
                        4672,
                    ):
                        logging.debug(
                            f"[MTF] {symbol}: цена в Premium зоне — LONG заблокирован"
                        )
                        return _audit_fail(
                            "MTF_FULL_SCAN_RAW_R4674",
                            'direction == "BULLISH" and _pd_price > _pd_mid',
                            locals(),
                            'direction == "BULLISH" and _pd_price > _pd_mid',
                            4674,
                        )
                    elif _audit_test(
                        "MTF_FULL_SCAN_RAW_G4675",
                        direction == "BEARISH" and _pd_price < _pd_mid,
                        'direction == "BEARISH" and _pd_price < _pd_mid',
                        'direction == "BEARISH" and _pd_price < _pd_mid',
                        4675,
                    ):
                        logging.debug(
                            f"[MTF] {symbol}: цена в Discount зоне — SHORT заблокирован"
                        )
                        return _audit_fail(
                            "MTF_FULL_SCAN_RAW_R4677",
                            'direction == "BEARISH" and _pd_price < _pd_mid',
                            locals(),
                            'direction == "BEARISH" and _pd_price < _pd_mid',
                            4677,
                        )
            except Exception as _mtf_pd_error:
                logging.warning(
                    "[MTF] %s: обязательная Premium/Discount проверка недоступна: %s",
                    symbol,
                    _mtf_pd_error,
                )
                return _audit_fail(
                    "MTF_FULL_SCAN_RAW_R4680",
                    "detector returned None",
                    locals(),
                    "",
                    4680,
                )
            price = live_price
            ob = find_ob(candles, direction)
            fvg = find_fvg(candles, direction)
            ob_data = self.get_orderbook(symbol)
            confluence = [f"✅ {mtf['match_count']}/{mtf['total']} ТФ совпали"]
            if ob:
                confluence.append(f"✅ Order Block: {ob['bottom']:.4f}–{ob['top']:.4f}")
            if fvg:
                confluence.append(f"✅ FVG: {fvg['bottom']:.4f}–{fvg['top']:.4f}")
            if ob_data:
                match = (
                    direction == "BULLISH"
                    and ob_data["bias"] == "BUY"
                    or (direction == "BEARISH" and ob_data["bias"] == "SELL")
                )
                if match:
                    confluence.append(f"✅ OrderBook: {ob_data['bias']}")
            try:
                mm_acc = detect_mm_accumulation(candles)
                mm_sig = mm_acc.get("signal", "NEUTRAL")
                mm_score = mm_acc.get("score", 0)
                if mm_sig == "STRONG_ACCUMULATION":
                    confluence.append(
                        f"✅ MM Накопление СИЛЬНОЕ (score {mm_score}/4) — фондовый паттерн (+15)"
                    )
                    for s in mm_acc.get("signals", []):
                        confluence.append(f"  {s}")
                elif mm_sig == "ACCUMULATION":
                    confluence.append(
                        f"✅ MM Накопление (score {mm_score}/4) — вероятен выход (+10)"
                    )
                    for s in mm_acc.get("signals", []):
                        confluence.append(f"  {s}")
                elif mm_sig == "WEAK_ACCUMULATION":
                    confluence.append(f"📦 MM Слабое накопление (score {mm_score}/4)")
                if mm_acc.get("pre_pump"):
                    confluence.append("🚀 PRE-PUMP: объём↑ диапазон↓ лои↑")
            except Exception as _mm_e:
                pass
            try:
                rmd = detect_rsi_macd_divergence(candles, direction)
                if rmd["found"]:
                    for s in rmd["signals"]:
                        confluence.append(s)
            except Exception:
                pass
            try:
                vwap_data = calculate_vwap(candles)
                if vwap_data["vwap"] > 0:
                    if vwap_data["near_vwap"]:
                        confluence.append(
                            f"📍 Цена у VWAP {vwap_data['vwap']:.4f} — зона интереса"
                        )
                    elif vwap_data["signal"] == direction:
                        confluence.append(f"✅ VWAP: {vwap_data['desc']} (+5)")
                    else:
                        confluence.append(f"⚠️ VWAP: {vwap_data['desc']}")
            except Exception:
                pass
            try:
                heatmap = get_liquidity_heatmap(candles)
                if direction == "BULLISH" and heatmap.get("nearest_buy_stops"):
                    lvl = heatmap["nearest_buy_stops"]
                    if lvl["strength"] == "HIGH":
                        confluence.append(
                            f"🎯 Buy Stops +{lvl['dist_pct']:.1f}% выше ({lvl['touches']} касаний) — цель"
                        )
                elif direction == "BEARISH" and heatmap.get("nearest_sell_stops"):
                    lvl = heatmap["nearest_sell_stops"]
                    if lvl["strength"] == "HIGH":
                        confluence.append(
                            f"🎯 Sell Stops -{lvl['dist_pct']:.1f}% ниже ({lvl['touches']} касаний) — цель"
                        )
            except Exception:
                pass
            try:
                breaker = detect_breaker_block(candles, direction)
                if breaker:
                    confluence.append(f"✅ {breaker['desc']} (+{breaker['weight']})")
            except Exception:
                pass
            _positive_confluence = [
                c for c in confluence if c.lstrip().startswith(("✅", "🎯", "🔥", "🚀"))
            ]
            min_conf = {"1h": 3, "4h": 4, "1d": 4, "1w": 3}
            _audit_observe(
                "mtf_numeric",
                {
                    "positive_confluence_count": len(_positive_confluence),
                    "positive_confluence_required": min_conf.get(timeframe, 4),
                },
            )
            if _audit_test(
                "MTF_FULL_SCAN_RAW_G4767",
                len(_positive_confluence) < min_conf.get(timeframe, 4),
                "сетапов нужны реальные положительные confluence.",
                "len(_positive_confluence) < min_conf.get(timeframe, 4)",
                4767,
            ):
                logging.debug(
                    f"[full_scan_raw] {symbol} {timeframe}: отфильтрован (positive confluence {len(_positive_confluence)} < {min_conf.get(timeframe, 4)})"
                )
                return _audit_fail(
                    "MTF_FULL_SCAN_RAW_R4769",
                    "сетапов нужны реальные положительные confluence.",
                    locals(),
                    "len(_positive_confluence) < min_conf.get(timeframe, 4)",
                    4769,
                )
            _dir_15m = self.smc_on_tf(symbol, "15m")
            _dir_1h = self.smc_on_tf(symbol, "1h")
            _dir_4h = self.smc_on_tf(symbol, "4h")
            _dir_1d = self.smc_on_tf(symbol, "1d")
            _tf_match = sum(
                [_dir_15m == direction, _dir_1h == direction, _dir_4h == direction]
            )
            _core_tf_match = sum([_dir_1h == direction, _dir_4h == direction])
            _audit_observe(
                "mtf_numeric", {"tf_match": _tf_match, "core_tf_match": _core_tf_match}
            )
            if _audit_test(
                "MTF_FULL_SCAN_RAW_G4785",
                _core_tf_match < 2,
                "lower timeframe is supposed to time.",
                "_core_tf_match < 2",
                4785,
            ):
                logging.debug(f"[MTF] {symbol}: 1h/4h не совпадают — пропускаем")
                return _audit_fail(
                    "MTF_FULL_SCAN_RAW_R4787",
                    "lower timeframe is supposed to time.",
                    locals(),
                    "_core_tf_match < 2",
                    4787,
                )
            _htf_1d_agrees = _dir_1d == direction
            if _audit_test(
                "MTF_FULL_SCAN_RAW_G4791",
                _dir_1d and (not _htf_1d_agrees),
                "Для редких сделок дневной тренд должен подтверждать направление.",
                "_dir_1d and not _htf_1d_agrees",
                4791,
            ):
                logging.debug(f"[MTF] {symbol}: 1d против направления — блок")
                return _audit_fail(
                    "MTF_FULL_SCAN_RAW_R4793",
                    "Для редких сделок дневной тренд должен подтверждать направление.",
                    locals(),
                    "_dir_1d and not _htf_1d_agrees",
                    4793,
                )
            _weak_mtf_warn = ""
            if _audit_test(
                "MTF_FULL_SCAN_RAW_G4798",
                timeframe not in ("1h", "4h"),
                "Только 1h и 4h — 1d/1w не торгуем (используем только для контекста)",
                'timeframe not in ("1h", "4h")',
                4798,
            ):
                return _audit_fail(
                    "MTF_FULL_SCAN_RAW_R4799",
                    "Только 1h и 4h — 1d/1w не торгуем (используем только для контекста)",
                    locals(),
                    'timeframe not in ("1h", "4h")',
                    4799,
                )
            levels = calc_smart_levels(candles, direction, price, timeframe)
            if _audit_test(
                "MTF_FULL_SCAN_RAW_G4803",
                not levels,
                "Расчёт уровней по реальной рыночной структуре (SMC)",
                "not levels",
                4803,
            ):
                return _audit_fail(
                    "MTF_FULL_SCAN_RAW_R4804",
                    "Расчёт уровней по реальной рыночной структуре (SMC)",
                    locals(),
                    "not levels",
                    4804,
                )
            entry = levels["entry"]
            _ob_mitigated = levels.get("mitigated", False)
            if _audit_test(
                "MTF_FULL_SCAN_RAW_G4807",
                _ob_mitigated,
                "_ob_mitigated",
                "_ob_mitigated",
                4807,
            ):
                logging.info(f"[MTF] {symbol} {direction} — OB mitigated, пропускаем")
                return _audit_fail(
                    "MTF_FULL_SCAN_RAW_R4809",
                    "_ob_mitigated",
                    locals(),
                    "_ob_mitigated",
                    4809,
                )
            entry = levels["entry"]
            sl = levels["sl"]
            tp1 = levels["tp1"]
            tp2 = levels["tp2"]
            tp3 = levels["tp3"]
            _rr_val = levels.get("rr", 0)
            _audit_observe("mtf_numeric", {"rr_value": _rr_val})
            if _audit_test(
                "MTF_FULL_SCAN_RAW_G4817",
                _rr_val < 2.0,
                "RR >= 2.0, no upper ceiling",
                "_rr_val < 2.0",
                4817,
            ):
                logging.debug(
                    f"[full_scan_raw] {symbol} {timeframe}: RR {_rr_val:.2f} < 2.0 — пропускаем"
                )
                return _audit_fail(
                    "MTF_FULL_SCAN_RAW_R4819",
                    "RR >= 2.0, no upper ceiling",
                    locals(),
                    "_rr_val < 2.0",
                    4819,
                )
            _atr_entry = average_true_range(candles, 14)
            if _audit_test(
                "MTF_FULL_SCAN_RAW_G4824",
                abs(price - entry) > _atr_entry * 0.75,
                "отправляем отложенный сетап как будто это вход прямо сейчас.",
                "abs(price - entry) > _atr_entry * 0.75",
                4824,
            ):
                logging.debug(f"[MTF] {symbol}: entry слишком далеко от текущей цены")
                return _audit_fail(
                    "MTF_FULL_SCAN_RAW_R4826",
                    "отправляем отложенный сетап как будто это вход прямо сейчас.",
                    locals(),
                    "abs(price - entry) > _atr_entry * 0.75",
                    4826,
                )
            vwap_warning = any(
                ("перекуплен" in c or "перепродан" in c for c in confluence)
            )
            est_hours, confidence, win_rate = get_estimated_time(symbol, timeframe)
            time_str = f"~{est_hours}ч" if est_hours < 24 else f"~{est_hours // 24}дн"
            wr_str = f"{win_rate:.0f}% WR" if win_rate > 0 else "нет истории"
            tf_label = TF_LABELS.get(timeframe, timeframe)
            conf_score = len(_positive_confluence) * 15
            emoji = "🟢" if direction == "BULLISH" else "🔴"
            conf_text = "\n".join(confluence)
            if direction == "BULLISH":
                tp1_pct = (tp1 - entry) / entry * 100
                tp2_pct = (tp2 - entry) / entry * 100
                tp3_pct = (tp3 - entry) / entry * 100
            else:
                tp1_pct = (entry - tp1) / entry * 100
                tp2_pct = (entry - tp2) / entry * 100
                tp3_pct = (entry - tp3) / entry * 100
            TF_TIME_LABEL = {
                "15m": "2-6ч",
                "1h": "12-48ч",
                "4h": "2-7дн",
                "1d": "1-4нед",
            }
            tf_time_hint = TF_TIME_LABEL.get(timeframe, time_str)
            tf_signal_names = {
                "1h": ("🔥", "СДЕЛКА"),
                "4h": ("🔥🔥", "СИЛЬНЫЙ СИГНАЛ"),
                "1d": ("🔥🔥🔥", "МЕГА"),
                "1w": ("💰", "ПРИОРИТЕТ 1"),
            }
            sig_emoji, sig_name = tf_signal_names.get(timeframe, ("🔥", "СИГНАЛ"))
            if any(("MM Накопление СИЛЬНОЕ" in c for c in confluence)):
                sig_emoji, sig_name = ("💎", "ПРЕМИУМ СИГНАЛ")
            risk_level = (
                "низкий"
                if levels.get("rr", 0) >= 3
                else "средний"
                if levels.get("rr", 0) >= 2
                else "высокий"
            )
            dir_label = "🟢LONG" if direction == "BULLISH" else "🔴SHORT"
            trend_icon = "📈" if direction == "BULLISH" else "📉"
            groq_logic = ""
            groq_time = tf_time_hint
            _groq_valid = True
            try:
                fg = self.get_fear_greed()
                funding = self.get_funding_rate(symbol)
                regime = self.get_market_regime(symbol)
                fg_val = f"{fg['value']} ({fg['label']})" if fg else "N/A"
                fund_val = f"{funding:+.4f}%" if funding is not None else "N/A"
                regime_val = (
                    regime.get("mode", "?") if isinstance(regime, dict) else str(regime)
                )
                htf_1d = self.smc_on_tf(symbol, "1d")
                htf_1w = self.smc_on_tf(symbol, "1w")
                _1w_conflict = False
                if htf_1w:
                    if direction == "BULLISH" and "BEARISH" in str(htf_1w).upper():
                        _1w_conflict = True
                    elif direction == "BEARISH" and "BULLISH" in str(htf_1w).upper():
                        _1w_conflict = True
                _1w_warn = "⚠️ ПРОТИВ 1w тренда" if _1w_conflict else ""
                _btc_1h = self.smc_on_tf("BTCUSDT", "1h")
                _btc_1d = self.smc_on_tf("BTCUSDT", "1d")
                _btc_str = f"BTC 1h: {_btc_1h}, 1d: {_btc_1d}"
                _ob_str = f"OB: {ob['bottom']:.6f}–{ob['top']:.6f}" if ob else "OB: нет"
                _fvg_str = (
                    f"FVG: {fvg['bottom']:.6f}–{fvg['top']:.6f}" if fvg else "FVG: нет"
                )
                _atr_mtf = average_true_range(candles, 14)
                _vol_last = candles[-1].get("volume", 0)
                _vol_avg = (
                    sum((c.get("volume", 0) for c in candles[-20:-1])) / 19
                    if len(candles) >= 20
                    else 0
                )
                _vol_str = (
                    f"Vol: {_vol_last:.0f} vs avg: {_vol_avg:.0f}"
                    if _vol_avg > 0
                    else ""
                )
                conf_short = "\n".join(confluence[:5]) if confluence else "нет данных"
                _pat_str = ""
                _sl_pct_mtf = (
                    round(abs(entry - sl) / entry * 100, 1) if entry > 0 else 0
                )
                groq_prompt = f"""Ты профессиональный SMC трейдер с 10-летним стажем. Торгуешь только лучшие сетапы — лучше пропустить 10 хороших чем взять 1 плохой. Ответь СТРОГО JSON: {{"logic": "причина входа макс 15 слов", "hours": число, "valid": true/false}}\n\nПРАВИЛА БЛОКИРОВКИ (верни valid: false если):\n- RR < 2.0 — риск не оправдан (RR сейчас: {levels.get("rr", 0)})\n- Стоп не находится за структурной инвалидацией (стоп сейчас: {_sl_pct_mtf}%)\n- Цена не у структурного уровня (OB/FVG/swing)\n- 4h или 1d против направления сигнала\n- Нет чёткого CHoCH или BOS подтверждающего вход\n- Рынок в боковике без импульса\n- Экстремальный funding — предупреждение, но не самостоятельный запрет\n\nПРАВИЛА ПОДТВЕРЖДЕНИЯ (valid: true если):\n- Цена чётко в OB или FVG зоне\n- 1h и 4h согласованы, а закрытая 15m свеча дала BOS/CHoCH триггер\n- Есть CHoCH или BOS после sweep ликвидности\n- RR ≥ 2.0, стоп за структурным уровнем\n- TP на реальном уровне (предыдущий swing, OB, ликвидность)\n- BTC подтверждает направление\n\nДанные: Пара: {symbol} ТФ: {tf_label} Направление: {direction}\nВход: {smart_price_fmt(entry)} SL: {smart_price_fmt(sl)} TP: {smart_price_fmt(tp1)}\n1d тренд (обязательный HTF-фильтр): {_dir_1d}\nТФ совпадение: {_tf_match}/3 (15m={_dir_15m} 1h={_dir_1h} 4h={_dir_4h})\nMTF: {mtf.get("match_count", 0)}/3 | 1d: {htf_1d} | 1w: {htf_1w} {_1w_warn}\nRR: {levels.get("rr", 0)} | Стоп: {_sl_pct_mtf}% | Fear&Greed: {fg_val} | Funding: {fund_val}\nРежим: {regime_val} | {_btc_str}\n{_ob_str} | {_fvg_str} | ATR: {smart_price_fmt(_atr_mtf)}\n{_vol_str}\nConfluence:\n{conf_short}{_pat_str}"""
                groq_response = (
                    ask_groq(groq_prompt, max_tokens=100)
                    if legacy_strategy_groq_enabled()
                    else None
                )
                if groq_response and len(groq_response) > 5:
                    try:
                        import json as _json, re as _re

                        clean = (
                            groq_response.strip()
                            .replace("```json", "")
                            .replace("```", "")
                            .strip()
                        )
                        json_match = _re.search("\\{[^}]+\\}", clean, _re.DOTALL)
                        if json_match:
                            clean = json_match.group()
                        parsed = _json.loads(clean)
                        if _audit_test(
                            "MTF_FULL_SCAN_RAW_G4964",
                            not parsed.get("valid", True),
                            "Groq как фильтр — если valid=false, блокируем",
                            'not parsed.get("valid", True)',
                            4964,
                        ):
                            logging.info(
                                f"[MTF Groq] {symbol} {direction}: Groq отклонил сигнал"
                            )
                            return _audit_fail(
                                "MTF_FULL_SCAN_RAW_R4966",
                                "Groq как фильтр — если valid=false, блокируем",
                                locals(),
                                'not parsed.get("valid", True)',
                                4966,
                            )
                        if parsed.get("logic") and len(str(parsed["logic"])) > 5:
                            raw_logic = str(parsed["logic"]).strip()
                            if raw_logic.startswith("{") or '"logic"' in raw_logic:
                                import re as _re2

                                m = _re2.search('"logic"\\s*:\\s*"([^"]+)"', raw_logic)
                                groq_logic = m.group(1) if m else raw_logic[:100]
                            else:
                                groq_logic = raw_logic
                        if parsed.get("hours"):
                            hrs = int(parsed["hours"])
                            groq_time = f"~{hrs}ч" if hrs < 24 else f"~{hrs // 24}дн"
                    except Exception:
                        clean_text = groq_response.strip().replace("\n", " ")
                        if len(clean_text) > 10 and (
                            not clean_text.upper() == clean_text
                        ):
                            groq_logic = clean_text[:80]
            except Exception:
                pass
            if not groq_logic:
                logic_lines = [
                    c
                    for c in confluence
                    if any(
                        (
                            w in c.lower()
                            for w in [
                                "свип",
                                "sweep",
                                "импульс",
                                "накопл",
                                "ликвидн",
                                "пробой",
                                "ob ",
                                "fvg",
                            ]
                        )
                    )
                ]
                groq_logic = (
                    "\n".join(logic_lines[:3])
                    if logic_lines
                    else "структурный вход по SMC"
                )
            _mtf_score = 0
            _mtf_score_vol = False
            _mtf_score_btc = False
            _mtf_score_session = False
            _mtf_score_bos = False
            _mtf_structure_event = None
            try:
                _avg_vol_m = sum((c["volume"] for c in candles[-20:-1])) / 19
                if _avg_vol_m > 0 and candles[-1]["volume"] > _avg_vol_m * 1.2:
                    _mtf_score += 1
                    _mtf_score_vol = True
            except Exception:
                pass
            try:
                _btc_m = self.get_candles("BTCUSDT", "1h", 5)
                if _btc_m and len(_btc_m) >= 3:
                    _btc_dir_m = (
                        "BULLISH"
                        if _btc_m[-1]["close"] > _btc_m[-3]["close"]
                        else "BEARISH"
                    )
                    if _btc_dir_m == direction:
                        _mtf_score += 1
                        _mtf_score_btc = True
            except Exception:
                pass
            import datetime as _dt_m

            _h_m = self.snapshot.as_of.hour
            if 8 <= _h_m <= 17:
                _mtf_score += 1
                _mtf_score_session = True
            try:
                _c15m_m = get_confirmed_candles(self.get_candles(symbol, "15m", 31))
                _mtf_structure_event = (
                    get_bos_choch_event(
                        _c15m_m, direction, lookback=15, max_break_age=1
                    )
                    if _c15m_m
                    else None
                )
                if _mtf_structure_event:
                    _mtf_score += 1
                    _mtf_score_bos = True
            except Exception:
                pass
            if passive_watch:
                _audit_test(
                    "MTF_PASSIVE_LTF_BOS",
                    not _mtf_score_bos,
                    "MTF passive watch: closed 15m BOS/CHoCH before candidate",
                    "passive_watch and not _mtf_score_bos",
                    5050,
                )
            if passive_watch and (not _mtf_score_bos):
                return {
                    "_pending_ltf": True,
                    "symbol": symbol,
                    "strategy": "MTF",
                    "direction": direction,
                    "required_timeframe": "15m",
                    "reason": "ожидается закрытый 15m BOS/CHoCH",
                }
            if _audit_test(
                "MTF_FULL_SCAN_RAW_G5053",
                _mtf_score < 2 or not _mtf_score_bos,
                "_mtf_score < 2 or not _mtf_score_bos",
                "_mtf_score < 2 or not _mtf_score_bos",
                5053,
            ):
                logging.debug(f"[MTF] {symbol}: score {_mtf_score}/4 — пропускаем")
                return _audit_fail(
                    "MTF_FULL_SCAN_RAW_R5055",
                    "_mtf_score < 2 or not _mtf_score_bos",
                    locals(),
                    "_mtf_score < 2 or not _mtf_score_bos",
                    5055,
                )
            _signal_strength = (
                "🔥 Сильный"
                if _mtf_score >= 3
                else "✅ Норм"
                if _mtf_score >= 2
                else "⚡ Базовый"
            )
            _must_text = f"1h+4h {direction} | 15m BOS/CHoCH ✅"
            _1d_text = (
                f"1d: {('✅' if _htf_1d_agrees else '⚠️')} {_dir_1d or '?'} (контекст)"
            )
            _confirm_items = []
            if _mtf_score_vol:
                _confirm_items.append("📊 Объём")
            if _mtf_score_btc:
                _confirm_items.append("₿ BTC")
            if _mtf_score_session:
                _confirm_items.append("⏰ Сессия")
            if _mtf_score_bos:
                _confirm_items.append("🔄 BOS")
            _confirm_text = " | ".join(_confirm_items) if _confirm_items else "—"
            _sl_pct_txt = round(abs(entry - sl) / entry * 100, 2) if entry > 0 else 0
            _dir_emoji = "🟢 LONG" if direction == "BULLISH" else "🔴 SHORT"
            text = f"📐 <b>MTF</b> | {symbol} — {_dir_emoji}\n━━━━━━━━━━━━━━━━\n📌 Основа: {_must_text}\n📋 {_1d_text}\n✅ Доп: {_mtf_score}/4 — {_confirm_text}\n💪 Сила: {_signal_strength}\n━━━━━━━━━━━━━━━━\n🎯 Вход:  <code>{smart_price_fmt(entry)}</code>\n🛑 Стоп:  <code>{smart_price_fmt(sl)}</code>  ({_sl_pct_txt}%)\n🎯 TP1:   <code>{smart_price_fmt(tp1)}</code>\n🎯 TP2:   <code>{smart_price_fmt(tp2)}</code>\n📊 RR:    {levels.get('rr', 0)}\n━━━━━━━━━━━━━━━━\n📈 Логика: {groq_logic}\n⏱ Горизонт: {groq_time}"
            text += "\n\n💡 Это аналитика, не совет. Торгуй осознанно"
            return {
                "symbol": symbol,
                "grade": sig_name,
                "grade_emoji": sig_emoji,
                "text": text,
                "direction": direction,
                "entry": entry,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "sl": sl,
                "rr": _rr_val,
                "timeframe": timeframe,
                "confluence_score": conf_score,
                "regime": "UNKNOWN",
                "estimated_hours": est_hours,
                "scan_type": "mtf",
                "technical_evidence": {
                    "causal_matrix_ready": True,
                    "timeframe_alignment": {
                        "15m": _dir_15m,
                        "1h": _dir_1h,
                        "4h": _dir_4h,
                        "1d": _dir_1d,
                    },
                    "mtf_match": _tf_match,
                    "positive_confluence": _positive_confluence[:8],
                    "confirmation_score": _mtf_score,
                    "bos_choch": _mtf_score_bos,
                    "structure_event": _mtf_structure_event,
                    "btc_confirmed": _mtf_score_btc,
                    "volume_confirmed": _mtf_score_vol,
                    "ob": ob,
                    "fvg": fvg,
                },
            }
        except Exception as e:
            logging.error(f"full_scan_raw error {symbol}: {e}")
            return _audit_fail(
                "MTF_FULL_SCAN_RAW_R5118", "detector returned None", locals(), "", 5118
            )

    def multi_tf_analysis(self, symbol: str, timeframes: list = None) -> dict | None:
        if timeframes is None:
            timeframes = ["15m", "1h", "4h"]
        TF_LABELS = {
            "1m": "1мин",
            "5m": "5мин",
            "15m": "15мин",
            "30m": "30мин",
            "1h": "1час",
            "2h": "2ч",
            "4h": "4ч",
            "1d": "1д",
        }
        Q_ICON = {"high": "🟢", "medium": "🟡", "low": "🟠", "none": "⚫"}
        tf_results = {tf: self.smc_tf(symbol, tf) for tf in timeframes}
        available = [tf for tf, r in tf_results.items() if r["direction"] is not None]
        if not available:
            for tf in ["1h", "4h", "1d"]:
                if tf not in timeframes:
                    r = self.smc_tf(symbol, tf)
                    if r["direction"]:
                        tf_results[tf] = r
                        available.append(tf)
            if not available:
                return None
        bullish = [tf for tf in available if tf_results[tf]["direction"] == "BULLISH"]
        bearish = [tf for tf in available if tf_results[tf]["direction"] == "BEARISH"]
        if len(bullish) > len(bearish):
            direction, matched = ("BULLISH", bullish)
        elif len(bearish) > len(bullish):
            direction, matched = ("BEARISH", bearish)
        else:
            for tf in ["1d", "4h", "2h", "1h", "30m", "15m"]:
                if tf in available:
                    direction = tf_results[tf]["direction"]
                    matched = [tf]
                    break
            else:
                return None
        mc = len(matched)
        total = len(available)
        q_s = {"high": 3, "medium": 2, "low": 1, "none": 0}
        avg_q = sum((q_s.get(tf_results[tf]["quality"], 0) for tf in matched)) / mc
        if mc == total and total >= 3 and (avg_q >= 2):
            grade, ge, stars = ("МЕГА ТОП", "🔥🔥🔥", "⭐⭐⭐⭐⭐")
        elif mc >= 3 or (mc == 2 and avg_q >= 2.5):
            grade, ge, stars = ("ТОП СДЕЛКА", "🔥🔥", "⭐⭐⭐⭐")
        elif mc == 2:
            grade, ge, stars = ("ХОРОШАЯ", "✅", "⭐⭐⭐")
        else:
            grade, ge, stars = ("СЛАБАЯ", "⚠️", "⭐⭐")
        tf_status = ""
        for tf in timeframes:
            r = tf_results[tf]
            d = r["direction"]
            qi = Q_ICON.get(r["quality"], "⚫")
            src_s = r["source"][:8] if r["source"] != "none" else "нет"
            arrow = "🟢" if d == "BULLISH" else "🔴" if d == "BEARISH" else "⚪"
            synth = "[синт]" if r.get("is_synthetic") else ""
            tf_status += (
                f"{arrow} {TF_LABELS.get(tf, tf)}: {d or 'нет'} {qi}{src_s}{synth}\n"
            )
        return {
            "direction": direction,
            "matched": matched,
            "match_count": mc,
            "total": total,
            "grade": grade,
            "grade_emoji": ge,
            "stars": stars,
            "tf_status": tf_status,
            "available_tfs": available,
        }


@_audit_strategy("FAST")
def detect_fast_deal(snapshot: MarketSnapshot, **kwargs):
    return SnapshotContext(snapshot).detect_fast_deal(snapshot.symbol, **kwargs)


@_audit_strategy("MTF")
def full_scan_raw(snapshot: MarketSnapshot, **kwargs):
    return SnapshotContext(snapshot).full_scan_raw(snapshot.symbol, **kwargs)


@_audit_strategy("SWING")
def detect_swing_setup(snapshot: MarketSnapshot, **kwargs):
    return SnapshotContext(snapshot).detect_swing_setup(snapshot.symbol, **kwargs)


@_audit_strategy("ZONE")
def detect_zone_setup(snapshot: MarketSnapshot, **kwargs):
    return SnapshotContext(snapshot).detect_zone_setup(snapshot.symbol, **kwargs)


@_audit_strategy("WYCKOFF", subtype="SPRING")
def detect_wyckoff_spring(snapshot: MarketSnapshot, **kwargs):
    return SnapshotContext(snapshot).detect_wyckoff_spring(snapshot.symbol, **kwargs)


@_audit_strategy("WYCKOFF", subtype="DISTRIBUTION")
def detect_wyckoff_distribution(snapshot: MarketSnapshot, **kwargs):
    return SnapshotContext(snapshot).detect_wyckoff_distribution(
        snapshot.symbol, **kwargs
    )


@_audit_strategy("WYCKOFF", subtype="REACCUMULATION")
def detect_wyckoff_reaccumulation(snapshot: MarketSnapshot, **kwargs):
    return SnapshotContext(snapshot).detect_wyckoff_reaccumulation(
        snapshot.symbol, **kwargs
    )
