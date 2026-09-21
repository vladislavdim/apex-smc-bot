"""Deterministic legacy entry-timing assessment."""

from __future__ import annotations


def check_entry_timing(
    candles: list,
    direction: str,
    entry_price: float,
    timeframe: str = "1h",
) -> dict:
    """Assess sweep, impulse and entry-zone drift using the legacy contract."""
    del timeframe  # Retained for compatibility with existing callers.
    if not candles or len(candles) < 5:
        return {"valid": True, "score": 0, "reasons": [], "wait": ""}

    reasons = []
    warnings = []
    score = 0
    last = candles[-1]
    current_price = last["close"]
    atr = sum(
        candle["high"] - candle["low"] for candle in candles[-14:]
    ) / min(14, len(candles))
    average_volume = (
        sum(candle.get("volume", 0) for candle in candles[-20:]) / 20
        if len(candles) >= 20 else 1
    )

    if direction == "BULLISH":
        swept = False
        for index in range(-3, 0):
            candle = candles[index]
            previous = candles[index - 1]
            if (
                candle["low"] < previous["low"]
                and candle["close"] > previous["low"]
            ):
                wick_size = (
                    (candle["close"] - candle["low"])
                    / (candle["high"] - candle["low"] + 0.000001)
                )
                if wick_size > 0.4:
                    swept = True
                    break
        if swept:
            score += 1
            reasons.append("✅ Sweep ликвидности — ложный пробой вниз")
        else:
            warnings.append("⚠️ Нет sweep — ждать ложного пробоя лоу")
    else:
        swept = False
        for index in range(-3, 0):
            candle = candles[index]
            previous = candles[index - 1]
            if (
                candle["high"] > previous["high"]
                and candle["close"] < previous["high"]
            ):
                wick_size = (
                    (candle["high"] - candle["close"])
                    / (candle["high"] - candle["low"] + 0.000001)
                )
                if wick_size > 0.4:
                    swept = True
                    break
        if swept:
            score += 1
            reasons.append("✅ Sweep ликвидности — ложный пробой хая")
        else:
            warnings.append("⚠️ Нет sweep — ждать ложного пробоя хая")

    candle_body = abs(last["close"] - last["open"])
    candle_range = last["high"] - last["low"]
    body_ratio = candle_body / candle_range if candle_range > 0 else 0
    last_volume = last.get("volume", 0)
    if direction == "BULLISH":
        is_impulse = (
            last["close"] > last["open"]
            and body_ratio > 0.5
            and candle_body > atr * 0.5
        )
    else:
        is_impulse = (
            last["close"] < last["open"]
            and body_ratio > 0.5
            and candle_body > atr * 0.5
        )
    if is_impulse:
        score += 1
        volume_note = (
            f" (объём x{round(last_volume / average_volume, 1)})"
            if average_volume > 0 else ""
        )
        reasons.append(f"✅ Импульсная свеча{volume_note}")
    else:
        warnings.append("⚠️ Нет импульса — ждать сильной свечи подтверждения")

    drift = abs(current_price - entry_price)
    maximum_drift = atr * 1.5
    drift_percent = round(drift / entry_price * 100, 2)
    if drift <= maximum_drift:
        score += 1
        reasons.append(
            f"✅ Цена в зоне входа (отклонение {drift_percent}%)"
        )
    else:
        warnings.append(
            f"⚠️ Цена ушла от зоны на {drift_percent}% — вход поздний"
        )

    valid = score >= 2
    wait_message = ""
    if not valid:
        if score == 0:
            wait_message = "Ждать: sweep + импульс"
        elif "sweep" in str(warnings):
            wait_message = "Ждать ложного пробоя уровня"
        elif "импульс" in str(warnings):
            wait_message = "Ждать импульсной свечи подтверждения"
        else:
            wait_message = "Ждать возврата цены в зону"
    return {
        "valid": valid,
        "score": score,
        "reasons": reasons,
        "warnings": warnings,
        "wait": wait_message,
        "swept": swept if direction == "BULLISH" else swept,
    }


__all__ = ["check_entry_timing"]
