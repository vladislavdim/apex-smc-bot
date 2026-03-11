"""
groq_extensions.py — Плагин APEX, который Groq может свободно изменять.

Этот файл изолирован от основного кода.
Если здесь что-то сломается — бот продолжает работать без этого файла.
Groq пишет сюда новые фильтры, индикаторы, логику.

Структура:
  - custom_signal_filters()  — фильтры сигналов (Groq добавляет)
  - custom_confluence_boost() — дополнительный вес для confluence
  - custom_market_analysis()  — дополнительный анализ рынка
  - GROQ_CHANGELOG            — лог всех изменений от Groq
"""

import logging
import sqlite3
import time
from datetime import datetime

DB_PATH = "brain.db"

# ═══════════════════════════════════════════════════════════════
# ЛОГА ИЗМЕНЕНИЙ — Groq добавляет запись при каждом изменении
# ═══════════════════════════════════════════════════════════════

GROQ_CHANGELOG = [
    {
        "date": "2026-03-11",
        "version": "1.0.0",
        "author": "APEX_init",
        "changes": "Создан файл groq_extensions.py. Groq получил доступ к плагину."
    }
]

# ═══════════════════════════════════════════════════════════════
# ФИЛЬТРЫ СИГНАЛОВ — Groq добавляет новые фильтры здесь
# Каждый фильтр: функция(symbol, direction, price, confluence, regime) -> (bool, reason)
# True = сигнал проходит, False = заблокировать
# ═══════════════════════════════════════════════════════════════

def filter_meme_coins_high_fg(symbol, direction, price, confluence, regime, fg=None):
    """
    Фильтр: не входить в мемкоины при экстремальной жадности.
    Groq может изменять пороги.
    """
    MEME_COINS = {"SHIBUSDT", "DOGEUSDT", "PEPEUSDT", "WIFUSDT", "BONKUSDT", "FLOKIUSDT"}
    FG_THRESHOLD = 75  # Groq может менять этот порог

    if symbol in MEME_COINS and direction == "BULLISH":
        if fg and isinstance(fg, dict) and fg.get("value", 0) > FG_THRESHOLD:
            return False, f"Мемкоин + F&G={fg['value']} > {FG_THRESHOLD} — высокий риск"
    return True, ""


def filter_low_confluence_sideways(symbol, direction, price, confluence, regime, fg=None):
    """
    Фильтр: в боковике требуем более высокий confluence.
    """
    MIN_CONFLUENCE_SIDEWAYS = 55  # Groq может менять

    if regime == "SIDEWAYS" and confluence < MIN_CONFLUENCE_SIDEWAYS:
        return False, f"Боковик + confluence={confluence} < {MIN_CONFLUENCE_SIDEWAYS}"
    return True, ""


def filter_consecutive_losses(symbol, direction, price, confluence, regime, fg=None):
    """
    Фильтр: при серии потерь по монете — пропускаем.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        recent = conn.execute("""
            SELECT result FROM signal_log
            WHERE symbol=? ORDER BY created_at DESC LIMIT 3
        """, (symbol,)).fetchall()
        conn.close()

        if len(recent) >= 3:
            all_sl = all(r[0] == "sl" for r in recent)
            if all_sl:
                return False, f"{symbol}: 3 SL подряд — пропускаем"
    except Exception:
        pass
    return True, ""


# Реестр активных фильтров — Groq может добавлять/убирать
ACTIVE_FILTERS = [
    filter_meme_coins_high_fg,
    filter_low_confluence_sideways,
    filter_consecutive_losses,
    # Groq добавляет новые фильтры сюда ↓
]


def run_all_filters(symbol, direction, price, confluence, regime, fg=None):
    """
    Запускает все активные фильтры.
    Возвращает (passed: bool, reason: str)
    """
    for f in ACTIVE_FILTERS:
        try:
            passed, reason = f(symbol, direction, price, confluence, regime, fg)
            if not passed:
                logging.info(f"[Extensions] ❌ {symbol} заблокирован фильтром {f.__name__}: {reason}")
                return False, reason
        except Exception as e:
            logging.debug(f"[Extensions] Фильтр {f.__name__} ошибка: {e}")
    return True, ""


# ═══════════════════════════════════════════════════════════════
# ДОПОЛНИТЕЛЬНЫЙ CONFLUENCE — Groq добавляет бонусные условия
# Возвращает дополнительный вес (int) и описание
# ═══════════════════════════════════════════════════════════════

def boost_strong_volume(candles, direction):
    """Бонус за аномальный объём в направлении сделки"""
    try:
        if not candles or len(candles) < 10:
            return 0, ""
        vols = [c.get("volume", 0) for c in candles[-20:]]
        avg = sum(vols) / len(vols) if vols else 0
        last_vol = vols[-1] if vols else 0
        last_candle = candles[-1]

        is_bullish_candle = last_candle["close"] > last_candle["open"]
        if avg > 0 and last_vol > avg * 1.8:
            if (direction == "BULLISH" and is_bullish_candle) or \
               (direction == "BEARISH" and not is_bullish_candle):
                return 8, "✅ Аномальный объём подтверждает направление (+8)"
    except Exception:
        pass
    return 0, ""


def boost_clean_structure(candles, direction):
    """Бонус если последние 5 свечей идут чётко в одном направлении"""
    try:
        if not candles or len(candles) < 5:
            return 0, ""
        last5 = candles[-5:]
        bullish_count = sum(1 for c in last5 if c["close"] > c["open"])
        if direction == "BULLISH" and bullish_count >= 4:
            return 5, "✅ Чистая бычья структура последних 5 свечей (+5)"
        if direction == "BEARISH" and bullish_count <= 1:
            return 5, "✅ Чистая медвежья структура последних 5 свечей (+5)"
    except Exception:
        pass
    return 0, ""


# Реестр буст-функций
CONFLUENCE_BOOSTERS = [
    boost_strong_volume,
    boost_clean_structure,
    # Groq добавляет новые бусты сюда ↓
]


def run_confluence_boosters(candles, direction):
    """
    Запускает все бустеры confluence.
    Возвращает (total_bonus: int, descriptions: list)
    """
    total = 0
    descs = []
    for booster in CONFLUENCE_BOOSTERS:
        try:
            bonus, desc = booster(candles, direction)
            if bonus > 0:
                total += bonus
                descs.append(desc)
        except Exception as e:
            logging.debug(f"[Extensions] Booster {booster.__name__}: {e}")
    return total, descs


# ═══════════════════════════════════════════════════════════════
# ДОПОЛНИТЕЛЬНЫЙ АНАЛИЗ РЫНКА — Groq добавляет свои индикаторы
# ═══════════════════════════════════════════════════════════════

def analyze_session_timing():
    """Анализ текущей торговой сессии"""
    hour = datetime.utcnow().hour
    if 8 <= hour <= 11:
        return {"session": "London Open", "quality": "HIGH", "note": "🇬🇧 Лондон — лучшее время"}
    elif 13 <= hour <= 16:
        return {"session": "NY Open", "quality": "HIGH", "note": "🇺🇸 Нью-Йорк — лучшее время"}
    elif 2 <= hour <= 5:
        return {"session": "Asia", "quality": "MEDIUM", "note": "🌏 Азиатская сессия"}
    elif 22 <= hour or hour <= 1:
        return {"session": "Dead Zone", "quality": "LOW", "note": "😴 Мёртвая зона — осторожно"}
    return {"session": "Overlap", "quality": "MEDIUM", "note": ""}


def analyze_price_momentum(candles):
    """Простой импульс цены"""
    try:
        if not candles or len(candles) < 10:
            return {}
        closes = [c["close"] for c in candles[-10:]]
        change_5 = (closes[-1] - closes[-5]) / closes[-5] * 100 if closes[-5] else 0
        change_10 = (closes[-1] - closes[0]) / closes[0] * 100 if closes[0] else 0
        return {
            "change_5c": round(change_5, 2),
            "change_10c": round(change_10, 2),
            "momentum": "STRONG" if abs(change_5) > 2 else "WEAK"
        }
    except Exception:
        return {}


# ═══════════════════════════════════════════════════════════════
# ИНТЕРФЕЙС ДЛЯ GROQ — функции которые Groq вызывает для записи
# изменений в этот файл через GitHub
# ═══════════════════════════════════════════════════════════════

def get_extensions_summary():
    """Сводка плагина для отображения в боте"""
    return {
        "filters": len(ACTIVE_FILTERS),
        "boosters": len(CONFLUENCE_BOOSTERS),
        "version": GROQ_CHANGELOG[-1]["version"] if GROQ_CHANGELOG else "1.0.0",
        "last_change": GROQ_CHANGELOG[-1]["date"] if GROQ_CHANGELOG else "—",
        "last_author": GROQ_CHANGELOG[-1]["author"] if GROQ_CHANGELOG else "—",
        "changelog": GROQ_CHANGELOG[-3:],  # последние 3 изменения
    }


def get_filter_names():
    """Список активных фильтров по именам"""
    return [f.__name__ for f in ACTIVE_FILTERS]


def get_booster_names():
    """Список активных бустеров по именам"""
    return [b.__name__ for b in CONFLUENCE_BOOSTERS]
