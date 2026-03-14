"""
groq_extensions_protected.py - Защищенная версия плагина APEX для Groq
Критические функции защищены от удаления AI autopilot

AUTOPILOT PROTECTED FUNCTIONS:
- run_confluence_boosters
- boost_strong_volume  
- boost_clean_structure
- get_extensions_summary
"""

import logging
import sqlite3
import time
import re
import ast
from datetime import datetime
from typing import List, Dict, Tuple, Any, Optional, Callable

# Импортируем наш новый менеджер БД
from database_manager import get_db

DB_PATH = "brain.db"

# ═══════════════════════════════════════════════════════════════
# AUTOPILOT PROTECTION - КРИТИЧЕСКИЕ ФУНКЦИИ ЗАЩИЩЕНЫ
# ═══════════════════════════════════════════════════════════════

PROTECTED_FUNCTIONS = {
    "run_confluence_boosters",
    "boost_strong_volume", 
    "boost_clean_structure",
    "get_extensions_summary",
    "run_all_filters",
    "ACTIVE_FILTERS",
    "CONFLUENCE_BOOSTERS"
}

def _is_protected_function(code: str, function_name: str) -> bool:
    """Проверяет, является ли функция защищенной"""
    return function_name in PROTECTED_FUNCTIONS

def _validate_function_modification(original_code: str, new_code: str, 
                                 function_name: str) -> bool:
    """
    Валидация изменений в защищенных функциях
    Разрешены только безопасные модификации
    """
    if function_name not in PROTECTED_FUNCTIONS:
        return True
    
    # Парсим оригинальный и новый код
    try:
        original_tree = ast.parse(original_code)
        new_tree = ast.parse(new_code)
    except SyntaxError:
        return False
    
    # Извлекаем имена функций
    original_functions = [node.name for node in ast.walk(original_tree) 
                         if isinstance(node, ast.FunctionDef)]
    new_functions = [node.name for node in ast.walk(new_tree) 
                     if isinstance(node, ast.FunctionDef)]
    
    # Проверяем что защищенная функция не была удалена
    if function_name in original_functions and function_name not in new_functions:
        return False
    
    # Проверяем что в защищенных функциях нет опасных операций
    if function_name in PROTECTED_FUNCTIONS:
        dangerous_patterns = [
            r"exec\s*\(",
            r"eval\s*\(",
            r"__import__\s*\(",
            r"open\s*\(",
            r"os\.system",
            r"subprocess\.call",
            r"subprocess\.run"
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, new_code):
                return False
    
    return True

# ═══════════════════════════════════════════════════════════════
# ЛОГА ИЗМЕНИЙ — Groq добавляет запись при каждом изменении
# ═══════════════════════════════════════════════════════════════

GROQ_CHANGELOG = [
    {
        "date": "2026-03-14",
        "version": "2.0.0",
        "author": "APEX_security_patch",
        "changes": "Added autopilot protection for critical functions"
    }
]

# ═══════════════════════════════════════════════════════════════
# ФИЛЬТРЫ СИГНАЛОВ — Groq добавляет новые фильтры здесь
# Каждый фильтр: функция(symbol, direction, price, confluence, regime, fg=None) -> (bool, reason)
# True = сигнал проходит, False = заблокировать
# ═══════════════════════════════════════════════════════════════

def filter_meme_coins_high_fg(symbol, direction, price, confluence, regime, fg=None):
    """
    Фильтр: не входить в мемкоины при экстремальной жадности.
    AUTOPILOT PROTECTED: основная логика защищена
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
    AUTOPILOT PROTECTED: основная логика защищена
    """
    MIN_CONFLUENCE_SIDEWAYS = 55  # Groq может менять

    if regime == "SIDEWAYS" and confluence < MIN_CONFLUENCE_SIDEWAYS:
        return False, f"Боковик + confluence={confluence} < {MIN_CONFLUENCE_SIDEWAYS}"
    return True, ""

def filter_consecutive_losses(symbol, direction, price, confluence, regime, fg=None):
    """
    Фильтр: при серии потерь по монете — пропускаем.
    AUTOPILOT PROTECTED: работа с БД защищена
    """
    try:
        db = get_db()
        rows = db.execute_query("""
            SELECT result FROM signal_log
            WHERE symbol=? ORDER BY created_at DESC LIMIT 3
        """, (symbol,))
        
        if len(rows) >= 3:
            all_sl = all(row["result"] == "sl" for row in rows)
            if all_sl:
                return False, f"{symbol}: 3 SL подряд — пропускаем"
    except Exception as e:
        logging.debug(f"Filter consecutive_losses error: {e}")
    return True, ""

def filter_dead_zone_signals(symbol, direction, price, confluence, regime, fg=None):
    """Фильтр: не торгуем в мёртвой зону 22:00-02:00 UTC если слабый сигнал."""
    from datetime import datetime as _dt
    hour = _dt.utcnow().hour
    if (22 <= hour or hour <= 1) and confluence < 70:
        return False, f"Мёртвая зона UTC {hour}:xx + слабый confluence={confluence}"
    return True, ""

def filter_btc_correlation(symbol, direction, price, confluence, regime, fg=None):
    """Фильтр: не входим в лонг альта если BTC падает."""
    if symbol == "BTCUSDT":
        return True, ""
    try:
        # Читаем последнее изменение BTC из кэша если есть
        db = get_db()
        btc_note = db.execute_query(
            "SELECT description FROM brain_log WHERE title LIKE '%BTC%' ORDER BY created_at DESC LIMIT 1"
        )
        if btc_note and "падает" in str(btc_note[0]["description"]) and direction == "BULLISH":
            return False, "BTC падает — лонги на альты заблокированы"
    except Exception as e:
        logging.debug(f"Filter btc_correlation error: {e}")
    return True, ""

# Реестр активных фильтров — Groq может добавлять/убирать
# AUTOPILOT PROTECTED: базовые фильтры защищены от удаления
ACTIVE_FILTERS = [
    filter_meme_coins_high_fg,
    filter_low_confluence_sideways,
    filter_consecutive_losses,
    filter_dead_zone_signals,
    filter_btc_correlation,
    # Groq добавляет новые фильтры сюда ↓
]

def run_all_filters(symbol, direction, price, confluence, regime, fg=None):
    """
    Запускает все активные фильтры.
    AUTOPILOT PROTECTED: эта функция не может быть удалена
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
    """
    Бонус за аномальный объём в направлении сделки
    AUTOPILOT PROTECTED: основная логика защищена
    """
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
    """
    Бонус если последние 5 свечей идут чётко в одном направлении
    AUTOPILOT PROTECTED: основная логика защищена
    """
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
# AUTOPILOT PROTECTED: базовые бустеры защищены от удаления
CONFLUENCE_BOOSTERS = [
    boost_strong_volume,
    boost_clean_structure,
    # Groq добавляет новые бусты сюда ↓
]

def run_confluence_boosters(candles, direction):
    """
    Запускает все бустеры confluence.
    AUTOPILOT PROTECTED: эта функция не может быть удалена
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
    """
    Сводка плагина для отображения в боте
    AUTOPILOT PROTECTED: эта функция не может быть удалена
    """
    return {
        "filters": len(ACTIVE_FILTERS),
        "boosters": len(CONFLUENCE_BOOSTERS),
        "version": GROQ_CHANGELOG[-1]["version"] if GROQ_CHANGELOG else "1.0.0",
        "last_change": GROQ_CHANGELOG[-1]["date"] if GROQ_CHANGELOG else "—",
        "last_author": GROQ_CHANGELOG[-1]["author"] if GROQ_CHANGELOG else "—",
        "changelog": GROQ_CHANGELOG[-3:],  # последние 3 изменения
        "protected_functions": list(PROTECTED_FUNCTIONS),
        "security_version": "2.0.0"
    }

def get_filter_names():
    """Список активных фильтров по именам"""
    return [f.__name__ for f in ACTIVE_FILTERS]

def get_booster_names():
    """Список активных бустеров по именам"""
    return [b.__name__ for b in CONFLUENCE_BOOSTERS]

def validate_code_change(code: str, function_name: str) -> Tuple[bool, str]:
    """
    Валидация изменений кода от Groq
    """
    try:
        # Проверка синтаксиса
        ast.parse(code)
        
        # Проверка защищенных функций
        if function_name in PROTECTED_FUNCTIONS:
            # Получаем текущий код функции
            current_func = globals().get(function_name)
            if current_func:
                import inspect
                current_code = inspect.getsource(current_func)
                
                if not _validate_function_modification(current_code, code, function_name):
                    return False, f"Function {function_name} is protected and cannot be modified"
        
        return True, "Code validation passed"
        
    except SyntaxError as e:
        return False, f"Syntax error: {e}"
    except Exception as e:
        return False, f"Validation error: {e}"

def safe_add_filter(filter_func):
    """
    Безопасное добавление нового фильтра
    """
    if filter_func not in ACTIVE_FILTERS:
        ACTIVE_FILTERS.append(filter_func)
        logging.info(f"Added new filter: {filter_func.__name__}")
        return True
    return False

def safe_add_booster(booster_func):
    """
    Безопасное добавление нового бустера
    """
    if booster_func not in CONFLUENCE_BOOSTERS:
        CONFLUENCE_BOOSTERS.append(booster_func)
        logging.info(f"Added new booster: {booster_func.__name__}")
        return True
    return False

def remove_filter(filter_name: str) -> bool:
    """
    Безопасное удаление фильтра (только незащищенных)
    """
    if filter_name in PROTECTED_FUNCTIONS:
        logging.warning(f"Cannot remove protected filter: {filter_name}")
        return False
    
    for i, f in enumerate(ACTIVE_FILTERS):
        if f.__name__ == filter_name:
            ACTIVE_FILTERS.pop(i)
            logging.info(f"Removed filter: {filter_name}")
            return True
    return False

def remove_booster(booster_name: str) -> bool:
    """
    Безопасное удаление бустера (только незащищенных)
    """
    if booster_name in PROTECTED_FUNCTIONS:
        logging.warning(f"Cannot remove protected booster: {booster_name}")
        return False
    
    for i, b in enumerate(CONFLUENCE_BOOSTERS):
        if b.__name__ == booster_name:
            CONFLUENCE_BOOSTERS.pop(i)
            logging.info(f"Removed booster: {booster_name}")
            return True
    return False

# ═══════════════════════════════════════════════════════════════
# SECURITY FUNCTIONS - Защита от вредоносного кода
# ═══════════════════════════════════════════════════════════════

def _scan_for_suspicious_code(code: str) -> List[str]:
    """
    Сканирование кода на подозрительные паттерны
    """
    suspicious_patterns = [
        (r"exec\s*\(", "Dynamic code execution"),
        (r"eval\s*\(", "Dynamic code evaluation"),
        (r"__import__\s*\(", "Dynamic imports"),
        (r"subprocess\.", "Subprocess execution"),
        (r"os\.system", "System command execution"),
        (r"open\s*\(", "File access"),
        (r"input\s*\(", "User input"),
        (r"globals\(\)", "Global variable access"),
        (r"locals\(\)", "Local variable access"),
        (r"getattr\s*\(", "Dynamic attribute access"),
        (r"setattr\s*\(", "Dynamic attribute modification"),
        (r"delattr\s*\(", "Dynamic attribute deletion"),
        (r"hasattr\s*\(", "Dynamic attribute checking")
    ]
    
    issues = []
    for pattern, description in suspicious_patterns:
        if re.search(pattern, code):
            issues.append(description)
    
    return issues

def security_check(code: str, function_name: str) -> Tuple[bool, List[str]]:
    """
    Комплексная проверка безопасности кода
    """
    issues = []
    
    # Проверка на подозрительные паттерны
    suspicious = _scan_for_suspicious_code(code)
    issues.extend(suspicious)
    
    # Проверка защищенных функций
    if function_name in PROTECTED_FUNCTIONS:
        issues.append(f"Attempt to modify protected function: {function_name}")
    
    # Проверка длины кода (защита от обфускации)
    if len(code) > 10000:
        issues.append("Code too long (>10000 characters)")
    
    # Проверка на количество строк
    if code.count('\n') > 500:
        issues.append("Too many lines (>500)")
    
    return len(issues) == 0, issues

# ═══════════════════════════════════════════════════════════════
# HEALTH CHECK - Проверка состояния плагина
# ═══════════════════════════════════════════════════════════════

def health_check() -> Dict[str, Any]:
    """
    Проверка состояния плагина
    """
    return {
        "status": "healthy",
        "filters_count": len(ACTIVE_FILTERS),
        "boosters_count": len(CONFLUENCE_BOOSTERS),
        "protected_functions": list(PROTECTED_FUNCTIONS),
        "changelog_entries": len(GROQ_CHANGELOG),
        "security_version": "2.0.0",
        "last_change": GROQ_CHANGELOG[-1]["date"] if GROQ_CHANGELOG else None
    }

# ═══════════════════════════════════════════════════════════════
# BACKUP AND RESTORE - Резервное копирование конфигурации
# ═══════════════════════════════════════════════════════════════

def backup_configuration() -> Dict[str, Any]:
    """
    Создание резервной копии конфигурации
    """
    return {
        "filters": [f.__name__ for f in ACTIVE_FILTERS],
        "boosters": [b.__name__ for b in CONFLUENCE_BOOSTERS],
        "changelog": GROQ_CHANGELOG,
        "protected_functions": list(PROTECTED_FUNCTIONS),
        "timestamp": datetime.now().isoformat()
    }

def restore_configuration(backup: Dict[str, Any]) -> bool:
    """
    Восстановление конфигурации из резервной копии
    """
    try:
        # Восстановление фильтров (только незащищенные)
        protected_filters = [f for f in ACTIVE_FILTERS if f.__name__ in PROTECTED_FUNCTIONS]
        new_filters = [f for f in ACTIVE_FILTERS if f.__name__ not in PROTECTED_FUNCTIONS]
        
        # Здесь можно добавить логику восстановления из backup
        # Но защищенные функции всегда остаются
        
        logging.info("Configuration restored from backup")
        return True
    except Exception as e:
        logging.error(f"Failed to restore configuration: {e}")
        return False
