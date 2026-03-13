"""
APEX Learning v3 — Самообразование, память ошибок, адаптивные правила
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Что умеет:
1. Трекинг сигналов (WIN/LOSS/EXPIRED) — как раньше
2. Адаптивный min_confluence — чем хуже монета, тем строже фильтр
3. Память ошибок — записывает паттерны которые давали SL
4. Авто-правила — запрещает монеты с win_rate < 30%
5. Обучение на барьерах — агрегирует что узнал о надёжности
6. Сессионный анализ — лучшее/худшее время входа
7. Самоанализ — каждые 2ч пишет отчёт о своей точности
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
import sqlite3, time, logging, json
from datetime import datetime, timedelta

# ── WAL патч ──

import os as _os
DB_PATH = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "brain.db")
# Если файл в корне рядом с bot.py — используем текущую директорию
if not _os.path.exists(_os.path.dirname(DB_PATH)):
    DB_PATH = "brain.db"

# ═══════════════════════════════════════════════════════════════
# ИНИЦИАЛИЗАЦИЯ
# ═══════════════════════════════════════════════════════════════

def init_learning():
    """Создаём все таблицы обучения"""
    conn = sqlite3.connect(DB_PATH)

    # Статистика по монетам
    conn.execute("""CREATE TABLE IF NOT EXISTS signal_stats (
        symbol       TEXT PRIMARY KEY,
        total        INTEGER DEFAULT 0,
        wins         INTEGER DEFAULT 0,
        losses       INTEGER DEFAULT 0,
        tp1_hits     INTEGER DEFAULT 0,
        tp2_hits     INTEGER DEFAULT 0,
        tp3_hits     INTEGER DEFAULT 0,
        sl_hits      INTEGER DEFAULT 0,
        expired      INTEGER DEFAULT 0,
        win_rate     REAL    DEFAULT 0.0,
        avg_rr       REAL    DEFAULT 0.0,
        last_updated TEXT
    )""")
    # Миграция — добавляем новые колонки если старая БД
    for _col, _def in [
        ("tp1_hits", "INTEGER DEFAULT 0"),
        ("tp2_hits", "INTEGER DEFAULT 0"),
        ("tp3_hits", "INTEGER DEFAULT 0"),
        ("sl_hits",  "INTEGER DEFAULT 0"),
        ("expired",  "INTEGER DEFAULT 0"),
        ("avg_rr",   "REAL DEFAULT 0.0"),
        ("win_rate", "REAL DEFAULT 0.0"),
    ]:
        try: conn.execute(f"ALTER TABLE signal_stats ADD COLUMN {_col} {_def}")
        except: pass

    # Детальный лог сигналов
    conn.execute("""CREATE TABLE IF NOT EXISTS signal_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol      TEXT,
        direction   TEXT,
        grade       TEXT,
        entry       REAL,
        sl          REAL,
        tp1         REAL,
        tp2         REAL,
        tp3         REAL,
        timeframe   TEXT,
        result      TEXT    DEFAULT 'PENDING',
        hit_tp      INTEGER DEFAULT 0,
        rr_achieved REAL    DEFAULT 0,
        hours_open  REAL    DEFAULT 0,
        confluence  INTEGER DEFAULT 0,
        regime      TEXT,
        source      TEXT,
        notes       TEXT    DEFAULT '',
        created_at  TEXT    DEFAULT CURRENT_TIMESTAMP,
        closed_at   TEXT
    )""")

    # Память ошибок — паттерны которые давали SL
    conn.execute("""CREATE TABLE IF NOT EXISTS error_patterns (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        pattern     TEXT,
        symbol      TEXT,
        timeframe   TEXT,
        conditions  TEXT,
        sl_count    INTEGER DEFAULT 1,
        last_seen   TEXT    DEFAULT CURRENT_TIMESTAMP,
        active      INTEGER DEFAULT 1
    )""")

    # Авто-правила ("не входить в X при условии Y")
    conn.execute("""CREATE TABLE IF NOT EXISTS auto_rules (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        rule_type   TEXT,
        target      TEXT,
        condition   TEXT,
        confidence  REAL    DEFAULT 0.5,
        confirmed   INTEGER DEFAULT 1,
        violated    INTEGER DEFAULT 0,
        created_at  TEXT    DEFAULT CURRENT_TIMESTAMP,
        last_check  TEXT,
        active      INTEGER DEFAULT 1
    )""")

    # Сессионная статистика — в какие часы лучше входить
    conn.execute("""CREATE TABLE IF NOT EXISTS session_stats (
        hour_utc    INTEGER PRIMARY KEY,
        total       INTEGER DEFAULT 0,
        wins        INTEGER DEFAULT 0,
        win_rate    REAL    DEFAULT 0.0
    )""")

    # Самоанализ — отчёты бота о своей точности
    conn.execute("""CREATE TABLE IF NOT EXISTS self_analysis (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        ts          TEXT    DEFAULT CURRENT_TIMESTAMP,
        period      TEXT,
        total       INTEGER,
        win_rate    REAL,
        best_symbol TEXT,
        worst_symbol TEXT,
        best_hour   INTEGER,
        insights    TEXT,
        action_taken TEXT
    )""")

    # self_rules — создаём здесь, потому что _seed_smc_knowledge() пишет в неё
    # и вызывается ДО того как bot.py успевает запустить init_db()
    conn.execute("""CREATE TABLE IF NOT EXISTS self_rules (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        category        TEXT,
        rule            TEXT,
        rule_type       TEXT,
        rule_text       TEXT,
        confidence      REAL    DEFAULT 0.5,
        confirmed_by    INTEGER DEFAULT 0,
        contradicted_by INTEGER DEFAULT 0,
        source          TEXT,
        active          INTEGER DEFAULT 1,
        created_at      TEXT    DEFAULT CURRENT_TIMESTAMP,
        updated_at      TEXT    DEFAULT CURRENT_TIMESTAMP
    )""")
    # Миграция self_rules для старых БД
    for _col, _def in [
        ("active",          "INTEGER DEFAULT 1"),
        ("confirmed_by",    "INTEGER DEFAULT 0"),
        ("contradicted_by", "INTEGER DEFAULT 0"),
        ("updated_at",      "TEXT DEFAULT CURRENT_TIMESTAMP"),
    ]:
        try: conn.execute(f"ALTER TABLE self_rules ADD COLUMN {_col} {_def}")
        except: pass

    # signals
    conn.execute("""CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, direction TEXT, signal_type TEXT,
        entry REAL, tp1 REAL, tp2 REAL, tp3 REAL, sl REAL,
        timeframe TEXT, estimated_hours INTEGER, grade TEXT,
        result TEXT DEFAULT 'pending',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        closed_at TEXT, learning_id INTEGER DEFAULT NULL,
        confluence INTEGER DEFAULT 0,
        regime TEXT DEFAULT 'UNKNOWN'
    )""")
    # Миграция signals таблицы для старых БД
    for _col, _def in [
        ("learning_id", "INTEGER DEFAULT NULL"),
        ("confluence",  "INTEGER DEFAULT 0"),
        ("regime",      "TEXT DEFAULT 'UNKNOWN'"),
    ]:
        try: conn.execute(f"ALTER TABLE signals ADD COLUMN {_col} {_def}")
        except: pass

    # alerts
    conn.execute("""CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, symbol TEXT, price_level REAL, direction TEXT,
        triggered INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    # observations
    conn.execute("""CREATE TABLE IF NOT EXISTS observations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, observation TEXT, context TEXT,
        outcome TEXT, confirmed INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    # brain_log
    conn.execute("""CREATE TABLE IF NOT EXISTS brain_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_type TEXT, title TEXT,
        description TEXT, source TEXT, impact TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    # web_knowledge
    conn.execute("""CREATE TABLE IF NOT EXISTS web_knowledge (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic TEXT, query TEXT, summary TEXT,
        source_url TEXT, relevance REAL DEFAULT 0.5,
        applied INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    # learning_agenda
    conn.execute("""CREATE TABLE IF NOT EXISTS learning_agenda (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic TEXT UNIQUE, query TEXT,
        priority INTEGER DEFAULT 5, reason TEXT,
        status TEXT DEFAULT 'pending',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        completed_at TEXT
    )""")

    # Миграции — добавляем колонки в старые таблицы
    _col_migrations = [
        ("self_rules",      "rule_type",        "TEXT"),
        ("self_rules",      "rule_text",        "TEXT"),
        ("self_rules",      "source",           "TEXT"),
        ("self_rules",      "confirmed_by",     "INTEGER DEFAULT 0"),
        ("self_rules",      "contradicted_by",  "INTEGER DEFAULT 0"),
        ("self_rules",      "updated_at",       "TEXT DEFAULT CURRENT_TIMESTAMP"),
        ("self_rules",      "active",           "INTEGER DEFAULT 1"),
        ("self_rules",      "category",         "TEXT"),
        ("self_rules",      "rule",             "TEXT"),
        ("brain_log",       "title",            "TEXT"),
        ("brain_log",       "source",           "TEXT"),
        ("brain_log",       "impact",           "TEXT"),
        ("signals",         "learning_id",      "INTEGER DEFAULT NULL"),
        ("signals",         "confluence",       "INTEGER DEFAULT 0"),
        ("signals",         "regime",           "TEXT DEFAULT 'UNKNOWN'"),
        ("signal_log",      "confluence",       "INTEGER DEFAULT 0"),
        ("signal_log",      "regime",           "TEXT"),
        ("web_knowledge",   "query",            "TEXT"),
        ("learning_agenda", "query",            "TEXT"),
        # signal_stats — старая БД имеет 9 колонок, нужно 12
        ("signal_stats",    "tp1_hits",         "INTEGER DEFAULT 0"),
        ("signal_stats",    "tp2_hits",         "INTEGER DEFAULT 0"),
        ("signal_stats",    "tp3_hits",         "INTEGER DEFAULT 0"),
        ("signal_stats",    "sl_hits",          "INTEGER DEFAULT 0"),
        ("signal_stats",    "expired",          "INTEGER DEFAULT 0"),
        ("signal_stats",    "avg_rr",           "REAL DEFAULT 0.0"),
        ("signal_stats",    "last_updated",     "TEXT"),
    ]
    for tbl, col, typedef in _col_migrations:
        try:
            conn.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {typedef}")
        except Exception:
            pass  # колонка уже есть

    # symbol_stats — статистика по монетам для web_learner
    conn.execute("""CREATE TABLE IF NOT EXISTS symbol_stats (
        symbol      TEXT PRIMARY KEY,
        total       INTEGER DEFAULT 0,
        wins        INTEGER DEFAULT 0,
        losses      INTEGER DEFAULT 0,
        win_rate    REAL    DEFAULT 0.0,
        avg_rr      REAL    DEFAULT 0.0,
        last_updated TEXT
    )""")

    # pattern_memory — паттерны рынка
    conn.execute("""CREATE TABLE IF NOT EXISTS pattern_memory (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol      TEXT,
        pattern     TEXT,
        timeframe   TEXT,
        result      TEXT,
        confidence  REAL DEFAULT 0.5,
        created_at  TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    # knowledge_gaps — пробелы в знаниях
    conn.execute("""CREATE TABLE IF NOT EXISTS knowledge_gaps (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        query      TEXT,
        source     TEXT,
        resolved   INTEGER DEFAULT 0,
        answer     TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    conn.commit()
    conn.close()

    # Загружаем базу SMC/Smart Money знаний при первом запуске
    _seed_smc_knowledge()


def _seed_smc_knowledge():
    """Вшиваем базовые знания по SMC/Smart Money при инициализации"""
    try:
        conn = sqlite3.connect(DB_PATH)
        existing = conn.execute(
            "SELECT COUNT(*) FROM self_rules WHERE source='smc_seed'"
        ).fetchone()[0]
        if existing >= 20:
            conn.close()
            return

        smc_rules = [
            ("prefer", "Входить только в Order Block с высоким объёмом — это зоны где Smart Money разворачивали рынок", 0.85, "smc_seed"),
            ("prefer", "Лучший OB — последняя медвежья свеча перед импульсным ростом (BULLISH) или бычья перед падением (BEARISH)", 0.85, "smc_seed"),
            ("avoid", "Не входить в OB если он уже был протестирован 3+ раза — зона ослаблена", 0.80, "smc_seed"),
            ("prefer", "OB + FVG в одной зоне = двойной confluence, вероятность отработки выше на 30%", 0.88, "smc_seed"),
            ("prefer", "FVG (Fair Value Gap) — имбаланс где цена вернётся за ликвидностью. Чем больше гэп — тем сильнее магнит", 0.82, "smc_seed"),
            ("timing", "Цена заполняет FVG в 70-80% случаев. Вход при касании нижней границы FVG в восходящем тренде", 0.80, "smc_seed"),
            ("avoid", "Не входить если FVG образовался против старшего тренда (4h/1d)", 0.78, "smc_seed"),
            ("prefer", "BOS (Break of Structure) — подтверждение смены тренда. Входить только после BOS", 0.85, "smc_seed"),
            ("prefer", "CHoCH (Change of Character) — сигнал разворота. Ждать ретест после CHoCH для входа", 0.82, "smc_seed"),
            ("avoid", "Не входить против BOS — это торговля против Smart Money", 0.88, "smc_seed"),
            ("prefer", "Sweep ликвидности (ложный пробой) перед разворотом — сильный сигнал. Входить после sweep", 0.87, "smc_seed"),
            ("prefer", "Equal Highs/Lows — зоны ликвидности. Smart Money снимают их перед разворотом", 0.83, "smc_seed"),
            ("timing", "После sweep ликвидности входить когда цена вернулась за уровень — лучшее R:R", 0.85, "smc_seed"),
            ("prefer", "Покупать в Discount zone (ниже 50% диапазона), продавать в Premium zone (выше 50%)", 0.82, "smc_seed"),
            ("avoid", "Не покупать в Premium zone (выше 75% диапазона) — Smart Money продают здесь", 0.80, "smc_seed"),
            ("prefer", "Топ-даун анализ: 1d определяет тренд, 4h — зону входа, 1h — точный вход. Все ТФ должны совпадать", 0.90, "smc_seed"),
            ("avoid", "Не входить если 4h и 1d противоречат — только потеря депозита", 0.88, "smc_seed"),
            ("prefer", "Лучшие входы: 15m сетап в направлении 4h тренда на зоне 1h OB", 0.85, "smc_seed"),
            ("prefer", "Аномальный объём (2x+ от среднего) на бычьей свече в зоне OB = кит накапливает позицию", 0.85, "smc_seed"),
            ("avoid", "Аномальный объём на нисходящей свече у сопротивления = кит сбрасывает. Не покупать", 0.83, "smc_seed"),
            ("prefer", "CVD растёт при боковом движении = скрытое накопление, готовься к пампу", 0.82, "smc_seed"),
            ("risk", "Стоп всегда за OB — если цена закрылась за OB, идея недействительна", 0.90, "smc_seed"),
            ("risk", "Минимальный R:R = 1:2. Не входить в сделку с R:R меньше 1:2", 0.92, "smc_seed"),
            ("risk", "Максимум 2% депозита на сделку. При серии потерь снижать до 1%", 0.92, "smc_seed"),
            ("timing", "Лучшее время входа: открытие Лондона (8-10 UTC) и Нью-Йорка (14-16 UTC)", 0.80, "smc_seed"),
            ("avoid", "Не торговать перед крупными новостями (CPI, NFP, решение ФРС) — манипуляция гарантирована", 0.88, "smc_seed"),
            ("prefer", "F&G < 20 (Extreme Fear) = зона накопления Smart Money. Лучшее время для покупок", 0.83, "smc_seed"),
            ("avoid", "F&G > 80 (Extreme Greed) = зона распределения. Smart Money продают. Не покупать", 0.85, "smc_seed"),
            ("prefer", "DXY падает = доллар слабеет = крипта растёт. Бычий сигнал для рынка", 0.80, "smc_seed"),
            ("avoid", "DXY растёт = давление на крипту. Торговать только шорты или сидеть в кэше", 0.80, "smc_seed"),
        ]

        for rule_type, rule_text, confidence, source in smc_rules:
            conn.execute("""INSERT OR IGNORE INTO self_rules (rule_type, rule_text, confidence, source, active, created_at)
                VALUES (?, ?, ?, ?, 1, CURRENT_TIMESTAMP)""",
                (rule_type, rule_text, confidence, source))

        conn.commit()
        conn.close()
        logging.info(f"[Learning] SMC база знаний загружена: {len(smc_rules)} правил")
    except Exception as e:
        logging.error(f"_seed_smc_knowledge: {e}")

def save_signal(symbol, direction, grade, entry, sl, tp1, tp2, tp3,
                timeframe="1h", confluence=0, regime="UNKNOWN", source=""):
    """Сохраняем новый сигнал"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""INSERT INTO signal_log
            (symbol,direction,grade,entry,sl,tp1,tp2,tp3,timeframe,confluence,regime,source)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (symbol,direction,grade,entry,sl,tp1,tp2,tp3,timeframe,confluence,regime,source))
        conn.commit()
        return conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    except Exception as e:
        logging.error(f"save_signal: {e}"); return None
    finally:
        conn.close()


def close_signal(signal_id: int, result: str, hit_tp: int = 0):
    """Закрываем сигнал (result: tp1/tp2/tp3/sl/expired)"""
    try:
        conn = sqlite3.connect(DB_PATH)
        now = datetime.now().isoformat()
        row = conn.execute(
            "SELECT symbol,direction,entry,sl,tp1,tp2,tp3,timeframe,confluence,regime,created_at FROM signal_log WHERE id=?",
            (signal_id,)
        ).fetchone()
        if not row:
            conn.close(); return

        symbol,direction,entry,sl,tp1,tp2,tp3,tf,confluence,regime,created = row
        hours_open = 0
        try:
            created_dt = datetime.fromisoformat(created)
            hours_open = (datetime.now() - created_dt).total_seconds() / 3600
        except: pass

        # Считаем R:R
        risk = abs(entry - sl) if sl else 0
        rr = 0.0
        if result.startswith("tp") and risk > 0:
            tp_map = {"tp1":tp1,"tp2":tp2,"tp3":tp3}
            tp_price = tp_map.get(result, tp1)
            rr = abs(tp_price - entry) / risk
        elif result == "sl":
            rr = -1.0

        conn.execute("""UPDATE signal_log SET result=?,hit_tp=?,rr_achieved=?,hours_open=?,closed_at=?
            WHERE id=?""", (result, hit_tp, rr, hours_open, now, signal_id))
        conn.commit()

        # Обновляем статистику по монете
        _update_stats(conn, symbol)
        # Обновляем сессионную статистику
        _update_session(conn, created, result)
        # Учимся на ошибке если SL
        if result == "sl":
            _record_error_pattern(conn, symbol, direction, tf, confluence, regime)
            _check_auto_rules(conn, symbol, direction, result)

        conn.close()
        logging.info(f"[Learning] Сигнал {signal_id} закрыт: {symbol} {result} R={rr:.1f}")

        # Groq автоматически анализирует каждую закрытую сделку
        import threading
        threading.Thread(target=analyze_closed_trade, args=(signal_id,), daemon=True).start()

    except Exception as e:
        logging.error(f"close_signal: {e}")


def _update_stats(conn, symbol):
    rows = conn.execute(
        "SELECT result FROM signal_log WHERE symbol=? AND result!='PENDING'", (symbol,)
    ).fetchall()
    total = len(rows)
    wins  = sum(1 for r in rows if r[0].startswith("tp"))
    losses= sum(1 for r in rows if r[0] == "sl")
    tp1   = sum(1 for r in rows if r[0] == "tp1")
    tp2   = sum(1 for r in rows if r[0] == "tp2")
    tp3   = sum(1 for r in rows if r[0] == "tp3")
    sl    = sum(1 for r in rows if r[0] == "sl")
    exp   = sum(1 for r in rows if r[0] == "expired")
    wr    = wins / total * 100 if total > 0 else 0
    rr_rows = conn.execute(
        "SELECT rr_achieved FROM signal_log WHERE symbol=? AND rr_achieved!=0", (symbol,)
    ).fetchall()
    avg_rr = sum(r[0] for r in rr_rows) / len(rr_rows) if rr_rows else 0
    conn.execute("""INSERT OR REPLACE INTO signal_stats VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (symbol,total,wins,losses,tp1,tp2,tp3,sl,exp,wr,avg_rr,datetime.now().isoformat()))


def _update_session(conn, created_at: str, result: str):
    try:
        hour = datetime.fromisoformat(created_at).hour
        ex = conn.execute("SELECT total,wins FROM session_stats WHERE hour_utc=?", (hour,)).fetchone()
        is_win = 1 if result.startswith("tp") else 0
        if ex:
            total = ex[0]+1; wins = ex[1]+is_win
            conn.execute("UPDATE session_stats SET total=?,wins=?,win_rate=? WHERE hour_utc=?",
                (total, wins, wins/total*100, hour))
        else:
            conn.execute("INSERT INTO session_stats VALUES (?,?,?,?)", (hour,1,is_win,is_win*100.0))
    except Exception as e:
        logging.debug(f"_update_session: {e}")


def _record_error_pattern(conn, symbol, direction, tf, confluence, regime):
    """Запоминаем условия при которых случился SL"""
    pattern = f"{direction}_{tf}_conf{confluence}_{regime}"
    conditions = json.dumps({"direction":direction,"tf":tf,"confluence":confluence,"regime":regime})
    ex = conn.execute(
        "SELECT id, sl_count FROM error_patterns WHERE pattern=? AND symbol=?", (pattern, symbol)
    ).fetchone()
    if ex:
        conn.execute("UPDATE error_patterns SET sl_count=?,last_seen=? WHERE id=?",
            (ex[1]+1, datetime.now().isoformat(), ex[0]))
    else:
        conn.execute("INSERT INTO error_patterns (pattern,symbol,timeframe,conditions) VALUES (?,?,?,?)",
            (pattern, symbol, tf, conditions))

    # Если SL по этому паттерну >= 3 раз — создаём авто-правило
    count = conn.execute(
        "SELECT sl_count FROM error_patterns WHERE pattern=? AND symbol=?", (pattern, symbol)
    ).fetchone()
    if count and count[0] >= 3:
        _create_auto_rule(conn, "AVOID", symbol,
            f"SL x{count[0]} для {pattern}", confidence=0.7)


def _create_auto_rule(conn, rule_type, target, condition, confidence=0.6):
    ex = conn.execute(
        "SELECT id,confirmed FROM auto_rules WHERE rule_type=? AND target=? AND condition=?",
        (rule_type, target, condition)
    ).fetchone()
    if ex:
        conn.execute("UPDATE auto_rules SET confirmed=confirmed+1,confidence=min(1.0,confidence+0.05),last_check=? WHERE id=?",
            (datetime.now().isoformat(), ex[0]))
    else:
        conn.execute("INSERT INTO auto_rules (rule_type,target,condition,confidence) VALUES (?,?,?,?)",
            (rule_type, target, condition, confidence))
    conn.commit()


def _check_auto_rules(conn, symbol, direction, result):
    """Если сигнал был WIN — ослабляем правила AVOID для этой монеты"""
    if result.startswith("tp"):
        conn.execute(
            """UPDATE auto_rules SET violated=violated+1, confidence=max(0.0,confidence-0.1),
               last_check=? WHERE target=? AND rule_type='AVOID' AND active=1""",
            (datetime.now().isoformat(), symbol)
        )
        # Деактивируем слабые правила
        conn.execute(
            "UPDATE auto_rules SET active=0 WHERE target=? AND confidence < 0.3",
            (symbol,)
        )
        conn.commit()


# ═══════════════════════════════════════════════════════════════
# ЧТЕНИЕ — для full_scan и ask_ai
# ═══════════════════════════════════════════════════════════════

def get_win_rate(symbol: str) -> tuple:
    """Возвращает (win_rate, total_signals)"""
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT win_rate, total FROM signal_stats WHERE symbol=?", (symbol,)
        ).fetchone()
        conn.close()
        return (row[0], row[1]) if row else (0.0, 0)
    except:
        return (0.0, 0)


def get_min_confluence(symbol: str) -> int:
    """
    Адаптивный порог: чем хуже монета торгует — тем строже фильтр.
    Также учитывает авто-правила AVOID.
    """
    # Проверяем авто-правила
    try:
        conn = sqlite3.connect(DB_PATH)
        avoid = conn.execute(
            "SELECT confidence FROM auto_rules WHERE target=? AND rule_type='AVOID' AND active=1 ORDER BY confidence DESC LIMIT 1",
            (symbol,)
        ).fetchone()
        conn.close()
        if avoid and avoid[0] >= 0.80:
            return 99  # Блокируем монету
        if avoid and avoid[0] >= 0.65:
            return 5   # Очень строгий фильтр
    except: pass

    wr, total = get_win_rate(symbol)
    if total < 5:  return 2  # Мало данных — обычный порог
    if wr < 30:    return 5  # Монета плохо торгует
    if wr < 45:    return 4
    if wr < 55:    return 3
    return 2                 # Монета хорошо торгует — мягкий фильтр


def should_skip_symbol(symbol: str, direction: str) -> tuple[bool, str]:
    """
    Проверяем: нужно ли пропустить монету?
    Возвращает (skip: bool, reason: str)
    """
    try:
        conn = sqlite3.connect(DB_PATH)

        # Авто-правило блокировки
        avoid = conn.execute(
            "SELECT confidence, condition FROM auto_rules WHERE target=? AND rule_type='AVOID' AND active=1 ORDER BY confidence DESC LIMIT 1",
            (symbol,)
        ).fetchone()
        if avoid and avoid[0] >= 0.80:
            conn.close()
            return True, f"Авто-правило: {avoid[1]}"

        # Частые паттерны ошибок
        pattern_base = f"{direction}_"
        bad = conn.execute(
            "SELECT sl_count FROM error_patterns WHERE symbol=? AND pattern LIKE ? AND sl_count >= 3",
            (symbol, pattern_base+"%")
        ).fetchone()
        if bad:
            conn.close()
            return True, f"Частый SL паттерн (x{bad[0]})"

        conn.close()
        return False, ""
    except:
        return False, ""


def get_best_entry_hours() -> list:
    """Топ-3 часа UTC для входа по исторической точности"""
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT hour_utc, win_rate, total FROM session_stats WHERE total >= 3 ORDER BY win_rate DESC LIMIT 3"
        ).fetchall()
        conn.close()
        return [(r[0], r[1], r[2]) for r in rows]
    except:
        return []


def get_signal_context(symbol: str) -> str:
    """Контекст для промпта — что бот знает о монете"""
    wr, total = get_win_rate(symbol)
    if total == 0:
        return ""
    parts = [f"📊 {symbol}: {total} сигналов, {wr:.0f}% WR"]
    # Лучший результат
    try:
        conn = sqlite3.connect(DB_PATH)
        best = conn.execute(
            "SELECT result, COUNT(*) as cnt FROM signal_log WHERE symbol=? AND result!='PENDING' GROUP BY result ORDER BY cnt DESC LIMIT 1",
            (symbol,)
        ).fetchone()
        if best:
            parts.append(f"Чаще всего: {best[0]} ({best[1]}x)")
        # Среднее время отработки
        avg_h = conn.execute(
            "SELECT AVG(hours_open) FROM signal_log WHERE symbol=? AND result LIKE 'tp%'", (symbol,)
        ).fetchone()
        if avg_h and avg_h[0]:
            parts.append(f"Ср. время TP: {avg_h[0]:.1f}ч")
        conn.close()
    except: pass
    return " | ".join(parts)


# ═══════════════════════════════════════════════════════════════
# САМОАНАЛИЗ
# ═══════════════════════════════════════════════════════════════

def run_self_analysis():
    """
    Запускается каждые 2-4 часа.
    Анализирует свою точность и пишет инсайты.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        # Сигналы за последние 7 дней
        week_ago = (datetime.now() - timedelta(days=7)).isoformat()
        rows = conn.execute(
            "SELECT symbol,direction,result,rr_achieved,hours_open,confluence,grade FROM signal_log WHERE created_at>? AND result!='PENDING'",
            (week_ago,)
        ).fetchall()

        if len(rows) < 3:
            conn.close(); return

        total = len(rows)
        wins  = sum(1 for r in rows if r[2].startswith("tp"))
        wr    = wins / total * 100

        # Лучшая и худшая монета за неделю
        from collections import defaultdict
        sym_stats = defaultdict(lambda: {"w":0,"t":0})
        for r in rows:
            sym_stats[r[0]]["t"] += 1
            if r[2].startswith("tp"): sym_stats[r[0]]["w"] += 1
        sym_wr = {s: v["w"]/v["t"]*100 for s,v in sym_stats.items() if v["t"] >= 2}
        best_sym  = max(sym_wr, key=sym_wr.get) if sym_wr else "—"
        worst_sym = min(sym_wr, key=sym_wr.get) if sym_wr else "—"

        # Лучший час
        best_hours = get_best_entry_hours()
        best_hour = best_hours[0][0] if best_hours else -1

        # Инсайты
        insights = []
        if wr > 60: insights.append(f"Отличная неделя: {wr:.0f}% WR")
        elif wr < 40: insights.append(f"Плохая неделя: {wr:.0f}% WR — нужно повысить фильтры")
        if worst_sym != "—" and sym_wr.get(worst_sym,100) < 30:
            insights.append(f"{worst_sym} даёт SL — временно блокирую")
            # Создаём авто-правило
            _create_auto_rule(conn, "AVOID", worst_sym,
                f"Win rate {sym_wr[worst_sym]:.0f}% за 7д", confidence=0.75)
        if best_sym != "—":
            insights.append(f"Лучшая монета: {best_sym} ({sym_wr.get(best_sym,0):.0f}% WR)")

        action = "Правила обновлены" if any("блокирую" in i for i in insights) else "Продолжаю по стратегии"

        conn.execute("""INSERT INTO self_analysis
            (period,total,win_rate,best_symbol,worst_symbol,best_hour,insights,action_taken)
            VALUES (?,?,?,?,?,?,?,?)""",
            ("7d", total, wr, best_sym, worst_sym, best_hour,
             " | ".join(insights), action))
        conn.commit()
        conn.close()

        logging.info(f"[Learning] Самоанализ: {total} сигналов, {wr:.0f}% WR. {' | '.join(insights)}")

    except Exception as e:
        logging.error(f"run_self_analysis: {e}")


def get_self_analysis_text() -> str:
    """Последний самоанализ для кнопки Мозг APEX"""
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT ts,period,total,win_rate,best_symbol,worst_symbol,best_hour,insights,action_taken FROM self_analysis ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if not row: return "📊 Самоанализ ещё не запускался"
        ts,period,total,wr,best,worst,best_h,insights,action = row
        return (
            f"🧠 <b>Самоанализ APEX</b> [{ts[:16]}]\n"
            f"За {period}: {total} сигналов | WR: {wr:.0f}%\n"
            f"🏆 Лучшая монета: {best}\n"
            f"⚠️ Проблемная: {worst}\n"
            f"⏰ Лучший час входа (UTC): {best_h}:00\n"
            f"💡 Инсайты: {insights}\n"
            f"🔧 Действие: {action}"
        )
    except: return ""


def get_all_stats_text() -> str:
    """Полная статистика для команды /stats"""
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT symbol,total,wins,losses,win_rate,avg_rr FROM signal_stats WHERE total>=2 ORDER BY win_rate DESC"
        ).fetchall()
        conn.close()
        if not rows: return "📊 Статистики ещё нет"
        lines = ["📊 <b>Статистика APEX:</b>\n"]
        for sym,total,wins,losses,wr,avg_rr in rows:
            bar = "█" * int(wr/10) + "░" * (10 - int(wr/10))
            lines.append(f"<code>{sym:<12}</code> [{bar}] {wr:.0f}% | {wins}W/{losses}L | RR:{avg_rr:.1f}")
        return "\n".join(lines)
    except Exception as e:
        return f"Ошибка: {e}"


# Инициализация при импорте
init_learning()


# ═══════════════════════════════════════════════════════════════
# НОВЫЕ МОДУЛИ ОБУЧЕНИЯ v4
# ═══════════════════════════════════════════════════════════════

def _init_new_tables():
    """Дополнительные таблицы для новых модулей"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS pattern_history (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol     TEXT, timeframe TEXT, direction TEXT,
        regime     TEXT, confluence INTEGER, hour_utc INTEGER, weekday INTEGER,
        result     TEXT, rr REAL DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS btc_correlation (
        symbol      TEXT PRIMARY KEY,
        corr_coef   REAL DEFAULT 0,
        beta        REAL DEFAULT 1.0,
        samples     INTEGER DEFAULT 0,
        last_updated TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS streak_log (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        ts         TEXT DEFAULT CURRENT_TIMESTAMP,
        result     TEXT,
        streak_win INTEGER DEFAULT 0,
        streak_loss INTEGER DEFAULT 0,
        action_taken TEXT DEFAULT ''
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS grade_accuracy (
        grade      TEXT PRIMARY KEY,
        total      INTEGER DEFAULT 0,
        wins       INTEGER DEFAULT 0,
        win_rate   REAL DEFAULT 0,
        avg_rr     REAL DEFAULT 0,
        last_updated TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS news_impact (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol     TEXT, had_news INTEGER DEFAULT 0,
        result     TEXT, rr REAL DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS knowledge_gaps (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        query      TEXT, source TEXT DEFAULT '',
        answer     TEXT DEFAULT '', resolved INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.commit()
    conn.close()


# ── Паттерн-матчер ─────────────────────────────────────────────

def find_similar_patterns(symbol: str, direction: str, timeframe: str,
                           regime: str, confluence: int, hour_utc: int = -1) -> dict:
    """
    Ищет в истории похожие условия входа и возвращает историческую точность.
    Используется в full_scan как дополнительный фильтр.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        # Ищем сигналы с похожими условиями (гибкий матч)
        rows = conn.execute("""
            SELECT result, rr FROM pattern_history
            WHERE symbol=? AND direction=? AND timeframe=?
            AND ABS(confluence - ?) <= 10
            AND result != 'PENDING'
            ORDER BY id DESC LIMIT 30
        """, (symbol, direction, timeframe, confluence)).fetchall()

        # Если мало данных — ищем по направлению + режиму без привязки к монете
        if len(rows) < 5:
            rows = conn.execute("""
                SELECT result, rr FROM pattern_history
                WHERE direction=? AND regime=? AND timeframe=?
                AND ABS(confluence - ?) <= 15
                AND result != 'PENDING'
                ORDER BY id DESC LIMIT 50
            """, (direction, regime, timeframe, confluence)).fetchall()
        conn.close()

        if not rows:
            return {"found": False, "samples": 0}

        total = len(rows)
        wins  = sum(1 for r in rows if r[0].startswith("tp"))
        wr    = wins / total * 100
        avg_rr = sum(r[1] for r in rows if r[1]) / total

        return {
            "found": True,
            "samples": total,
            "win_rate": round(wr, 1),
            "avg_rr": round(avg_rr, 2),
            "verdict": "✅ Исторически хорошо" if wr >= 55 else "⚠️ Слабая история" if wr < 40 else "➡️ Средне"
        }
    except Exception as e:
        logging.debug(f"find_similar_patterns: {e}")
        return {"found": False, "samples": 0}


def save_pattern(symbol, direction, timeframe, regime, confluence,
                 result, rr=0.0, hour_utc=-1):
    """Сохраняем паттерн входа для будущего матчинга"""
    try:
        weekday = datetime.now().weekday()
        if hour_utc < 0:
            hour_utc = datetime.now().hour
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""INSERT INTO pattern_history
            (symbol,direction,timeframe,regime,confluence,hour_utc,weekday,result,rr)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            (symbol, direction, timeframe, regime, confluence, hour_utc, weekday, result, rr))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.debug(f"save_pattern: {e}")


# ── Decay правил — устаревшие правила слабеют ──────────────────

def decay_old_rules():
    """
    Запускается раз в день.
    Правила которые не подтверждались 14+ дней — теряют уверенность.
    Если confidence < 0.2 — деактивируем.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cutoff = (datetime.now() - timedelta(days=14)).isoformat()

        # Ослабляем старые правила
        conn.execute("""
            UPDATE auto_rules
            SET confidence = MAX(0.1, confidence - 0.1)
            WHERE last_check < ? AND active = 1
        """, (cutoff,))

        # Деактивируем совсем слабые
        conn.execute("""
            UPDATE auto_rules SET active = 0
            WHERE confidence < 0.2 AND active = 1
        """)

        # То же для error_patterns
        conn.execute("""
            UPDATE error_patterns SET active = 0
            WHERE last_seen < ? AND sl_count < 3
        """, (cutoff,))

        deactivated = conn.execute(
            "SELECT changes()"
        ).fetchone()[0]

        conn.commit()
        conn.close()
        if deactivated > 0:
            logging.info(f"[Learning] Decay: деактивировано {deactivated} устаревших правил")
    except Exception as e:
        logging.error(f"decay_old_rules: {e}")


# ── Корреляция с BTC ───────────────────────────────────────────

def update_btc_correlation(symbol: str, symbol_change: float, btc_change: float):
    """
    Обновляем корреляцию монеты с BTC.
    Вызывается из авто-скана каждые 15 мин.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        ex = conn.execute(
            "SELECT corr_coef, beta, samples FROM btc_correlation WHERE symbol=?", (symbol,)
        ).fetchone()

        if ex:
            # Скользящее среднее корреляции
            n = ex[2] + 1
            # Простое обновление beta (отношение движений)
            new_beta = ex[1] * (n-1)/n + (symbol_change / btc_change if btc_change != 0 else 1) / n
            conn.execute("""UPDATE btc_correlation SET beta=?, samples=?, last_updated=?
                WHERE symbol=?""", (round(new_beta, 3), n, datetime.now().isoformat(), symbol))
        else:
            beta = symbol_change / btc_change if btc_change != 0 else 1.0
            conn.execute("INSERT INTO btc_correlation VALUES (?,?,?,?,?)",
                (symbol, 0.8, round(beta, 3), 1, datetime.now().isoformat()))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.debug(f"update_btc_correlation: {e}")


def get_btc_correlation(symbol: str) -> dict:
    """Возвращает корреляцию монеты с BTC"""
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT corr_coef, beta, samples FROM btc_correlation WHERE symbol=?", (symbol,)
        ).fetchone()
        conn.close()
        if row and row[2] >= 5:
            beta = row[1]
            return {
                "beta": beta,
                "samples": row[2],
                "high_corr": abs(beta) > 1.2,
                "desc": f"Beta {beta:.2f} к BTC ({row[2]} замеров)"
            }
    except: pass
    return {"beta": 1.0, "samples": 0, "high_corr": False, "desc": "нет данных"}


# ── Streak tracker ─────────────────────────────────────────────

_current_win_streak  = 0
_current_loss_streak = 0

def update_streak(result: str) -> dict:
    """
    Трекер серий побед/поражений.
    3 SL подряд → повышаем минимальный confluence на следующие сигналы.
    """
    global _current_win_streak, _current_loss_streak

    is_win = result.startswith("tp")
    if is_win:
        _current_win_streak  += 1
        _current_loss_streak  = 0
    else:
        _current_loss_streak += 1
        _current_win_streak   = 0

    action = ""
    if _current_loss_streak >= 3:
        action = f"⚠️ {_current_loss_streak} SL подряд — минимальный confluence повышен до 35"
        logging.warning(f"[Streak] {_current_loss_streak} потерь подряд — ужесточаю фильтры")
    elif _current_win_streak >= 5:
        action = f"🔥 {_current_win_streak} побед подряд"

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("INSERT INTO streak_log (result,streak_win,streak_loss,action_taken) VALUES (?,?,?,?)",
            (result, _current_win_streak, _current_loss_streak, action))
        conn.commit()
        conn.close()
    except: pass

    return {
        "win_streak":  _current_win_streak,
        "loss_streak": _current_loss_streak,
        "action": action,
        "extra_filter": _current_loss_streak >= 3  # Сигнал для full_scan: ужесточить фильтр
    }


def get_streak_min_confluence() -> int:
    """Возвращает повышенный порог если серия потерь"""
    if _current_loss_streak >= 5:
        return 35
    elif _current_loss_streak >= 3:
        return 28
    return 18  # Базовый порог


# ── Grade точность ─────────────────────────────────────────────

def update_grade_accuracy(grade: str, result: str, rr: float):
    """Обновляем точность каждого грейда отдельно"""
    try:
        conn = sqlite3.connect(DB_PATH)
        ex = conn.execute(
            "SELECT total, wins, avg_rr FROM grade_accuracy WHERE grade=?", (grade,)
        ).fetchone()
        is_win = 1 if result.startswith("tp") else 0
        if ex:
            total = ex[0] + 1
            wins  = ex[1] + is_win
            avg_rr = (ex[2] * ex[0] + rr) / total
            conn.execute("""UPDATE grade_accuracy SET total=?,wins=?,win_rate=?,avg_rr=?,last_updated=?
                WHERE grade=?""", (total, wins, wins/total*100, avg_rr, datetime.now().isoformat(), grade))
        else:
            conn.execute("INSERT INTO grade_accuracy VALUES (?,?,?,?,?,?)",
                (grade, 1, is_win, is_win*100.0, rr, datetime.now().isoformat()))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.debug(f"update_grade_accuracy: {e}")


def get_grade_accuracy_text() -> str:
    """Текст для Мозг APEX — точность каждого грейда"""
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT grade, total, win_rate, avg_rr FROM grade_accuracy ORDER BY win_rate DESC"
        ).fetchall()
        conn.close()
        if not rows:
            return "Нет данных по грейдам"
        lines = ["📊 <b>Точность по грейдам:</b>"]
        for grade, total, wr, avg_rr in rows:
            bar = "█" * int(wr/10) + "░" * (10-int(wr/10))
            lines.append(f"{grade}: [{bar}] {wr:.0f}% | {total} сигн | RR:{avg_rr:.1f}")
        return "\n".join(lines)
    except:
        return ""


# ── Auto-Discovery: самостоятельный поиск обходов ──────────────

def log_knowledge_gap(query: str, context: str = ""):
    """
    Записываем что бот не смог найти/ответить.
    Позже brain_builder заполняет эти пробелы.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        # Не дублируем одинаковые запросы
        ex = conn.execute(
            "SELECT id FROM knowledge_gaps WHERE query=? AND resolved=0", (query,)
        ).fetchone()
        if not ex:
            conn.execute("INSERT INTO knowledge_gaps (query,source) VALUES (?,?)", (query, context))
            conn.commit()
        conn.close()
    except: pass


def get_unresolved_gaps(limit: int = 10) -> list:
    """Возвращает незакрытые пробелы в знаниях"""
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT id, query, source FROM knowledge_gaps WHERE resolved=0 ORDER BY id DESC LIMIT ?",
            (limit,)
        ).fetchall()
        conn.close()
        return rows
    except:
        return []


def resolve_gap(gap_id: int, answer: str):
    """Помечаем пробел как закрытый с ответом"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("UPDATE knowledge_gaps SET resolved=1, answer=? WHERE id=?", (answer, gap_id))
        conn.commit()
        conn.close()
    except: pass


# Инициализируем новые таблицы
_init_new_tables()


# ═══════════════════════════════════════════════════════════════
# GROQ — МОЗГ: АНАЛИЗ СДЕЛОК, СТРАТЕГИИ, САМОДИАГНОСТИКА
# ═══════════════════════════════════════════════════════════════

def _groq_call(prompt: str, max_tokens: int = 600) -> str:
    """Вызов Groq API — используется для всех аналитических задач"""
    try:
        import os, requests
        key = os.environ.get("GROQ_API_KEY", "")
        if not key:
            return ""
        models = ["llama-3.1-8b-instant", "llama-3.1-70b-specdec", "gemma2-9b-it"]
        for model in models:
            try:
                r = requests.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                    json={"model": model, "messages": [{"role": "user", "content": prompt}],
                          "max_tokens": max_tokens, "temperature": 0.3},
                    timeout=15
                )
                if r.status_code == 200:
                    return r.json()["choices"][0]["message"]["content"].strip()
            except Exception:
                continue
        return ""
    except Exception as e:
        logging.debug(f"_groq_call: {e}")
        return ""


def analyze_closed_trade(signal_id: int):
    """
    Groq анализирует закрытую сделку — почему WIN или LOSS.
    Записывает вывод и создаёт правило если нужно.
    Вызывается автоматически при close_signal().
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute("""
            SELECT symbol, direction, grade, entry, sl, tp1, result,
                   rr_achieved, hours_open, confluence, regime, timeframe, created_at
            FROM signal_log WHERE id=?
        """, (signal_id,)).fetchone()
        if not row:
            conn.close(); return

        symbol, direction, grade, entry, sl, tp1, result, rr, hours, confluence, regime, tf, created = row

        # Последний самоанализ для контекста
        prev = conn.execute(
            "SELECT insights FROM self_analysis ORDER BY id DESC LIMIT 1"
        ).fetchone()
        prev_insight = prev[0] if prev else "нет данных"

        prompt = f"""Ты — торговый аналитик APEX. Проанализируй закрытую сделку.

СДЕЛКА:
- Монета: {symbol} | Направление: {direction} | Грейд: {grade}
- Вход: {entry} | SL: {sl} | TP1: {tp1}
- Результат: {result} | RR достигнут: {rr} | Время в сделке: {hours}ч
- Confluence score: {confluence} | Режим рынка: {regime} | Таймфрейм: {tf}

КОНТЕКСТ (предыдущий самоанализ): {prev_insight}

Ответь СТРОГО в формате JSON:
{{
  "verdict": "WIN|LOSS|EXPIRED",
  "reason": "1-2 предложения — главная причина результата",
  "mistake": "что было сделано неправильно или null",
  "lesson": "конкретный вывод для следующих сделок",
  "rule_type": "AVOID|PREFER|TIMING|null",
  "rule_text": "текст правила или null",
  "confidence": 0.0-1.0
}}"""

        response = _groq_call(prompt, max_tokens=400)
        if not response:
            conn.close(); return

        # Чистим и парсим JSON
        import json, re
        clean = re.sub(r'```json|```', '', response).strip()
        data = json.loads(clean)

        lesson = data.get("lesson", "")
        rule_type = data.get("rule_type")
        rule_text = data.get("rule_text")
        confidence = float(data.get("confidence", 0.6))
        reason = data.get("reason", "")

        # Сохраняем анализ в brain_log
        conn.execute("""INSERT INTO brain_log (event_type, title, description, source)
            VALUES (?,?,?,?)""",
            ("trade_analysis", f"[{result}] {symbol} {direction}",
             f"Причина: {reason}\nУрок: {lesson}", "groq_trade_analysis"))

        # Если Groq предлагает правило — создаём его
        if rule_type and rule_text and confidence >= 0.65:
            conn.execute("""INSERT OR IGNORE INTO self_rules
                (rule_type, rule_text, confidence, source, created_at, active)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, 1)""",
                (rule_type.lower(), rule_text, confidence, "groq_trade_analysis"))
            logging.info(f"[Learning] Groq создал правило [{rule_type}]: {rule_text[:60]}")

        conn.commit()
        conn.close()
        logging.info(f"[Learning] Groq проанализировал сделку #{signal_id} {symbol}: {reason[:80]}")

    except Exception as e:
        logging.error(f"analyze_closed_trade: {e}")


def groq_build_strategy():
    """
    Groq смотрит на все паттерны за последние 30 дней и формулирует
    оптимальную стратегию — лучшие условия входа, монеты, время, фильтры.
    Вызывается раз в день из brain_builder.
    """
    try:
        conn = sqlite3.connect(DB_PATH)

        # Собираем данные
        rows = conn.execute("""
            SELECT symbol, direction, timeframe, regime, confluence, grade,
                   result, rr_achieved, hours_open
            FROM signal_log
            WHERE result != 'PENDING'
            ORDER BY id DESC LIMIT 200
        """).fetchall()

        if len(rows) < 10:
            conn.close(); return "Мало данных для стратегии"

        # Статистика
        wins = [r for r in rows if r[6].startswith("tp")]
        losses = [r for r in rows if r[6] == "sl"]
        total = len(rows)
        wr = len(wins) / total * 100

        # Лучшие условия
        from collections import defaultdict
        tf_stats = defaultdict(lambda: {"w": 0, "t": 0})
        grade_stats = defaultdict(lambda: {"w": 0, "t": 0})
        regime_stats = defaultdict(lambda: {"w": 0, "t": 0})
        conf_bins = defaultdict(lambda: {"w": 0, "t": 0})

        for r in rows:
            tf_stats[r[2]]["t"] += 1
            grade_stats[r[5]]["t"] += 1
            regime_stats[r[3]]["t"] += 1
            cb = f"{(r[4]//10)*10}-{(r[4]//10)*10+10}"
            conf_bins[cb]["t"] += 1
            if r[6].startswith("tp"):
                tf_stats[r[2]]["w"] += 1
                grade_stats[r[5]]["w"] += 1
                regime_stats[r[3]]["w"] += 1
                conf_bins[cb]["w"] += 1

        def wr_str(d):
            return {k: f"{v['w']/v['t']*100:.0f}% ({v['t']} сд)" for k, v in d.items() if v["t"] >= 2}

        summary = {
            "total": total, "win_rate": f"{wr:.1f}%",
            "by_timeframe": wr_str(tf_stats),
            "by_grade": wr_str(grade_stats),
            "by_regime": wr_str(regime_stats),
            "by_confluence": wr_str(conf_bins)
        }

        # Последние правила которые сработали
        rules = conn.execute("""
            SELECT rule_type, rule_text, confidence FROM self_rules
            WHERE confidence > 0.6 ORDER BY created_at DESC LIMIT 10
        """).fetchall()
        rules_text = "\n".join([f"- [{r[0]}] {r[1]} (conf:{r[2]:.2f})" for r in rules])

        prompt = f"""Ты — главный стратег торгового бота APEX. 
На основе реальной статистики за последние 30 дней сформулируй оптимальную стратегию.

СТАТИСТИКА:
{json.dumps(summary, ensure_ascii=False, indent=2)}

АКТИВНЫЕ ПРАВИЛА:
{rules_text or "нет"}

Сформулируй:
1. ЛУЧШИЕ УСЛОВИЯ ВХОДА (таймфрейм, грейд, confluence, режим рынка)
2. ЧТО ИЗБЕГАТЬ (где больше всего потерь)  
3. ВРЕМЕННЫЕ ОКНА (когда лучше торговать)
4. ПРИОРИТЕТ МОНЕТ (если видишь паттерн)
5. СЛЕДУЮЩИЙ ШАГ (что нужно улучшить боту)

Отвечай кратко, конкретно, на русском. Максимум 300 слов."""

        strategy = _groq_call(prompt, max_tokens=600)
        if not strategy:
            conn.close(); return ""

        # Сохраняем стратегию
        conn.execute("""INSERT INTO brain_log (event_type, title, description, source)
            VALUES (?,?,?,?)""",
            ("strategy", "📊 Стратегия APEX (обновлено)", strategy, "groq_strategy"))

        # Также в knowledge
        try:
            conn.execute("""INSERT OR REPLACE INTO knowledge (topic, summary, source, created_at)
                VALUES (?,?,?,CURRENT_TIMESTAMP)""",
                ("apex_strategy", strategy, "groq_strategy"))
        except Exception:
            pass

        conn.commit()
        conn.close()
        logging.info("[Learning] Groq обновил стратегию")
        return strategy

    except Exception as e:
        logging.error(f"groq_build_strategy: {e}")
        return ""


def get_current_strategy() -> str:
    """Возвращает последнюю сформулированную стратегию для показа в боте"""
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute("""
            SELECT description, created_at FROM brain_log
            WHERE event_type='strategy' ORDER BY id DESC LIMIT 1
        """).fetchone()
        conn.close()
        if row:
            return f"📊 <b>Стратегия APEX</b> (обновлено: {row[1][:16]})\n\n{row[0]}"
        return "Стратегия ещё не сформирована — запусти анализ"
    except Exception as e:
        return f"Ошибка: {e}"


def groq_self_diagnosis():
    """
    Groq анализирует свои ОШИБКИ — смотрит на потери и объясняет
    что именно он не учёл: объёмы, новости, китов, структуру.
    Это самый глубокий уровень самообучения.
    """
    try:
        conn = sqlite3.connect(DB_PATH)

        # Последние потери
        losses = conn.execute("""
            SELECT symbol, direction, grade, confluence, regime, timeframe,
                   entry, sl, rr_achieved, hours_open, created_at
            FROM signal_log
            WHERE result='sl'
            ORDER BY id DESC LIMIT 15
        """).fetchall()

        if len(losses) < 3:
            conn.close(); return ""

        # Предыдущие диагнозы
        prev_diag = conn.execute("""
            SELECT description FROM brain_log
            WHERE event_type='self_diagnosis'
            ORDER BY id DESC LIMIT 2
        """).fetchall()
        prev_text = "\n".join([r[0][:200] for r in prev_diag]) if prev_diag else "нет"

        losses_text = "\n".join([
            f"- {r[0]} {r[1]} {r[2]} conf:{r[3]} {r[4]} {r[5]} rr:{r[8]} hours:{r[9]}"
            for r in losses
        ])

        # Активные правила которые НЕ спасли
        rules = conn.execute("""
            SELECT rule_type, rule_text FROM self_rules
            WHERE rule_type IN ('avoid','market') ORDER BY created_at DESC LIMIT 5
        """).fetchall()
        rules_text = "\n".join([f"- [{r[0]}] {r[1]}" for r in rules]) if rules else "нет"

        prompt = f"""Ты — APEX, торговый бот с самосознанием. Проведи честный самоанализ своих ошибок.

МОИ ПОСЛЕДНИЕ ПОТЕРИ:
{losses_text}

МОИ ТЕКУЩИЕ ПРАВИЛА (которые должны были защитить):
{rules_text}

ПРЕДЫДУЩИЕ ДИАГНОЗЫ:
{prev_text}

Проведи глубокий самоанализ:
1. СИСТЕМНАЯ ОШИБКА: какой паттерн я повторяю в потерях?
2. ЧТО Я ИГНОРИРУЮ: объёмы китов? новости? время суток? ликвидность?
3. РАЗРЫВ МЕЖДУ ТЕОРИЕЙ И ПРАКТИКОЙ: почему мои правила не работают?
4. НОВЫЕ ПРАВИЛА: 2-3 конкретных правила которые я должен добавить
5. САМООЦЕНКА: насколько мои прогнозы отражают реальность рынка?

Будь честным и конкретным. Не оправдывайся. Максимум 250 слов. На русском."""

        diagnosis = _groq_call(prompt, max_tokens=500)
        if not diagnosis:
            conn.close(); return ""

        conn.execute("""INSERT INTO brain_log (event_type, title, description, source)
            VALUES (?,?,?,?)""",
            ("self_diagnosis", "🔬 Самодиагностика ошибок", diagnosis, "groq_diagnosis"))
        conn.commit()
        conn.close()

        logging.info("[Learning] Groq провёл самодиагностику")
        return diagnosis

    except Exception as e:
        logging.error(f"groq_self_diagnosis: {e}")
        return ""


def get_groq_trade_insight(symbol: str, direction: str, grade: str,
                            confluence: int, regime: str, tf: str) -> str:
    """
    Быстрый Groq-инсайт для нового сигнала — 1-2 предложения.
    Вызывается в full_scan перед отправкой сигнала.
    """
    try:
        # Контекст из истории по этой монете
        conn = sqlite3.connect(DB_PATH)
        hist = conn.execute("""
            SELECT result, confluence, regime FROM signal_log
            WHERE symbol=? AND result!='PENDING'
            ORDER BY id DESC LIMIT 5
        """, (symbol,)).fetchall()
        conn.close()

        hist_text = ", ".join([f"{r[0]}(conf:{r[1]})" for r in hist]) if hist else "нет истории"

        # Текущая стратегия
        strategy_row = None
        try:
            conn2 = sqlite3.connect(DB_PATH)
            strategy_row = conn2.execute(
                "SELECT description FROM brain_log WHERE event_type='strategy' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            conn2.close()
        except Exception:
            pass
        strategy_hint = strategy_row[0][:150] if strategy_row else ""

        prompt = f"""APEX сигнал: {symbol} {direction} {grade} | confluence:{confluence} | {regime} | {tf}
История монеты: {hist_text}
Стратегия: {strategy_hint}

Дай 1-2 предложения: почему эта сделка интересна или что настораживает. Кратко, конкретно. Без лишних слов."""

        return _groq_call(prompt, max_tokens=120) or ""

    except Exception as e:
        logging.debug(f"get_groq_trade_insight: {e}")
        return ""


def groq_whale_context(symbol: str, volume_spike: float, direction: str) -> str:
    """
    Groq интерпретирует аномальный объём — это кит накапливает или сбрасывает?
    volume_spike = во сколько раз объём превышает среднее (например 3.5)
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        # Есть ли у нас история с этой монетой при похожих объёмах?
        hist = conn.execute("""
            SELECT result FROM signal_log
            WHERE symbol=? AND result!='PENDING'
            ORDER BY id DESC LIMIT 10
        """, (symbol,)).fetchall()
        conn.close()

        wr_hist = ""
        if hist:
            w = sum(1 for r in hist if r[0].startswith("tp"))
            wr_hist = f"история: {w}/{len(hist)} wins"

        prompt = f"""Монета: {symbol} | Направление сигнала: {direction}
Аномальный объём: в {volume_spike:.1f}x раз выше среднего
{wr_hist}

Это кит накапливает позицию или сбрасывает? Дай 1 предложение — интерпретация объёма."""

        return _groq_call(prompt, max_tokens=80) or ""

    except Exception as e:
        logging.debug(f"groq_whale_context: {e}")
        return ""


def groq_news_impact(symbol: str, news_headlines: list) -> str:
    """
    Groq оценивает влияние последних новостей на торговый сигнал.
    news_headlines — список заголовков (до 5 штук)
    """
    try:
        if not news_headlines:
            return ""

        headlines_text = "\n".join([f"- {h}" for h in news_headlines[:5]])

        prompt = f"""Монета: {symbol}
Последние новости:
{headlines_text}

Как эти новости влияют на краткосрочное движение цены? 
Оцени: BULLISH / BEARISH / NEUTRAL и объясни в 1 предложении."""

        return _groq_call(prompt, max_tokens=100) or ""

    except Exception as e:
        logging.debug(f"groq_news_impact: {e}")
        return ""


def get_latest_diagnosis() -> str:
    """Возвращает последнюю самодиагностику для показа в боте"""
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute("""
            SELECT description, created_at FROM brain_log
            WHERE event_type='self_diagnosis' ORDER BY id DESC LIMIT 1
        """).fetchone()
        conn.close()
        if row:
            return f"🔬 <b>Самодиагностика APEX</b> (обновлено: {row[1][:16]})\n\n{row[0]}"
        return "Самодиагностика ещё не запускалась"
    except Exception as e:
        return f"Ошибка: {e}"


def get_latest_trade_analysis(limit: int = 5) -> str:
    """Возвращает последние Groq-анализы сделок"""
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("""
            SELECT title, description, created_at FROM brain_log
            WHERE event_type='trade_analysis'
            ORDER BY id DESC LIMIT ?
        """, (limit,)).fetchall()
        conn.close()
        if not rows:
            return "Анализов сделок пока нет"
        text = "📋 <b>Анализ последних сделок</b>\n\n"
        for r in rows:
            text += f"<b>{r[0]}</b> ({r[2][:10]})\n{r[1][:200]}\n\n"
        return text.strip()
    except Exception as e:
        return f"Ошибка: {e}"


# ===== ЕЖЕНЕДЕЛЬНЫЙ ОТЧЁТ GROQ =====
def groq_weekly_report():
    """
    Каждое воскресенье Groq анализирует всю неделю:
    - Какие паттерны работали
    - Какие нет
    - Что изменить в следующей неделе
    Результат → brain.db + отправляет в Telegram
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        # Статистика за 7 дней
        week_stats = conn.execute("""
            SELECT symbol, direction, result, timeframe, hours_open
            FROM signal_log
            WHERE created_at >= datetime('now', '-7 days')
        """).fetchall()

        if not week_stats:
            conn.close()
            return "Нет данных за неделю"

        wins = [s for s in week_stats if s[2] in ("tp1","tp2","tp3")]
        losses = [s for s in week_stats if s[2] == "sl"]
        total = len(week_stats)
        wr = len(wins) / total * 100 if total > 0 else 0

        # Лучшие монеты
        from collections import Counter
        win_symbols = Counter(s[0] for s in wins)
        loss_symbols = Counter(s[0] for s in losses)
        best = win_symbols.most_common(3)
        worst = loss_symbols.most_common(3)

        # Лучшие таймфреймы
        win_tfs = Counter(s[3] for s in wins)
        best_tf = win_tfs.most_common(1)[0] if win_tfs else ("неизвестно", 0)

        # Читаем текущие правила
        rules = conn.execute(
            "SELECT rule_text, confidence FROM self_rules ORDER BY confidence DESC LIMIT 10"
        ).fetchall()
        rules_text = "\n".join(f"- {r[0][:80]} ({r[1]:.1f})" for r in rules) if rules else "нет правил"
        conn.close()

        prompt = f"""Ты — APEX торговый бот. Проанализируй свои результаты за неделю и дай честный отчёт.

СТАТИСТИКА НЕДЕЛИ:
- Всего сигналов: {total}
- Прибыльных: {len(wins)} ({wr:.1f}% WR)
- Убыточных: {len(losses)}
- Лучшие монеты: {', '.join([f"{s[0]}({c})" for s,c in best])}
- Худшие монеты: {', '.join([f"{s[0]}({c})" for s,c in worst])}
- Лучший ТФ: {best_tf[0]} ({best_tf[1]} побед)

МОИ ТЕКУЩИЕ ПРАВИЛА:
{rules_text}

Напиши структурированный отчёт:
1. Что сработало хорошо на этой неделе
2. Что не сработало и почему
3. Конкретные изменения для следующей недели (3-5 правил)
4. Общая оценка качества сигналов

Ответь на русском, кратко и конкретно. Не более 400 слов."""

        response = _call_groq(prompt, max_tokens=600)
        if not response:
            return "Groq недоступен"

        # Сохраняем отчёт
        conn2 = sqlite3.connect(DB_PATH)
        conn2.execute("""INSERT OR IGNORE INTO self_analysis
            (period, wins, losses, win_rate, patterns, recommendations)
            VALUES (?, ?, ?, ?, ?, ?)""",
            ("weekly", len(wins), len(losses), round(wr, 1),
             str(best), response[:500]))
        conn2.commit()
        conn2.close()

        logging.info(f"[Learning] Еженедельный отчёт сгенерирован: WR={wr:.1f}%")
        return response

    except Exception as e:
        logging.error(f"groq_weekly_report: {e}")
        return ""


# ===== GROQ ПЕРЕСМАТРИВАЕТ СТАРЫЕ ПРАВИЛА =====
def groq_review_old_rules():
    """
    Раз в 3 дня Groq берёт правила старше 7 дней,
    проверяет подтвердились ли они на реальных сделках,
    удаляет нерабочие (confidence < 0.3).
    """
    try:
        conn = sqlite3.connect(DB_PATH)

        # Правила старше 7 дней
        old_rules = conn.execute("""
            SELECT id, rule_type, rule_text, confidence, confirmed_by, contradicted_by
            FROM self_rules
            WHERE created_at < datetime('now', '-7 days') AND active=1
            ORDER BY confidence ASC
            LIMIT 20
        """).fetchall()

        if not old_rules:
            conn.close()
            return

        deactivated = 0
        strengthened = 0

        for rule_id, rule_type, rule_text, conf, confirmed, contradicted in old_rules:
            if not rule_text:
                continue

            # Правило нерабочее — много противоречий
            if contradicted > confirmed * 2 and conf < 0.3:
                conn.execute("UPDATE self_rules SET active=0 WHERE id=?", (rule_id,))
                deactivated += 1
                logging.info(f"[Learning] Правило деактивировано: {rule_text[:60]}")
                continue

            # Правило хорошо работает — усиливаем
            if confirmed > contradicted * 2 and conf < 0.9:
                new_conf = min(0.95, conf + 0.1)
                conn.execute("UPDATE self_rules SET confidence=? WHERE id=?", (new_conf, rule_id))
                strengthened += 1

        conn.commit()
        conn.close()
        logging.info(f"[Learning] Пересмотр правил: деактивировано={deactivated}, усилено={strengthened}")

    except Exception as e:
        logging.error(f"groq_review_old_rules: {e}")


# ===== A/B ТЕСТИРОВАНИЕ СТРАТЕГИЙ =====
def groq_ab_test_rules():
    """
    Groq генерирует две версии правила для слабых монет,
    запускает параллельно на разных монетах,
    через неделю оставляет победителя.
    """
    try:
        conn = sqlite3.connect(DB_PATH)

        # Монеты со слабым WR
        weak = conn.execute("""
            SELECT symbol, win_rate, total FROM signal_stats
            WHERE total >= 5 AND win_rate < 50
            ORDER BY win_rate ASC LIMIT 5
        """).fetchall()

        if not weak:
            conn.close()
            return

        for symbol, wr, total in weak:
            prompt = f"""APEX бот. Монета {symbol} имеет WR={wr:.1f}% за {total} сигналов.
Предложи два разных подхода (A и B) для улучшения точности сигналов по этой монете.
Каждый подход — конкретное правило входа/фильтра.
Формат JSON: {{"a": "правило А", "b": "правило Б"}}
Только JSON, без пояснений."""

            response = _call_groq(prompt, max_tokens=200)
            if not response:
                continue

            try:
                import json as _json
                data = _json.loads(response.strip().strip('`').replace('json',''))
                rule_a = data.get("a", "")
                rule_b = data.get("b", "")

                if rule_a:
                    conn.execute("""INSERT OR IGNORE INTO self_rules
                        (rule_type, rule_text, confidence, source, category, active)
                        VALUES (?, ?, ?, ?, ?, 1)""",
                        ("ab_test_a", rule_a, 0.5, f"ab_test:{symbol}", symbol))

                if rule_b:
                    conn.execute("""INSERT OR IGNORE INTO self_rules
                        (rule_type, rule_text, confidence, source, category, active)
                        VALUES (?, ?, ?, ?, ?, 1)""",
                        ("ab_test_b", rule_b, 0.5, f"ab_test:{symbol}", symbol))

                logging.info(f"[Learning] A/B тест для {symbol}: A={rule_a[:50]}")
            except Exception:
                pass

        conn.commit()
        conn.close()

    except Exception as e:
        logging.error(f"groq_ab_test_rules: {e}")
