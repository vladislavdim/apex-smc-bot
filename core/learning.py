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

    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════
# СОХРАНЕНИЕ И ОБНОВЛЕНИЕ СИГНАЛОВ
# ═══════════════════════════════════════════════════════════════

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
                (rule_type, rule_text, confidence, source, created_at)
                VALUES (?,?,?,?,CURRENT_TIMESTAMP)""",
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
