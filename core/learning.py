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
