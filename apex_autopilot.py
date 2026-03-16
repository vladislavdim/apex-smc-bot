"""
apex_autopilot.py — Автономный мозг APEX.

Полный замкнутый цикл без участия пользователя:

  1. LIVE АНАЛИЗ РЫНКА    — каждые 15 мин читает рынок, ищет паттерны
  2. МОНИТОРИНГ СДЕЛОК    — следит за открытыми позициями в реальном времени
  3. РАЗБОР РЕЗУЛЬТАТОВ   — после закрытия сделки глубокий анализ почему WIN/LOSS
  4. ДИАГНОСТИКА ОШИБОК   — ищет системные паттерны в потерях
  5. ЗАПИСЬ ФИКСА         — пишет конкретное исправление в groq_extensions.py
  6. ДЕПЛОЙ               — пушит на GitHub → Render подхватывает
  7. ВЕРИФИКАЦИЯ          — через N сделок проверяет помог ли фикс

Всё это крутится само, 24/7.
"""

import logging
import sqlite3
import json
import re
import time
import base64
import os
import ast
import requests
from datetime import datetime, timedelta

# ── WAL патч ──

DB_PATH     = "brain.db"

def _ensure_db_schema():
    """Гарантируем актуальную схему БД — запускается при импорте."""
    try:
        conn = __import__("sqlite3").connect(DB_PATH, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        for col, defval in [
            ("active",          "INTEGER DEFAULT 1"),
            ("confirmed_by",    "INTEGER DEFAULT 0"),
            ("contradicted_by", "INTEGER DEFAULT 0"),
            ("updated_at",      "TEXT DEFAULT CURRENT_TIMESTAMP"),
        ]:
            try: conn.execute(f"ALTER TABLE self_rules ADD COLUMN {col} {defval}")
            except: pass
        # Активируем старые записи
        try: conn.execute("UPDATE self_rules SET active=1 WHERE active IS NULL")
        except: pass
        # signal_stats миграция
        for col, defval in [
            ("tp1_hits", "INTEGER DEFAULT 0"), ("tp2_hits", "INTEGER DEFAULT 0"),
            ("tp3_hits", "INTEGER DEFAULT 0"), ("sl_hits",  "INTEGER DEFAULT 0"),
            ("expired",  "INTEGER DEFAULT 0"), ("avg_rr",   "REAL DEFAULT 0.0"),
        ]:
            try: conn.execute(f"ALTER TABLE signal_stats ADD COLUMN {col} {defval}")
            except: pass
        conn.commit()
        conn.close()
    except Exception as e:
        pass

_ensure_db_schema()
EXT_FILE    = "groq_extensions.py"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO  = os.environ.get("GITHUB_REPO", "")

# ─── Groq вызов с retry и правильными моделями ────────────────
# Актуальные модели Groq на март 2026:
_GROQ_MODELS = ["llama-3.1-8b-instant", "llama-3.1-8b-instant", "llama-3.1-8b-instant"]
# Глобальный кулдаун для autopilot — не спамим Groq
_AP_LAST_GROQ_CALL = 0
_AP_GROQ_COOLDOWN = 30  # секунд между вызовами из autopilot
_AP_GROQ_LOCK = __import__("threading").Lock()  # защита от параллельных вызовов

# Ротация ключей для обхода лимитов
GROQ_KEYS = [k for k in [os.environ.get("GROQ_API_KEY", "")] + [os.environ.get(f"GROQ_API_KEY_{i}", "") for i in range(2, 20)] if k]
logging.info(f"[Autopilot] Groq ключей загружено: {len(GROQ_KEYS)}")
_ap_groq_key_index = 0

def _groq(prompt: str, max_tokens: int = 800) -> str:
    global _AP_LAST_GROQ_CALL, _ap_groq_key_index
    # Lock + кулдаун — строго один вызов за раз
    with _AP_GROQ_LOCK:
        elapsed = time.time() - _AP_LAST_GROQ_CALL
        if elapsed < _AP_GROQ_COOLDOWN:
            time.sleep(_AP_GROQ_COOLDOWN - elapsed)
        _AP_LAST_GROQ_CALL = time.time()
    
    for attempt in range(len(GROQ_KEYS) * 3):
        model = _GROQ_MODELS[attempt % len(_GROQ_MODELS)]
        key_index = (attempt // len(_GROQ_MODELS)) % len(GROQ_KEYS)
        key = GROQ_KEYS[key_index]
        if not key:
            continue
        try:
            from groq import Groq
            client = Groq(api_key=key)
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
                temperature=0.3,
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            err = str(e)
            if "429" in err:
                # Парсим точное время ожидания из ошибки
                wait = 15.0
                try:
                    m = re.search(r"try again in ([\d.]+)s", err)
                    if m:
                        wait = float(m.group(1)) + 1.5
                except Exception:
                    pass
                logging.warning(f"[Autopilot] Groq rate limit (ключ {key_index+1}), жду {wait:.0f}с...")
                time.sleep(wait)
                continue
            elif any(x in err for x in ["decommissioned", "not exist", "model_not_found", "does not exist"]):
                logging.warning(f"[Autopilot] Модель {model} недоступна, пробую следующую")
                break  # следующая модель
            else:
                logging.error(f"[Autopilot] Groq error (ключ {key_index+1}): {e}")
                if attempt < len(GROQ_KEYS) * 3 - 1:
                    time.sleep(5)
                    continue
                return ""
    return ""


# ═══════════════════════════════════════════════════════════════
# 1. LIVE АНАЛИЗ РЫНКА
# ═══════════════════════════════════════════════════════════════

def live_market_analysis() -> dict:
    """
    Groq читает текущее состояние рынка и записывает наблюдения в brain.db.
    Вызывается каждые 15 минут.
    """
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)

        # Последние сигналы
        recent_signals = conn.execute("""
            SELECT symbol, direction, grade, confluence, regime, result, created_at
            FROM signal_log
            WHERE created_at > datetime('now', '-6 hours')
            ORDER BY created_at DESC LIMIT 10
        """).fetchall()

        # Статистика за сегодня
        today_stats = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN result IN ('tp1','tp2','tp3') THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'sl' THEN 1 ELSE 0 END) as losses,
                AVG(confluence) as avg_conf
            FROM signal_log
            WHERE created_at > datetime('now', '-24 hours')
        """).fetchone()

        total, wins, losses, avg_conf = today_stats if today_stats else (0, 0, 0, 0)
        total = total or 0
        wins = wins or 0
        losses = losses or 0
        avg_conf = avg_conf or 0
        wr = round(wins / (total or 1) * 100, 1)

        # Активные правила (топ по confidence)
        top_rules = conn.execute("""
            SELECT rule_type, rule_text, confidence
            FROM self_rules
            ORDER BY confidence DESC LIMIT 5
        """).fetchall()

        # Последние знания из веб-обучения
        web_knowledge = conn.execute("""
            SELECT topic, summary FROM web_knowledge
            ORDER BY created_at DESC LIMIT 3
        """).fetchall()

        conn.close()

        signals_text = "\n".join([
            f"- {r[0]} {r[1]} {r[2]} conf:{r[3]} режим:{r[4]} → {r[5] or 'PENDING'}"
            for r in recent_signals
        ]) or "нет сигналов"

        rules_text = "\n".join([
            f"- [{r[0]}] {r[1][:70]} ({r[2]:.0%})"
            for r in top_rules
        ]) or "нет правил"

        web_text = "\n".join([
            f"- {r[0]}: {(r[1] or '')[:80]}"
            for r in web_knowledge
        ]) or "нет"

        prompt = f"""Ты — APEX, торговый AI. Проведи быстрый live-анализ рынка.

СТАТИСТИКА СЕГОДНЯ (24ч):
- Всего сигналов: {total} | WIN: {wins} | LOSS: {losses} | WR: {wr}%
- Средний confluence: {round(avg_conf or 0, 1)}

ПОСЛЕДНИЕ СИГНАЛЫ (6ч):
{signals_text}

МОИ АКТИВНЫЕ ПРАВИЛА:
{rules_text}

ПОСЛЕДНИЕ ЗНАНИЯ ИЗ ИНТЕРНЕТА:
{web_text}

Текущее время UTC: {datetime.utcnow().strftime('%H:%M')}

Ответь в JSON:
{{
  "market_state": "BULLISH|BEARISH|NEUTRAL|VOLATILE",
  "main_observation": "главное что видишь прямо сейчас (1 предложение)",
  "risk_level": "LOW|MEDIUM|HIGH",
  "action": "TRADE_NORMAL|INCREASE_FILTERS|PAUSE|REDUCE_SIZE",
  "reason": "почему такое действие (1 предложение)",
  "watch_coins": ["монеты за которыми следить"],
  "pattern_spotted": "паттерн который замечаю или null"
}}"""

        response = _groq(prompt, max_tokens=400)
        if not response:
            return {}

        clean = re.sub(r'```json|```', '', response).strip()
        try:
            data = json.loads(clean)
        except Exception:
            logging.warning(f"[Autopilot] live_market_analysis: не удалось распарсить JSON: {clean[:100]}")
            return {}
        if not isinstance(data, dict):
            logging.warning(f"[Autopilot] live_market_analysis: ожидался dict, получен {type(data)}")
            return {}

        # Сохраняем наблюдение
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.execute("""INSERT INTO brain_log (event_type, title, description, source)
            VALUES (?,?,?,?)""",
            ("live_analysis",
             f"🌡 Рынок: {data.get('market_state')} | Риск: {data.get('risk_level')}",
             f"{data.get('main_observation','')} | Действие: {data.get('action','')} — {data.get('reason','')}",
             "autopilot_live"))
        conn.commit()
        conn.close()

        logging.info(f"[Autopilot] 🌡 Live анализ: {data.get('market_state')} | {data.get('action')} | {data.get('main_observation','')[:60]}")
        return data

    except Exception as e:
        logging.error(f"live_market_analysis: {e}")
        return {}


# ═══════════════════════════════════════════════════════════════
# 2. МОНИТОРИНГ ОТКРЫТЫХ СДЕЛОК
# ═══════════════════════════════════════════════════════════════

def monitor_open_trades():
    """
    Следит за открытыми сделками.
    Если видит что рынок разворачивается — записывает предупреждение.
    Вызывается каждые 15 минут.
    """
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        open_trades = conn.execute("""
            SELECT id, symbol, direction, entry, sl, tp1, tp2, tp3,
                   timeframe, confluence, created_at
            FROM signal_log
            WHERE result = 'PENDING'
            ORDER BY created_at ASC
        """).fetchall()
        conn.close()

        if not open_trades:
            return []

        warnings = []
        for trade in open_trades:
            tid, symbol, direction, entry, sl, tp1, tp2, tp3, tf, conf, created = trade

            # Считаем сколько часов открыта
            try:
                opened = datetime.fromisoformat(created)
                hours_open = (datetime.utcnow() - opened).total_seconds() / 3600
            except Exception:
                hours_open = 0

            # Слишком долго открыта — предупреждение
            MAX_HOURS = {"5m": 4, "15m": 12, "1h": 48, "4h": 120, "1d": 240}
            max_h = MAX_HOURS.get(tf, 48)

            if hours_open > max_h:
                conn = sqlite3.connect(DB_PATH, timeout=30)
                conn.execute("""UPDATE signal_log SET result='expired', closed_at=CURRENT_TIMESTAMP
                    WHERE id=? AND result='PENDING'""", (tid,))
                conn.commit()
                conn.close()
                logging.info(f"[Autopilot] ⏰ {symbol} закрыта по истечению времени ({hours_open:.0f}ч > {max_h}ч)")
                # Запускаем анализ закрытой сделки
                _analyze_after_close(tid, symbol, direction, "expired", hours_open, conf)

        return warnings

    except Exception as e:
        logging.error(f"monitor_open_trades: {e}")
        return []


# ═══════════════════════════════════════════════════════════════
# 3. ГЛУБОКИЙ РАЗБОР СДЕЛКИ + ЗАПИСЬ ФИКСА
# ═══════════════════════════════════════════════════════════════

def _analyze_after_close(signal_id, symbol, direction, result, hours_open, confluence):
    """
    Глубокий анализ закрытой сделки.
    Если LOSS — Groq сам пишет фикс в groq_extensions.py.
    """
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)

        # Полные данные сделки
        row = conn.execute("""
            SELECT symbol, direction, grade, entry, sl, tp1, result,
                   rr_achieved, hours_open, confluence, regime, timeframe
            FROM signal_log WHERE id=?
        """, (signal_id,)).fetchone()

        # Похожие прошлые сделки по этой монете
        similar = conn.execute("""
            SELECT result, confluence, regime, rr_achieved
            FROM signal_log
            WHERE symbol=? AND direction=?
            ORDER BY created_at DESC LIMIT 10
        """, (symbol, direction)).fetchall()

        # Текущие правила
        rules = conn.execute("""
            SELECT rule_type, rule_text, confidence
            FROM self_rules WHERE rule_type IN ('avoid','prefer','timing')
            ORDER BY confidence DESC LIMIT 8
        """).fetchall()

        conn.close()

        if not row:
            return

        sym, dirn, grade, entry, sl, tp1, res, rr, h_open, conf, regime, tf = row

        similar_text = "\n".join([
            f"- {r[0]} conf:{r[1]} режим:{r[2]} rr:{r[3]}"
            for r in similar
        ]) or "нет истории"

        rules_text = "\n".join([
            f"- [{r[0]}] {r[1][:60]}"
            for r in rules
        ]) or "нет правил"

        is_loss = res in ("sl", "expired")

        prompt = f"""Ты — APEX, торговый AI. Проведи ГЛУБОКИЙ разбор {'проигрышной' if is_loss else 'выигрышной'} сделки.

СДЕЛКА:
- Монета: {sym} | Направление: {dirn} | Грейд: {grade}
- Вход: {entry} | SL: {sl} | TP1: {tp1}
- Результат: {res} | RR: {rr} | Время: {h_open:.1f}ч
- Confluence: {conf} | Режим: {regime} | ТФ: {tf}

ИСТОРИЯ ЭТОЙ МОНЕТЫ (последние 10 сделок):
{similar_text}

МОИ ПРАВИЛА (которые должны были {'защитить' if is_loss else 'помочь'}):
{rules_text}

{'ЗАДАЧА: Найди КОНКРЕТНУЮ причину потери. Что именно я пропустил?' if is_loss else 'ЗАДАЧА: Найди что сработало. Как усилить этот паттерн?'}

Ответь в JSON:
{{
  "root_cause": "корневая причина (очень конкретно, 1 предложение)",
  "missed_signal": "какой сигнал я пропустил или проигнорировал",
  "pattern": "системный паттерн который нужно исправить",
  "fix_type": "NEW_FILTER|MODIFY_FILTER|NEW_BOOST|MODIFY_BOOST|NEW_RULE|null",
  "fix_description": "что именно изменить в коде (конкретно)",
  "fix_code_hint": "подсказка для кода фикса или null",
  "rule_type": "avoid|prefer|timing|risk",
  "rule_text": "новое правило для self_rules",
  "confidence": 0.0-1.0,
  "priority": 1-10
}}"""

        response = _groq(prompt, max_tokens=1000)
        if not response:
            return

        clean = re.sub(r'```json|```', '', response).strip()
        # Находим начало JSON объекта
        start = clean.find('{')
        end = clean.rfind('}')
        if start >= 0 and end > start:
            clean = clean[start:end+1]
        try:
            data = json.loads(clean)
        except Exception:
            # Groq обрезал JSON — пробуем восстановить минимально
            try:
                # Берём только первые поля до обрыва
                partial = clean[:clean.rfind('",') + 2] + '"_truncated": true}'
                data = json.loads(partial)
            except Exception:
                logging.warning(f"[Autopilot] monitor_open_trades: JSON parse error: {clean[:80]}")
                return
        if not isinstance(data, dict):
            return

        root_cause = data.get("root_cause", "")
        fix_type = data.get("fix_type")
        fix_desc = data.get("fix_description", "")
        rule_text = data.get("rule_text", "")
        rule_type = data.get("rule_type", "avoid")
        confidence = float(data.get("confidence", 0.6))
        priority = int(data.get("priority", 5))

        # Сохраняем разбор
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.execute("""INSERT INTO brain_log (event_type, title, description, source)
            VALUES (?,?,?,?)""",
            ("deep_analysis",
             f"[{res.upper()}] {sym} {dirn} — {root_cause[:60]}",
             json.dumps(data, ensure_ascii=False),
             "autopilot_deep"))

        # Новое правило в self_rules
        if rule_text and confidence >= 0.65:
            conn.execute("""INSERT OR IGNORE INTO self_rules
                (rule_type, rule_text, confidence, source, active, created_at)
                VALUES (?,?,?,?,1,CURRENT_TIMESTAMP)""",
                (rule_type, rule_text, confidence, "autopilot"))
            logging.info(f"[Autopilot] 📌 Новое правило [{rule_type}]: {rule_text[:70]}")

        conn.commit()
        conn.close()

        # Если это потеря с высоким приоритетом — пишем фикс в extensions
        if is_loss and fix_type and fix_type != "null" and priority >= 6:
            logging.info(f"[Autopilot] 🔧 Пишу фикс в extensions: {fix_desc[:60]}")
            _write_fix_to_extensions(
                signal_id=signal_id,
                symbol=sym,
                result=res,
                fix_type=fix_type,
                fix_description=fix_desc,
                fix_code_hint=data.get("fix_code_hint", ""),
                root_cause=root_cause,
                confidence=confidence
            )

        logging.info(f"[Autopilot] ✅ Разбор сделки #{signal_id} {sym}: {root_cause[:80]}")

    except Exception as e:
        logging.error(f"_analyze_after_close: {e}")


# ═══════════════════════════════════════════════════════════════
# 4. ЗАПИСЬ ФИКСА В groq_extensions.py → GitHub
# ═══════════════════════════════════════════════════════════════

def _write_fix_to_extensions(signal_id, symbol, result, fix_type,
                               fix_description, fix_code_hint, root_cause, confidence):
    """
    ОТКЛЮЧЕНО: Groq не может изменять код файлов.
    Только записывает анализ в мозги (observations).
    """
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.execute("""INSERT OR IGNORE INTO observations
            (symbol, observation, context, outcome, confirmed)
            VALUES (?,?,?,?,0)""",
            (symbol, f"[{fix_type}] {fix_description[:200]}",
             f"result={result} root_cause={root_cause[:100]}",
             f"confidence={confidence}"))
        conn.commit()
        conn.close()
        logging.info(f"[Autopilot] 📝 Анализ сохранён в мозги: {symbol} {fix_type}")
    except Exception as e:
        logging.error(f"_write_fix_to_extensions observation: {e}")
    return False  # Никогда не деплоим код

def system_error_diagnosis():
    """
    Раз в 4 часа Groq смотрит на паттерны потерь глобально.
    Если находит системную проблему — пишет фикс.
    """
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)

        # Потери за последние 48 часов
        losses = conn.execute("""
            SELECT symbol, direction, grade, confluence, regime, timeframe,
                   rr_achieved, hours_open
            FROM signal_log
            WHERE result='sl'
            AND created_at > datetime('now', '-48 hours')
        """).fetchall()

        if len(losses) < 2:
            conn.close()
            return ""

        # Недавние фиксы
        recent_fixes = conn.execute("""
            SELECT title, description FROM brain_log
            WHERE event_type='fix_deployed'
            ORDER BY id DESC LIMIT 3
        """).fetchall()

        # Статистика по режимам
        regime_stats = conn.execute("""
            SELECT regime,
                   COUNT(*) as total,
                   SUM(CASE WHEN result='sl' THEN 1 ELSE 0 END) as losses
            FROM signal_log
            WHERE created_at > datetime('now', '-7 days')
            GROUP BY regime
        """).fetchall()

        conn.close()

        losses_text = "\n".join([
            f"- {r[0]} {r[1]} {r[2]} conf:{r[3]} {r[4]} {r[5]} rr:{r[6]} hours:{r[7]}"
            for r in losses
        ])

        regime_text = "\n".join([
            f"- {r[0]}: {r[2]}/{r[1]} потерь ({round(r[2]/r[1]*100) if r[1] else 0}%)"
            for r in regime_stats
        ]) or "нет данных"

        fixes_text = "\n".join([r[0] for r in recent_fixes]) or "не было"

        prompt = f"""Ты — APEX, AI трейдер. Проведи системную диагностику.

ПОТЕРИ ЗА 48 ЧАСОВ ({len(losses)} штук):
{losses_text}

СТАТИСТИКА ПО РЕЖИМАМ РЫНКА (7 дней):
{regime_text}

НЕДАВНИЕ АВТОФИКСЫ:
{fixes_text}

Найди СИСТЕМНУЮ ПРОБЛЕМУ — паттерн который повторяется в потерях.

Ответь в JSON:
{{
  "system_problem": "главная системная проблема (конкретно)",
  "affected_conditions": "при каких условиях проблема возникает",
  "fix_priority": 1-10,
  "fix_type": "NEW_FILTER|MODIFY_FILTER|NEW_BOOST|null",
  "fix_description": "что конкретно изменить",
  "expected_improvement": "на сколько % улучшится WR",
  "requires_deploy": true|false
}}"""

        response = _groq(prompt, max_tokens=500)
        if not response:
            return ""

        clean = re.sub(r'```json|```', '', response).strip()
        try:
            data = json.loads(clean)
        except Exception:
            logging.warning(f"[Autopilot] system_error_diagnosis: JSON parse error: {clean[:100]}")
            return ""
        if not isinstance(data, dict):
            return ""

        problem = data.get("system_problem", "")
        fix_priority = int(data.get("fix_priority", 5))
        requires_deploy = data.get("requires_deploy", False)

        # Сохраняем диагноз
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.execute("""INSERT INTO brain_log (event_type, title, description, source)
            VALUES (?,?,?,?)""",
            ("system_diagnosis",
             f"🔬 Системная проблема: {problem[:60]}",
             json.dumps(data, ensure_ascii=False),
             "autopilot_diagnosis"))
        conn.commit()
        conn.close()

        logging.info(f"[Autopilot] 🔬 Системный диагноз: {problem[:80]} | Приоритет: {fix_priority}")

        # Если критично и нужен деплой — пишем фикс
        if requires_deploy and fix_priority >= 7 and data.get("fix_type") and data.get("fix_type") != "null":
            logging.info(f"[Autopilot] 🔧 Автофикс системной проблемы (приоритет {fix_priority})")
            _write_fix_to_extensions(
                signal_id=0,
                symbol="SYSTEM",
                result="system_fix",
                fix_type=data.get("fix_type"),
                fix_description=data.get("fix_description", ""),
                fix_code_hint="",
                root_cause=problem,
                confidence=0.75
            )

        return problem

    except Exception as e:
        logging.error(f"system_error_diagnosis: {e}")
        return ""


# ═══════════════════════════════════════════════════════════════
# 6. ВЕРИФИКАЦИЯ ФИКСОВ
# ═══════════════════════════════════════════════════════════════

def verify_recent_fixes():
    """
    Проверяет — помогли ли последние фиксы.
    Сравнивает WR до и после каждого деплоя.
    """
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)

        # Последние задеплоенные фиксы
        fixes = conn.execute("""
            SELECT id, title, description, created_at FROM brain_log
            WHERE event_type='fix_deployed'
            ORDER BY id DESC LIMIT 5
        """).fetchall()

        if not fixes:
            conn.close()
            return

        for fix_id, title, desc, fix_time in fixes:
            # WR за 24ч ДО фикса
            before = conn.execute("""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN result IN ('tp1','tp2','tp3') THEN 1 ELSE 0 END) as wins
                FROM signal_log
                WHERE created_at BETWEEN datetime(?, '-24 hours') AND ?
                AND result != 'PENDING'
            """, (fix_time, fix_time)).fetchone()

            # WR за 24ч ПОСЛЕ фикса
            after = conn.execute("""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN result IN ('tp1','tp2','tp3') THEN 1 ELSE 0 END) as wins
                FROM signal_log
                WHERE created_at > ?
                AND result != 'PENDING'
                LIMIT 20
            """, (fix_time,)).fetchone()

            before_wr = round((before[1] or 0) / (before[0] or 1) * 100, 1)
            after_wr  = round((after[1]  or 0) / (after[0]  or 1) * 100, 1)

            if after[0] >= 5:  # Достаточно данных для сравнения
                delta = after_wr - before_wr
                verdict = "✅ ПОМОГ" if delta > 0 else "❌ НЕ ПОМОГ" if delta < -5 else "➡️ НЕЙТРАЛЬНО"

                conn.execute("""INSERT OR IGNORE INTO brain_log (event_type, title, description, source)
                    VALUES (?,?,?,?)""",
                    ("fix_verification",
                     f"{verdict} | WR: {before_wr}% → {after_wr}% (Δ{delta:+.1f}%)",
                     f"Фикс: {title}\nДо: {before[0]} сделок WR={before_wr}%\nПосле: {after[0]} сделок WR={after_wr}%",
                     "autopilot_verify"))

                logging.info(f"[Autopilot] 📊 Верификация фикса: {verdict} | {before_wr}% → {after_wr}%")

        conn.commit()
        conn.close()

    except Exception as e:
        logging.error(f"verify_recent_fixes: {e}")


# ═══════════════════════════════════════════════════════════════
# 7. ГЛАВНЫЙ ЦИКЛ — вызывается из bot.py
# ═══════════════════════════════════════════════════════════════

def run_autopilot_cycle():
    """
    Быстрый цикл (каждые 15 мин):
    - Live анализ рынка
    - Мониторинг открытых сделок
    """
    try:
        logging.info("[Autopilot] 🔄 Быстрый цикл...")
        live_market_analysis()
        monitor_open_trades()
    except Exception as e:
        logging.error(f"run_autopilot_cycle: {e}")


def run_deep_autopilot():
    """
    Глубокий цикл (каждые 4 часа):
    - Системная диагностика ошибок
    - Верификация фиксов
    """
    try:
        logging.info("[Autopilot] 🔬 Глубокий цикл...")
        system_error_diagnosis()
        verify_recent_fixes()
    except Exception as e:
        logging.error(f"run_deep_autopilot: {e}")


def on_trade_closed(signal_id: int, symbol: str, direction: str,
                    result: str, hours_open: float, confluence: int):
    """
    Вызывается из bot.py когда сделка закрылась.
    Запускает разбор в отдельном потоке.
    """
    import threading
    t = threading.Thread(
        target=_analyze_after_close,
        args=(signal_id, symbol, direction, result, hours_open, confluence),
        daemon=True
    )
    t.start()


def get_autopilot_status() -> str:
    """Статус автопилота для отображения в боте"""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)

        # Последний live анализ
        last_live = conn.execute("""
            SELECT title, created_at FROM brain_log
            WHERE event_type='live_analysis'
            ORDER BY id DESC LIMIT 1
        """).fetchone()

        # Кол-во автофиксов
        fixes_count = conn.execute("""
            SELECT COUNT(*) FROM brain_log WHERE event_type='fix_deployed'
        """).fetchone()[0]

        # Последний фикс
        last_fix = conn.execute("""
            SELECT title, created_at FROM brain_log
            WHERE event_type='fix_deployed'
            ORDER BY id DESC LIMIT 1
        """).fetchone()

        # Верификации
        verifs = conn.execute("""
            SELECT title FROM brain_log
            WHERE event_type='fix_verification'
            ORDER BY id DESC LIMIT 3
        """).fetchall()

        conn.close()

        lines = ["🤖 <b>Автопилот APEX</b>\n"]

        if last_live:
            lines.append(f"🌡 Последний анализ: {last_live[0]}")
            lines.append(f"   <i>{last_live[1]}</i>")

        lines.append(f"\n🔧 Автофиксов задеплоено: <b>{fixes_count}</b>")

        if last_fix:
            lines.append(f"   Последний: {last_fix[0][:60]}")

        if verifs:
            lines.append("\n📊 Верификация фиксов:")
            for v in verifs:
                lines.append(f"   {v[0][:70]}")

        return "\n".join(lines)

    except Exception as e:
        return f"Автопилот: {e}"
