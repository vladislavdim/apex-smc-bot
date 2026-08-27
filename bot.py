Warning: truncated output (original token count: 62257)
Total output lines: 5161

"""
bot.py — Telegram хендлеры, команды, scheduler, запуск APEX.
Вся рыночная логика — в market.py
"""
import asyncio
import logging
logging.getLogger("asyncio").setLevel(logging.CRITICAL)
logging.getLogger("aiohttp").setLevel(logging.CRITICAL)
import os
import sqlite3
import time
import json
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler

# EMERGENCY PATCHES - исправление критических ошибок
try:
    from emergency_fix import apply_all_patches
    patches = apply_all_patches()
    logging.info("🎯 Emergency patches applied successfully")
    
except ImportError as e:
    logging.warning(f"⚠️ Emergency fix module not found: {e}")
    patches = {}

# WAL патч — решает "database is locked"
import sqlite3 as _sq
if not getattr(_sq, "_wal_patched", False):
    _orig_sq_connect = _sq.connect
    def _wal_sq_connect(db, timeout=60, **kw):
        kw.setdefault("check_same_thread", False)
        conn = _orig_sq_connect(db, timeout=timeout, **kw)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA synchronous=NORMAL")
        except Exception:
            pass
        return conn
    _sq.connect = _wal_sq_connect
    _sq._wal_patched = True

from groq import Groq
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ChatMemberUpdated
# Патч edit_text и edit_reply_markup — подавляем "message is not modified"
import aiogram.types.message as _msg_module
_orig_edit_text = _msg_module.Message.edit_text
_orig_edit_markup = _msg_module.Message.edit_reply_markup
async def _safe_edit_text(self, *args, **kwargs):
    try:
        return await _orig_edit_text(self, *args, **kwargs)
    except Exception as e:
        if "message is not modified" in str(e):
            return None
        raise
async def _safe_edit_markup(self, *args, **kwargs):
    try:
        return await _orig_edit_markup(self, *args, **kwargs)
    except Exception as e:
        if "message is not modified" in str(e):
            return None
        raise
_msg_module.Message.edit_text = _safe_edit_text
_msg_module.Message.edit_reply_markup = _safe_edit_markup
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ── Импортируем всю рыночную логику из market.py ──
from market import *

# Финальная проверка внешнего рыночного контекста. Она вызывается только после
# того, как стратегия уже рассчитала готовый кандидат, и не меняет его уровни.
try:
    from core.signal_quality_gate import (
        mark_candidate_not_sent as _mark_candidate_not_sent,
        review_signal_candidate as _review_signal_candidate,
    )
    _SIGNAL_QUALITY_GATE_OK = True
except Exception as _quality_gate_import_error:
    _SIGNAL_QUALITY_GATE_OK = False
    logging.warning(f"Signal quality gate недоступен: {_quality_gate_import_error}")

# Путь к базе данных
import os as _os_bot
DB_PATH = _os_bot.path.join(_os_bot.path.dirname(_os_bot.path.abspath(__file__)), "brain.db")

# ── Trailing stop columns migration ──
try:
    _mig_conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    for _col, _type in [("tp1_hit", "INTEGER DEFAULT 0"),
                         ("trailing_sl", "REAL DEFAULT 0"),
                         ("best_price", "REAL DEFAULT 0")]:
        try:
            _mig_conn.execute(f"ALTER TABLE signals ADD COLUMN {_col} {_type}")
        except Exception:
            pass
    _mig_conn.commit()
    _mig_conn.close()
except Exception:
    pass

# Fallback флаги — на случай если market.py не экспортировал их
try: _LEARNING_OK
except NameError: _LEARNING_OK = False
try: _SMC_ENGINE_OK
except NameError: _SMC_ENGINE_OK = False
try: _EXT_OK
except NameError: _EXT_OK = False
try: _ROUTER_OK
except NameError: _ROUTER_OK = False
try: _AUTOPILOT_OK
except NameError: _AUTOPILOT_OK = False
try: _WEB_LEARNER_OK
except NameError: _WEB_LEARNER_OK = False
try: _brain_router
except NameError:
    class _DummyRouter:
        def __getattr__(self, n): return lambda *a, **k: ""
    _brain_router = _DummyRouter()

# Groq токены — определяются в market.py, fallback на случай если не экспортировались
try: _GROQ_DAILY_LIMIT
except NameError: _GROQ_DAILY_LIMIT = 480_000
try: _groq_tokens_used
except NameError: _groq_tokens_used = 0

# ===== DATABASE HELPERS =====

def get_binance_klines(symbol, interval, limit=200):
    """Надежное получение свечей с Binance с retry логикой"""
    import requests
    import time
    
    binance_intervals = {
        "1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
        "1h": "1h", "4h": "4h", "1d": "1d"
    }
    binance_interval = binance_intervals.get(interval, "1h")
    
    for retry in range(3):
        try:
            r = requests.get(
                "https://api.binance.com/api/v3/klines",
                params={
                    "symbol": symbol,
                    "interval": binance_interval,
                    "limit": limit
                },
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10
            )
            data = r.json()
            if not isinstance(data, list):
                logging.warning(f"Binance invalid response type for {symbol}: {type(data)}")
                if retry < 2:
                    time.sleep(2)
                    continue
                return []
            if len(data) == 0:
                logging.warning(f"Binance empty candles for {symbol} (retry {retry+1})")
                if retry < 2:
                    time.sleep(2)
                    continue
                return []
            
            # Проверка на валидность данных свечей
            candles = []
            for c in data:
                try:
                    if len(c) >= 5:
                        candles.append({
                            "open": float(c[0]),
                            "high": float(c[1]),
                            "low": float(c[2]),
                            "close": float(c[3]),
                            "volume": float(c[4])
                        })
                except (ValueError, TypeError, IndexError) as candle_error:
                    logging.warning(f"Invalid candle data: {candle_error}")
                    continue
            
            if candles:
                return candles
            else:
                logging.warning(f"Binance no valid candles for {symbol}")
                return []
        except Exception as e:
            logging.warning(f"Binance klines error (retry {retry+1}): {e}")
            if retry < 2:
                time.sleep(2)
                continue
            return []
    
    logging.error(f"Binance klines failed after 3 retries for {symbol}")
    return []

# ===== KEYBOARDS =====

def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎯 Найти сделки", callback_data="menu_find_deals"),
         InlineKeyboardButton(text="📊 Рынок сейчас", callback_data="menu_market")],
        [InlineKeyboardButton(text="🔍 Сканировать рынок", callback_data="menu_scan"),
         InlineKeyboardButton(text="📈 Статистика", callback_data="menu_stats")],
        [InlineKeyboardButton(text="📰 Новости", callback_data="menu_news"),
         InlineKeyboardButton(text="📦 Накопления", callback_data="menu_pump")],
        [InlineKeyboardButton(text="🏆 Удачные сделки", callback_data="menu_wins"),
         InlineKeyboardButton(text="🔍 Ошибки бота", callback_data="menu_errors")],
        [InlineKeyboardButton(text="🧠 Мозг APEX", callback_data="menu_brain")]
    ])

def tf_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="5 мин", callback_data="tf_5m"),
         InlineKeyboardButton(text="15 мин", callback_data="tf_15m"),
         InlineKeyboardButton(text="1 час", callback_data="tf_1h")],
        [InlineKeyboardButton(text="4 часа", callback_data="tf_4h"),
         InlineKeyboardButton(text="1 день", callback_data="tf_1d")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
    ])

def pairs_keyboard(action="scan", page=0):
    """Клавиатура монет с пагинацией — топ-80"""
    all_pairs = get_top_pairs(60)
    page_size = 20  # монет на странице
    total_pages = (len(all_pairs) + page_size - 1) // page_size
    page = max(0, min(page, total_pages - 1))

    start = page * page_size
    page_pairs = all_pairs[start:start + page_size]

    buttons = []
    row = []
    for i, pair in enumerate(page_pairs):
        row.append(InlineKeyboardButton(
            text=pair.replace("USDT", ""),
            callback_data=f"{action}_{pair}"
        ))
        if len(row) == 4:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    # Навигация
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"pairs_{action}_{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"pairs_{action}_{page+1}"))
    if nav:
        buttons.append(nav)

    buttons.append([InlineKeyboardButton(text="🔙 Меню", callback_data="menu_back")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def backtest_tf_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="15 мин", callback_data="bt_15m"),
         InlineKeyboardButton(text="1 час", callback_data="bt_1h"),
         InlineKeyboardButton(text="4 часа", callback_data="bt_4h")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_backtest")]
    ])

def live_tf_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="15м — где мы?", callback_data="live_15m"),
         InlineKeyboardButton(text="1ч — где мы?",  callback_data="live_1h"),
         InlineKeyboardButton(text="4ч — где мы?",  callback_data="live_4h")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_backtest")]
    ])

# Хранилище состояний пользователей
user_states = {}

# ===== HANDLERS =====

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    user_id = message.from_user.id
    name = message.from_user.first_name or "трейдер"
    update_user_memory(user_id, name=name)
    mem = get_user_memory(user_id)
    greeting = f"С возвращением, {name}! 👊" if mem["messages"] > 1 else f"Привет, {name}!"
    await message.answer(
        f"⚡️ <b>APEX — AI трейдер по SMC</b>\n\n{greeting}\n\nВыбирай что нужно 👇",
        parse_mode="HTML",
        reply_markup=main_menu()
    )

@dp.message(Command("menu"))
async def cmd_menu(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Главное меню 👇", reply_markup=main_menu())

@dp.message(Command("scan"))
async def cmd_scan(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Выбери монету для скана:", reply_markup=pairs_keyboard("scan"))

@dp.message(Command("backtest"))
async def cmd_backtest(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    args = message.text.split()
    if len(args) == 3:
        symbol = args[1].upper()
        tf = args[2].lower()
        await run_backtest(message, symbol, tf)
    else:
        await message.answer(
            "Выбери таймфрейм для бектеста:\n(монета выбирается на следующем шаге)",
            reply_markup=backtest_tf_keyboard()
        )

@dp.message(Command("risk"))
async def cmd_risk(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    args = message.text.split()
    mem = get_user_memory(message.from_user.id)
    if len(args) == 2:
        try:
            deposit = float(args[1])
            update_user_memory(message.from_user.id, deposit=deposit)
            await message.answer(
                f"✅ Депозит сохранён: <b>${deposit:,.2f}</b>\n\n"
                f"Теперь при каждом сигнале я буду считать размер позиции.\n"
                f"Риск на сделку: {mem['risk']}%\n\n"
                f"Изменить риск: /setrisk 2",
                parse_mode="HTML"
            )
        except Exception as e:
            await message.answer(f"Ошибка депозита: {e}")
    else:
        deposit = mem["deposit"]
        if deposit > 0:
            await message.answer(
                f"💰 <b>Риск калькулятор</b>\n\n"
                f"Твой депозит: <b>${deposit:,.2f}</b>\n"
                f"Риск на сделку: <b>{mem['risk']}%</b>\n"
                f"Риск в $: <b>${deposit * mem['risk'] / 100:.2f}</b>\n\n"
                f"Изменить депозит: /risk 5000\n"
                f"Изменить риск %: /setrisk 2",
                parse_mode="HTML"
            )
        else:
            await message.answer(
                "💰 <b>Риск калькулятор</b>\n\nУкажи свой депозит:\n/risk 1000",
                parse_mode="HTML"
            )

@dp.message(Command("setrisk"))
async def cmd_setrisk(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    args = message.text.split()
    if len(args) == 2:
        try:
            risk = float(args[1])
            if 0.1 <= risk <= 10:
                update_user_memory(message.from_user.id, risk=risk)
                await message.answer(f"✅ Риск на сделку: <b>{risk}%</b>", parse_mode="HTML")
            else:
                await message.answer("Риск должен быть от 0.1% до 10%")
        except Exception as e:
            await message.answer(f"Ошибка риска: {e}")

@dp.message(Command("alert"))
async def cmd_alert(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    args = message.text.split()
    if len(args) == 3:
        symbol = args[1].upper()
        try:
            level = float(args[2])
            prices = get_live_prices()
            if not prices or not isinstance(prices, dict):
                await message.answer("Ошибка получения цен")
                return
            current = prices.get(symbol, {}).get("price", 0)
            direction = "above" if level > current else "below"
            
            # Retry логика для базы данных
            for retry in range(3):
                try:
                    conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
                    conn.execute("PRAGMA journal_mode=WAL")
                    conn.execute("PRAGMA busy_timeout=30000")
                    conn.execute(
                        "INSERT INTO alerts VALUES (NULL,?,?,?,?,0,CURRENT_TIMESTAMP)",
                        (message.from_user.id, symbol, level, direction)
                    )
                    conn.commit()
                    conn.close()
                    break
                except Exception as db_error:
                    if retry < 2:
                        logging.warning(f"DB retry {retry+1}: {db_error}")
                        await asyncio.sleep(1)
                        continue
                    else:
                        raise db_error
            arrow = "⬆️" if direction == "above" else "⬇️"
            await message.answer(
                f"🔔 Алерт установлен!\n{arrow} <b>{symbol}</b> → <code>{level}</code>\nТекущая цена: <code>{current:.4f}</code>",
                parse_mode="HTML"
            )
        except Exception as e:
            await message.answer(f"Ошибка алерта: {e}")
    else:
        await message.answer(
            "🔔 <b>Алерты на пробой уровня</b>\n\nКогда цена достигает твоего уровня — пишу сразу.\n\nУстановить: /alert BTCUSDT 70000",
            parse_mode="HTML"
        )

@dp.message(Command("journal"))
async def cmd_journal(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    args = message.text.split(maxsplit=1)
    user_id = message.from_user.id

    if len(args) == 1:
        # Показываем дневник
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
        rows = conn.execute(
            "SELECT symbol, direction, entry, exit_price, result, pnl_percent, note, created_at FROM journal WHERE user_id=? ORDER BY id DESC LIMIT 10",
            (user_id,)
        ).fetchall()
        conn.close()

        if not rows:
            await message.answer(
                "📓 <b>Дневник сделок</b>\n\nПусто. Добавь сделку:\n/journal BTC LONG 65000 67000 win\n\n"
                "Формат: /journal МОНЕТА НАПРАВЛЕНИЕ ВХОД ВЫХОД win/loss",
                parse_mode="HTML"
            )
            return

        total = len(rows)
        wins = sum(1 for r in rows if r[4] == "win")
        wr = round(wins / total * 100, 1) if total > 0 else 0

        text = f"📓 <b>Дневник сделок</b> (последние 10)\nWin Rate: {wr}%\n\n"
        for r in rows:
            emoji = "✅" if r[4] == "win" else "❌"
            text += f"{emoji} {r[0]} {r[1]}: {r[2]} → {r[3]} ({r[5]:+.1f}%)\n"

        # AI анализ ошибок
        if len(rows) >= 3:
            losses = [r for r in rows if r[4] == "loss"]
            if losses:
                loss_text = "\n".join([f"{r[0]} {r[1]} вход:{r[2]} выход:{r[3]}" for r in losses[:3]])
                analysis = ask_groq(
                    f"Проанализируй проигрышные сделки трейдера и дай 2-3 конкретных совета:\n{loss_text}",
                    max_tokens=300
                )
                if analysis:
                    text += f"\n🧠 <b>Анализ ошибок:</b>\n{analysis}"

        await message.answer(text, parse_mode="HTML")

    else:
        # Добавляем сделку
        try:
            parts = args[1].split()
            if len(parts) < 5:
                await message.answer("Использование: /journal BTC LONG 65000 68000 win взял на OB")
                return
            symbol = parts[0].upper()
            direction = parts[1].upper()
            entry = float(parts[2])
            exit_price = float(parts[3])
            result = parts[4].lower()
            note = " ".join(parts[5:]) if len(parts) > 5 else ""

            pnl = (exit_price - entry) / entry * 100
            if direction == "SHORT":
                pnl = -pnl

            conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
            conn.execute(
                "INSERT INTO journal VALUES (NULL,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)",
                (user_id, symbol, direction, entry, exit_price, result, note, round(pnl, 2))
            )
            conn.commit()
            conn.close()

            emoji = "✅" if result == "win" else "❌"
            await message.answer(
                f"{emoji} Сделка добавлена в дневник\n"
                f"{symbol} {direction}: {entry} → {exit_price} ({pnl:+.2f}%)",
                parse_mode="HTML"
            )
        except Exception as e:
            import logging
            logging.error(e)
            await message.answer(
                "Формат: /journal BTC LONG 65000 67000 win [заметка]\n"
                "Пример: /journal ETH SHORT 3200 3050 win взял на OB"
            )

@dp.message(Command("improve"))
async def cmd_improve(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    """
    /improve <запрос> — Groq пишет улучшение в groq_extensions.py и деплоит.
    Только для ADMIN_ID.
    """
    user_id = message.from_user.id
    if user_id not in ADMIN_IDS:
        await message.answer("⛔️ Только для администратора.")
        return

    text = message.text.replace("/improve", "").strip()
    if not text:
        await message.answer(
            "✏️ <b>Команда улучшения APEX</b>\n\n"
            "Напиши что изменить:\n"
            "<code>/improve добавь фильтр — не торговать XRP при объёме ниже среднего</code>\n"
            "<code>/improve убери фильтр мемкоинов</code>\n"
            "<code>/improve повысь порог confluence до 60 в боковике</code>\n\n"
            "Groq напишет код, протестирует и задеплоит автоматически.",
            parse_mode="HTML"
        )
        return

    await message.answer(f"🧠 <b>Groq анализирует запрос...</b>\n\n<i>{text}</i>", parse_mode="HTML")

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, _groq_write_extension, text, message)

    if result.get("success"):
        await message.answer(
            f"✅ <b>Улучшение применено!</b>\n\n"
            f"📝 <b>Что сделано:</b> {result['description']}\n"
            f"🔧 <b>Изменено:</b> {result['what_changed']}\n"
            f"🚀 Деплой на Render через ~2 минуты",
            parse_mode="HTML"
        )
    else:
        await message.answer(
            f"❌ <b>Ошибка:</b> {result.get('error', 'неизвестно')}\n\n"
            f"Попробуй переформулировать запрос.",
            parse_mode="HTML"
        )


def _groq_write_extension(user_request: str, message=None) -> dict:
    """
    Groq читает groq_extensions.py → понимает структуру →
    пишет изменение → проверяет синтаксис → пушит на GitHub.
    """
    try:
        import base64, ast

        # 1. Читаем текущий groq_extensions.py из GitHub
        if not GITHUB_TOKEN or not GITHUB_REPO:
            return {"success": False, "error": "GitHub не настроен"}

        r = requests.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/contents/groq_extensions.py",
            headers={"Authorization": f"token {GITHUB_TOKEN}",
                     "Accept": "application/vnd.github.v3+json"},
            timeout=15
        )
        if r.status_code != 200:
            return {"success": False, "error": f"GitHub read error: {r.status_code}"}

        data = r.json()
        current_code = base64.b64decode(data["content"]).decode("utf-8")
        sha = data["sha"]

        # 2. Groq анализирует запрос и пишет изменение
        prompt = f"""Ты — AI разработчик торгового бота APEX. Тебе нужно изменить файл groq_extensions.py.

ЗАПРОС ПОЛЬЗОВАТЕЛЯ: {user_request}

ТЕКУЩИЙ КОД groq_extensions.py:
```python
{current_code[:4000]}
```

ПРАВИЛА:
1. Верни ТОЛЬКО полный обновлённый Python файл — без markdown, без объяснений
2. Сохрани всю существующую структуру и функции
3. Добавь запись в GROQ_CHANGELOG с датой {datetime.now().strftime('%Y-%m-%d')}, version увеличь на 0.0.1, author="Groq"
4. Если добавляешь новый фильтр — добавь его функцию И добавь в список ACTIVE_FILTERS
5. Если добавляешь новый буст — добавь функцию И в CONFLUENCE_BOOSTERS
6. Если удаляешь — убери из списка (функцию можно оставить закомментированной)
7. Код должен быть рабочим Python 3.11
8. description_of_change: первая строка комментария = краткое описание что сделал

ВАЖНО: верни только Python код, начиная с первой строки файла."""

        new_code = ask_groq(prompt, max_tokens=3000)
        if not new_code:
            return {"success": False, "error": "Groq не ответил"}

        # Убираем markdown если Groq добавил
        new_code = new_code.strip()
        if new_code.startswith("```python"):
            new_code = new_code[9:]
        if new_code.startswith("```"):
            new_code = new_code[3:]
        if new_code.endswith("```"):
            new_code = new_code[:-3]
        new_code = new_code.strip()

        # 3. Проверяем синтаксис — если сломан, не деплоим
        try:
            ast.parse(new_code)
        except SyntaxError as se:
            return {"success": False, "error": f"Синтаксическая ошибка в коде Groq: {se}"}

        # 4. Извлекаем описание из changelog
        description = user_request[:80]
        what_changed = "groq_extensions.py"
        try:
            # Ищем последнюю запись changelog в новом коде
            for line in new_code.split("\n"):
                if '"changes":' in line and "Groq" not in line.split('"changes":')[0]:
                    description = line.split('"changes":')[1].strip().strip('"').strip("'").rstrip('",')
                    break
        except Exception as e:
            import logging
            logging.error(e)
            pass

        # 5. Пушим на GitHub
        encoded = base64.b64encode(new_code.encode("utf-8")).decode("utf-8")
        r2 = requests.put(
            f"https://api.github.com/repos/{GITHUB_REPO}/contents/groq_extensions.py",
            headers={"Authorization": f"token {GITHUB_TOKEN}",
                     "Accept": "application/vnd.github.v3+json"},
            json={
                "message": f"🧠 Groq extension: {user_request[:60]}",
                "content": encoded,
                "sha": sha
            },
            timeout=20
        )

        if r2.status_code in (200, 201):
            logging.info(f"[Extensions] ✅ Groq задеплоил изменение: {user_request[:60]}")
            return {
                "success": True,
                "description": description,
                "what_changed": what_changed,
                "commit": r2.json().get("commit", {}).get("sha", "")[:7]
            }
        else:
            return {"success": False, "error": f"GitHub push error: {r2.status_code}"}

    except Exception as e:
        logging.error(f"_groq_write_extension: {e}")
        return {"success": False, "error": str(e)}


@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    user_id = message.from_user.id
    mem = get_user_memory(user_id)
    try:
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
        total = (conn.execute("SELECT COUNT(*) FROM signals").fetchone() or [0])[0]
        wins = (conn.execute("SELECT COUNT(*) FROM signals WHERE result LIKE 'tp%'").fetchone() or [0])[0]
        losses = (conn.execute("SELECT COUNT(*) FROM signals WHERE result='sl'").fetchone() or [0])[0]
        pending = (conn.execute("SELECT COUNT(*) FROM signals WHERE result='pending'").fetchone() or [0])[0]
        top = conn.execute(
            "SELECT symbol, win_rate, total, avg_hours_to_tp FROM signal_learning ORDER BY win_rate DESC LIMIT 5"
        ).fetchall()
        # Статистика по стратегиям
        strategy_rows = conn.execute(
            "SELECT signal_type, COUNT(*), SUM(CASE WHEN result LIKE 'tp%' THEN 1 ELSE 0 END) FROM signals WHERE signal_type IS NOT NULL GROUP BY signal_type"
        ).fetchall()
        conn.close()
    except:
        total = wins = losses = pending = 0
        top = []
        strategy_rows = []

    wr = round(wins / total * 100, 1) if total > 0 else 0
    top_text = "\n".join([f"• {r[0]}: {r[1]:.0f}% WR, avg {r[3]:.0f}ч ({r[2]} сигн.)" for r in top]) or "Нет данных"

    # Формируем текст по стратегиям
    strategy_icons = {"MTF": "📐", "SWING": "🔄", "WYCKOFF": "🌊", "FAST": "⚡", "ZONE": "📦"}
    strategy_lines = []
    for stype, s_total, s_wins in strategy_rows:
        s_wr = round(s_wins / s_total * 100, 1) if s_total > 0 else 0
        icon = strategy_icons.get((stype or "").upper(), "📊")
        strategy_lines.append(f"{icon} {stype}: {s_total} сигн. | WR {s_wr}%")
    strategy_text = "\n".join(strategy_lines) or "Нет данных"

    profile_text = ""
    if mem["profile"]:
        profile_text = f"\n\n👤 <b>Что я о тебе знаю:</b>\n{mem['profile']}"
        if mem["coins"]:
            profile_text += f"\n💎 Монеты: {mem['coins']}"
        if mem["deposit"] > 0:
            profile_text += f"\n💰 Депозит: ${mem['deposit']:,.0f} | Риск: {mem['risk']}%"

    await message.answer(
        f"📈 <b>Статистика APEX</b>\n\n"
        f"Сигналов: {total} | ✅ {wins} | ❌ {losses} | ⏳ {pending}\n"
        f"🎯 Win Rate: <b>{wr}%</b>\n\n"
        f"📊 <b>По стратегиям:</b>\n{strategy_text}\n\n"
        f"🏆 <b>Топ монеты:</b>\n{top_text}"
        f"{profile_text}",
        parse_mode="HTML"
    )

@dp.message(Command("news"))
async def cmd_news(message: types.Message):
    await message.answer("📰 Собираю свежие новости...")
    now_str = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    crypto_news = await asyncio.get_running_loop().run_in_executor(None, get_crypto_news)
    macro_news = await asyncio.get_running_loop().run_in_executor(None, get_market_impact_news)
    crypto_text = format_news(crypto_news[:5])
    macro_text = format_news(macro_news[:3])
    all_titles = "\n".join([item["title"] for item in (crypto_news + macro_news)[:10]])
    analysis = ask_groq(
        f"Оцени эти новости для трейдера — что важно прямо сейчас? (3 пункта кратко):\n{all_titles}",
        max_tokens=250
    )
    save_news("crypto news", all_titles[:500])
    msg = (
        f"📰 <b>Новости крипторынка</b>\n"
        f"🕐 {now_str}\n{'━'*24}\n\n"
        f"<b>🔥 Крипто:</b>\n{crypto_text}\n\n"
        f"<b>🌍 Макро:</b>\n{macro_text}\n\n"
        f"<b>⚡️ APEX:</b>\n{analysis or 'Анализирую...'}"
    )
    await message.answer(msg[:4000], parse_mode="HTML")

async def run_backtest(target, symbol, timeframe):
    """Запуск бектеста"""
    send = target.message.answer if hasattr(target, "message") else target.answer
    await send(f"🔬 Запускаю бектест {symbol} {TF_LABELS.get(timeframe, timeframe)}...")
    result = backtest(symbol, timeframe)
    if not result:
        await send("Недостаточно данных для бектеста")
        return

    grade = "🔥 Отличная" if result["win_rate"] >= 60 else "✅ Рабочая" if result["win_rate"] >= 50 else "⚠️ Слабая"
    await send(
        f"🔬 <b>Бектест {symbol} [{TF_LABELS.get(timeframe, timeframe)}]</b>\n\n"
        f"Сигналов: {result['total']}\n"
        f"✅ Выигрыши: {result['wins']}\n"
        f"❌ Проигрыши: {result['losses']}\n"
        f"🎯 Win Rate: <b>{result['win_rate']}%</b>\n"
        f"Оценка: {grade}\n\n"
        f"_На основе {result['periods']} свечей_",
        parse_mode="HTML"
    )

def scan_diagnostics(symbol):
    """Объясняет почему нет сигнала — что именно не прошло"""
    try:
        lines = [f"😴 <b>{symbol} — сигнал не найден</b>\n"]

        candles = get_candles(symbol, "1h", 150)
        if not candles or len(candles) < 20:
            lines.append("⚠️ Данные временно недоступны (CoinGecko rate limit)")
            lines.append("\n<i>Подожди 30 секунд и попробуй снова</i>")
            return "\n".join(lines)

        price = candles[-1]["close"]
        ps = f"${price:,.4f}" if price < 1 else f"${price:,.2f}"
        lines.append(f"💰 Цена: <code>{ps}</code>\n")

        results = {}
        for tf in ["15m", "1h", "4h"]:
            d = smc_on_tf(symbol, tf)
            results[tf] = d
            icon = "🟢" if d == "BULLISH" else "🔴" if d == "BEARISH" else "⚪️"
            lines.append(f"{icon} {TF_LABELS.get(tf, tf)}: {d or 'нет структуры'}")

        bullish = [tf for tf, d in results.items() if d == "BULLISH"]
        bearish = [tf for tf, d in results.items() if d == "BEARISH"]

        if not bullish and not bearish:
            lines.append("\n⚠️ SMC структура не определена — рынок в боковике")
        elif len(bullish) == len(bearish):
            lines.append("\n⚠️ Таймфреймы конфликтуют — нет чёткого направления")
        else:
            direction = "BULLISH" if len(bullish) > len(bearish) else "BEARISH"
            lines.append(f"\n{'🟢' if direction == 'BULLISH' else '🔴'} Направление: {direction}")
            ob = find_ob(candles, direction)
            fvg = find_fvg(candles, direction)
            lines.append(f"{'✅' if ob else '❌'} Order Block: {'найден' if ob else 'не найден'}")
            lines.append(f"{'✅' if fvg else '❌'} FVG: {'найден' if fvg else 'не найден'}")
            regime = get_market_regime(symbol)
            if not isinstance(regime, dict):
                regime = {"mode": str(regime) if regime else "UNKNOWN", "direction": "NONE", "confidence": 0}
            lines.append(f"🧠 Режим: {regime['mode']} (уверенность {regime['confidence']}%)")
            if regime["mode"] == "SIDEWAYS" and regime["confidence"] > 85:
                lines.append("⛔️ Заблокировано: рынок в глубоком боковике")
            lines.append(f"\n📊 Confluence набрал меньше 25 очков — сигнал слабый")

        lines.append("\n<i>Попробуй через 15-30 мин или выбери другую монету</i>")
        return "\n".join(lines)

    except Exception as e:
        return f"😴 {symbol}\n⚠️ Временная ошибка: {e}\n\n<i>Попробуй снова через минуту</i>"

# ===== CALLBACK HANDLERS =====

@dp.callback_query()
async def handle_callback(callback: CallbackQuery):
    data = callback.data
    user_id = callback.from_user.id
    # Обновляем флаги и функции из market модуля напрямую
    global _ROUTER_OK, _LEARNING_OK, _AUTOPILOT_OK, _WEB_LEARNER_OK
    global _learn_grade_text, _learn_trade_analysis, _learn_self_diag
    global _learn_latest_diag, _learn_get_strategy, _learn_build_strategy
    global _brain_router, _autopilot_status
    import market as _market_module
    _ROUTER_OK = getattr(_market_module, '_ROUTER_OK', False)
    _LEARNING_OK = getattr(_market_module, '_LEARNING_OK', False)
    _AUTOPILOT_OK = getattr(_market_module, '_AUTOPILOT_OK', False)
    _WEB_LEARNER_OK = getattr(_market_module, '_WEB_LEARNER_OK', False)
    if _LEARNING_OK:
        _learn_grade_text = getattr(_market_module, '_learn_grade_text', lambda: "")
        _learn_trade_analysis = getattr(_market_module, '_learn_trade_analysis', lambda n=5: "")
        _learn_self_diag = getattr(_market_module, '_learn_self_diag', lambda: "")
        _learn_latest_diag = getattr(_market_module, '_learn_latest_diag', lambda: "")
        _learn_get_strategy = getattr(_market_module, '_learn_get_strategy', lambda: "")
        _learn_build_strategy = getattr(_market_module, '_learn_build_strategy', lambda: "")
    if _ROUTER_OK:
        _brain_router = getattr(_market_module, '_brain_router', _brain_router)
    if _AUTOPILOT_OK:
        _autopilot_status = getattr(_market_module, '_autopilot_status', lambda: "")
    # Обновляем EXT флаг
    global _EXT_OK, _ext_summary, _ext_session
    _EXT_OK = getattr(_market_module, '_EXT_OK', False)
    if _EXT_OK:
        _ext_summary = getattr(_market_module, '_ext_summary', lambda: {})
        _ext_session = getattr(_market_module, '_ext_session', lambda: {})
    # Обновляем WEB LEARNER функции
    global _web_knowledge_summary, _web_learn_cycle, _web_groq_agenda, _web_self_improve
    _WEB_LEARNER_OK = getattr(_market_module, '_WEB_LEARNER_OK', False)
    if _WEB_LEARNER_OK:
        _web_knowledge_summary = getattr(_market_module, '_web_knowledge_summary', lambda: "")
        _web_learn_cycle = getattr(_market_module, '_web_learn_cycle', lambda: [])
        _web_groq_agenda = getattr(_market_module, '_web_groq_agenda', lambda: [])
        _web_self_improve = getattr(_market_module, '_web_self_improve', lambda: [])
    try:
        await callback.answer()
    except Exception:
        pass

    if data == "menu_back":
        await callback.message.edit_text("Главное меню 👇", reply_markup=main_menu())

    elif data == "menu_scan":
        try:
            await callback.message.edit_text(
                "🔍 <b>Выбери монету</b> (топ-60 по объёму):",
                parse_mode="HTML",
                reply_markup=pairs_keyboard("scan", 0)
            )
        except Exception:
            await callback.message.answer(
                "🔍 <b>Выбери монету</b> (топ-60 по объёму):",
                parse_mode="HTML",
                reply_markup=pairs_keyboard("scan", 0)
            )

    elif data.startswith("pairs_"):
        # Пагинация: pairs_scan_0, pairs_scan_1 ...
        parts = data.split("_")
        action = parts[1]
        page = int(parts[2]) if len(parts) > 2 else 0
        try:
            await callback.message.edit_reply_markup(reply_markup=pairs_keyboard(action, page))
        except Exception as e:
            import logging
            logging.error(e)
            pass

    elif data == "noop":
        pass  # Кнопка номера страницы — ничего не делаем

    elif data.startswith("patch_apply_"):
        patch_id = data.replace("patch_apply_", "")
        await callback.message.edit_text("⏳ Применяю патч и пушу в GitHub...")
        success, result = await apply_patch(patch_id)
        if success:
            ok_text = "✅ <b>Патч применён!</b>\n\n" + "Commit: <code>" + str(result) + "</code>\n" + "Render сейчас задеплоит новую версию автоматически.\n\n⏳ 1-3 мин."
            await callback.message.edit_text(ok_text, parse_mode="HTML")
        else:
            err_text = "❌ <b>Ошибка при пуше в GitHub:</b>\n<code>" + str(result) + "</code>"
            await callback.message.edit_text(err_text, parse_mode="HTML")

    elif data.startswith("patch_cancel_"):
        patch_id = data.replace("patch_cancel_", "")
        if patch_id in pending_patches:
            del pending_patches[patch_id]
        await callback.message.edit_text("❌ Патч отменён. Код не изменён.")

    elif data == "menu_market":
        await callback.message.edit_text("📊 Собираю данные рынка...")
        fg = get_fear_greed()
        dxy = get_dxy_signal()
        regime_btc = get_market_regime("BTCUSDT")
        econ = get_upcoming_events()

        # Блок настроения
        sentiment_block = ""
        if fg:
            fg_bar = "█" * (fg["value"] // 10) + "░" * (10 - fg["value"] // 10)
            fg_emoji = "😱" if fg["value"] < 25 else "😨" if fg["value"] < 45 else "😐" if fg["value"] < 55 else "😊" if fg["value"] < 75 else "🤑"
            sentiment_block += f"{fg_emoji} <b>Fear & Greed:</b> {fg['value']} [{fg_bar}] {fg['label']}\n"

        if dxy:
            dxy_emoji = "📈" if dxy["signal"] == "STRONG" else "📉" if dxy["signal"] == "WEAK" else "➡️"
            warn = " ⚠️ давит на крипту" if dxy["signal"] == "STRONG" else " ✅ хорошо для крипты" if dxy["signal"] == "WEAK" else ""
            sentiment_block += f"{dxy_emoji} <b>DXY:</b> {dxy['value']} ({dxy['change']:+.2f}%){warn}\n"

        if regime_btc:
            regime_emoji = "🔥" if regime_btc["mode"] == "TRENDING" else "😴" if regime_btc["mode"] == "SIDEWAYS" else "⚡️"
            sentiment_block += f"{regime_emoji} <b>Режим BTC:</b> {regime_btc['mode']} {regime_btc['direction']}\n"

        if econ:
            sentiment_block += f"\n⚠️ <b>Макро:</b> {econ}\n"

        # Тепловая карта накоплений топ-монет
        accum_block = ""
        try:
            top_syms = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "TONUSDT", "AVAXUSDT", "LINKUSDT"]
            accum_lines = []
            for sym in top_syms:
                acc = detect_accumulation(sym)
                if acc and acc.get("score", 0) >= 50:
                    score = acc["score"]
                    bar_len = min(10, score // 10)
                    bar = "█" * bar_len + "░" * (10 - bar_len)
                    phase = acc.get("phase", "")
                    emoji = "🔥" if score >= 70 else "🟡"
                    accum_lines.append(f"{emoji} <b>{sym.replace('USDT','')}</b> [{bar}] {score}/100 {phase}")
            if accum_lines:
                accum_block = "\n🗺 <b>Тепловая карта накоплений:</b>\n" + "\n".join(accum_lines[:5]) + "\n"
        except Exception as _e:
            import logging
            logging.error(_e)

        # Крупная ликвидность — зоны перед пампом
        liq_block = ""
        try:
            liq_lines = []
            for sym in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
                candles = get_candles(sym, "4h", 100)
                if candles and len(candles) > 20:
                    highs = [c["high"] for c in candles[-50:]]
                    lows = [c["low"] for c in candles[-50:]]
                    vols = [c.get("volume", 0) for c in candles[-50:]]
                    avg_vol = sum(vols) / len(vols) if vols else 0
                    # Свечи с аномальным объёмом — кит
                    whale_candles = [(c, v) for c, v in zip(candles[-10:], vols[-10:]) if avg_vol > 0 and v > avg_vol * 2]
                    if whale_candles:
                        last_whale = whale_candles[-1]
                        direction_whale = "🟢 Накопление" if last_whale[0]["close"] > last_whale[0]["open"] else "🔴 Сброс"
                        liq_lines.append(f"🐋 <b>{sym.replace('USDT','')}</b>: {direction_whale} (объём ×{last_whale[1]/avg_vol:.1f})")
            if liq_lines:
                liq_block = "\n🐋 <b>Крупная ликвидность (4h):</b>\n" + "\n".join(liq_lines) + "\n"
        except Exception as _e:
            import logging
            logging.error(_e)

        # Groq анализ рынка с учётом накоплений
        comment = ask_groq(
            f"3 предложения по рынку для трейдера. F&G:{fg}, DXY:{dxy}, BTC режим:{regime_btc}. "
            f"Учти накопления и ликвидность. Дай конкретный совет — что делать сейчас.",
            max_tokens=150
        )

        await callback.message.edit_text(
            f"📊 <b>Рынок сейчас</b>\n{'━'*24}\n\n"
            f"{sentiment_block}"
            f"{accum_block}"
            f"{liq_block}"
            f"\n💬 <i>{comment or ''}</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data="menu_market"),
                 InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
            ])
        )

    elif data == "menu_tf":
        await callback.message.edit_text(
            "⏱ <b>Выбери таймфрейм для анализа</b>\n\nПосле выбора бот просканирует все монеты на этом ТФ:",
            parse_mode="HTML", reply_markup=tf_keyboard()
        )

    elif data.startswith("tf_"):
        tf = data.replace("tf_", "")
        pairs = get_top_pairs(60)
        await callback.message.edit_text(
            f"🔍 Сканирую топ-60 на {TF_LABELS.get(tf, tf)}...\n⏳ ~20 сек"
        )
        signals = []
        for symbol in pairs:
            try:
                sig = await asyncio.get_running_loop().run_in_executor(
                    None, full_scan_raw, symbol, tf
                )
                if sig:
                    signals.append(sig)
                await asyncio.sleep(0.1)
            except Exception as e:
                import logging
                logging.error(e)
                pass

        if not signals:
            text = f"😴 На {TF_LABELS.get(tf, tf)} чётких сетапов нет.\nПопробуй другой таймфрейм."
            await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_tf")]
            ]))
            return

        # Сортируем: МЕГА ТОП первые
        grade_order = {"МЕГА ТОП": 0, "ТОП СДЕЛКА": 1, "ХОРОШАЯ": 2}
        signals.sort(key=lambda x: grade_order.get(x.get("grade", ""), 3))

        # Отправляем первый сигнал в текущее сообщение
        top = signals[0]
        direction = top.get("direction", "")
        emoji = "🟢" if direction == "BULLISH" else "🔴"

        # Показываем краткую сводку всех + полный топ сигнал
        summary_lines = []
        for s in signals[:8]:
            d = s.get("direction", "")
            ic = "🟢" if d == "BULLISH" else "🔴"
            grade_short = s.get("grade", "")
            fire = "🔥🔥🔥" if grade_short == "МЕГА ТОП" else "🔥🔥" if grade_short == "ТОП СДЕЛКА" else "✅"
            summary_lines.append(f"{fire} {ic} {s['symbol'].replace('USDT','')} — {d}")

        summary = "\n".join(summary_lines)
        header = (
            f"⏱ <b>Скан {TF_LABELS.get(tf, tf)}</b> | найдено: {len(signals)}\n"
            f"{'━'*22}\n\n"
            f"{summary}\n\n"
            f"{'━'*22}\n"
            f"<b>Лучший сигнал:</b>\n\n"
            + top["text"]
        )

        if len(header) > 4000:
            header = header[:3990] + "..."

        await callback.message.edit_text(
            header,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data=data)],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_tf")]
            ])
        )

    elif data.startswith("scan_"):
        symbol = data.replace("scan_", "")
        await callback.message.edit_text(f"🔍 Анализирую {symbol}...")
        sig = full_scan(symbol)

        mem = get_user_memory(user_id)
        risk_text = ""
        if mem["deposit"] > 0 and sig:
            prices = get_live_prices()
            if symbol in prices:
                price = prices[symbol]["price"]
                sl_price = price * 0.985
                rc = calc_risk(mem["deposit"], mem["risk"], price, sl_price)
                if rc:
                    risk_text = (
                        f"\n\n💰 <b>Риск-менеджмент:</b>\n"
                        f"Риск в $: <b>${rc['risk_amount']}</b>\n"
                        f"Размер позиции: <b>${rc['position_size']:.0f}</b>\n"
                        f"Рекомендуемое плечо: <b>x{rc['leverage']}</b>"
                    )

        if sig:
            await callback.message.edit_text(
                sig + risk_text,
                parse_mode="HTML",
       …37257 tokens truncated…nt',0)}/3 | 1d: {htf_1d} | 1w: {htf_1w} {_1w_warn}\n"
                f"RR: {levels.get('rr',0)} | Стоп: {_sl_pct_mtf}% | Fear&Greed: {fg_val} | Funding: {fund_val}\n"
                f"Режим: {regime_val} | {_btc_str}\n"
                f"{_ob_str} | {_fvg_str} | ATR: {smart_price_fmt(_atr_mtf)}\n"
                f"{_vol_str}\n"
                f"Confluence:\n{conf_short}"
                f"{_pat_str}"
                f"{_self_rules}"
            )
            groq_response = ask_groq(groq_prompt, max_tokens=100)
            if groq_response and len(groq_response) > 5:
                try:
                    import json as _json, re as _re
                    clean = groq_response.strip().replace("```json", "").replace("```", "").strip()
                    json_match = _re.search(r'\{[^}]+\}', clean, _re.DOTALL)
                    if json_match:
                        clean = json_match.group()
                    parsed = _json.loads(clean)
                    # Groq как фильтр — если valid=false, блокируем
                    if not parsed.get("valid", True):
                        logging.info(f"[MTF Groq] {symbol} {direction}: Groq отклонил сигнал")
                        return None
                    if parsed.get("logic") and len(str(parsed["logic"])) > 5:
                        raw_logic = str(parsed["logic"]).strip()
                        # Убираем JSON артефакты если Groq вернул сырой JSON
                        if raw_logic.startswith("{") or '"logic"' in raw_logic:
                            import re as _re2
                            m = _re2.search(r'"logic"\s*:\s*"([^"]+)"', raw_logic)
                            groq_logic = m.group(1) if m else raw_logic[:100]
                        else:
                            groq_logic = raw_logic
                    if parsed.get("hours"):
                        hrs = int(parsed["hours"])
                        groq_time = f"~{hrs}ч" if hrs < 24 else f"~{hrs//24}дн"
                except Exception:
                    clean_text = groq_response.strip().replace("\n", " ")
                    if len(clean_text) > 10 and not clean_text.upper() == clean_text:
                        groq_logic = clean_text[:80]
        except Exception:
            pass

        # Fallback если Groq не ответил
        if not groq_logic:
            logic_lines = [c for c in confluence if any(w in c.lower() for w in
                ["свип", "sweep", "импульс", "накопл", "ликвидн", "пробой", "ob ", "fvg"])]
            groq_logic = "\n".join(logic_lines[:3]) if logic_lines else "структурный вход по SMC"

        # ══════════════════════════════════════
        # 🟢 ДОП — score минимум 1/4
        # ══════════════════════════════════════
        _mtf_score = 0
        _mtf_score_vol = False
        _mtf_score_btc = False
        _mtf_score_session = False
        _mtf_score_bos = False

        # Volume spike ≥1.2x
        try:
            _avg_vol_m = sum(c["volume"] for c in candles[-20:-1]) / 19
            if _avg_vol_m > 0 and candles[-1]["volume"] > _avg_vol_m * 1.2:
                _mtf_score += 1
                _mtf_score_vol = True
        except Exception:
            pass

        # BTC совпадает
        try:
            _btc_m = get_candles("BTCUSDT", "1h", 5)
            if _btc_m and len(_btc_m) >= 3:
                _btc_dir_m = "BULLISH" if _btc_m[-1]["close"] > _btc_m[-3]["close"] else "BEARISH"
                if _btc_dir_m == direction:
                    _mtf_score += 1
                    _mtf_score_btc = True
        except Exception:
            pass

        # Активная сессия (London/NY)
        import datetime as _dt_m
        _h_m = _dt_m.datetime.utcnow().hour
        if 8 <= _h_m <= 17:
            _mtf_score += 1
            _mtf_score_session = True

        # BOS/CHoCH на 15m
        try:
            _c15m_m = get_candles(symbol, "15m", 30)
            if _c15m_m and detect_bos_choch(_c15m_m, direction, lookback=15):
                _mtf_score += 1
                _mtf_score_bos = True
        except Exception:
            pass

        # Для редких MTF-сетапов нужны минимум 3 из 4 подтверждений,
        # включая реальную структуру (BOS/CHoCH), а не только сессию.
        if _mtf_score < 3 or not _mtf_score_bos:
            logging.debug(f"[MTF] {symbol}: score {_mtf_score}/4 — пропускаем")
            return None

        _signal_strength = "🔥 Сильный" if _mtf_score >= 3 else "✅ Норм" if _mtf_score >= 2 else "⚡ Базовый"

        # ══════════════════════════════════════
        # 📝 ТЕКСТ СИГНАЛА
        # ══════════════════════════════════════
        _must_text = f"15m+1h+4h {direction} | {_tf_match}/3 ТФ ✅"
        _1d_text = f"1d: {'✅' if _htf_1d_agrees else '⚠️'} {_dir_1d or '?'} (контекст)"

        _confirm_items = []
        if _mtf_score_vol:     _confirm_items.append("📊 Объём")
        if _mtf_score_btc:     _confirm_items.append("₿ BTC")
        if _mtf_score_session: _confirm_items.append("⏰ Сессия")
        if _mtf_score_bos:     _confirm_items.append("🔄 BOS")
        _confirm_text = " | ".join(_confirm_items) if _confirm_items else "—"

        _sl_pct_txt = round(abs(entry - sl) / entry * 100, 2) if entry > 0 else 0
        _dir_emoji = "🟢 LONG" if direction == "BULLISH" else "🔴 SHORT"

        text = (
            f"📐 <b>MTF</b> | {symbol} — {_dir_emoji}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📌 Основа: {_must_text}\n"
            f"📋 {_1d_text}\n"
            f"✅ Доп: {_mtf_score}/4 — {_confirm_text}\n"
            f"💪 Сила: {_signal_strength}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"🎯 Вход:  <code>{smart_price_fmt(entry)}</code>\n"
            f"🛑 Стоп:  <code>{smart_price_fmt(sl)}</code>  ({_sl_pct_txt}%)\n"
            f"🎯 TP1:   <code>{smart_price_fmt(tp1)}</code>\n"
            f"🎯 TP2:   <code>{smart_price_fmt(tp2)}</code>\n"
            f"📊 RR:    {levels.get('rr', 0)}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📈 Логика: {groq_logic}\n"
            f"⏱ Горизонт: {groq_time}"
        )
        text += "\n\n💡 Это аналитика, не совет. Торгуй осознанно"

        # Сохраняем в БД
        if auto:
            save_signal_db(symbol, direction, "MTF", entry, tp1, tp2, tp3, sl, timeframe, est_hours, mtf["grade"],
                           confluence=conf_score, regime="UNKNOWN")

        return {"symbol": symbol, "grade": sig_name, "grade_emoji": sig_emoji, "text": text, "direction": direction, "entry": entry, "tp1": tp1, "tp2": tp2, "tp3": tp3, "sl": sl, "rr": _rr_val, "timeframe": timeframe, "confluence_score": conf_score, "regime": "UNKNOWN"}

    except Exception as e:
        logging.error(f"full_scan_raw error {symbol}: {e}")
        return None


# ===== АВТО-ПАТЧ GITHUB =====
# Бот сам чинит код: ловит ошибку → анализирует → спрашивает разрешения → пушит коммит

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO = os.environ.get("GITHUB_REPO", "")   # например: vladislavdim/apex-smc-bot
GITHUB_FILE = os.environ.get("GITHUB_FILE", "bot.py")

# Очередь ожидающих патчей: patch_id -> {code, description, error}
pending_patches = {}
patch_counter = 0

# Кэш отправленных сигналов — symbol:direction -> timestamp (не спамим одним сигналом)
_sent_signal_cache: dict = {}
_SIGNAL_COOLDOWN_HOURS = 4  # один и тот же сигнал не чаще раз в 4 часа

def github_get_file():
    """Читаем текущий bot.py прямо из GitHub"""
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return None, None
    try:
        r = requests.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}",
            headers={
                "Authorization": f"token {GITHUB_TOKEN}",
                "Accept": "application/vnd.github.v3+json"
            },
            timeout=15
        )
        data = r.json()
        if "content" in data:
            import base64
            code = base64.b64decode(data["content"]).decode("utf-8")
            sha = data["sha"]
            return code, sha
        return None, None
    except Exception as e:
        logging.error(f"GitHub get file: {e}")
        return None, None


def github_push_patch(new_code, sha, commit_message):
    """Пушим исправленный код в GitHub"""
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return False, "GITHUB_TOKEN или GITHUB_REPO не заданы"
    try:
        import base64
        encoded = base64.b64encode(new_code.encode("utf-8")).decode("utf-8")
        r = requests.put(
            f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}",
            headers={
                "Authorization": f"token {GITHUB_TOKEN}",
                "Accept": "application/vnd.github.v3+json"
            },
            json={
                "message": commit_message,
                "content": encoded,
                "sha": sha
            },
            timeout=20
        )
        if r.status_code in (200, 201):
            return True, r.json().get("commit", {}).get("sha", "")[:7]
        return False, f"GitHub API error: {r.status_code} — {r.text[:200]}"
    except Exception as e:
        return False, str(e)


async def analyze_and_patch(error_text, error_source="runtime"):
    """
    Анализирует ошибку и записывает вывод в мозги бота.
    Авто-деплой ОТКЛЮЧЁН — только обучение.
    """
    try:
        prompt = f"""Ты senior Python разработчик. В боте произошла ошибка.

ОШИБКА:
{error_text[:600]}

Кратко (1-2 предложения): что пошло не так и как это можно исправить вручную?"""

        response = ask_groq(prompt, max_tokens=300)
        if not response:
            return

        # Записываем в мозги как наблюдение
        try:
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute(
                    "INSERT OR IGNORE INTO observations (category, content, source, created_at) VALUES (?,?,?,?)",
                    ("error_analysis", f"[{error_source}] {error_text[:200]}\n→ {response}", "auto_analyze", datetime.now().isoformat())
                )
                conn.commit()
            logging.info(f"analyze_and_patch: ошибка записана в мозги ({error_source})")
        except Exception as db_e:
            logging.error(f"analyze_and_patch DB: {db_e}")

    except Exception as e:
        logging.error(f"analyze_and_patch error: {e}")

async def apply_patch(patch_id):
    """Применяем патч — пушим в GitHub"""
    patch = pending_patches.get(patch_id)
    if not patch:
        return False, "Патч не найден или устарел"

    success, result = github_push_patch(
        patch["new_code"],
        patch["sha"],
        f"🤖 APEX auto-fix: {patch['description'][:60]}"
    )

    del pending_patches[patch_id]
    return success, result


# Обработчик глобальных ошибок — ловим всё что падает в боте
last_error_time = {}
error_cooldown = 300  # 5 минут между одинаковыми ошибками

# ─── Буфер логов для Groq-анализа ───────────────────────────────────────────
_log_buffer = []          # последние N строк логов
_log_buffer_max = 200     # размер буфера

class LogBufferHandler(logging.Handler):
    """Записывает все WARNING/ERROR логи в буфер для Groq-анализа"""
    def emit(self, record):
        if record.levelno >= logging.WARNING:
            msg = self.format(record)
            _log_buffer.append(msg)
            if len(_log_buffer) > _log_buffer_max:
                _log_buffer.pop(0)

def get_recent_errors(limit=30) -> list:
    """Возвращает последние ошибки из буфера"""
    errors = [l for l in _log_buffer if "ERROR" in l or "WARNING" in l]
    return errors[-limit:]

def get_candle_failures() -> dict:
    """Считает сколько раз каждая монета/интервал не получила свечи"""
    failures = {}
    for line in _log_buffer:
        if "нет свечей для" in line.lower():
            parts = line.lower().split("нет свечей для ")
            if len(parts) > 1:
                key = parts[1].strip()[:20]
                failures[key] = failures.get(key, 0) + 1
    return dict(sorted(failures.items(), key=lambda x: x[1], reverse=True)[:10])

async def groq_analyze_logs():
    """
    Groq читает буфер логов каждые 30 минут и:
    1. Выявляет паттерны ошибок
    2. Предлагает исправления
    3. Применяет патчи к коду для WARNING уровня
    4. Уведомляет о критических проблемах
    """
    if not _log_buffer:
        return

    errors = get_recent_errors(50)
    if not errors:
        return

    candle_fails = get_candle_failures()

    prompt = f"""Ты DevOps-инженер и Python-разработчик. Проанализируй логи торгового бота APEX.

ПОСЛЕДНИЕ ОШИБКИ И ПРЕДУПРЕЖДЕНИЯ (последние 30 минут):
{chr(10).join(errors[-30:])}

МОНЕТЫ БЕЗ СВЕЧЕЙ (топ проблемных):
{candle_fails}

Ответь JSON без markdown:
{{
  "summary": "краткое описание главной проблемы",
  "root_cause": "корневая причина (1-2 предложения)",
  "candle_fix": "конкретный способ получить свечи для проблемных монет (какой API использовать)",
  "severity": "low/medium/high/critical",
  "auto_fixable": true/false,
  "action": "что бот должен сделать прямо сейчас"
}}"""

    try:
        response = ask_groq(prompt, max_tokens=400)
        if not response:
            return

        import json as _j, re as _re
        clean = _re.sub(r'```json|```', '', response).strip()
        data = _j.loads(clean)

        summary = data.get("summary", "")
        severity = data.get("severity", "low")
        candle_fix = data.get("candle_fix", "")
        root_cause = data.get("root_cause", "")
        action = data.get("action", "")

        # Сохраняем анализ в brain.db
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
        desc = "Причина: " + root_cause + "\nИсправление свечей: " + candle_fix + "\nДействие: " + action
        conn.execute(
            "INSERT INTO brain_log (event_type, title, description, source) VALUES (?,?,?,?)",
            ("log_analysis", "[" + severity.upper() + "] " + summary, desc, "groq_log_analyzer")
        )
        conn.commit()
        conn.close()

        logging.info(f"[LogAnalyzer] {severity}: {summary[:80]}")

        # Критические ошибки — уведомляем сразу
        if severity in ("high", "critical") and ADMIN_ID:
            msg = (
                "\u26a0\ufe0f <b>APEX LogAnalyzer [" + severity.upper() + "]</b>\n\n"
                "<b>\u041f\u0440\u043e\u0431\u043b\u0435\u043c\u0430:</b> " + summary + "\n"
                "<b>\u041f\u0440\u0438\u0447\u0438\u043d\u0430:</b> " + root_cause + "\n"
                "<b>\u0414\u0435\u0439\u0441\u0442\u0432\u0438\u0435:</b> " + action
            )
            await bot.send_message(ADMIN_ID, msg, parse_mode="HTML")
    except Exception as e:
        logging.debug(f"groq_analyze_logs: {e}")


class ErrorCapture(logging.Handler):
    """Перехватывает ERROR логи и запускает авто-патч — только реальные ошибки кода"""

    # Эти сообщения — не ошибки кода, игнорируем
    IGNORE_PATTERNS = [
        "нет свечей", "no candles", "свечей для", "klines",
        "bybit klines", "binance futures", "binance spot", "coingecko",
        "cryptocompare candles", "yahoo finance", "messari",
        "tavily", "rss parse", "pump detector",
        "накопление", "accumulation detect",
    ]

    def emit(self, record):
        if record.levelno >= logging.ERROR:
            error_text = self.format(record)
            error_lower = error_text.lower()

            # Игнорируем не-ошибки (проблемы с внешними API — это нормально)
            if any(pattern in error_lower for pattern in self.IGNORE_PATTERNS):
                return

            # Только реальные ошибки Python — Traceback, Exception
            if not any(kw in error_text for kw in ["Traceback", "Exception", "Error:", "raise ", "line "]):
                return

            # Дедупликация — не спамим одной ошибкой
            error_key = error_text[:100]
            now = time.time()
            if now - last_error_time.get(error_key, 0) < error_cooldown:
                return
            last_error_time[error_key] = now

            # Запускаем авто-патч асинхронно
            try:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.create_task(analyze_and_patch(error_text, "runtime"))
            except:
                pass


def setup_error_capture():
    """Подключаем перехватчики ошибок"""
    # Буфер логов для Groq-анализа
    log_buf = LogBufferHandler()
    log_buf.setLevel(logging.WARNING)
    logging.getLogger().addHandler(log_buf)
    # Авто-патч критических ошибок
    handler = ErrorCapture()
    handler.setLevel(logging.ERROR)
    logging.getLogger().addHandler(handler)
    logging.info("ErrorCapture + LogBuffer активированы — авто-патч и анализ логов включены")


# ===== MAIN =====

async def restore_db_from_github():
    """При старте скачиваем brain.db из GitHub только если GitHub версия больше локальной"""
    try:
        gh_token = os.environ.get("GITHUB_TOKEN", "")
        gh_repo = os.environ.get("GITHUB_REPO", "")
        if not gh_token or not gh_repo:
            logging.info("GH_TOKEN/GH_REPO не заданы — пропускаем восстановление DB")
            return
        import base64, sqlite3 as _sq
        loop = asyncio.get_event_loop()
        r = await loop.run_in_executor(
            None,
            lambda: requests.get(
                f"https://api.github.com/repos/{gh_repo}/contents/brain.db",
                headers={"Authorization": f"token {gh_token}", "Accept": "application/vnd.github.v3+json"},
                timeout=10
            )
        )
        if r.status_code != 200:
            logging.info("brain.db в GitHub не найден — начинаем с чистой базы")
            return

        github_size = r.json().get("size", 0)

        # Проверяем локальную БД
        local_size = 0
        local_knowledge = 0
        if os.path.exists("brain.db"):
            local_size = os.path.getsize("brain.db")
            try:
                _conn = _sq.connect("brain.db", timeout=5)
                local_knowledge = (_conn.execute("SELECT COUNT(*) FROM knowledge").fetchone() or [0])[0]
                _conn.close()
            except Exception:
                local_knowledge = 0

        # Восстанавливаем из GitHub только если локальная БД пустая
        # или GitHub БД значительно больше
        should_restore = (
            local_knowledge == 0 and local_size < 100_000
        ) or (
            github_size > local_size * 2 and github_size > 500_000
        )

        if should_restore:
            content = base64.b64decode(r.json()["content"])
            if len(content) < 100_000:
                logging.warning(f"GitHub brain.db слишком маленькая ({len(content)//1024}KB) — пропускаем")
                return
            with open("brain.db", "wb") as f:
                f.write(content)
            logging.info(f"brain.db восстановлен из GitHub ({len(content)//1024}KB)")
        else:
            logging.info(f"brain.db локальная актуальна (local={local_size//1024}KB знаний={local_knowledge}) — пропускаем")
    except Exception as e:
        logging.warning(f"restore_db_from_github: {e}")


async def backup_db_to_github():
    """Сохраняем brain.db в GitHub"""
    try:
        gh_token = os.environ.get("GITHUB_TOKEN", "")
        gh_repo = os.environ.get("GITHUB_REPO", "")
        if not gh_token or not gh_repo:
            return
        import base64
        with open("brain.db", "rb") as f:
            content = f.read()
        encoded = base64.b64encode(content).decode()
        # Получаем SHA для обновления
        r = requests.get(
            f"https://api.github.com/repos/{gh_repo}/contents/brain.db",
            headers={"Authorization": f"token {gh_token}", "Accept": "application/vnd.github.v3+json"},
            timeout=10
        )
        sha = r.json().get("sha", "") if r.status_code == 200 else ""
        payload = {
            "message": f"brain.db backup {datetime.now().strftime('%Y-%m-%d %H:%M')} [skip ci]",
            "content": encoded,
            "branch": "main"
        }
        if sha:
            payload["sha"] = sha
        r2 = requests.put(
            f"https://api.github.com/repos/{gh_repo}/contents/brain.db",
            headers={"Authorization": f"token {gh_token}", "Accept": "application/vnd.github.v3+json"},
            json=payload,
            timeout=20
        )
        if r2.status_code in (200, 201):
            logging.info(f"brain.db сохранён в GitHub ({len(content)//1024}KB)")
        else:
            logging.warning(f"backup_db_to_github: {r2.status_code}")
    except Exception as e:
        logging.warning(f"backup_db_to_github: {e}")


# ===== BRAIN BUILDER ИНТЕГРАЦИЯ =====
try:
    from brain_builder import (
        run_brain_builder, get_brain_summary,
        init_brain_db, DB_PATH as BRAIN_DB_PATH
    )
    BRAIN_BUILDER_AVAILABLE = True
    logging.info("brain_builder.py подключён ✅")
except Exception as _bbe:
    BRAIN_BUILDER_AVAILABLE = False
    logging.warning(f"brain_builder.py не загружен: {_bbe}")

    def run_brain_builder(full=False):
        return {}

    def get_brain_summary():
        try:
            conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("""CREATE TABLE IF NOT EXISTS web_knowledge (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT, content TEXT, source TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS self_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT, rule TEXT, confidence REAL DEFAULT 0.5,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
            conn.commit()
            kc = conn.execute("SELECT COUNT(*) FROM web_knowledge").fetchone()
            rc = conn.execute("SELECT COUNT(*) FROM self_rules").fetchone()
            conn.close()
            return {
                "knowledge_count": kc[0] if kc else 0,
                "coin_count": rc[0] if rc else 0,
                "pattern_count": 0,
                "macro_summary": "Нет данных",
                "macro_time": ""
            }
        except Exception as e:
            logging.error(f"get_brain_summary: {e}")
            return {"knowledge_count": 0, "coin_count": 0, "pattern_count": 0,
                    "macro_summary": "Нет данных", "macro_time": ""}


async def run_brain_builder_async():
    """Быстрый цикл brain builder (каждый час)"""
    try:
        loop = asyncio.get_running_loop()
        stats = await loop.run_in_executor(None, run_brain_builder, False)
        if stats:
            logging.info(f"🧠 Brain Builder (быстрый): знаний={stats.get('knowledge',0)} правил={stats.get('rules',0)}")
        # Бэкап БД в GitHub после обучения
        await backup_db_to_github()
    except Exception as e:
        logging.error(f"run_brain_builder_async: {e}")


async def run_brain_builder_full_async():
    """Полный цикл brain builder (раз в сутки в 3:00)"""
    try:
        loop = asyncio.get_running_loop()
        stats = await loop.run_in_executor(None, run_brain_builder, True)
        if stats:
            logging.info(
                f"🧠 Brain Builder (полный): "
                f"знаний={stats.get('knowledge',0)} правил={stats.get('rules',0)} "
                f"паттернов={stats.get('patterns',0)} монет={stats.get('coins',0)}"
            )
        await backup_db_to_github()
    except Exception as e:
        logging.error(f"run_brain_builder_full_async: {e}")



async def keepalive_heartbeat():
    """Каждые 10 минут — не даёт Render усыплять сервис"""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE IF NOT EXISTS heartbeat (id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT DEFAULT CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO heartbeat (ts) VALUES (CURRENT_TIMESTAMP)")
        conn.execute("DELETE FROM heartbeat WHERE id NOT IN (SELECT id FROM heartbeat ORDER BY id DESC LIMIT 100)")
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Heartbeat: {e}")

async def on_startup(app):
    # Логирование конфигурации при старте
    logging.info(f"WEBHOOK_URL = {os.environ.get('WEBHOOK_URL', 'НЕТ')}")
    logging.info(f"TOKEN exists = {bool(os.environ.get('TELEGRAM_TOKEN'))}")
    logging.info(f"ADMIN_ID = {os.environ.get('ADMIN_ID')}")

    await restore_db_from_github()  # сначала восстанавливаем БД из GitHub
    init_db()                        # потом применяем миграции к восстановленной БД
    start_db_writer()
    if BRAIN_BUILDER_AVAILABLE:
        try:
            init_brain_db()
            logging.info("init_brain_db() — таблицы мозга созданы")
        except Exception as _ibe:
            logging.warning(f"init_brain_db: {_ibe}")
    # Применяем миграции learning.py (signal_stats, self_rules, confirmed_by и др.)
    if _LEARNING_OK:
        try:
            from learning import init_learning
            init_learning()
            logging.info("init_learning() — миграции применены")
        except Exception as _ile:
            logging.warning(f"init_learning: {_ile}")
    if _WEB_LEARNER_OK:
        _web_init_db()
    threading.Thread(target=get_top_pairs, daemon=True).start()

    WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
    if WEBHOOK_URL:
        await bot.set_webhook(f"{WEBHOOK_URL}/webhook", drop_pending_updates=True)
        logging.info(f"Webhook установлен: {WEBHOOK_URL}/webhook")
    else:
        logging.warning("WEBHOOK_URL не задан — работаем в polling режиме")

    # ── Webhook режим: настройка планировщика (сигналы + мозг) ──
    # BUG FIX: этот блок был случайно перемещён внутрь recheck_timing_queue.
    # Теперь он правильно инициализируется при старте webhook-сервера.
    webhook_scheduler = AsyncIOScheduler(job_defaults={"misfire_grace_time": 60, "coalesce": True, "max_instances": 1})

    # Основные сигналы
    webhook_scheduler.add_job(auto_scan_job,        "interval", minutes=5,  jitter=20,  max_instances=1, coalesce=True)
    webhook_scheduler.add_job(auto_scan_1h,         "interval", minutes=10, jitter=60,  max_instances=1, coalesce=True)
    webhook_scheduler.add_job(auto_scan_swing,      "interval", minutes=15, jitter=60,  max_instances=1, coalesce=True)
    webhook_scheduler.add_job(auto_zone_scan,       "interval", minutes=20, jitter=60,  max_instances=1, coalesce=True)  # ZONE — каждые 20 мин
    webhook_scheduler.add_job(auto_fast_deal_scan,  "interval", minutes=5,  jitter=30,  max_instances=1, coalesce=True)
    webhook_scheduler.add_job(auto_wyckoff_scan,    "interval", hours=4,    jitter=600, max_instances=1, coalesce=True)
    webhook_scheduler.add_job(auto_accumulation_scan, "interval", hours=1, max_instances=1, coalesce=True)
    webhook_scheduler.add_job(keepalive_heartbeat,  "interval", minutes=10, max_instances=1, coalesce=True)
    # timing_queue отключена — MTF отправляет сигналы напрямую по скору
    # webhook_scheduler.add_job(recheck_timing_queue, "interval", minutes=15, jitter=30,  max_instances=1, coalesce=True)
    webhook_scheduler.add_job(check_alerts,         "interval", minutes=5,  max_instances=1, coalesce=True)
    webhook_scheduler.add_job(auto_research,        "interval", hours=2,    max_instances=1, coalesce=True)
    webhook_scheduler.add_job(night_brain_tasks,    "interval", minutes=30, jitter=180, max_instances=1, coalesce=True)
    webhook_scheduler.add_job(autonomous_learning_cycle, "interval", hours=1, jitter=120, max_instances=1, coalesce=True)

    # Мозг / самообучение
    async def _weekly_report_job():
        try:
            loop = asyncio.get_running_loop()
            report = await loop.run_in_executor(None, _learn_weekly_report)
            if report and ADMIN_ID:
                await bot.send_message(ADMIN_ID, report, parse_mode="HTML")
        except Exception as e:
            logging.warning(f"Weekly report error: {e}")
    webhook_scheduler.add_job(_weekly_report_job, "cron", day_of_week="sun", hour=8, minute=0, timezone="UTC")

    async def _review_rules_job():
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _learn_review_rules)
            await loop.run_in_executor(None, _learn_ab_test)
        except Exception as e:
            logging.error(f"review_rules_job: {e}")
    webhook_scheduler.add_job(_review_rules_job, "interval", days=3, start_date="2026-01-01 04:00:00")

    async def _self_diagnose_job():
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self_diagnose_and_grow)
        await loop.run_in_executor(None, auto_fill_knowledge_gaps)
    webhook_scheduler.add_job(_self_diagnose_job, "interval", hours=6, jitter=1200)

    async def _run_self_analysis():
        if _LEARNING_OK:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _learn_self_analysis)
    webhook_scheduler.add_job(_run_self_analysis, "interval", hours=3, jitter=600)

    async def _run_decay():
        if _LEARNING_OK:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _learn_decay)
    webhook_scheduler.add_job(_run_decay, "cron", hour=4, minute=30)

    async def _run_strategy_update():
        if _LEARNING_OK:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _learn_build_strategy)
            logging.info("[Scheduler] Стратегия Groq обновлена")
    webhook_scheduler.add_job(_run_strategy_update, "cron", hour=5, minute=0)

    async def _run_groq_diagnosis():
        if _LEARNING_OK:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _learn_self_diag)
            logging.info("[Scheduler] Groq самодиагностика завершена")
    webhook_scheduler.add_job(_run_groq_diagnosis, "interval", hours=12, jitter=600, max_instances=1, coalesce=True)

    webhook_scheduler.add_job(groq_analyze_logs, "interval", minutes=30, jitter=120, max_instances=1, coalesce=True)

    async def _router_daily_review():
        if _ROUTER_OK:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _brain_router.daily_review)
            logging.info("[Scheduler] Router: ежедневная стратегия обновлена")
    webhook_scheduler.add_job(_router_daily_review, "cron", hour=5, minute=30)

    webhook_scheduler.add_job(run_brain_builder_async,     "interval", hours=1,  jitter=300, max_instances=1, coalesce=True)
    webhook_scheduler.add_job(run_brain_builder_full_async, "cron",     hour=3,   minute=0, max_instances=1, coalesce=True)

    async def _run_web_learner():
        if _WEB_LEARNER_OK:
            loop = asyncio.get_running_loop()
            results = await loop.run_in_executor(None, _web_learn_cycle)
            if results:
                logging.info(f"[WebLearner] Изучено тем: {len(results)}")
            await backup_db_to_github()
    webhook_scheduler.add_job(_run_web_learner, "interval", hours=1, jitter=300, max_instances=1, coalesce=True)
    webhook_scheduler.add_job(_run_web_learner, "date",
        run_date=datetime.now().replace(second=0) + timedelta(minutes=5))

    async def _run_self_improve():
        if _WEB_LEARNER_OK:
            loop = asyncio.get_running_loop()
            improvements = await loop.run_in_executor(None, _web_self_improve)
            if improvements:
                logging.info(f"[SelfImprove] Groq добавил {len(improvements)} улучшений")
    webhook_scheduler.add_job(_run_self_improve, "interval", hours=8, jitter=1800, max_instances=1, coalesce=True)

    async def _run_autopilot_fast():
        if _AUTOPILOT_OK:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _autopilot_fast)
    webhook_scheduler.add_job(_run_autopilot_fast, "interval", minutes=15, jitter=60, max_instances=1, coalesce=True)

    async def _run_autopilot_deep():
        if _AUTOPILOT_OK:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _autopilot_deep)
    webhook_scheduler.add_job(_run_autopilot_deep, "interval", hours=4, jitter=600, max_instances=1, coalesce=True)

    # Backup БД в GitHub — раз в час
    webhook_scheduler.add_job(backup_db_to_github, "interval", hours=1, jitter=300, max_instances=1, coalesce=True)

    webhook_scheduler.start()
    setup_error_capture()

    # Прогрев кеша при старте (webhook)
    async def _warmup_cache_wh():
        try:
            logging.info("[Cache] Прогрев кеша (webhook)...")
            top = get_top_pairs(20)
            candles_map = await fetch_candles_batch(top, "4h", 100)
            for s, c in candles_map.items():
                if c:
                    get_precomputed_indicators(s, "4h")
                await asyncio.sleep(0.05)
            logging.info(f"[Cache] Прогрев завершён: {len(candles_map)} пар")
        except Exception as e:
            logging.warning(f"[Cache] Ошибка прогрева: {e}")

    asyncio.create_task(_warmup_cache_wh())
    asyncio.get_running_loop().call_later(300, lambda: asyncio.create_task(run_brain_builder_async()))
    logging.info("APEX запущен! (webhook mode)")


async def recheck_timing_queue():
    """Каждые 15 мин перепроверяет очередь тайминга"""
    try:
        expired_count = expire_timing_queue()
        if expired_count > 0:
            logging.info(f"[TimingQueue] Истекло {expired_count} сигналов")

        queue = get_timing_queue()
        if not queue:
            return

        logging.info(f"[TimingQueue] Перепроверяем {len(queue)} сигналов...")

        for row in queue:
            queue_id, symbol, direction, timeframe, entry, sl, tp1, tp2, tp3, grade, signal_text, old_score, expires_at = row
            try:
                candles = get_candles(symbol, timeframe, 50)
                if not candles:
                    continue

                current_price = candles[-1]["close"]
                atr = sum(c["high"] - c["low"] for c in candles[-14:]) / 14

                # Цена ушла далеко от зоны — удаляем
                if abs(current_price - entry) > atr * 3:
                    remove_from_timing_queue(queue_id)
                    logging.info(f"[TimingQueue] {symbol} {direction} — цена ушла из зоны, удалён")
                    continue

                timing = check_entry_timing(candles, direction, entry, timeframe)
                new_score = timing.get("score", 0)
                logging.info(f"[TimingQueue] {symbol} {direction} {timeframe}: {old_score}/3 → {new_score}/3")

                if timing["valid"] and new_score >= 3:
                    # Проверяем RR по текущей цене (минимум 2.0 для MTF)
                    _risk = abs(entry - sl)
                    _reward = abs(tp1 - entry)
                    _rr_now = _reward / _risk if _risk > 0 else 0
                    if _rr_now < 2.0:
                        logging.info(f"[TimingQueue] {symbol} {direction} — RR {_rr_now:.2f} < 2.0, ждём")
                        continue

                    # Обновляем текст — добавляем пометку
                    updated_text = "\U0001F514 <b>\u0422\u0410\u0419\u041c\u0418\u041d\u0413 \u041f\u041e\u0414\u0422\u0412\u0415\u0420\u0416\u0414\u0401\u041d!</b>\n" + signal_text.replace(
                        f"⏰ <b>Тайминг:</b> ⏳",
                        f"⏰ <b>Тайминг:</b> ✅ Готов к входу ({new_score}/3) —"
                    )
                    sd = {
                        "symbol": symbol, "direction": direction, "timeframe": timeframe,
                        "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
                        "grade": grade, "text": updated_text,
                    }
                    await _send_signal(sd)
                    remove_from_timing_queue(queue_id)
                    logging.info(f"[TimingQueue] {symbol} {direction} → ОТПРАВЛЕН (score {new_score}/3, RR {_rr_now:.2f})")

            except Exception as e:
                logging.warning(f"[TimingQueue] {symbol}: {e}")
    except Exception as e:
        logging.error(f"[TimingQueue] recheck error: {e}")

async def on_startup_diagnose(app):
    """Первая самодиагностика через 8 мин после старта"""
    await asyncio.sleep(480)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, self_diagnose_and_grow)
    logging.info("[SelfGrow] Стартовая диагностика завершена")

async def on_shutdown(app):
    logging.info("APEX остановлен")


def main():
    # Файловый лок — предотвращает запуск двух инстансов
    # Удаляем старый lock от предыдущего контейнера Render перед созданием нового
    import fcntl, os as _os_lock
    _lock_path = "/tmp/apex_bot.lock"
    try:
        _os_lock.remove(_lock_path)
        logging.info("Старый lock файл удалён — перезапуск контейнера")
    except FileNotFoundError:
        pass
    except Exception as _le:
        logging.warning(f"Не смог удалить lock: {_le}")
    lock_file = open(_lock_path, "w")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        logging.error("Другой инстанс уже запущен — выходим")
        return
    WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")

    if WEBHOOK_URL:
        # Webhook — ручная реализация, работает с любой версией aiogram 3.x
        app = web.Application()

        async def health(request):
            # Включаем статистику токенов в health endpoint
            token_pct = round(_groq_tokens_used / _GROQ_DAILY_LIMIT * 100) if _GROQ_DAILY_LIMIT > 0 else 0
            return web.Response(text=f"APEX OK | tokens: {_groq_tokens_used}/{_GROQ_DAILY_LIMIT} ({token_pct}%)")
        app.router.add_get("/", health)
        app.router.add_get("/health", health)
        app.router.add_head("/", health)  # Render шлёт HEAD запросы

        async def handle_webhook(request):
            try:
                import json as _json
                data = await request.read()
                update = types.Update(**_json.loads(data))
                await dp.feed_update(bot, update)
            except Exception as e:
                logging.error(f"Webhook error: {e}")
            return web.Response(text="OK")

        async def token_stats(request):
            token_pct = round(_groq_tokens_used / _GROQ_DAILY_LIMIT * 100) if _GROQ_DAILY_LIMIT > 0 else 0
            return web.json_response({
                "tokens_used": _groq_tokens_used,
                "tokens_limit": _GROQ_DAILY_LIMIT,
                "percent": token_pct,
                "available": _tokens_available()
            })
        app.router.add_post("/webhook", handle_webhook)
        app.router.add_get("/tokens", token_stats)
        app.on_startup.append(on_startup)
        app.on_startup.append(on_startup_diagnose)
        app.on_shutdown.append(on_shutdown)

        port = int(os.environ.get("PORT", 10000))
        logging.info(f"Запуск в webhook режиме на порту {port}")
        web.run_app(app, host="0.0.0.0", port=port)
    else:
        # Polling режим
        async def safe_delete_webhook():
            for i in range(5):
                try:
                    await bot.delete_webhook(drop_pending_updates=True)
                    logging.info("Webhook удалён")
                    return
                except Exception as e:
                    logging.warning(f"delete_webhook попытка {i+1}: {e}")
                    await asyncio.sleep(2)

        async def polling_main():
            # Восстанавливаем БД из GitHub с таймаутом 30 сек чтобы не блокировать деплой
            try:
                await asyncio.wait_for(restore_db_from_github(), timeout=30)
            except asyncio.TimeoutError:
                logging.warning("restore_db_from_github: таймаут 30с — продолжаем без восстановления")
            except Exception as _re:
                logging.warning(f"restore_db_from_github: {_re}")
            init_db()
            start_db_writer()
            if BRAIN_BUILDER_AVAILABLE:
                try:
                    init_brain_db()
                except Exception as _ibe:
                    logging.warning(f"init_brain_db: {_ibe}")
            if _LEARNING_OK:
                try:
                    from learning import init_learning
                    init_learning()
                except Exception as _ile:
                    logging.warning(f"init_learning: {_ile}")
            # Health сервер — держит бота живым для UptimeRobot
            threading.Thread(target=run_server, daemon=True).start()
            threading.Thread(target=get_top_pairs, daemon=True).start()
            await safe_delete_webhook()
            await asyncio.sleep(12)  # ждём завершения старого инстанса
            scheduler = AsyncIOScheduler(job_defaults={"misfire_grace_time": 60, "coalesce": True, "max_instances": 1})
            scheduler.add_job(auto_scan_job, "interval", minutes=5, jitter=20)         # проверка закрытых
            scheduler.add_job(auto_scan_1h, "interval", minutes=10, jitter=60, max_instances=1, coalesce=True)       # 1h — каждые 10 мин (единственный MTF скан)
            scheduler.add_job(auto_scan_swing, "interval", minutes=15, jitter=60, max_instances=1, coalesce=True)    # swing 4h — каждые 15 мин
            scheduler.add_job(auto_zone_scan, "interval", minutes=20, jitter=60, max_instances=1, coalesce=True)  # ZONE — каждые 20 мин
            # 1d и 1w — только контекст, сигналы не генерируем
            # scheduler.add_job(auto_scan_1d, ...)
            # scheduler.add_job(auto_scan_1w, ...)
            scheduler.add_job(keepalive_heartbeat, "interval", minutes=10)
            scheduler.add_job(auto_accumulation_scan, "interval", hours=1)
            scheduler.add_job(auto_fast_deal_scan, "interval", minutes=5, jitter=30, max_instances=1, coalesce=True)  # Fast Deal 5m
            scheduler.add_job(auto_wyckoff_scan, "interval", hours=4, jitter=600)     # Wyckoff Spring — каждые 4ч
            scheduler.add_job(auto_research, "interval", hours=2)
            scheduler.add_job(check_alerts, "interval", minutes=5)
            scheduler.add_job(night_brain_tasks, "interval", minutes=30, jitter=180)
            # backup_db_to_github убран из heartbeat-цикла — вызывает disk I/O ошибки
            # Бэкап всё ещё происходит после отправки сигналов и после brain_builder
            scheduler.add_job(autonomous_learning_cycle, "interval", hours=1, jitter=120)
            if _LEARNING_OK:
                # В polling-режиме раньше не было ни decay, ни пересмотра
                # правил — self_rules только росли.
                scheduler.add_job(_learn_decay, "cron", hour=4, minute=30, timezone="UTC")
                scheduler.add_job(_learn_review_rules, "interval", days=3, max_instances=1, coalesce=True)
            # BUG FIX: recheck_timing_queue — перепроверяет очередь тайминга и отправляет сигналы
            # timing_queue отключена — MTF отправляет напрямую
            # scheduler.add_job(recheck_timing_queue, "interval", minutes=15, jitter=30, max_instances=1, coalesce=True)
            # backup_db_to_github убран из scheduler — вызывает disk I/O ошибки
            # Бэкап происходит только после отправки сигналов
            scheduler.start()

            # Прогрев кеша при старте — загружаем топ пары асинхронно
            async def _warmup_cache():
                try:
                    logging.info("[Cache] Прогрев кеша...")
                    top = get_top_pairs(20)
                    candles_map = await fetch_candles_batch(top, "4h", 100)
                    for s, c in candles_map.items():
                        if c:
                            get_precomputed_indicators(s, "4h")
                        await asyncio.sleep(0.05)
                    logging.info(f"[Cache] Прогрев завершён: {len(candles_map)} пар")
                except Exception as e:
                    logging.warning(f"[Cache] Ошибка прогрева: {e}")

            asyncio.create_task(_warmup_cache())
            asyncio.get_running_loop().call_later(30, lambda: asyncio.create_task(autonomous_learning_cycle()))
            logging.info("APEX запущен в polling режиме")
            await dp.start_polling(
                bot,
                allowed_updates=dp.resolve_used_update_types()
            )

        # Watchdog — перезапускаем polling если упал
        max_restarts = 10
        restart_count = 0
        while restart_count < max_restarts:
            try:
                asyncio.run(polling_main())
            except Exception as e:
                restart_count += 1
                logging.error(f"Polling упал ({restart_count}/{max_restarts}): {e}")
                import time as _t
                _t.sleep(10)
                logging.info("Перезапускаем polling...")
            else:
                break


if __name__ == "__main__":
    main()
