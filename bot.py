"""
bot.py — Telegram хендлеры, команды, scheduler, запуск APEX.
Вся рыночная логика — в market.py
"""
import asyncio
import logging
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

from groq import Groq
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ── Импортируем всю рыночную логику из market.py ──
from market import *

# Путь к базе данных
import os as _os_bot
DB_PATH = _os_bot.path.join(_os_bot.path.dirname(_os_bot.path.abspath(__file__)), "brain.db")

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
    """Клавиатура монет с пагинацией — топ-50 из Bybit"""
    all_pairs = get_top_pairs(50)
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
    await message.answer("Главное меню 👇", reply_markup=main_menu())

@dp.message(Command("scan"))
async def cmd_scan(message: types.Message):
    await message.answer("Выбери монету для скана:", reply_markup=pairs_keyboard("scan"))

@dp.message(Command("backtest"))
async def cmd_backtest(message: types.Message):
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
    user_id = message.from_user.id
    mem = get_user_memory(user_id)
    try:
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
        total = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
        wins = conn.execute("SELECT COUNT(*) FROM signals WHERE result LIKE 'tp%'").fetchone()[0]
        losses = conn.execute("SELECT COUNT(*) FROM signals WHERE result='sl'").fetchone()[0]
        pending = conn.execute("SELECT COUNT(*) FROM signals WHERE result='pending'").fetchone()[0]
        top = conn.execute(
            "SELECT symbol, win_rate, total, avg_hours_to_tp FROM signal_learning ORDER BY win_rate DESC LIMIT 5"
        ).fetchall()
        conn.close()
    except:
        total = wins = losses = pending = 0
        top = []

    wr = round(wins / total * 100, 1) if total > 0 else 0
    top_text = "\n".join([f"• {r[0]}: {r[1]:.0f}% WR, avg {r[3]:.0f}ч ({r[2]} сигн.)" for r in top]) or "Нет данных"

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
                "🔍 <b>Выбери монету</b> (топ-50 по объёму):",
                parse_mode="HTML",
                reply_markup=pairs_keyboard("scan", 0)
            )
        except Exception:
            await callback.message.answer(
                "🔍 <b>Выбери монету</b> (топ-50 по объёму):",
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
        pairs = get_top_pairs(50)
        await callback.message.edit_text(
            f"🔍 Сканирую топ-50 на {TF_LABELS.get(tf, tf)}...\n⏳ ~20 сек"
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
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 К монетам", callback_data="menu_scan")]
                ])
            )
        else:
            # Диагностика — объясняем почему нет сигнала
            diag = await asyncio.get_running_loop().run_in_executor(None, scan_diagnostics, symbol)
            await callback.message.edit_text(
                diag,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Повторить", callback_data=f"scan_{symbol}"),
                     InlineKeyboardButton(text="🔙 К монетам", callback_data="menu_scan")]
                ])
            )

    elif data == "menu_backtest":
        await callback.message.edit_text(
            "🔬 <b>Анализ монеты</b>\n\n"
            "• <b>Бектест</b> — историческая точность стратегии\n"
            "• <b>Где мы сейчас</b> — живой анализ: OB, FVG, уровни, что делать",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔬 Бектест (история)", callback_data="menu_bt_select")],
                [InlineKeyboardButton(text="📍 Где мы сейчас (live)", callback_data="menu_live_select")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")],
            ])
        )

    elif data == "menu_bt_select":
        await callback.message.edit_text(
            "🔬 <b>Бектест</b>\n\nВыбери таймфрейм:",
            parse_mode="HTML", reply_markup=backtest_tf_keyboard()
        )

    elif data == "menu_live_select":
        await callback.message.edit_text(
            "📍 <b>Живой анализ — где мы сейчас?</b>\n\nВыбери таймфрейм:",
            parse_mode="HTML", reply_markup=live_tf_keyboard()
        )

    elif data.startswith("live_"):
        parts = data.split("_")
        # live_15m / live_1h / live_4h — выбор таймфрейма, дальше просят монету
        if len(parts) == 2:
            tf = parts[1]
            user_states[user_id] = {"action": "live_analysis", "tf": tf}
            await callback.message.edit_text(
                f"📍 Анализ на {TF_LABELS.get(tf, tf)} — напиши монету (BTC, SOL, ETHUSDT...):"
            )
        # live_now_BTCUSDT_1h или live_refresh_BTCUSDT_1h — прямой показ
        elif len(parts) >= 4:
            symbol = parts[2]; tf = parts[3]
            await callback.message.edit_text(f"📍 Обновляю {symbol} {TF_LABELS.get(tf,tf)}...")
            result = await asyncio.get_running_loop().run_in_executor(None, live_position_analysis, symbol, tf)
            if result:
                await callback.message.edit_text(result, parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"live_refresh_{symbol}_{tf}")],
                        [InlineKeyboardButton(text="🔙 Меню", callback_data="menu_back")],
                    ]))
            else:
                await callback.message.edit_text(f"Нет данных по {symbol}",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_backtest")]]))

    elif data.startswith("bt_"):
        tf = data.replace("bt_", "")
        user_states[user_id] = {"action": "backtest", "tf": tf}
        await callback.message.edit_text(
            f"Бектест на {TF_LABELS.get(tf, tf)}.\n\nНапиши название монеты (например: BTC или ETHUSDT):"
        )

    elif data == "menu_risk":
        mem = get_user_memory(user_id)
        if mem["deposit"] > 0:
            text = (f"💰 <b>Риск калькулятор</b>\n\n"
                    f"Депозит: <b>${mem['deposit']:,.2f}</b>\n"
                    f"Риск на сделку: <b>{mem['risk']}%</b>\n"
                    f"Макс риск в $: <b>${mem['deposit'] * mem['risk'] / 100:.2f}</b>\n\n"
                    f"Изменить: /risk 5000 или /setrisk 2")
        else:
            text = "💰 <b>Риск калькулятор</b>\n\nУкажи депозит командой:\n/risk 1000"
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
        ]))

    elif data == "menu_journal":
        await callback.message.edit_text(
            "📓 <b>Дневник сделок</b>\n\n"
            "/journal — посмотреть историю + анализ ошибок\n\n"
            "Добавить сделку:\n/journal BTC LONG 65000 67000 win",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
            ])
        )

    elif data == "menu_alerts":
        await callback.message.edit_text(
            "🔔 <b>Алерты на пробой уровня</b>\n\n"
            "Установить:\n/alert BTCUSDT 70000\n\n"
            "Как только цена достигнет уровня — пришлю сразу ⚡️",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
            ])
        )

    elif data == "menu_stats":
        try:
            conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
            total = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
            wins = conn.execute("SELECT COUNT(*) FROM signals WHERE result LIKE 'tp%'").fetchone()[0]
            losses = conn.execute("SELECT COUNT(*) FROM signals WHERE result='sl'").fetchone()[0]
            pending = conn.execute("SELECT COUNT(*) FROM signals WHERE result='pending'").fetchone()[0]
            top = conn.execute(
                "SELECT symbol, win_rate, total, avg_hours_to_tp FROM signal_learning ORDER BY win_rate DESC LIMIT 5"
            ).fetchall()
            errors_count = conn.execute("SELECT COUNT(*) FROM bot_errors WHERE fixed=0").fetchone()[0]
            patterns = conn.execute(
                "SELECT error_type, count FROM error_patterns ORDER BY count DESC LIMIT 3"
            ).fetchall()
            conn.close()
        except Exception as e:
            import logging
            logging.error(e)
            total = wins = losses = pending = errors_count = 0
            top = []
            patterns = []

        wr = round(wins / total * 100, 1) if total > 0 else 0
        top_text = "\n".join([f"  {r[0]}: {r[1]:.0f}% WR за {r[2]} сигн." for r in top]) or "  Нет данных"

        err_text = ""
        if errors_count > 0:
            err_text = f"\n\n⚠️ <b>Открытых ошибок:</b> {errors_count}"
            if patterns:
                err_text += "\n" + "\n".join([f"  • {ERROR_TYPES.get(p[0], p[0])}: {p[1]}x" for p in patterns])

        await callback.message.edit_text(
            f"📈 <b>Статистика APEX</b>\n\n"
            f"Всего сигналов: <b>{total}</b>\n"
            f"✅ Прибыльных: <b>{wins}</b>\n"
            f"❌ Убыточных: <b>{losses}</b>\n"
            f"⏳ В работе: <b>{pending}</b>\n"
            f"🎯 Win Rate: <b>{wr}%</b>\n\n"
            f"🏆 <b>Лучшие монеты:</b>\n{top_text}"
            f"{err_text}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🏆 Удачные сделки", callback_data="menu_wins"),
                 InlineKeyboardButton(text="🔍 Ошибки", callback_data="menu_errors")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
            ])
        )

    elif data == "menu_brain":
        try:
            # Данные из основной БД
            conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
            rule_count = conn.execute("SELECT COUNT(*) FROM self_rules").fetchone()[0]
            top_rules = conn.execute(
                "SELECT category, rule, confidence FROM self_rules ORDER BY confidence DESC LIMIT 5"
            ).fetchall()
            obs_count = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
            model_count = conn.execute("SELECT COUNT(*) FROM market_model").fetchone()[0]
            # avoid_count — проверяем оба варианта (старый category и новый rule_type)
            avoid_count = conn.execute(
                "SELECT COUNT(*) FROM self_rules WHERE rule_type='avoid' OR category='avoid'"
            ).fetchone()[0]
            # knowledge_count — из таблицы knowledge напрямую
            knowledge_count = conn.execute("SELECT COUNT(*) FROM knowledge").fetchone()[0]
            # pattern_count — из signal_log
            try:
                pattern_count = conn.execute("SELECT COUNT(*) FROM signal_log").fetchone()[0]
            except Exception as e:
                import logging
                logging.error(e)
                pattern_count = 0
            # coin_count — правила по монетам
            try:
                coin_count = conn.execute(
                    "SELECT COUNT(DISTINCT symbol) FROM signal_log WHERE symbol IS NOT NULL"
                ).fetchone()[0]
            except Exception as e:
                import logging
                logging.error(e)
                coin_count = 0
            conn.close()

            # Данные из brain_builder (если доступен)
            brain = (get_brain_summary() or {}) if BRAIN_BUILDER_AVAILABLE else {}
            if brain.get("knowledge_count", 0) > knowledge_count:
                knowledge_count = brain.get("knowledge_count", 0)
            if brain.get("pattern_count", 0) > pattern_count:
                pattern_count = brain.get("pattern_count", 0)
            macro_summary = brain.get("macro_summary", "")[:200]
            macro_time = brain.get("macro_time", "")
            bb_rules = brain.get("top_rules", "")
        except Exception as e:
            import logging
            logging.error(e)
            rule_count = obs_count = model_count = avoid_count = 0
            knowledge_count = pattern_count = coin_count = 0
            top_rules = []
            macro_summary = bb_rules = macro_time = ""

        cat_emoji = {
            "entry": "🎯", "exit": "🏁", "filter": "🔍",
            "timing": "⏱", "risk": "💰", "avoid": "⛔️",
            "best_setup": "🌟", "market": "📊"
        }

        macro_block = (
            f"\n📊 <b>Последний макро анализ</b> ({macro_time}):\n"
            f"<i>{macro_summary}</i>\n"
        ) if macro_summary else ""

        recent_knowledge = ""

        await callback.message.edit_text(
            f"🧠 <b>Мозг APEX</b>\n"
            f"{'━'*24}\n\n"
            f"📌 Торговых правил: <b>{rule_count}</b>\n"
            f"⛔️ Антипаттернов: <b>{avoid_count}</b>\n"
            f"👁 Наблюдений: <b>{obs_count}</b>\n"
            f"🗂 Моделей монет: <b>{model_count}</b>\n"
            f"📚 Знаний (Groq): <b>{knowledge_count}</b>\n"
            f"📈 SMC паттернов: <b>{pattern_count}</b>\n"
            f"🪙 Правил по монетам: <b>{coin_count}</b>\n"
            f"{macro_block}"
            f"{recent_knowledge}\n"
            f"<i>🔄 Groq обучается каждый час: SMC, макро, новости, сделки, веб-поиск</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⚡️ Обучить сейчас", callback_data="brain_learn_now"),
                 InlineKeyboardButton(text="🌍 Макро анализ", callback_data="brain_macro")],
                [InlineKeyboardButton(text="🏅 Точность грейдов", callback_data="brain_grade_accuracy"),
                 InlineKeyboardButton(text="📋 Анализ сделок", callback_data="brain_trade_analysis")],
                [InlineKeyboardButton(text="🔍 Диагноз ошибок", callback_data="brain_diagnosis"),
                 InlineKeyboardButton(text="📊 Анализ логов", callback_data="brain_logs")],
                [InlineKeyboardButton(text="🌐 Веб-знания", callback_data="brain_web_knowledge"),
                 InlineKeyboardButton(text="📚 История обучения", callback_data="menu_evolution")],
                [InlineKeyboardButton(text="🤖 Автопилот", callback_data="brain_autopilot"),
                 InlineKeyboardButton(text="🔌 Плагин Groq", callback_data="brain_extensions")],
                [InlineKeyboardButton(text="📡 Надёжность источников", callback_data="brain_router_sources"),
                 InlineKeyboardButton(text="🧩 Роутер инсайты", callback_data="brain_router_insights")],
                [InlineKeyboardButton(text="📅 Стратегия роутера", callback_data="brain_router_strategy"),
                 InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")],
            ])
        )

    elif data == "brain_sources":
        await callback.message.edit_text("📡 Загружаю статистику источников...")
        try:
            if _SMC_ENGINE_OK:
                stats_text = get_source_stats()
                barrier_text = get_barrier_summary()
                full_text = stats_text + (f"\n\n{barrier_text}" if barrier_text else "")
            else:
                full_text = (
                    "📡 <b>Статус источников данных</b>\n\n"
                    "<b>Свечи (приоритет):</b>\n"
                    "1️⃣ CryptoCompare — ✅ основной\n"
                    "2️⃣ Binance Futures REST — ✅ fallback\n"
                    "3️⃣ Binance Spot REST — ✅ fallback\n"
                    "4️⃣ Binance API (авторизованный) — ✅\n"
                    "5️⃣ TwelveData — ✅ (если есть ключ)\n"
                    "6️⃣ CoinGecko — ✅ последний резерв\n\n"
                    "<b>Цены:</b> Binance Futures + Spot + CryptoCompare + Yahoo Finance\n"
                    "<b>Пары:</b> Binance Futures топ-100 → Spot fallback\n"
                    "<b>Новости:</b> CoinTelegraph, CoinDesk, Decrypt, Reuters RSS\n\n"
                    "<i>ℹ️ smc_engine.py не в корне — используется встроенный SMC движок бота.</i>"
                )
        except Exception as e:
            full_text = f"Ошибка: {e}"
        await callback.message.edit_text(
            full_text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_self_analysis":
        # Последний самоанализ точности
        await callback.message.edit_text("📊 Загружаю самоанализ...")
        try:
            if _LEARNING_OK:
                analysis_text = _learn_self_analysis_text()
                stats_text = _learn_all_stats()
                full = analysis_text
                if stats_text:
                    full += "\n\n" + stats_text
            else:
                # learning.py нет — показываем статистику из brain.db напрямую
                try:
                    conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
                    total_sig = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
                    wins = conn.execute("SELECT COUNT(*) FROM signals WHERE result='win'").fetchone()[0]
                    losses = conn.execute("SELECT COUNT(*) FROM signals WHERE result='loss'").fetchone()[0]
                    pending = conn.execute("SELECT COUNT(*) FROM signals WHERE result='pending'").fetchone()[0]
                    conn.close()
                    wr = round(wins/(wins+losses)*100) if (wins+losses) > 0 else 0
                    full = (
                        f"📊 <b>Статистика сигналов</b>\n\n"
                        f"Всего сигналов: {total_sig}\n"
                        f"✅ Победы: {wins} | ❌ Потери: {losses} | ⏳ Открыты: {pending}\n"
                        f"Win Rate: {wr}%\n\n"
                        f"<i>Для расширенного самоанализа добавь learning.py в корень репо.</i>"
                    )
                except Exception as db_e:
                    full = f"Нет данных: {db_e}"
        except Exception as e:
            full = f"Ошибка: {e}"
        await callback.message.edit_text(
            full or "Данных пока нет", parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Запустить анализ", callback_data="brain_run_analysis")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_grade_accuracy":
        try:
            grade_text = _learn_grade_text() if _LEARNING_OK else "learning.py не загружен"
        except Exception as e:
            grade_text = f"Ошибка: {e}"
        await callback.message.edit_text(
            grade_text or "Данных пока нет — нужно больше закрытых сигналов",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_run_analysis":
        await callback.message.edit_text("🧠 Запускаю самоанализ...")
        try:
            if _LEARNING_OK:
                import asyncio as _a
                await _a.get_event_loop().run_in_executor(None, _learn_self_analysis)
                text = _learn_self_analysis_text()
            else:
                text = "learning.py не загружен"
        except Exception as e:
            text = f"Ошибка: {e}"
        await callback.message.edit_text(
            text or "Нет данных", parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="brain_self_analysis")]
            ])
        )

    elif data == "brain_api_status":
        await callback.message.edit_text(
            get_api_status_text(), parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_self_diagnose":
        await callback.message.edit_text("🔬 Запускаю самодиагностику...")
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self_diagnose_and_grow)
            await loop.run_in_executor(None, auto_fill_knowledge_gaps)
            conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
            priority_row = conn.execute(
                "SELECT content FROM knowledge WHERE topic='priority_action' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            new_rules = conn.execute(
                "SELECT COUNT(*) FROM self_rules WHERE category='self_improve'"
            ).fetchone()[0]
            suggested = conn.execute(
                "SELECT COUNT(*) FROM knowledge WHERE topic='suggested_api'"
            ).fetchone()[0]
            conn.close()
            priority_txt = priority_row[0][:200] if priority_row else "нет"
            result_text = (
                "<b>Самодиагностика завершена</b>\n\n"
                f"Правил самоулучшения: {new_rules}\n"
                f"Предложено новых API: {suggested}\n\n"
                f"<b>Приоритет:</b> <i>{priority_txt}</i>"
            )
        except Exception as e:
            result_text = f"Ошибка: {e}"
        await callback.message.edit_text(
            result_text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔑 Статус API", callback_data="brain_api_status")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_trade_analysis":
        await callback.message.edit_text("📋 Загружаю анализ сделок от Groq...")
        try:
            if _LEARNING_OK:
                loop = asyncio.get_running_loop()
                text = await loop.run_in_executor(None, _learn_trade_analysis, 7)
            else:
                text = "learning.py не загружен"
        except Exception as e:
            text = f"Ошибка: {e}"
        await callback.message.edit_text(
            text or "Анализов пока нет — нужно закрыть несколько сделок",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_strategy":
        await callback.message.edit_text("📈 Загружаю стратегию...")
        try:
            if _LEARNING_OK:
                loop = asyncio.get_running_loop()
                # Сначала показываем текущую, потом обновляем
                text = await loop.run_in_executor(None, _learn_get_strategy)
                if "не сформирована" in text:
                    text = await loop.run_in_executor(None, _learn_build_strategy)
                    if not text:
                        text = "Недостаточно данных (нужно минимум 10 закрытых сделок)"
            else:
                text = "learning.py не загружен"
        except Exception as e:
            text = f"Ошибка: {e}"
        await callback.message.edit_text(
            text or "Стратегия пока не сформирована",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить стратегию", callback_data="brain_strategy_refresh")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_strategy_refresh":
        await callback.message.edit_text("⏳ Groq анализирует паттерны и формулирует стратегию...")
        try:
            if _LEARNING_OK:
                loop = asyncio.get_running_loop()
                text = await loop.run_in_executor(None, _learn_build_strategy)
                if not text:
                    text = "Недостаточно данных (нужно минимум 10 закрытых сделок)"
            else:
                text = "learning.py не загружен"
        except Exception as e:
            text = f"Ошибка: {e}"
        await callback.message.edit_text(
            text or "Не удалось сформировать стратегию",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_diagnosis":
        await callback.message.edit_text("🔍 Groq анализирует ошибки и паттерны потерь...")
        try:
            if _LEARNING_OK:
                loop = asyncio.get_running_loop()
                # Пробуем получить последний или запустить новый
                text = await loop.run_in_executor(None, _learn_latest_diag)
                if "не запускалась" in text:
                    text = await loop.run_in_executor(None, _learn_self_diag)
                    if not text:
                        text = "Недостаточно потерь для анализа (нужно минимум 3)"
            else:
                text = "learning.py не загружен"
        except Exception as e:
            text = f"Ошибка: {e}"
        await callback.message.edit_text(
            text or "Диагноз недоступен",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Запустить диагноз", callback_data="brain_diagnosis_run")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_logs":
        await callback.message.edit_text("📊 Анализирую последние логи через Groq...")
        try:
            await groq_analyze_logs()
            conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
            row = conn.execute(
                "SELECT title, description, created_at FROM brain_log "
                "WHERE event_type='log_analysis' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            candle_fails = get_candle_failures()
            conn.close()
            if row:
                text = "\U0001f4ca <b>\u0410\u043d\u0430\u043b\u0438\u0437 \u043b\u043e\u0433\u043e\u0432</b> (" + row[2][:16] + ")\n\n"
                text += "<b>" + str(row[0]) + "</b>\n\n" + str(row[1])
                if candle_fails:
                    text += "\n\n<b>\u041c\u043e\u043d\u0435\u0442\u044b \u0431\u0435\u0437 \u0441\u0432\u0435\u0447\u0435\u0439:</b>\n"
                    text += "\n".join(["\u2022 " + k + ": " + str(v) + "x" for k, v in list(candle_fails.items())[:5]])
            else:
                text = "\u0410\u043d\u0430\u043b\u0438\u0437 \u0435\u0449\u0451 \u043d\u0435 \u0437\u0430\u043f\u0443\u0441\u043a\u0430\u043b\u0441\u044f"
        except Exception as e:
            text = "\u041e\u0448\u0438\u0431\u043a\u0430: " + str(e)
        await callback.message.edit_text(
            text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="\U0001f504 \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c", callback_data="brain_logs")],
                [InlineKeyboardButton(text="\U0001f519 \u041d\u0430\u0437\u0430\u0434", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_router_sources":
        await callback.message.edit_text("📡 Загружаю статистику источников роутера...")
        try:
            text = _brain_router.source_stats() if _ROUTER_OK else "brain_router.py не загружен"
        except Exception as e:
            text = f"Ошибка: {e}"
        await callback.message.edit_text(
            text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data="brain_router_sources")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_router_insights":
        await callback.message.edit_text("🧩 Загружаю инсайты роутера...")
        try:
            text = _brain_router.insights() if _ROUTER_OK else "brain_router.py не загружен"
        except Exception as e:
            text = f"Ошибка: {e}"
        await callback.message.edit_text(
            text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data="brain_router_insights")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_router_strategy":
        await callback.message.edit_text("📅 Загружаю стратегию роутера...")
        try:
            if _ROUTER_OK:
                text = _brain_router.strategy()
                if not text or "не сформирована" in text:
                    text = ("📅 <b>Стратегия роутера</b>\n\n"
                            "Стратегия формируется после накопления истории сделок.\n"
                            "Groq анализирует паттерны ежедневно в 05:00 UTC.\n\n"
                            "Данных пока недостаточно — дайте боту поработать.")
                else:
                    text = f"📅 <b>Стратегия роутера</b>\n\n{text}"
            else:
                text = "brain_router.py не загружен"
        except Exception as e:
            text = f"Ошибка: {e}"
        await callback.message.edit_text(
            text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Пересчитать", callback_data="brain_router_strategy_refresh")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_router_strategy_refresh":
        await callback.message.edit_text("🔄 Groq пересчитывает стратегию...")
        try:
            if _ROUTER_OK:
                import threading
                threading.Thread(target=_brain_router.daily_review, daemon=True).start()
                text = "✅ Стратегия запущена на пересчёт. Вернитесь через 1-2 минуты."
            else:
                text = "brain_router.py не загружен"
        except Exception as e:
            text = f"Ошибка: {e}"
        await callback.message.edit_text(
            text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📅 Посмотреть стратегию", callback_data="brain_router_strategy")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_web_knowledge":
        await callback.message.edit_text("🌐 Загружаю веб-знания...")
        try:
            summary = _web_knowledge_summary()
        except Exception as e:
            summary = f"Ошибка: {e}"
        await callback.message.edit_text(
            f"🌐 <b>Знания из интернета</b>\n\n{summary}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔍 Изучить сейчас", callback_data="brain_web_learn_now")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_autopilot":
        await callback.message.edit_text("🤖 Загружаю статус автопилота...")
        try:
            status = _autopilot_status() if _AUTOPILOT_OK else "❌ apex_autopilot.py не загружен"
        except Exception as e:
            status = f"Ошибка: {e}"
        await callback.message.edit_text(
            status,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_extensions":
        await callback.message.edit_text("🔌 Загружаю плагин...")
        try:
            if _EXT_OK:
                s = _ext_summary()
                session = _ext_session()
                changelog_text = ""
                for ch in reversed(s.get("changelog", [])):
                    changelog_text += f"\n• <b>v{ch.get('version','?')}</b> [{ch.get('date','?')}] — {ch.get('changes','?')[:80]}"

                text = (
                    f"🔌 <b>Плагин Groq Extensions</b>\n"
                    f"{'━'*24}\n\n"
                    f"📦 Версия: <b>{s.get('version','?')}</b>\n"
                    f"🔧 Фильтров активно: <b>{s.get('filters', 0)}</b>\n"
                    f"⚡️ Бустеров confluence: <b>{s.get('boosters', 0)}</b>\n"
                    f"✏️ Последнее изменение: <b>{s.get('last_change','—')}</b>\n"
                    f"👤 Автор: <b>{s.get('last_author','—')}</b>\n\n"
                    f"📋 <b>История изменений:</b>{changelog_text or ' нет'}\n\n"
                    f"⏰ Сейчас: <b>{session.get('session','?')}</b> {session.get('note','')}\n\n"
                    f"<i>Используй /improve &lt;запрос&gt; чтобы Groq внёс изменение</i>\n"
                    f"<i>Пример: /improve не торговать SHIB в выходные</i>"
                )
            else:
                text = (
                    "🔌 <b>Плагин Groq Extensions</b>\n\n"
                    "❌ groq_extensions.py не найден в репо.\n\n"
                    "Загрузи файл groq_extensions.py в корень репозитория."
                )
        except Exception as e:
            text = f"Ошибка: {e}"

        await callback.message.edit_text(
            text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_web_learn_now":
        await callback.message.edit_text("🔍 Groq составляет агенду и начинает поиск...")
        try:
            loop = asyncio.get_running_loop()
            results = await loop.run_in_executor(None, _web_learn_cycle)
            text = f"✅ Изучено тем: {len(results)}\n"
            for r in results[:3]:
                text += f"\n• {r['topic']}: {str(r.get('result',{}).get('summary',''))[:100]}"
        except Exception as e:
            text = f"Ошибка: {e}"
        await callback.message.edit_text(text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="brain_web_knowledge")]
            ]))

    elif data == "brain_diagnosis_run":
        await callback.message.edit_text("⏳ Groq проводит глубокий самоанализ ошибок...")
        try:
            if _LEARNING_OK:
                loop = asyncio.get_running_loop()
                text = await loop.run_in_executor(None, _learn_self_diag)
                if not text:
                    text = "Недостаточно потерь для анализа (нужно минимум 3 sl)"
            else:
                text = "learning.py не загружен"
        except Exception as e:
            text = f"Ошибка: {e}"
        await callback.message.edit_text(
            text or "Не удалось запустить диагноз",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_macro":
        await callback.message.edit_text("🌍 Запрашиваю макро анализ...")
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: __import__('brain_builder').learn_macro_trends())
            brain = (get_brain_summary() or {}) if callable(get_brain_summary) else {}
            macro = brain.get("macro_summary", "Нет данных")[:500]
            macro_time = brain.get("macro_time", "")
            await callback.message.edit_text(
                f"🌍 <b>Макро анализ</b> ({macro_time})\n{'━'*24}\n\n{macro}",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
                ])
            )
        except Exception as e:
            await callback.message.edit_text(
                f"❌ Ошибка макро анализа: {e}",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
                ])
            )

    elif data == "brain_learn_now":
        await callback.message.edit_text(
            "🧠 Запускаю полное обучение...\n"
            "⏳ Groq анализирует: макро + новости + SMC + история сделок\n"
            "Займёт ~30 секунд"
        )
        await run_brain_builder_async()
        await autonomous_learning_cycle()
        brain = (get_brain_summary() or {}) if BRAIN_BUILDER_AVAILABLE else {}
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
        rule_count = conn.execute("SELECT COUNT(*) FROM self_rules").fetchone()[0]
        conn.close()
        await callback.message.edit_text(
            f"✅ <b>Обучение завершено</b>\n\n"
            f"📌 Торговых правил: <b>{rule_count}</b>\n"
            f"📚 Знаний Groq: <b>{brain.get('knowledge_count', 0)}</b>\n"
            f"🪙 Правил по монетам: <b>{brain.get('coin_count', 0)}</b>\n"
            f"📈 SMC паттернов: <b>{brain.get('pattern_count', 0)}</b>\n\n"
            f"<i>База сохранена в GitHub — знания не пропадут при рестарте</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🧠 Открыть мозг", callback_data="menu_brain")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
            ])
        )


    elif data == "menu_evolution":
        try:
            conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
            history      = conn.execute("SELECT title, description, after_value, impact_score, created_at FROM learning_history ORDER BY id DESC LIMIT 20").fetchall()
            total_events = conn.execute("SELECT COUNT(*) FROM learning_history").fetchone()[0]
            rules_total  = conn.execute("SELECT COUNT(*) FROM self_rules").fetchone()[0]
            rules_strong = conn.execute("SELECT COUNT(*) FROM self_rules WHERE confidence >= 0.7").fetchone()[0]
            errors_fixed = conn.execute("SELECT COUNT(*) FROM bot_errors WHERE fixed=1").fetchone()[0]
            knowledge_cnt= conn.execute("SELECT COUNT(*) FROM knowledge").fetchone()[0]
            conn.close()
        except:
            history = []; total_events = rules_total = rules_strong = errors_fixed = knowledge_cnt = 0

        if not history:
            evo_text = ("📚 <b>Эволюция APEX</b>\n" + "━"*26 + "\n\n🆕 История пуста.\n\n<i>Нажми «Запустить обучение» чтобы бот начал читать интернет и накапливать знания</i>")
        else:
            lines_evo = [
                "📚 <b>Эволюция APEX</b>", "━"*26,
                f"📊 Событий: <b>{total_events}</b>  |  📌 Правил: <b>{rules_total}</b>  |  💪 Сильных: <b>{rules_strong}</b>",
                f"🔧 Исправлено ошибок: <b>{errors_fixed}</b>  |  📖 Знаний: <b>{knowledge_cnt}</b>",
                "━"*26, "<b>Хронология:</b>", "",
            ]
            for title, desc, after, score, created in history[:15]:
                ts  = (created or "")[:16]
                bar = "█" * int((score or 0.5) * 5)
                lines_evo += [
                    f"<b>{title}</b>  <code>{ts}</code>",
                    f"  {(desc or '')[:90]}",
                    *([ f"  → {after[:60]}" ] if after else []),
                    f"  {bar} {int((score or 0.5)*100)}%",
                    "",
                ]
            evo_text = "\n".join(lines_evo)

        if len(evo_text) > 4000:
            evo_text = evo_text[:4000] + "\n\n<i>...показаны последние события</i>"

        await callback.message.edit_text(evo_text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data="menu_evolution")],
                [InlineKeyboardButton(text="🧠 Запустить обучение", callback_data="brain_learn_now")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")],
            ])
        )

    elif data == "menu_wins":
        try:
            conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
            wins_list = conn.execute(
                """SELECT symbol, direction, entry, tp1, result, grade, timeframe,
                   created_at, closed_at,
                   ROUND((julianday(closed_at) - julianday(created_at)) * 24, 1) as hours
                   FROM signals WHERE result LIKE 'tp%'
                   ORDER BY closed_at DESC LIMIT 15"""
            ).fetchall()
            total_wins = conn.execute("SELECT COUNT(*) FROM signals WHERE result LIKE 'tp%'").fetchone()[0]
            total_sigs = conn.execute("SELECT COUNT(*) FROM signals WHERE result != 'pending'").fetchone()[0]
            avg_hours = conn.execute(
                "SELECT AVG((julianday(closed_at)-julianday(created_at))*24) FROM signals WHERE result LIKE 'tp%'"
            ).fetchone()[0] or 0
            best = conn.execute(
                "SELECT symbol, win_rate FROM signal_learning ORDER BY win_rate DESC LIMIT 3"
            ).fetchall()
            conn.close()
        except Exception as e:
            wins_list = []
            total_wins = total_sigs = 0
            avg_hours = 0
            best = []

        wr = round(total_wins / total_sigs * 100, 1) if total_sigs > 0 else 0

        if not wins_list:
            text = (
                "🏆 <b>Удачные сделки</b>\n\n"
                "Пока нет закрытых прибыльных сделок.\n\n"
                "<i>Сигналы отслеживаются автоматически — как только сработает TP, сделка появится здесь.</i>"
            )
        else:
            lines = []
            tp_emoji = {"tp1": "🥉", "tp2": "🥈", "tp3": "🥇"}
            for w in wins_list:
                symbol, direction, entry, tp1, result, grade, tf, created, closed, hours = w
                emoji = "🟢" if direction == "BULLISH" else "🔴"
                tp_icon = tp_emoji.get(result, "✅")
                hours_str = f"{hours:.0f}ч" if hours and hours < 48 else f"{hours/24:.1f}дн" if hours else "?"
                date_str = closed[:10] if closed else created[:10]
                lines.append(
                    f"{tp_icon} <b>{symbol}</b> {emoji} {result.upper()} | {grade or '-'}\n"
                    f"   Вход: {entry:.4f} → TP: {tp1:.4f} | {hours_str} | {date_str}"
                )

            best_text = " | ".join([f"{b[0]} {b[1]:.0f}%" for b in best]) if best else "—"

            text = (
                f"🏆 <b>Удачные сделки APEX</b>\n"
                f"{'━'*24}\n\n"
                f"✅ Всего побед: <b>{total_wins}</b> из {total_sigs}\n"
                f"🎯 Win Rate: <b>{wr}%</b>\n"
                f"⏱ Среднее время: <b>{avg_hours:.1f}ч</b>\n"
                f"🌟 Лучшие: {best_text}\n"
                f"{'━'*24}\n\n"
                + "\n\n".join(lines[:10])
            )

        await callback.message.edit_text(
            text[:4000],
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data="menu_wins"),
                 InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
            ])
        )

    elif data == "menu_errors":
        try:
            conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
            total_err = conn.execute("SELECT COUNT(*) FROM bot_errors").fetchone()[0]
            unfixed = conn.execute("SELECT COUNT(*) FROM bot_errors WHERE fixed=0").fetchone()[0]
            fixed = conn.execute("SELECT COUNT(*) FROM bot_errors WHERE fixed=1").fetchone()[0]
            errors = conn.execute(
                """SELECT id, symbol, direction, error_type, result, fixed, created_at
                   FROM bot_errors ORDER BY id DESC LIMIT 8"""
            ).fetchall()
            patterns = conn.execute(
                "SELECT error_type, count, rule_added FROM error_patterns ORDER BY count DESC LIMIT 5"
            ).fetchall()
            conn.close()
        except:
            total_err = unfixed = fixed = 0
            errors = []
            patterns = []

        errors_text = ""
        for e in errors:
            status = "✅" if e[5] else "❌"
            errors_text += f"{status} #{e[0]} <b>{e[1]}</b> {e[2]} — {ERROR_TYPES.get(e[3], e[3])} [{e[6][:10]}]\n"

        patterns_text = ""
        for p in patterns:
            rule_icon = "📌" if p[2] else "⚠️"
            patterns_text += f"{rule_icon} {ERROR_TYPES.get(p[0], p[0])}: {p[1]}x\n"
            if p[2]:
                patterns_text += f"   → {p[2][:60]}\n"

        await callback.message.edit_text(
            f"🔍 <b>Ошибки бота APEX</b>\n"
            f"{'━'*24}\n\n"
            f"Всего: {total_err} | ❌ Открыто: {unfixed} | ✅ Исправлено: {fixed}\n\n"
            f"<b>Последние ошибки:</b>\n{errors_text or 'Нет ошибок'}\n"
            f"<b>Паттерны:</b>\n{patterns_text or 'Нет повторений'}\n"
            f"{'━'*24}\n"
            f"<i>/errors [id] — детальный разбор\n"
            f"/errors fix [id] — отметить как исправленное</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data="menu_errors"),
                 InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
            ])
        )

    elif data == "menu_news":
        await callback.message.edit_text("📰 Собираю свежие новости...")
        now_str = datetime.now().strftime("%d.%m.%Y %H:%M:%S")

        # Крипто новости
        crypto_news = await asyncio.get_running_loop().run_in_executor(None, get_crypto_news)
        # Макро новости
        macro_news = await asyncio.get_running_loop().run_in_executor(None, get_market_impact_news)

        crypto_text = format_news(crypto_news[:6])
        macro_text = format_news(macro_news[:4])

        # AI анализ влияния на рынок
        all_titles = "\n".join([item["title"] for item in (crypto_news + macro_news)[:10]])
        analysis = ask_groq(
            f"Ты крипто трейдер. Оцени эти новости — что важно для рынка прямо сейчас? (3-4 пункта, дерзко и кратко):\n{all_titles}",
            max_tokens=300
        )
        save_news("crypto news", all_titles[:500])

        msg = (
            f"📰 <b>Новости крипторынка</b>\n"
            f"🕐 Обновлено: {now_str}\n"
            f"{'━'*24}\n\n"
            f"<b>🔥 Крипто:</b>\n{crypto_text}\n\n"
            f"{'━'*24}\n"
            f"<b>🌍 Макро (влияет на рынок):</b>\n{macro_text}\n\n"
            f"{'━'*24}\n"
            f"<b>⚡️ APEX анализ:</b>\n{analysis or 'Анализирую...'}"
        )

        # Telegram лимит 4096 символов
        if len(msg) > 4000:
            msg = msg[:3990] + "..."

        await callback.message.edit_text(
            msg,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data="menu_news"),
                 InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
            ])
        )

    elif data in ("menu_find_deals", "menu_find_deals_refresh"):
        await callback.message.edit_text(
            "🎯 <b>Ищу сделки...</b>\n\n"
            "⏳ Сканирую топ-40 пар по SMC: OB, FVG, мультитаймфрейм\n"
            "<i>~20-30 секунд</i>",
            parse_mode="HTML"
        )
        signals = await asyncio.get_running_loop().run_in_executor(None, scan_all_for_deals, 40)

        if not signals:
            await callback.message.edit_text(
                "🎯 <b>Сделок нет</b>\n\n"
                "😴 Прошёлся по топ-40 монетам — чётких сетапов не нашёл.\n"
                "Рынок в боковике или сигналы ещё не сформировались.\n\n"
                "<i>Обычно сигналы появляются после пробоя уровней или выхода новостей</i>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Попробовать снова", callback_data="menu_find_deals_refresh")],
                    [InlineKeyboardButton(text="🔙 Меню", callback_data="menu_back")],
                ])
            )
            return

        grade_icons = {"🔥🔥🔥 МЕГА ТОП": "🔥🔥🔥", "🔥🔥 ТОП СДЕЛКА": "🔥🔥", "✅ ХОРОШАЯ": "✅"}
        lines = [f"🎯 <b>Найдено сделок: {len(signals)}</b>", "━"*24, ""]
        for s in signals:
            emoji = "🟢" if s["direction"] == "BULLISH" else "🔴"
            icon  = grade_icons.get(s["grade"], "✅")
            lines.append(f"{icon} {emoji} <b>{s['symbol'].replace('USDT','')}</b> — {s['direction']}")
        lines += ["", "<i>Выбери монету для полного сигнала 👇</i>"]

        buttons = []
        row = []
        for s in signals[:12]:
            emoji = "🟢" if s["direction"] == "BULLISH" else "🔴"
            fire  = "🔥" if "ТОП" in s["grade"] else ""
            row.append(InlineKeyboardButton(
                text=f"{fire}{emoji} {s['symbol'].replace('USDT','')}",
                callback_data=f"deal_open_{s['symbol']}"
            ))
            if len(row) == 3:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([
            InlineKeyboardButton(text="🔄 Обновить", callback_data="menu_find_deals_refresh"),
            InlineKeyboardButton(text="🔙 Меню",     callback_data="menu_back"),
        ])
        await callback.message.edit_text(
            "\n".join(lines),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )

    elif data.startswith("deal_open_"):
        symbol = data.replace("deal_open_", "")
        await callback.message.edit_text(f"📊 Загружаю сигнал по <b>{symbol}</b>...", parse_mode="HTML")
        result = await asyncio.get_running_loop().run_in_executor(None, full_scan_raw, symbol, "1h")
        if result:
            mem = get_user_memory(user_id)
            risk_block = ""
            if mem["deposit"] > 0:
                try:
                    rc = calc_risk(mem["deposit"], mem["risk"], result.get("entry", 0), result.get("sl", 0))
                    if rc:
                        risk_block = (
                            f"\n💰 <b>Риск-менеджмент:</b>\n"
                            f"Размер позиции: <b>{rc['position_size']:.2f}</b> USDT\n"
                            f"Риск в $: <b>${rc['risk_amount']:.2f}</b> ({mem['risk']}%)\n"
                        )
                except:
                    pass
            await callback.message.edit_text(
                result["text"] + risk_block,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Обновить сигнал", callback_data=data)],
                    [InlineKeyboardButton(text="🔙 К списку сделок", callback_data="menu_find_deals")],
                ])
            )
        else:
            await callback.message.edit_text(
                f"😴 <b>{symbol}</b> — сигнал пропал.\n<i>Рынок изменился пока ты смотрел список</i>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 К списку сделок", callback_data="menu_find_deals")],
                ])
            )

    elif data.startswith("menu_trade_") or data.startswith("trade_scalp_") or data.startswith("trade_swing_") or data.startswith("trade_long_"):
        # Старые хендлеры — редиректим на новый
        await callback.message.edit_text("🔄", parse_mode="HTML")
        await asyncio.sleep(0.1)
        # Имитируем нажатие menu_find_deals
        signals = await asyncio.get_running_loop().run_in_executor(None, scan_all_for_deals, 40)
        await callback.message.edit_text(
            f"🎯 Найдено сделок: {len(signals)}" if signals else "😴 Сигналов нет",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🎯 Найти сделки", callback_data="menu_find_deals")],
                [InlineKeyboardButton(text="🔙 Меню", callback_data="menu_back")],
            ])
        )

    elif data == "menu_pump":
        await callback.message.edit_text("📦 Сканирую топ-50 на накопление перед пампом...\n⏳ ~30 секунд")
        pairs = await asyncio.get_running_loop().run_in_executor(None, get_top_pairs, 50)
        found = []
        for symbol in pairs:
            try:
                acc = await asyncio.get_running_loop().run_in_executor(None, detect_accumulation, symbol)
                if acc and acc["score"] >= 50:
                    found.append(acc)
            except:
                pass

        found.sort(key=lambda x: x["score"], reverse=True)

        if not found:
            await callback.message.edit_text(
                "📦 Накоплений не найдено.\nРынок в движении — боковиков нет.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Обновить", callback_data="menu_pump"),
                     InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
                ])
            )
            return

        summary = f"📦 <b>Накопления перед пампом</b>\nНайдено: {len(found)} монет\n{'━'*24}\n\n"
        for acc in found[:3]:
            p = acc["price"]
            ps = f"${p:,.4f}" if p < 1 else f"${p:,.2f}"
            bar = "█" * (acc["score"] // 10) + "░" * (10 - acc["score"] // 10)
            summary += (
                f"📦 <b>{acc['symbol']}</b> | {ps}\n"
                f"Скор: [{bar}] {acc['score']}/100\n"
                f"{acc['signals'][0] if acc['signals'] else ''}\n\n"
            )

        summary += f"<i>Полный разбор каждой — команда /pump BTCUSDT</i>"

        await callback.message.edit_text(
            summary,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data="menu_pump"),
                 InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
            ])
        )

@dp.message(Command("patch"))
async def cmd_patch(message: types.Message):
    """Ручной запуск авто-патча — /patch [описание ошибки]"""
    if message.from_user.id not in ADMIN_IDS:
        return
    args = message.text.split(maxsplit=1)
    error_text = args[1].strip() if len(args) > 1 else "manual patch request"
    await message.answer("🔧 Запускаю анализ кода через Groq...")
    await analyze_and_patch(error_text, "manual")


@dp.message(Command("pump"))
async def cmd_pump(message: types.Message):
    args = message.text.split()
    if len(args) == 2:
        symbol = args[1].upper().replace("USDT","") + "USDT"
        await message.answer(f"📦 Анализирую накопление {symbol}...")
        acc = await asyncio.get_running_loop().run_in_executor(None, detect_accumulation, symbol)
        if acc:
            await message.answer(format_accumulation(acc), parse_mode="HTML")
        else:
            await message.answer(f"😴 {symbol} — накоплений не обнаружено.")
    else:
        await message.answer("📦 Сканирую топ-20 на накопление...")
        pairs = get_top_pairs(20)
        found = []
        for symbol in pairs:
            acc = detect_accumulation(symbol)
            if acc and acc["score"] >= 50:
                found.append(acc)
            time.sleep(0.2)
        found.sort(key=lambda x: x["score"], reverse=True)
        if not found:
            await message.answer("😴 Накоплений не найдено.")
            return
        await message.answer(f"📦 Найдено накоплений: {len(found)}")
        for acc in found[:3]:
            await message.answer(format_accumulation(acc), parse_mode="HTML")
            await asyncio.sleep(0.5)


@dp.message(Command("trade"))
async def cmd_trade(message: types.Message):
    """
    /trade BTC          — все типы сделок (скальп + свинг + долгосрок)
    /trade BTC scalp    — только скальп
    /trade BTC swing    — только свинг
    /trade BTC long     — только долгосрок
    """
    args = message.text.split()
    if len(args) < 2:
        await message.answer(
            "📊 <b>Анализ по типу сделки</b>\n\n"
            "Использование:\n"
            "/trade BTC — все типы\n"
            "/trade BTC scalp — скальп (1m/5m/15m)\n"
            "/trade BTC swing — свинг (1h/4h)\n"
            "/trade BTC long — долгосрок (1d/1w/1M)\n\n"
            "<i>Примеры: /trade TON, /trade ETH swing, /trade SOL long</i>",
            parse_mode="HTML"
        )
        return

    # Распознаём символ через алиасы
    raw = args[1].lower()
    symbol = SYMBOL_ALIASES.get(raw, raw.upper())
    if not symbol.endswith("USDT"):
        symbol = symbol.upper() + "USDT"

    trade_type = args[2].lower() if len(args) >= 3 else "all"
    valid_types = {"scalp", "swing", "long", "all"}
    if trade_type not in valid_types:
        trade_type = "all"

    types_to_run = ["scalp", "swing", "long"] if trade_type == "all" else [trade_type]

    type_labels = {"scalp": "⚡️ Скальп", "swing": "🔄 Свинг", "long": "📈 Долгосрок"}
    await message.answer(
        f"🔍 Анализирую <b>{symbol}</b>\n"
        f"Типы: {' | '.join([type_labels[t] for t in types_to_run])}\n"
        f"⏳ Подожди...",
        parse_mode="HTML"
    )

    found_any = False
    for tt in types_to_run:
        result = await asyncio.get_running_loop().run_in_executor(
            None, analyze_trade_type, symbol, tt
        )
        if result:
            found_any = True
            await message.answer(result["text"], parse_mode="HTML")
            await asyncio.sleep(0.5)
        else:
            await message.answer(
                f"{type_labels[tt]}: нет чёткого сигнала по {symbol} на таймфреймах {', '.join(TF_CATEGORIES[tt])}"
            )

    if not found_any:
        await message.answer(
            f"😴 <b>{symbol}</b> — нет сигналов ни по одному типу сделки.\n"
            f"Рынок, возможно, в боковике или данных недостаточно.",
            parse_mode="HTML"
        )

@dp.message(Command("think"))
async def cmd_think(message: types.Message):
    """Бот думает вслух — глубокий ресёрч по теме"""
    args = message.text.split(maxsplit=1)
    topic = args[1].strip() if len(args) > 1 else "bitcoin market analysis"
    await message.answer(f"🧠 Думаю над темой: <b>{topic}</b>...\n⏳ Ищу в интернете, анализирую...", parse_mode="HTML")
    result = await asyncio.get_running_loop().run_in_executor(None, deep_research, topic)
    if result:
        await message.answer(
            f"🧠 <b>Глубокий анализ: {topic}</b>\n\n{result}",
            parse_mode="HTML"
        )
    else:
        await message.answer("Не удалось найти достаточно данных.")

@dp.message(Command("brain"))
async def cmd_brain(message: types.Message):
    """Показываем что бот знает — его база знаний"""
    try:
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
        total_k = conn.execute("SELECT COUNT(*) FROM knowledge").fetchone()[0]
        sources = conn.execute(
            "SELECT source, COUNT(*) as cnt FROM knowledge GROUP BY source ORDER BY cnt DESC LIMIT 8"
        ).fetchall()
        recent = conn.execute(
            "SELECT topic, source, created_at FROM knowledge ORDER BY id DESC LIMIT 5"
        ).fetchall()
        reflections = conn.execute(
            "SELECT COUNT(*) FROM knowledge WHERE source='self-reflection'"
        ).fetchone()[0]
        comparisons = conn.execute(
            "SELECT COUNT(*) FROM knowledge WHERE source='self-compare'"
        ).fetchone()[0]
        conn.close()

        sources_text = "\n".join([f"• {r[0]}: {r[1]} записей" for r in sources])
        recent_text = "\n".join([f"• [{r[2][:10]}] {r[0][:40]} ({r[1]})" for r in recent])

        await message.answer(
            f"🧠 <b>Мозг APEX</b>\n\n"
            f"📚 Всего знаний: <b>{total_k}</b>\n"
            f"🔄 Само-рефлексий: <b>{reflections}</b>\n"
            f"📊 Сравнений прогнозов: <b>{comparisons}</b>\n\n"
            f"<b>Источники знаний:</b>\n{sources_text}\n\n"
            f"<b>Последние 5 знаний:</b>\n{recent_text}\n\n"
            f"<i>Используй /think [тема] — заставить думать над конкретным вопросом</i>",
            parse_mode="HTML"
        )
    except Exception as e:
        await message.answer(f"Ошибка: {e}")

@dp.message(Command("errors"))
async def cmd_errors(message: types.Message):
    """Раздел ошибок бота — просмотр, анализ, исправления"""
    args = message.text.split()

    # /errors fix <id> — отметить ошибку как исправленную
    if len(args) == 3 and args[1] == "fix":
        try:
            error_id = int(args[2])
            conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
            row = conn.execute(
                "SELECT symbol, error_type, ai_next_time FROM bot_errors WHERE id=?",
                (error_id,)
            ).fetchone()

            if not row:
                await message.answer(f"Ошибка #{error_id} не найдена.")
                conn.close()
                return

            # AI формулирует что именно исправлено
            fix_prompt = f"""Ошибка типа "{ERROR_TYPES.get(row[1], row[1])}" по монете {row[0]} отмечена как исправленная.
Правило было: {row[2]}

Напиши 1-2 предложения:
1. Что именно было исправлено в стратегии
2. Как бот будет поступать теперь"""

            fix_desc = ask_groq(fix_prompt, max_tokens=150)

            conn.execute(
                "UPDATE bot_errors SET fixed=1, fix_description=?, fixed_at=CURRENT_TIMESTAMP WHERE id=?",
                (fix_desc or "Исправлено вручную", error_id)
            )
            conn.commit()
            conn.close()

            await message.answer(
                f"✅ <b>Ошибка #{error_id} отмечена как исправленная</b>\n\n"
                f"<b>{row[0]}</b> | {ERROR_TYPES.get(row[1], row[1])}\n\n"
                f"📌 <b>Что изменено:</b>\n{fix_desc or 'Исправлено'}",
                parse_mode="HTML"
            )
        except Exception as e:
            await message.answer(f"Ошибка: {e}")
        return

    # /errors <id> — детальный просмотр конкретной ошибки
    if len(args) == 2:
        try:
            error_id = int(args[1])
            conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
            row = conn.execute(
                """SELECT id, symbol, direction, entry, sl, result, error_type,
                   error_description, ai_analysis, ai_lesson, ai_next_time,
                   fixed, fix_description, hours_in_trade, market_context, created_at
                   FROM bot_errors WHERE id=?""",
                (error_id,)
            ).fetchone()
            conn.close()

            if not row:
                await message.answer(f"Ошибка #{error_id} не найдена.")
                return

            fixed_block = ""
            if row[11]:  # fixed == 1
                fixed_block = f"\n\n✅ <b>ИСПРАВЛЕНО:</b>\n{row[12]}"
            else:
                fixed_block = f"\n\n❌ Ещё не исправлено\n/errors fix {error_id} — отметить как исправленное"

            await message.answer(
                f"🔍 <b>Разбор ошибки #{row[0]}</b>\n"
                f"{'━'*24}\n\n"
                f"📊 <b>Сделка:</b> {row[1]} {row[2]}\n"
                f"💰 Вход: <code>{row[3]}</code> | Стоп: <code>{row[4]}</code>\n"
                f"❌ Результат: {row[5]} за {row[13]}ч\n"
                f"🏷 Тип ошибки: <b>{ERROR_TYPES.get(row[6], row[6])}</b>\n"
                f"📅 {row[15][:16]}\n\n"
                f"📋 <b>Рыночный контекст:</b>\n{row[14]}\n\n"
                f"🧠 <b>Анализ:</b>\n{row[8]}\n\n"
                f"📚 <b>Урок:</b>\n{row[9]}\n\n"
                f"📌 <b>В следующий раз:</b>\n{row[10]}"
                f"{fixed_block}",
                parse_mode="HTML"
            )
        except Exception as e:
            await message.answer(f"Ошибка: {e}")
        return

    # /errors — список всех ошибок
    try:
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
        total = conn.execute("SELECT COUNT(*) FROM bot_errors").fetchone()[0]
        unfixed = conn.execute("SELECT COUNT(*) FROM bot_errors WHERE fixed=0").fetchone()[0]
        fixed = conn.execute("SELECT COUNT(*) FROM bot_errors WHERE fixed=1").fetchone()[0]

        # Последние 10 ошибок
        errors = conn.execute(
            """SELECT id, symbol, direction, error_type, result, fixed, created_at
               FROM bot_errors ORDER BY id DESC LIMIT 10"""
        ).fetchall()

        # Паттерны — повторяющиеся ошибки
        patterns = conn.execute(
            "SELECT error_type, count, rule_added FROM error_patterns ORDER BY count DESC LIMIT 5"
        ).fetchall()
        conn.close()

        errors_text = ""
        for e in errors:
            status = "✅" if e[5] else "❌"
            errors_text += f"{status} #{e[0]} <b>{e[1]}</b> {e[2]} — {ERROR_TYPES.get(e[3], e[3])} ({e[4]}) [{e[6][:10]}]\n"

        patterns_text = ""
        for p in patterns:
            rule_icon = "📌" if p[2] else "⚠️"
            patterns_text += f"{rule_icon} {ERROR_TYPES.get(p[0], p[0])}: {p[1]}x"
            if p[2]:
                patterns_text += f" → правило: {p[2][:60]}"
            patterns_text += "\n"

        await message.answer(
            f"🔍 <b>Ошибки бота APEX</b>\n"
            f"{'━'*24}\n\n"
            f"Всего: {total} | ❌ Открыто: {unfixed} | ✅ Исправлено: {fixed}\n\n"
            f"<b>Последние ошибки:</b>\n{errors_text}\n"
            f"<b>Паттерны (повторяющиеся):</b>\n{patterns_text}\n"
            f"{'━'*24}\n"
            f"<i>/errors [id] — детальный разбор\n"
            f"/errors fix [id] — отметить как исправленное</i>",
            parse_mode="HTML"
        )
    except Exception as e:
        await message.answer(f"Ошибка: {e}")

@dp.message()
async def handle_text(message: types.Message):
    user_id = message.from_user.id
    user_name = message.from_user.first_name or "трейдер"
    text = message.text
    if not text:
        return

    # Проверяем состояние (ожидаем ввод монеты)
    if user_id in user_states:
        state = user_states.pop(user_id)

        if state.get("action") == "live_analysis":
            symbol = text.upper().replace("USDT", "") + "USDT"
            tf = state.get("tf", "1h")
            thinking = await message.answer(f"📍 Анализирую {symbol} {TF_LABELS.get(tf, tf)}...")
            result = await asyncio.get_running_loop().run_in_executor(None, live_position_analysis, symbol, tf)
            try: await thinking.delete()
            except: pass
            if result:
                await message.answer(result, parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"live_refresh_{symbol}_{tf}")],
                        [InlineKeyboardButton(text="🔙 Меню", callback_data="menu_back")],
                    ]))
            else:
                await message.answer(f"Нет данных по {symbol}. Попробуй: BTC, ETH, SOL, BNB")
            return

        if state.get("action") == "backtest":
            symbol = text.upper().replace("USDT", "") + "USDT"
            tf = state.get("tf", "1h")
            thinking = await message.answer(f"🔬 Запускаю бектест {symbol} {TF_LABELS.get(tf, tf)}...")
            result = await asyncio.get_running_loop().run_in_executor(None, backtest, symbol, tf)
            try: await thinking.delete()
            except: pass
            if result:
                grade = "🔥 Отличная" if result["win_rate"] >= 60 else "✅ Рабочая" if result["win_rate"] >= 50 else "⚠️ Слабая"
                await message.answer(
                    f"🔬 <b>Бектест {symbol} [{TF_LABELS.get(tf, tf)}]</b>\n\n"
                    f"Сигналов: {result['total']}\n"
                    f"✅ Выигрыши: {result['wins']}\n"
                    f"❌ Проигрыши: {result['losses']}\n"
                    f"🎯 Win Rate: <b>{result['win_rate']}%</b>\n"
                    f"Оценка: {grade}",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="📍 Где мы сейчас?", callback_data=f"live_now_{symbol}_{tf}")],
                        [InlineKeyboardButton(text="🔙 Меню", callback_data="menu_back")],
                    ])
                )
            else:
                await message.answer("Недостаточно данных для бектеста")
            return

    save_chat_log(user_id, "user", text)
    thinking = await message.answer("⚡️")
    # ✅ ФИКС: run_in_executor — ask_ai не блокирует event loop
    reply = await asyncio.get_running_loop().run_in_executor(None, ask_ai, user_id, user_name, text)
    try:
        await thinking.delete()
    except:
        pass
    if reply:
        save_chat_log(user_id, "assistant", reply)
        await message.answer(reply)
        asyncio.create_task(
            asyncio.to_thread(extract_and_save_profile, user_id, user_name, text, reply)
        )
    else:
        await message.answer("⚡️ Перегружен, попробуй через минуту.")

# ===== AUTO TASKS =====

async def auto_research():
    topics = ["bitcoin analysis today", "crypto market today", "altcoins 2025"]
    for topic in topics:
        try:
            result = tavily_search(topic, max_results=3)
            summary = ask_groq(f"Вывод для трейдера (2 предложения):\n{result[:600]}", max_tokens=150)
            if summary:
                save_knowledge(topic, summary, "auto")
                save_news(topic, summary)
            await asyncio.sleep(15)
        except:
            pass

async def deep_market_scan(limit=200):
    """
    Глубокий скан всего рынка по запросу пользователя.
    Проверяет 200 монет со всех бирж, ищет сигналы + накопления.
    Возвращает отсортированный список сигналов.
    """
    all_pairs = get_all_market_pairs()
    scan_pairs = all_pairs[:limit]

    signals = []
    accumulations = []

    # Сканируем батчами по 10 монет параллельно
    async def scan_one(symbol):
        try:
            loop = asyncio.get_running_loop()
            # Таймаут 8 сек на монету — не зависаем
            sig = await asyncio.wait_for(
                loop.run_in_executor(None, full_scan_raw, symbol, "1h"),
                timeout=8.0
            )
            if sig and sig.get("grade") in ("МЕГА ТОП", "ТОП СДЕЛКА", "ХОРОШАЯ",
                                             "🔥🔥🔥 МЕГА ТОП", "🔥🔥 ТОП СДЕЛКА", "✅ ХОРОШАЯ"):
                signals.append(sig)
            # Накопление — отдельный таймаут
            acc = await asyncio.wait_for(
                loop.run_in_executor(None, detect_accumulation, symbol),
                timeout=6.0
            )
            if acc and acc.get("score", 0) >= 72:  # поднят порог качества
                accumulations.append({
                    "symbol": symbol,
                    "score": acc["score"],
                    "signal": acc.get("signal", ""),
                    "price": acc.get("price", 0)
                })
        except asyncio.TimeoutError:
            logging.debug(f"deep_scan timeout: {symbol}")
        except Exception as e:
            logging.debug(f"deep_scan error {symbol}: {e}")

    # Батчи по 10
    for i in range(0, min(len(scan_pairs), limit), 10):
        batch = scan_pairs[i:i+10]
        await asyncio.gather(*[scan_one(sym) for sym in batch])
        await asyncio.sleep(0.5)  # Не перегружаем API

    # Сортируем по приоритету
    grade_order = {"🔥🔥🔥 МЕГА ТОП": 0, "🔥🔥 ТОП СДЕЛКА": 1, "✅ ХОРОШАЯ": 2}
    signals.sort(key=lambda x: grade_order.get(x.get("grade", ""), 3))
    accumulations.sort(key=lambda x: x["score"], reverse=True)

    return signals, accumulations


def format_deep_scan_result(signals, accumulations, total_scanned):
    """Форматирует результат глубокого скана для Telegram"""
    if not signals and not accumulations:
        return (
            f"🔍 <b>Глубокий скан завершён</b>\n"
            f"Проверено монет: {total_scanned}\n\n"
            f"😴 Рынок спокоен — нет чётких сетапов\n"
            f"Попробуй позже или смени таймфрейм"
        )

    parts = [f"🔍 <b>Глубокий скан</b> | {total_scanned} монет со всех бирж\n{'━'*24}\n"]

    # Сигналы SMC
    if signals:
        parts.append(f"\n📡 <b>Найдено сигналов: {len(signals)}</b>\n")
        for s in signals[:5]:  # Топ-5
            sym = s.get("symbol", "")
            direction = s.get("direction", "")
            entry = s.get("entry", 0)
            tp1 = s.get("tp1", 0)
            tp2 = s.get("tp2", 0)
            tp3 = s.get("tp3", 0)
            sl = s.get("sl", 0)
            grade = s.get("grade", "")
            emoji = "🟢" if "BULL" in direction else "🔴"

            parts.append(
                f"\n{grade}\n"
                f"{emoji} <b>{sym}</b> — {direction}\n"
                f"💰 Вход: <code>{entry:.4f}</code>\n"
                f"🛑 Стоп: <code>{sl:.4f}</code>\n"
                f"🎯 TP1: <code>{tp1:.4f}</code>\n"
                f"🎯 TP2: <code>{tp2:.4f}</code>\n"
                f"🎯 TP3: <code>{tp3:.4f}</code>\n"
            )

    # Накопления (потенциальные памп кандидаты)
    if accumulations:
        parts.append(f"\n📦 <b>Накопление (Wyckoff) — {len(accumulations)} монет:</b>\n")
        for a in accumulations[:5]:
            score = a["score"]
            fire = "🔥🔥🔥" if score >= 80 else "🔥🔥" if score >= 70 else "🔥"
            parts.append(
                f"{fire} <b>{a['symbol']}</b> — скор {score}/100\n"
                f"   {a['signal']}\n"
            )

    return "".join(parts)


def _format_channel_signal(sd: dict) -> str:
    """Красивое сообщение сигнала для Telegram канала."""
    symbol   = sd.get("symbol", "???")
    direction= sd.get("direction", "")
    grade    = sd.get("grade", "")
    entry    = sd.get("entry", 0)
    tp1      = sd.get("tp1", 0)
    tp2      = sd.get("tp2", 0)
    tp3      = sd.get("tp3", 0)
    sl       = sd.get("sl", 0)
    tf       = sd.get("timeframe", "1h")
    score    = sd.get("confluence_score", 0)
    regime   = sd.get("regime", "")

    dir_icon = "🟢 LONG" if direction == "BULLISH" else "🔴 SHORT"
    grade_icon = {"МЕГА ТОП": "🔥🔥🔥", "ТОП СДЕЛКА": "🔥🔥", "ХОРОШАЯ": "🔥"}.get(grade, "✅")

    def fmt(p):
        if not p: return "—"
        return f"${p:,.4f}" if p < 1 else f"${p:,.2f}"

    lines = [
        f"{grade_icon} <b>APEX SIGNAL</b> | {dir_icon}",
        f"",
        f"💎 <b>{symbol}</b> · {tf} · {regime}",
        f"",
        f"📥 Вход:  <code>{fmt(entry)}</code>",
        f"🎯 TP1:   <code>{fmt(tp1)}</code>",
        f"🎯 TP2:   <code>{fmt(tp2)}</code>",
        f"🎯 TP3:   <code>{fmt(tp3)}</code>",
        f"🛑 SL:    <code>{fmt(sl)}</code>",
        f"",
        f"📊 Confluence: <b>{score}</b>/100 · Грейд: <b>{grade}</b>",
        f"",
        f"⏱ {datetime.now().strftime('%d.%m %H:%M')} UTC",
        f"",
        f"<i>APEX SMC Bot · Сигнал сгенерирован AI</i>",
    ]
    return "\n".join(lines)


async def _send_signal(sd):
    """Отправляет сигнал всем админам и в каналы"""
    if not ADMIN_IDS:
        return
    now_ts = time.time()
    cache_key = f"{sd['symbol']}:{sd['direction']}:{sd.get('timeframe','1h')}"
    last_sent = _sent_signal_cache.get(cache_key, 0)
    if now_ts - last_sent < _SIGNAL_COOLDOWN_HOURS * 3600:
        return  # уже слали этот сигнал недавно
    _sent_signal_cache[cache_key] = now_ts
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, sd["text"], parse_mode="HTML")
        except Exception as e:
            logging.warning(f"send_signal admin {admin_id}: {e}")
    try:
        channel_text = _format_channel_signal(sd)
        await bot.send_message(SIGNAL_CHANNEL, channel_text, parse_mode="HTML")
    except Exception as ce:
        logging.warning(f"Channel send error: {ce}")


async def _scan_tf(timeframe: str, pairs_limit: int = 50):
    """Сканирует топ пары на одном таймфрейме, возвращает сигналы"""
    pairs = get_top_pairs(pairs_limit)
    signals = []
    for symbol in pairs:
        try:
            sig_data = full_scan_raw(symbol, timeframe)
            if sig_data and sig_data.get("grade") in ("МЕГА ТОП", "ТОП СДЕЛКА"):
                sig_data["timeframe"] = timeframe
                signals.append(sig_data)
            await asyncio.sleep(0.3)
        except:
            pass
    return signals


async def auto_scan_job():
    """Каждые 30 мин: скан 5m и 15m таймфреймов"""
    logging.info("⚡ auto_scan_job ЗАПУЩЕН")
    closed = check_pending_signals()
    for c in closed:
        if c["is_win"]:
            tp_icons = {"tp1": "🎯", "tp2": "🎯🎯", "tp3": "🎯🎯🎯"}
            icon = tp_icons.get(c["result"], "✅")
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(
                        admin_id,
                        f"{icon} <b>{c['symbol']}</b> — {c['result'].upper()}!\n"
                        f"⏱ Закрыто за {c['hours']}ч | {c.get('grade', '-')}",
                        parse_mode="HTML"
                    )
                except:
                    pass

    # 5m и 15m — быстрые скальп сигналы
    for tf in ["5m", "15m"]:
        signals = await _scan_tf(tf, pairs_limit=30)
        logging.info(f"Скан {tf}: сигналов {len(signals)}")
        for sd in signals[:3]:
            await _send_signal(sd)
            await asyncio.sleep(1)


async def auto_scan_1h():
    """Каждые 2 часа: скан 1h таймфрейма"""
    signals = await _scan_tf("1h", pairs_limit=50)
    logging.info(f"Скан 1h: сигналов {len(signals)}")
    for sd in signals[:3]:
        await _send_signal(sd)
        await asyncio.sleep(1)


async def auto_scan_4h():
    """Каждые 6 часов: скан 4h таймфрейма"""
    signals = await _scan_tf("4h", pairs_limit=50)
    logging.info(f"Скан 4h: сигналов {len(signals)}")
    for sd in signals[:3]:
        await _send_signal(sd)
        await asyncio.sleep(1)


async def auto_scan_1d():
    """Каждые 12 часов: скан 1d таймфрейма"""
    signals = await _scan_tf("1d", pairs_limit=30)
    logging.info(f"Скан 1d: сигналов {len(signals)}")
    for sd in signals[:3]:
        await _send_signal(sd)
        await asyncio.sleep(1)


async def auto_accumulation_scan():
    """Каждый час: сканируем все топ-50 на накопление перед пампом"""
    pairs = get_top_pairs(50)
    found = []

    for symbol in pairs:
        try:
            acc = detect_accumulation(symbol)
            if acc and acc["score"] >= 72:
                found.append(acc)
            await asyncio.sleep(0.3)
        except:
            pass

    # Сортируем по скору
    found.sort(key=lambda x: x["score"], reverse=True)

    if found and ADMIN_ID:
        await bot.send_message(
            ADMIN_ID,
            f"📦 <b>Накопления перед пампом: {len(found)}</b>\n"
            f"Топ монеты по скору накопления:",
            parse_mode="HTML"
        )
        for acc in found[:4]:
            try:
                await bot.send_message(ADMIN_ID, format_accumulation(acc), parse_mode="HTML")
                await asyncio.sleep(1)
            except:
                pass

    logging.info(f"Накопление скан: {len(pairs)} пар | найдено: {len(found)}")


def scan_all_for_deals(limit=40):
    """
    Сканирует топ пары и возвращает ВСЕ найденные сигналы.
    Используется кнопкой "🎯 Найти сделки".
    Возвращает список: [{symbol, direction, grade, grade_emoji, entry, sl, tp1, tp2, tp3, text}]
    """
    import concurrent.futures
    pairs = get_top_pairs(limit)
    found = []

    def scan_one(symbol):
        try:
            return full_scan_raw(symbol, "1h")
        except:
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(scan_one, pairs))

    for r in results:
        if r and r.get("grade") in ("🔥🔥🔥 МЕГА ТОП", "🔥🔥 ТОП СДЕЛКА", "✅ ХОРОШАЯ"):
            found.append(r)

    # Сортируем: лучшие первыми
    grade_order = {"🔥🔥🔥 МЕГА ТОП": 0, "🔥🔥 ТОП СДЕЛКА": 1, "✅ ХОРОШАЯ": 2}
    found.sort(key=lambda x: grade_order.get(x.get("grade",""), 3))
    return found


def full_scan_raw(symbol, timeframe="1h"):
    """Возвращает dict с текстом и grade для фильтрации"""
    try:
        # Проверяем есть ли уже открытый сигнал по этому символу в БД
        try:
            with sqlite3.connect(DB_PATH) as _chk:
                _row = _chk.execute(
                    "SELECT id FROM signals WHERE symbol=? AND timeframe=? AND result=\'pending\' LIMIT 1",
                    (symbol, timeframe)
                ).fetchone()
                if _row:
                    return None  # уже есть открытая сделка по этой паре+ТФ — не дублируем
        except Exception as e:
            import logging
            logging.error(e)
            pass

        mtf = multi_tf_analysis(symbol, ["15m", "1h", "4h"])
        if not mtf:
            return None

        direction = mtf["direction"]
        candles = get_candles(symbol, timeframe, 200)
        if len(candles) < 20:
            return None

        price = candles[-1]["close"]
        ob = find_ob(candles, direction)
        fvg = find_fvg(candles, direction)
        ob_data = get_orderbook(symbol)

        confluence = [f"✅ {mtf['match_count']}/{mtf['total']} ТФ совпали"]
        if ob:
            confluence.append(f"✅ Order Block: {ob['bottom']:.4f}–{ob['top']:.4f}")
        if fvg:
            confluence.append(f"✅ FVG: {fvg['bottom']:.4f}–{fvg['top']:.4f}")
        if ob_data:
            match = (direction == "BULLISH" and ob_data["bias"] == "BUY") or \
                    (direction == "BEARISH" and ob_data["bias"] == "SELL")
            if match:
                confluence.append(f"✅ OrderBook: {ob_data['bias']}")

        if len(confluence) < 2:
            return None

        # Риск и минимальный TP по таймфрейму
        # 15m: цель 1-3%, 1h: 3-8%, 4h: 8-20%, 1d: 20-60%
        TF_RISK_MAP = {
            "15m": 0.008,   # TP1=1.6%, TP3=4%
            "1h":  0.018,   # TP1=3.6%, TP3=9%
            "4h":  0.040,   # TP1=8%,   TP3=20%
            "1d":  0.090,   # TP1=18%,  TP3=45%
        }
        MIN_TP1_PCT = {
            "15m": 0.012,   # минимум 1.2%
            "1h":  0.030,   # минимум 3%
            "4h":  0.070,   # минимум 7%
            "1d":  0.150,   # минимум 15%
        }
        risk_pct = TF_RISK_MAP.get(timeframe, 0.018)
        risk = price * risk_pct
        min_tp1 = price * MIN_TP1_PCT.get(timeframe, 0.030)

        if direction == "BULLISH":
            entry = ob["top"] if ob else price
            sl = smart_round(entry - risk)
            tp1 = smart_round(entry + risk * 2)
            tp2 = smart_round(entry + risk * 3)
            tp3 = smart_round(entry + risk * 5)
            # Проверяем минимальный TP1
            if tp1 - entry < min_tp1:
                return None
        else:
            entry = ob["bottom"] if ob else price
            sl = smart_round(entry + risk)
            tp1 = smart_round(entry - risk * 2)
            tp2 = smart_round(entry - risk * 3)
            tp3 = smart_round(entry - risk * 5)
            # Проверяем минимальный TP1
            if entry - tp1 < min_tp1:
                return None

        est_hours, confidence, win_rate = get_estimated_time(symbol, timeframe)
        time_str = f"~{est_hours}ч" if est_hours < 24 else f"~{est_hours//24}дн"
        wr_str = f"{win_rate:.0f}% WR" if win_rate > 0 else "нет истории"
        tf_label = TF_LABELS.get(timeframe, timeframe)

        conf_score = len(confluence) * 15  # приблизительный score
        save_signal_db(symbol, direction, "MTF", entry, tp1, tp2, tp3, sl, timeframe, est_hours, mtf["grade"],
                       confluence=conf_score, regime="UNKNOWN")
        emoji = "🟢" if direction == "BULLISH" else "🔴"
        conf_text = "\n".join(confluence)

        # Считаем % прибыли для TP
        if direction == "BULLISH":
            tp1_pct = (tp1 - entry) / entry * 100
            tp2_pct = (tp2 - entry) / entry * 100
            tp3_pct = (tp3 - entry) / entry * 100
        else:
            tp1_pct = (entry - tp1) / entry * 100
            tp2_pct = (entry - tp2) / entry * 100
            tp3_pct = (entry - tp3) / entry * 100

        # Время по таймфрейму
        TF_TIME_LABEL = {"15m": "2-6ч", "1h": "12-48ч", "4h": "2-7дн", "1d": "1-4нед"}
        tf_time_hint = TF_TIME_LABEL.get(timeframe, time_str)

        text = (
            f"{'━'*26}\n"
            f"{mtf['grade_emoji']} <b>{mtf['grade']}</b> [{tf_label}]\n"
            f"{emoji} <b>{symbol}</b> — {direction}\n"
            f"{'━'*26}\n\n"
            f"📐 <b>Таймфреймы:</b>\n{mtf['tf_status']}\n"
            f"{mtf['stars']}\n\n"
            f"💰 <b>Вход:</b> <code>{smart_price_fmt(entry)}</code>\n"
            f"🛑 <b>Стоп:</b> <code>{smart_price_fmt(sl)}</code>\n"
            f"🎯 <b>TP1:</b> <code>{smart_price_fmt(tp1)}</code> (+{tp1_pct:.1f}%)\n"
            f"🎯 <b>TP2:</b> <code>{smart_price_fmt(tp2)}</code> (+{tp2_pct:.1f}%)\n"
            f"🎯 <b>TP3:</b> <code>{smart_price_fmt(tp3)}</code> (+{tp3_pct:.1f}%)\n\n"
            f"⏱ <b>Горизонт:</b> {tf_time_hint}\n"
            f"📊 <b>Точность:</b> {wr_str} | {confidence}\n\n"
            f"📋 <b>Confluence:</b>\n{conf_text}\n"
            f"{'━'*26}"
        )

        return {"symbol": symbol, "grade": mtf["grade"], "text": text, "direction": direction, "entry": entry, "tp1": tp1, "tp2": tp2, "tp3": tp3, "sl": sl, "timeframe": timeframe, "confluence_score": conf_score, "regime": "UNKNOWN"}

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
    """При старте скачиваем brain.db из GitHub"""
    try:
        gh_token = os.environ.get("GITHUB_TOKEN", "")
        gh_repo = os.environ.get("GITHUB_REPO", "")
        if not gh_token or not gh_repo:
            logging.info("GH_TOKEN/GH_REPO не заданы — пропускаем восстановление DB")
            return
        import base64
        r = requests.get(
            f"https://api.github.com/repos/{gh_repo}/contents/brain.db",
            headers={"Authorization": f"token {gh_token}", "Accept": "application/vnd.github.v3+json"},
            timeout=10
        )
        if r.status_code == 200:
            content = base64.b64decode(r.json()["content"])
            with open("brain.db", "wb") as f:
                f.write(content)
            logging.info(f"brain.db восстановлен из GitHub ({len(content)//1024}KB)")
        else:
            logging.info("brain.db в GitHub не найден — начинаем с чистой базы")
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

    scheduler = AsyncIOScheduler(job_defaults={"misfire_grace_time": 60, "coalesce": True, "max_instances": 1})

    # Еженедельный отчёт — каждое воскресенье в 08:00 UTC
    async def _weekly_report_job():
        try:
            loop = asyncio.get_running_loop()
            report = await loop.run_in_executor(None, _learn_weekly_report)
            if report and ADMIN_ID:
                await bot.send_message(ADMIN_ID, report, parse_mode="HTML")
        except Exception as e:
            logging.warning(f"Weekly report error: {e}")

    scheduler.add_job(_weekly_report_job, "cron", day_of_week="sun", hour=8, minute=0, timezone="UTC")

    # Пересмотр правил — каждые 3 дня
    async def _review_rules_job():
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _learn_review_rules)
            await loop.run_in_executor(None, _learn_ab_test)
        except Exception as e:
            logging.error(f"review_rules_job: {e}")

    scheduler.add_job(_review_rules_job, "interval", days=3, start_date="2026-01-01 04:00:00")

async def keepalive_heartbeat():
    """Каждые 10 минут пишет heartbeat в БД — не даёт Render усыплять сервис"""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""CREATE TABLE IF NOT EXISTS heartbeat (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT DEFAULT CURRENT_TIMESTAMP)""")
        conn.execute("INSERT INTO heartbeat (ts) VALUES (CURRENT_TIMESTAMP)")
        conn.execute("DELETE FROM heartbeat WHERE id NOT IN (SELECT id FROM heartbeat ORDER BY id DESC LIMIT 100)")
        conn.commit()
        conn.close()
        logging.debug("Heartbeat OK")
    except Exception as e:
        logging.error(f"Heartbeat: {e}")
    async def _self_diagnose_job():
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self_diagnose_and_grow)
        await loop.run_in_executor(None, auto_fill_knowledge_gaps)
    scheduler.add_job(_self_diagnose_job, "interval", hours=6, jitter=1200)
    # Самоанализ точности + обновление авто-правил
    async def _run_self_analysis():
        if _LEARNING_OK:
            import asyncio as _a
            await _a.get_event_loop().run_in_executor(None, _learn_self_analysis)
    scheduler.add_job(_run_self_analysis, "interval", hours=3, jitter=600)

    # Decay правил — раз в сутки ослабляем устаревшие правила
    async def _run_decay():
        if _LEARNING_OK:
            import asyncio as _a
            await _a.get_event_loop().run_in_executor(None, _learn_decay)
    scheduler.add_job(_run_decay, "cron", hour=4, minute=30)

    # Groq стратегия — обновляется раз в день в 5:00
    async def _run_strategy_update():
        if _LEARNING_OK:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _learn_build_strategy)
            logging.info("[Scheduler] Стратегия Groq обновлена")
    scheduler.add_job(_run_strategy_update, "cron", hour=5, minute=0)

    # Groq самодиагностика ошибок — каждые 12 часов
    async def _run_groq_diagnosis():
        if _LEARNING_OK:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _learn_self_diag)
            logging.info("[Scheduler] Groq самодиагностика завершена")
    scheduler.add_job(_run_groq_diagnosis, "interval", hours=12, jitter=600)

    # Groq читает логи и анализирует ошибки каждые 30 минут
    scheduler.add_job(groq_analyze_logs, "interval", minutes=30, jitter=120)

    # Brain Router — ежедневный обзор стратегии в 05:30 (после strategy_update)
    async def _router_daily_review():
        if _ROUTER_OK:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _brain_router.daily_review)
            logging.info("[Scheduler] Router: ежедневная стратегия обновлена")
    scheduler.add_job(_router_daily_review, "cron", hour=5, minute=30)

    # Brain Builder — каждые 3ч быстрый цикл (экономим токены), раз в сутки полный
    scheduler.add_job(run_brain_builder_async, "interval", hours=3, jitter=600)
    scheduler.add_job(run_brain_builder_full_async, "cron", hour=3, minute=0)

    # Web Learner — автономный поиск знаний каждые 4 часа
    async def _run_web_learner():
        if _WEB_LEARNER_OK:
            loop = asyncio.get_running_loop()
            results = await loop.run_in_executor(None, _web_learn_cycle)
            if results:
                logging.info(f"[WebLearner] Изучено тем: {len(results)}")
            await backup_db_to_github()
    scheduler.add_job(_run_web_learner, "interval", hours=4, jitter=900)
    scheduler.add_job(_run_web_learner, "date",
        run_date=datetime.now().replace(second=0) + timedelta(minutes=5))

    # Groq самоулучшение — каждые 8 часов анализирует результаты и добавляет правила
    async def _run_self_improve():
        if _WEB_LEARNER_OK:
            loop = asyncio.get_running_loop()
            improvements = await loop.run_in_executor(None, _web_self_improve)
            if improvements:
                logging.info(f"[SelfImprove] Groq добавил {len(improvements)} улучшений")
    scheduler.add_job(_run_self_improve, "interval", hours=8, jitter=1800)

    # Автопилот — быстрый цикл каждые 15 минут
    async def _run_autopilot_fast():
        if _AUTOPILOT_OK:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _autopilot_fast)
    scheduler.add_job(_run_autopilot_fast, "interval", minutes=15, jitter=60)

    # Автопилот — глубокий цикл каждые 4 часа
    async def _run_autopilot_deep():
        if _AUTOPILOT_OK:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _autopilot_deep)
    scheduler.add_job(_run_autopilot_deep, "interval", hours=4, jitter=600)

    scheduler.start()
    setup_error_capture()
    # Первый цикл обучения — через 60 сек после старта
    asyncio.get_running_loop().call_later(300, lambda: asyncio.create_task(run_brain_builder_async()))  # 5 мин после старта
    logging.info("APEX запущен!")


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
    import fcntl
    lock_file = open("/tmp/apex_bot.lock", "w")
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
            init_db()
            if BRAIN_BUILDER_AVAILABLE:
                try:
                    init_brain_db()
                except Exception as _ibe:
                    logging.warning(f"init_brain_db: {_ibe}")
            # Health сервер — держит бота живым для UptimeRobot
            threading.Thread(target=run_server, daemon=True).start()
            threading.Thread(target=get_top_pairs, daemon=True).start()
            await safe_delete_webhook()
            await asyncio.sleep(12)  # ждём завершения старого инстанса
            scheduler = AsyncIOScheduler(job_defaults={"misfire_grace_time": 60, "coalesce": True, "max_instances": 1})
            scheduler.add_job(auto_scan_job, "interval", minutes=30)
            scheduler.add_job(auto_scan_1h, "interval", hours=2)
            scheduler.add_job(auto_scan_4h, "interval", hours=6)
            scheduler.add_job(auto_scan_1d, "interval", hours=12)
            scheduler.add_job(keepalive_heartbeat, "interval", minutes=10)
            scheduler.add_job(auto_accumulation_scan, "interval", hours=1)
            scheduler.add_job(auto_research, "interval", hours=2)
            scheduler.add_job(check_alerts, "interval", minutes=5)
            scheduler.add_job(night_brain_tasks, "interval", hours=4)
            scheduler.add_job(backup_db_to_github, "interval", hours=1)
            scheduler.add_job(realtime_pump_detector, "interval", minutes=15)
            scheduler.add_job(autonomous_learning_cycle, "interval", hours=2, jitter=300)
            scheduler.start()
            asyncio.get_running_loop().call_later(30, lambda: asyncio.create_task(autonomous_learning_cycle()))
            logging.info("APEX запущен в polling режиме")
            await dp.start_polling(
                bot,
                allowed_updates=dp.resolve_used_update_types()
            )

        asyncio.run(polling_main())


if __name__ == "__main__":
    main()
