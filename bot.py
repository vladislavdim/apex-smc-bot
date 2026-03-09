import asyncio
import logging
import os
import requests
import sqlite3
import threading
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

genai.configure(api_key=GEMINI_KEY)
model = genai.GenerativeModel("gemini-1.5-flash-002")from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler

TOKEN = os.environ.get("TELEGRAM_TOKEN")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
GEMINI_KEY = os.environ.get("GEMINI_API_KEY")

bot = Bot(token=TOKEN)
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)

genai.configure(api_key=GEMINI_KEY)
model = genai.GenerativeModel("gemini-1.5-flash-002")

chat_history = {}

# ===== HEALTH SERVER =====

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args):
        pass

def run_server():
    server = HTTPServer(("0.0.0.0", 10000), HealthHandler)
    server.serve_forever()

# ===== DATABASE =====

def init_db():
    conn = sqlite3.connect("brain.db")
    conn.execute("""CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, direction TEXT, signal_type TEXT,
        entry REAL, tp REAL, sl REAL, result TEXT DEFAULT 'pending',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS knowledge (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic TEXT, content TEXT, source TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS memory (
        user_id INTEGER PRIMARY KEY,
        profile TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
    conn.commit()
    conn.close()

def save_signal(symbol, direction, signal_type, entry, tp, sl):
    try:
        conn = sqlite3.connect("brain.db")
        conn.execute("INSERT INTO signals VALUES (NULL,?,?,?,?,?,?,'pending',CURRENT_TIMESTAMP)",
                     (symbol, direction, signal_type, entry, tp, sl))
        conn.commit()
        conn.close()
    except:
        pass

def save_knowledge(topic, content, source="auto"):
    try:
        conn = sqlite3.connect("brain.db")
        conn.execute("INSERT INTO knowledge VALUES (NULL,?,?,?,CURRENT_TIMESTAMP)",
                     (topic, content, source))
        conn.commit()
        conn.close()
    except:
        pass

def get_knowledge(topic):
    try:
        conn = sqlite3.connect("brain.db")
        rows = conn.execute(
            "SELECT content FROM knowledge WHERE topic LIKE ? ORDER BY created_at DESC LIMIT 3",
            (f"%{topic}%",)
        ).fetchall()
        conn.close()
        return "\n".join([r[0] for r in rows])
    except:
        return ""

def save_user_profile(user_id, profile):
    try:
        conn = sqlite3.connect("brain.db")
        conn.execute("INSERT OR REPLACE INTO memory VALUES (?,?,CURRENT_TIMESTAMP)",
                     (user_id, profile))
        conn.commit()
        conn.close()
    except:
        pass

def get_user_profile(user_id):
    try:
        conn = sqlite3.connect("brain.db")
        row = conn.execute("SELECT profile FROM memory WHERE user_id=?", (user_id,)).fetchone()
        conn.close()
        return row[0] if row else ""
    except:
        return ""

def get_signal_stats():
    try:
        conn = sqlite3.connect("brain.db")
        total = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
        pending = conn.execute("SELECT COUNT(*) FROM signals WHERE result='pending'").fetchone()[0]
        conn.close()
        return total, pending
    except:
        return 0, 0

# ===== CRYPTO DATA =====

BYBIT_URL = "https://api.bybit.com/v5/market/kline"

def get_klines(symbol, interval="60", limit=100):
    try:
        params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
        r = requests.get(BYBIT_URL, params=params, timeout=10)
        data = r.json()["result"]["list"]
        candles = []
        for row in reversed(data):
            candles.append({
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5])
            })
        return candles
    except:
        return []

def get_market_overview():
    try:
        r = requests.get(
            "https://api.bybit.com/v5/market/tickers",
            params={"category": "linear"},
            timeout=10
        )
        tickers = r.json()["result"]["list"]
        tickers.sort(key=lambda x: float(x.get("turnover24h", 0)), reverse=True)
        top = tickers[:10]
        overview = []
        for t in top:
            if t["symbol"].endswith("USDT"):
                change = float(t.get("price24hPcnt", 0)) * 100
                overview.append(f"{t['symbol']}: ${float(t['lastPrice']):.4f} ({change:+.2f}%)")
        return "\n".join(overview)
    except:
        return "Данные недоступны"

# ===== SMC ENGINE =====

def find_swings(candles, lookback=5):
    highs, lows = [], []
    for i in range(lookback, len(candles) - lookback):
        window_h = [c["high"] for c in candles[i-lookback:i+lookback+1]]
        window_l = [c["low"] for c in candles[i-lookback:i+lookback+1]]
        if candles[i]["high"] == max(window_h):
            highs.append((i, candles[i]["high"]))
        if candles[i]["low"] == min(window_l):
            lows.append((i, candles[i]["low"]))
    return highs, lows

def detect_signals(candles, highs, lows):
    signals = []
    if len(highs) < 2 or len(lows) < 2:
        return signals
    last_close = candles[-1]["close"]
    if last_close > highs[-1][1] and highs[-1][1] > highs[-2][1]:
        signals.append({"type": "BOS", "direction": "BULLISH"})
    if last_close < lows[-1][1] and lows[-1][1] < lows[-2][1]:
        signals.append({"type": "BOS", "direction": "BEARISH"})
    if last_close > highs[-1][1] and highs[-1][1] < highs[-2][1]:
        signals.append({"type": "CHoCH", "direction": "BULLISH"})
    if last_close < lows[-1][1] and lows[-1][1] > lows[-2][1]:
        signals.append({"type": "CHoCH", "direction": "BEARISH"})
    return signals

def find_fvg(candles):
    fvgs = []
    for i in range(1, len(candles) - 1):
        if candles[i+1]["low"] > candles[i-1]["high"]:
            fvgs.append("BULL")
        if candles[i+1]["high"] < candles[i-1]["low"]:
            fvgs.append("BEAR")
    return fvgs

def analyze(symbol, interval="60"):
    candles = get_klines(symbol, interval)
    if not candles:
        return None
    highs, lows = find_swings(candles)
    signals = detect_signals(candles, highs, lows)
    fvgs = find_fvg(candles)
    score = 0
    if signals:
        score += 2
        if signals[0]["type"] == "CHoCH":
            score += 1
    if fvgs:
        score += 1
    return {"symbol": symbol, "price": candles[-1]["close"], "signals": signals, "score": score}

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
           "TONUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "ARBUSDT"]

def scan_market():
    results = []
    for symbol in SYMBOLS:
        for interval in ["60", "240"]:
            try:
                r = analyze(symbol, interval)
                if not r or not r["signals"] or r["score"] < 3:
                    continue
                sig = r["signals"][0]
                price = r["price"]
                direction = sig["direction"]
                tp = round(price * 1.03, 4) if direction == "BULLISH" else round(price * 0.97, 4)
                sl = round(price * 0.985, 4) if direction == "BULLISH" else round(price * 1.015, 4)
                save_signal(symbol, direction, sig["type"], price, tp, sl)
                tf = "1H" if interval == "60" else "4H"
                emoji = "🟢" if direction == "BULLISH" else "🔴"
                msg = (f"{emoji} <b>{symbol}</b> [{tf}]\n"
                       f"📊 {sig['type']} — {direction}\n"
                       f"💰 Вход: {price}\n"
                       f"🎯 TP: {tp} (+3%)\n"
                       f"🛡 SL: {sl} (-1.5%)\n"
                       f"⚡️ Сила: {r['score']}/4")
                results.append(msg)
            except Exception as e:
                logging.error(f"Scan error {symbol}: {e}")
    return results

# ===== AI BRAIN =====

def ask_ai(user_id, user_message):
    profile = get_user_profile(user_id)
    knowledge = get_knowledge(user_message[:50])
    market = get_market_overview()
    history = chat_history.get(user_id, [])
    history_text = "\n".join(history[-10:])
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    prompt = f"""Ты APEX — продвинутый AI трейдер и личный ассистент. Дата: {now}

ХАРАКТЕР:
- Дерзкий, прямой, умный. Говоришь как опытный трейдер
- Не льёшь воду, отвечаешь конкретно и по делу
- Используешь эмодзи умеренно

ЗНАНИЯ:
- SMC: BOS, CHoCH, Order Blocks, FVG, Liquidity Zones
- Bybit фьючерсы, крипто рынки, DeFi, NFT, TON
- Технический анализ, управление рисками, психология трейдинга

ТЕКУЩИЙ РЫНОК:
{market}

ЧТО Я УЖЕ ЗНАЮ (из исследований):
{knowledge if knowledge else "Накапливается..."}

ПРОФИЛЬ ПОЛЬЗОВАТЕЛЯ:
{profile if profile else "Новый пользователь"}

ИСТОРИЯ ЧАТА:
{history_text}

ПРАВИЛА:
- Для сигналов говори /scan
- Для обзора рынка говори /market
- Если анализируешь монету — давай конкретные уровни
- Запоминай что пользователь говорит о себе

Пользователь: {user_message}
APEX:"""

    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        logging.error(f"Gemini error: {e}")
        return None

def update_profile_sync(user_id, message, response):
    profile = get_user_profile(user_id)
    try:
        prompt = f"""Проанализируй диалог и обнови профиль трейдера одним абзацем (макс 150 слов).
Текущий профиль: {profile if profile else "Пустой"}
Сообщение: {message}
Если упомянуты монеты, суммы, стратегии, опыт — добавь. Иначе верни текущий профиль."""
        result = model.generate_content(prompt)
        save_user_profile(user_id, result.text.strip())
    except:
        pass

# ===== AUTO TASKS =====

async def auto_research():
    market = get_market_overview()
    topics = ["BTC market structure", "crypto sentiment today"]
    for topic in topics:
        try:
            prompt = f"""Ты крипто аналитик. Тема: "{topic}"
Данные рынка: {market}
Напиши 3-4 предложения с конкретными выводами для трейдера."""
            response = model.generate_content(prompt)
            save_knowledge(topic, response.text.strip(), "auto")
            await asyncio.sleep(5)
        except Exception as e:
            logging.error(f"Research error: {e}")

async def auto_scan():
    signals = scan_market()
    if signals and ADMIN_ID:
        for s in signals:
            try:
                await bot.send_message(ADMIN_ID, s, parse_mode="HTML")
                await asyncio.sleep(1)
            except Exception as e:
                logging.error(f"Send error: {e}")

# ===== HANDLERS =====

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    name = message.from_user.first_name or "трейдер"
    await message.answer(
        f"⚡️ <b>APEX — твой AI трейдер</b>\n\n"
        f"Привет, {name}. Я думаю, анализирую и учусь.\n\n"
        f"/scan — SMC сигналы\n"
        f"/market — обзор рынка\n"
        f"/stats — статистика\n"
        f"/help — что умею\n\n"
        f"Или просто пиши — отвечу на всё 👇",
        parse_mode="HTML"
    )

@dp.message(Command("scan"))
async def cmd_scan(message: types.Message):
    await message.answer("🔍 Сканирую по SMC...")
    signals = scan_market()
    if signals:
        await message.answer(f"⚡️ Найдено: {len(signals)}")
        for s in signals:
            await message.answer(s, parse_mode="HTML")
    else:
        await message.answer("😴 Чисто. Нет достаточного confluence. Следующий скан через 30 мин.")

@dp.message(Command("market"))
async def cmd_market(message: types.Message):
    await message.answer("📊 Собираю данные...")
    market = get_market_overview()
    try:
        commentary = model.generate_content(
            f"Дай краткий комментарий (3 предложения) по рынку:\n{market}\nЧто интересного?"
        )
        comment = commentary.text
    except:
        comment = "Анализ недоступен."
    await message.answer(f"📊 <b>Рынок</b>\n\n{market}\n\n💬 {comment}", parse_mode="HTML")

@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    total, pending = get_signal_stats()
    profile = get_user_profile(message.from_user.id)
    await message.answer(
        f"📈 <b>Статистика</b>\n\n"
        f"Сигналов: {total} | В ожидании: {pending}\n\n"
        f"👤 <b>Твой профиль:</b>\n{profile if profile else 'Общайся — накоплю данные о тебе'}",
        parse_mode="HTML"
    )

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    await message.answer(
        "🧠 <b>APEX умеет:</b>\n\n"
        "📊 SMC сигналы каждые 30 мин — /scan\n"
        "💹 Обзор рынка — /market\n"
        "💬 Отвечает на вопросы про крипту\n"
        "🔍 Каждые 2 часа сам изучает рынок\n"
        "🧠 Помнит твои интересы и портфель\n\n"
        "Просто пиши 👇",
        parse_mode="HTML"
    )

@dp.message()
async def handle_text(message: types.Message):
    user_id = message.from_user.id
    text = message.text
    if not text:
        return

    if user_id not in chat_history:
        chat_history[user_id] = []
    chat_history[user_id].append(f"Пользователь: {text}")
    if len(chat_history[user_id]) > 30:
        chat_history[user_id] = chat_history[user_id][-30:]

    thinking = await message.answer("⚡️")

    reply = ask_ai(user_id, text)

    try:
        await thinking.delete()
    except:
        pass

    if reply:
        chat_history[user_id].append(f"APEX: {reply[:200]}")
        await message.answer(reply)
        asyncio.create_task(asyncio.to_thread(update_profile_sync, user_id, text, reply))
    else:
        await message.answer("⚡️ Перегружен, попробуй через минуту.")

# ===== MAIN =====

async def main():
    init_db()
    threading.Thread(target=run_server, daemon=True).start()
    scheduler = AsyncIOScheduler()
    scheduler.add_job(auto_scan, "interval", minutes=30)
    scheduler.add_job(auto_research, "interval", hours=2)
    scheduler.start()
    logging.info("APEX started!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
