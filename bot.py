import asyncio
import logging
import os
import requests
import sqlite3
import threading
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

from groq import Groq
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler

TOKEN = os.environ.get("TELEGRAM_TOKEN")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
GROQ_KEY = os.environ.get("GROQ_API_KEY")
TAVILY_KEY = os.environ.get("TAVILY_API_KEY")

bot = Bot(token=TOKEN)
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)

groq_client = Groq(api_key=GROQ_KEY)
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
    conn.execute("""CREATE TABLE IF NOT EXISTS news_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        query TEXT, content TEXT, url TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
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
            "SELECT content, source FROM knowledge WHERE topic LIKE ? ORDER BY created_at DESC LIMIT 5",
            (f"%{topic}%",)
        ).fetchall()
        conn.close()
        return "\n".join([f"[{r[1]}] {r[0]}" for r in rows])
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

def save_news(query, content, url=""):
    try:
        conn = sqlite3.connect("brain.db")
        conn.execute("INSERT INTO news_cache VALUES (NULL,?,?,?,CURRENT_TIMESTAMP)",
                     (query, content, url))
        conn.commit()
        conn.close()
    except:
        pass

def get_recent_news():
    try:
        conn = sqlite3.connect("brain.db")
        rows = conn.execute(
            "SELECT query, content FROM news_cache ORDER BY created_at DESC LIMIT 5"
        ).fetchall()
        conn.close()
        return "\n\n".join([f"📰 {r[0]}:\n{r[1]}" for r in rows])
    except:
        return ""

# ===== TAVILY SEARCH =====

def tavily_search(query, max_results=5):
    """Поиск в интернете через Tavily"""
    try:
        r = requests.post(
            "https://api.tavily.com/search",
            json={
                "api_key": TAVILY_KEY,
                "query": query,
                "search_depth": "advanced",
                "max_results": max_results,
                "include_answer": True,
                "include_domains": [
                    "cointelegraph.com", "coindesk.com", "decrypt.co",
                    "theblock.co", "cryptonews.com", "bitcoinmagazine.com",
                    "investing.com", "tradingview.com", "coingecko.com",
                    "cryptopanic.com", "ambcrypto.com", "beincrypto.com"
                ]
            },
            timeout=20
        )
        data = r.json()
        results = []

        if data.get("answer"):
            results.append(f"Ответ: {data['answer']}")

        for item in data.get("results", []):
            title = item.get("title", "")
            content = item.get("content", "")[:300]
            url = item.get("url", "")
            results.append(f"• {title}\n  {content}\n  🔗 {url}")

        return "\n\n".join(results) if results else "Результатов не найдено"
    except Exception as e:
        logging.error(f"Tavily error: {e}")
        return "Поиск временно недоступен"

def search_crypto_news(coin="bitcoin"):
    """Новости по конкретной монете"""
    query = f"{coin} crypto news price analysis today 2025"
    result = tavily_search(query, max_results=5)
    save_news(f"Новости {coin}", result[:500])
    return result

def search_market_sentiment():
    """Общий сентимент рынка"""
    query = "crypto market sentiment bitcoin analysis today"
    return tavily_search(query, max_results=4)

def search_youtube_analysis(coin="bitcoin"):
    """Ищем аналитику с YouTube и соцсетей"""
    query = f"{coin} technical analysis youtube crypto 2025"
    return tavily_search(query, max_results=3)

# ===== MARKET DATA (CoinGecko) =====

COINGECKO_IDS = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
    "SOLUSDT": "solana",
    "BNBUSDT": "binancecoin",
    "XRPUSDT": "ripple",
    "TONUSDT": "the-open-network",
    "DOGEUSDT": "dogecoin",
    "AVAXUSDT": "avalanche-2",
    "LINKUSDT": "chainlink",
    "ARBUSDT": "arbitrum",
}

def get_market_overview():
    try:
        ids = ",".join(COINGECKO_IDS.values())
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={
                "ids": ids,
                "vs_currencies": "usd",
                "include_24hr_change": "true",
            },
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        data = r.json()
        lines = []
        for symbol, cg_id in COINGECKO_IDS.items():
            if cg_id in data:
                price = data[cg_id].get("usd", 0)
                change = data[cg_id].get("usd_24h_change", 0) or 0
                emoji = "🟢" if change >= 0 else "🔴"
                if price >= 1000:
                    price_str = f"${price:,.2f}"
                elif price >= 1:
                    price_str = f"${price:.3f}"
                else:
                    price_str = f"${price:.5f}"
                lines.append(f"{emoji} {symbol.replace('USDT','')}: {price_str} ({change:+.2f}%)")
        return "\n".join(lines) if lines else "Данные недоступны"
    except Exception as e:
        logging.error(f"CoinGecko error: {e}")
        return "Данные временно недоступны"

def get_coin_price(coin_name):
    """Цена конкретной монеты"""
    try:
        # Пробуем найти в нашем словаре
        cg_id = None
        coin_upper = coin_name.upper()
        for symbol, cg in COINGECKO_IDS.items():
            if coin_upper in symbol:
                cg_id = cg
                break

        if not cg_id:
            # Ищем через CoinGecko search
            rs = requests.get(
                "https://api.coingecko.com/api/v3/search",
                params={"query": coin_name},
                timeout=10,
                headers={"User-Agent": "Mozilla/5.0"}
            )
            coins = rs.json().get("coins", [])
            if coins:
                cg_id = coins[0]["id"]

        if not cg_id:
            return None

        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": cg_id, "vs_currencies": "usd", "include_24hr_change": "true"},
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        data = r.json()
        if cg_id in data:
            return {
                "price": data[cg_id]["usd"],
                "change": data[cg_id].get("usd_24h_change", 0) or 0,
                "id": cg_id
            }
    except:
        pass
    return None

# ===== SMC ENGINE =====

def get_ohlc(cg_id, days=1):
    try:
        r = requests.get(
            f"https://api.coingecko.com/api/v3/coins/{cg_id}/ohlc",
            params={"vs_currency": "usd", "days": days},
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        data = r.json()
        return [{"open": float(c[1]), "high": float(c[2]), "low": float(c[3]), "close": float(c[4])} for c in data]
    except:
        return []

def find_swings(candles, lookback=3):
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

def analyze(symbol, cg_id):
    candles = get_ohlc(cg_id, days=1)
    if len(candles) < 10:
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

def scan_market():
    results = []
    for symbol, cg_id in COINGECKO_IDS.items():
        try:
            r = analyze(symbol, cg_id)
            if not r or not r["signals"] or r["score"] < 3:
                continue
            sig = r["signals"][0]
            price = r["price"]
            direction = sig["direction"]
            tp = round(price * 1.03, 4) if direction == "BULLISH" else round(price * 0.97, 4)
            sl = round(price * 0.985, 4) if direction == "BULLISH" else round(price * 1.015, 4)
            save_signal(symbol, direction, sig["type"], price, tp, sl)
            emoji = "🟢" if direction == "BULLISH" else "🔴"
            msg = (f"{emoji} <b>{symbol}</b>\n"
                   f"📊 {sig['type']} — {direction}\n"
                   f"💰 Вход: ${price:,.4f}\n"
                   f"🎯 TP: ${tp:,.4f} (+3%)\n"
                   f"🛡 SL: ${sl:,.4f} (-1.5%)\n"
                   f"⚡️ Сила: {r['score']}/4")
            results.append(msg)
            asyncio.sleep(1)  # не спамим CoinGecko
        except Exception as e:
            logging.error(f"Scan error {symbol}: {e}")
    return results

# ===== AI BRAIN =====

def ask_groq(prompt, max_tokens=800):
    response = groq_client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=max_tokens
    )
    return response.choices[0].message.content

def ask_ai(user_id, user_message):
    profile = get_user_profile(user_id)
    knowledge = get_knowledge(user_message[:50])
    recent_news = get_recent_news()
    market = get_market_overview()
    history = chat_history.get(user_id, [])
    history_text = "\n".join(history[-10:])
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Определяем нужен ли поиск
    search_keywords = ["новост", "аналитик", "прогноз", "что думают", "youtube", "твиттер",
                       "почему", "новини", "сейчас", "сегодня", "рынок упал", "рынок вырос",
                       "что случилось", "почему упал", "почему вырос", "анализ"]
    need_search = any(kw in user_message.lower() for kw in search_keywords)

    search_results = ""
    if need_search:
        search_results = tavily_search(f"crypto {user_message} 2025", max_results=3)

    prompt = f"""Ты APEX — продвинутый AI трейдер. Дата: {now}

ХАРАКТЕР: дерзкий, умный, прямой. Говоришь как опытный трейдер. Не льёшь воду.

ТЕКУЩИЕ ЦЕНЫ (живые данные):
{market}

ПОСЛЕДНИЕ НОВОСТИ ИЗ ИНТЕРНЕТА:
{search_results if search_results else recent_news if recent_news else "Обновляются..."}

МОИ ЗНАНИЯ О РЫНКЕ:
{knowledge if knowledge else "Накапливаются..."}

ПРОФИЛЬ ПОЛЬЗОВАТЕЛЯ:
{profile if profile else "Новый пользователь"}

ИСТОРИЯ ЧАТА:
{history_text}

ПРАВИЛА:
- Используй ЖИВЫЕ ЦЕНЫ из блока выше, не придумывай
- Для SMC сигналов говори /scan
- Для обзора рынка говори /market  
- Для новостей говори /news
- Давай конкретные уровни при анализе монет
- Отвечай коротко и по делу

Пользователь: {user_message}
APEX:"""

    try:
        return ask_groq(prompt)
    except Exception as e:
        logging.error(f"AI error: {e}")
        return None

def update_profile_sync(user_id, message, response):
    profile = get_user_profile(user_id)
    try:
        prompt = f"""Обнови профиль трейдера одним абзацем (макс 150 слов).
Текущий профиль: {profile if profile else "Пустой"}
Сообщение пользователя: {message}
Если упомянуты монеты, суммы, стратегии, опыт — добавь в профиль. Иначе верни текущий."""
        result = ask_groq(prompt, max_tokens=200)
        save_user_profile(user_id, result.strip())
    except:
        pass

# ===== AUTO TASKS =====

async def auto_research():
    """Каждые 2 часа сам ищет информацию о рынке"""
    topics = [
        "bitcoin price analysis today",
        "crypto market news today",
        "ethereum defi news",
        "altcoin season 2025",
        "crypto whale movements today"
    ]
    for topic in topics:
        try:
            result = tavily_search(topic, max_results=3)
            summary_prompt = f"""Ты крипто аналитик. Сделай краткий вывод (3 предложения) для трейдера:
Тема: {topic}
Данные: {result[:800]}
Вывод:"""
            summary = ask_groq(summary_prompt, max_tokens=200)
            save_knowledge(topic, summary, "tavily-auto")
            save_news(topic, summary)
            logging.info(f"Research done: {topic}")
            await asyncio.sleep(10)
        except Exception as e:
            logging.error(f"Research error {topic}: {e}")

async def auto_scan():
    """Каждые 30 минут сканирует рынок"""
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
        f"Привет, {name}!\n\n"
        f"🔍 /scan — SMC сигналы\n"
        f"📊 /market — живые цены\n"
        f"📰 /news — новости рынка\n"
        f"🔎 /research [монета] — глубокий анализ\n"
        f"📈 /stats — статистика\n\n"
        f"Или просто пиши — отвечу на всё 👇",
        parse_mode="HTML"
    )

@dp.message(Command("scan"))
async def cmd_scan(message: types.Message):
    await message.answer("🔍 Сканирую по SMC...")
    signals = scan_market()
    if signals:
        await message.answer(f"⚡️ Найдено сигналов: {len(signals)}")
        for s in signals:
            await message.answer(s, parse_mode="HTML")
    else:
        await message.answer("😴 Чисто. Нет confluence 3+. Следующий скан через 30 мин.")

@dp.message(Command("market"))
async def cmd_market(message: types.Message):
    await message.answer("📊 Получаю живые данные...")
    market = get_market_overview()
    sentiment = search_market_sentiment()
    try:
        comment = ask_groq(
            f"Дай краткий комментарий (3 предложения) по рынку:\n{market}\nНовости: {sentiment[:400]}\nЧто важно трейдеру?",
            max_tokens=250
        )
    except:
        comment = "Анализ временно недоступен."
    await message.answer(
        f"📊 <b>Рынок сейчас</b>\n\n{market}\n\n💬 <b>APEX:</b> {comment}",
        parse_mode="HTML"
    )

@dp.message(Command("news"))
async def cmd_news(message: types.Message):
    await message.answer("📰 Собираю последние новости...")
    news = tavily_search("crypto bitcoin ethereum news today latest", max_results=5)
    try:
        summary = ask_groq(
            f"Сделай краткую выжимку главных новостей крипторынка (5-7 пунктов):\n{news[:1500]}",
            max_tokens=400
        )
    except:
        summary = news[:800]
    save_news("crypto news", summary)
    await message.answer(f"📰 <b>Новости крипторынка</b>\n\n{summary}", parse_mode="HTML")

@dp.message(Command("research"))
async def cmd_research(message: types.Message):
    args = message.text.split(maxsplit=1)
    coin = args[1] if len(args) > 1 else "bitcoin"
    await message.answer(f"🔎 Исследую {coin}...")

    # Живая цена
    price_data = get_coin_price(coin)
    price_info = ""
    if price_data:
        p = price_data["price"]
        c = price_data["change"]
        price_str = f"${p:,.2f}" if p >= 1 else f"${p:.5f}"
        price_info = f"Цена: {price_str} ({c:+.2f}% за 24ч)\n"

    # Поиск новостей
    news = search_crypto_news(coin)
    # Поиск аналитики
    analysis = search_youtube_analysis(coin)

    try:
        report = ask_groq(
            f"""Сделай полный анализ {coin} для трейдера:

{price_info}
НОВОСТИ:
{news[:800]}

АНАЛИТИКА:
{analysis[:600]}

Включи: текущая ситуация, ключевые уровни, прогноз, риски. Конкретно и по делу.""",
            max_tokens=600
        )
    except:
        report = "Анализ временно недоступен."

    save_knowledge(coin, report, "research")
    await message.answer(
        f"🔎 <b>Анализ {coin.upper()}</b>\n\n{price_info}{report}",
        parse_mode="HTML"
    )

@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    total, pending = get_signal_stats()
    profile = get_user_profile(message.from_user.id)
    await message.answer(
        f"📈 <b>Статистика APEX</b>\n\n"
        f"Сигналов всего: {total}\n"
        f"В ожидании: {pending}\n\n"
        f"👤 <b>Твой профиль:</b>\n{profile if profile else 'Пиши мне — накоплю данные о тебе'}",
        parse_mode="HTML"
    )

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    await message.answer(
        "🧠 <b>APEX умеет:</b>\n\n"
        "📊 /scan — SMC сигналы каждые 30 мин\n"
        "💹 /market — живые цены + AI комментарий\n"
        "📰 /news — последние новости рынка\n"
        "🔎 /research BTC — глубокий анализ монеты\n"
        "📈 /stats — статистика сигналов\n\n"
        "🤖 Каждые 2 часа сам ресёрчит рынок\n"
        "🧠 Помнит твои интересы и портфель\n\n"
        "Просто пиши — отвечу на всё 👇",
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
