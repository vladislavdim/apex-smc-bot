import asyncio
import logging
import os
import requests
import sqlite3
import threading
import time
import json
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler

from groq import Groq
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler

TOKEN = os.environ.get(“TELEGRAM_TOKEN”)
ADMIN_ID = int(os.environ.get(“ADMIN_ID”, “0”))
GROQ_KEY = os.environ.get(“GROQ_API_KEY”)
TAVILY_KEY = os.environ.get(“TAVILY_API_KEY”, “”)

bot = Bot(token=TOKEN)
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)
groq_client = Groq(api_key=GROQ_KEY)

# ===== HEALTH SERVER =====

class HealthHandler(BaseHTTPRequestHandler):
def do_GET(self):
self.send_response(200)
self.end_headers()
self.wfile.write(b”OK”)
def log_message(self, format, *args):
pass

def run_server():
server = HTTPServer((“0.0.0.0”, 10000), HealthHandler)
server.serve_forever()

# ===== DATABASE =====

def init_db():
conn = sqlite3.connect(“brain.db”)
c = conn.cursor()

```
c.execute("""CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT, direction TEXT, signal_type TEXT,
    entry REAL, tp1 REAL, tp2 REAL, tp3 REAL, sl REAL,
    timeframe TEXT, estimated_hours INTEGER,
    result TEXT DEFAULT 'pending',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    closed_at TEXT)""")

c.execute("""CREATE TABLE IF NOT EXISTS knowledge (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic TEXT, content TEXT, source TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

# Долгосрочная память пользователя — не теряется никогда
c.execute("""CREATE TABLE IF NOT EXISTS user_memory (
    user_id INTEGER PRIMARY KEY,
    name TEXT,
    profile TEXT,
    preferences TEXT,
    coins_mentioned TEXT,
    total_messages INTEGER DEFAULT 0,
    first_seen TEXT,
    last_seen TEXT)""")

# История диалогов — хранится долго
c.execute("""CREATE TABLE IF NOT EXISTS chat_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    role TEXT,
    content TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

c.execute("""CREATE TABLE IF NOT EXISTS news_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query TEXT, content TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

# Статистика по сигналам для самообучения
c.execute("""CREATE TABLE IF NOT EXISTS signal_learning (
    symbol TEXT PRIMARY KEY,
    total INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    avg_hours_to_tp REAL DEFAULT 0,
    best_timeframe TEXT,
    win_rate REAL DEFAULT 0)""")

conn.commit()
conn.close()
```

# ===== ПАМЯТЬ ПОЛЬЗОВАТЕЛЯ =====

def get_user_memory(user_id):
“”“Загружаем ВСЁ что знаем о пользователе”””
try:
conn = sqlite3.connect(“brain.db”)
row = conn.execute(
“SELECT name, profile, preferences, coins_mentioned, total_messages FROM user_memory WHERE user_id=?”,
(user_id,)
).fetchone()
conn.close()
if row:
return {
“name”: row[0] or “”,
“profile”: row[1] or “”,
“preferences”: row[2] or “”,
“coins”: row[3] or “”,
“messages”: row[4] or 0
}
return {“name”: “”, “profile”: “”, “preferences”: “”, “coins”: “”, “messages”: 0}
except:
return {“name”: “”, “profile”: “”, “preferences”: “”, “coins”: “”, “messages”: 0}

def update_user_memory(user_id, name=””, profile=None, preferences=None, coins=None):
“”“Обновляем память о пользователе”””
try:
conn = sqlite3.connect(“brain.db”)
now = datetime.now().isoformat()
existing = conn.execute(
“SELECT user_id FROM user_memory WHERE user_id=?”, (user_id,)
).fetchone()

```
    if existing:
        updates = ["total_messages = total_messages + 1", "last_seen = ?"]
        params = [now]
        if name:
            updates.append("name = ?")
            params.append(name)
        if profile:
            updates.append("profile = ?")
            params.append(profile)
        if preferences:
            updates.append("preferences = ?")
            params.append(preferences)
        if coins:
            updates.append("coins_mentioned = ?")
            params.append(coins)
        params.append(user_id)
        conn.execute(f"UPDATE user_memory SET {', '.join(updates)} WHERE user_id=?", params)
    else:
        conn.execute(
            "INSERT INTO user_memory VALUES (?,?,?,?,?,1,?,?)",
            (user_id, name, profile or "", preferences or "", coins or "", now, now)
        )
    conn.commit()
    conn.close()
except Exception as e:
    logging.error(f"Memory update error: {e}")
```

def save_chat_log(user_id, role, content):
“”“Сохраняем каждое сообщение навсегда”””
try:
conn = sqlite3.connect(“brain.db”)
conn.execute(
“INSERT INTO chat_log VALUES (NULL,?,?,?,CURRENT_TIMESTAMP)”,
(user_id, role, content[:2000])
)
conn.commit()
conn.close()
except:
pass

def get_chat_history(user_id, limit=20):
“”“Загружаем историю из БД”””
try:
conn = sqlite3.connect(“brain.db”)
rows = conn.execute(
“SELECT role, content, created_at FROM chat_log WHERE user_id=? ORDER BY id DESC LIMIT ?”,
(user_id, limit)
).fetchall()
conn.close()
return list(reversed(rows))
except:
return []

def extract_and_save_profile(user_id, user_name, message, ai_response):
“”“AI извлекает факты о пользователе и сохраняет”””
try:
mem = get_user_memory(user_id)
prompt = f””“Ты анализируешь сообщение трейдера и извлекаешь факты о нём.

Текущий профиль: {mem[‘profile’] or ‘пустой’}
Известные монеты интереса: {mem[‘coins’] or ‘нет’}
Известные предпочтения: {mem[‘preferences’] or ‘нет’}

Новое сообщение пользователя: {message}

Верни JSON (только JSON, без пояснений):
{{
“profile”: “обновлённый профиль в 1-2 предложения (стиль торговли, опыт, цели)”,
“coins”: “список монет через запятую которые упомянул или интересуют”,
“preferences”: “предпочтения (таймфрейм, стиль, риск-менеджмент)”
}}

Если в сообщении нет новой информации, верни текущие значения.”””

```
    r = groq_client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=300
    )
    text = r.choices[0].message.content.strip()
    # Парсим JSON
    start = text.find("{")
    end = text.rfind("}") + 1
    if start >= 0 and end > start:
        data = json.loads(text[start:end])
        update_user_memory(
            user_id,
            name=user_name,
            profile=data.get("profile"),
            coins=data.get("coins"),
            preferences=data.get("preferences")
        )
except Exception as e:
    logging.error(f"Profile extract error: {e}")
    update_user_memory(user_id, name=user_name)
```

# ===== SIGNAL LEARNING =====

def get_estimated_time(symbol, signal_type, timeframe):
“”“Предсказываем время отработки на основе истории”””
try:
conn = sqlite3.connect(“brain.db”)
row = conn.execute(
“SELECT avg_hours_to_tp, win_rate, total FROM signal_learning WHERE symbol=?”,
(symbol,)
).fetchone()
conn.close()

```
    # Базовые оценки по таймфреймам
    base_hours = {
        "15m": 4,
        "1h": 12,
        "4h": 48,
        "1d": 120
    }
    base = base_hours.get(timeframe, 24)

    if row and row[0] and row[2] > 5:
        avg = row[0]
        win_rate = row[1]
        confidence = "высокая" if win_rate > 60 else "средняя" if win_rate > 45 else "низкая"
        return int(avg), confidence, win_rate
    else:
        confidence = "нет данных (первый сигнал)"
        return base, confidence, 0
except:
    return 24, "нет данных", 0
```

def update_signal_learning(symbol, hours_to_close, is_win, timeframe):
“”“Обновляем статистику после закрытия сигнала”””
try:
conn = sqlite3.connect(“brain.db”)
existing = conn.execute(
“SELECT total, wins, avg_hours_to_tp FROM signal_learning WHERE symbol=?”,
(symbol,)
).fetchone()

```
    if existing:
        total = existing[0] + 1
        wins = existing[1] + (1 if is_win else 0)
        # Скользящее среднее времени
        avg_h = (existing[2] * existing[0] + hours_to_close) / total
        win_rate = round(wins / total * 100, 1)
        conn.execute(
            "UPDATE signal_learning SET total=?, wins=?, avg_hours_to_tp=?, win_rate=?, best_timeframe=? WHERE symbol=?",
            (total, wins, round(avg_h, 1), win_rate, timeframe, symbol)
        )
    else:
        conn.execute(
            "INSERT INTO signal_learning VALUES (?,1,?,?,?,?)",
            (symbol, 1 if is_win else 0, float(hours_to_close), timeframe,
             100.0 if is_win else 0.0)
        )
    conn.commit()
    conn.close()
except Exception as e:
    logging.error(f"Learning update error: {e}")
```

def check_pending_signals():
“”“Проверяем открытые сигналы — закрылись ли”””
try:
conn = sqlite3.connect(“brain.db”)
pending = conn.execute(
“SELECT id, symbol, direction, entry, tp1, sl, created_at FROM signals WHERE result=‘pending’”
).fetchall()
conn.close()

```
    closed = []
    for row in pending:
        sig_id, symbol, direction, entry, tp1, sl, created_at = row
        prices = get_live_prices()
        if symbol not in prices:
            continue
        current = prices[symbol]["price"]
        created = datetime.fromisoformat(created_at)
        hours_elapsed = (datetime.now() - created).total_seconds() / 3600

        result = None
        if direction == "BULLISH":
            if current >= tp1:
                result = "tp1"
            elif current <= sl:
                result = "sl"
        else:
            if current <= tp1:
                result = "tp1"
            elif current >= sl:
                result = "sl"

        # Экспайри через 72 часа
        if not result and hours_elapsed > 72:
            result = "expired"

        if result:
            conn2 = sqlite3.connect("brain.db")
            conn2.execute(
                "UPDATE signals SET result=?, closed_at=CURRENT_TIMESTAMP WHERE id=?",
                (result, sig_id)
            )
            conn2.commit()
            conn2.close()
            is_win = result == "tp1"
            update_signal_learning(symbol, hours_elapsed, is_win, "1h")
            closed.append({"symbol": symbol, "result": result, "hours": round(hours_elapsed, 1)})

    return closed
except Exception as e:
    logging.error(f"Check signals error: {e}")
    return []
```

# ===== BINANCE DATA =====

BINANCE = “https://api.binance.com”
BINANCE_FUTURES = “https://fapi.binance.com”

PAIRS = [
“BTCUSDT”, “ETHUSDT”, “SOLUSDT”, “BNBUSDT”,
“XRPUSDT”, “TONUSDT”, “DOGEUSDT”, “AVAXUSDT”,
“LINKUSDT”, “ARBUSDT”
]

price_cache = {}
last_price_update = 0

def get_live_prices():
global price_cache, last_price_update
if time.time() - last_price_update < 20 and price_cache:
return price_cache
market = {}
for pair in PAIRS:
try:
r = requests.get(
f”{BINANCE}/api/v3/ticker/24hr”,
params={“symbol”: pair},
timeout=8
)
d = r.json()
market[pair] = {
“price”: float(d[“lastPrice”]),
“change”: float(d[“priceChangePercent”]),
“volume”: float(d[“quoteVolume”])
}
except:
pass
price_cache = market
last_price_update = time.time()
return market

def format_market():
market = get_live_prices()
if not market:
return “Данные недоступны”
lines = []
for pair, d in market.items():
emoji = “🟢” if d[“change”] >= 0 else “🔴”
p = d[“price”]
ps = f”${p:,.2f}” if p >= 1000 else f”${p:.3f}” if p >= 1 else f”${p:.6f}”
lines.append(f”{emoji} {pair.replace(‘USDT’,’’)}: {ps} ({d[‘change’]:+.2f}%)”)
return “\n”.join(lines)

def get_candles(symbol, interval=“1h”, limit=150):
try:
r = requests.get(
f”{BINANCE_FUTURES}/fapi/v1/klines”,
params={“symbol”: symbol, “interval”: interval, “limit”: limit},
timeout=10
)
return [{
“open”: float(c[1]), “high”: float(c[2]),
“low”: float(c[3]), “close”: float(c[4]),
“volume”: float(c[5])
} for c in r.json()]
except:
return []

def get_orderbook(symbol):
try:
r = requests.get(f”{BINANCE}/api/v3/depth”, params={“symbol”: symbol, “limit”: 20}, timeout=8)
d = r.json()
bids = sum(float(b[1]) for b in d[“bids”])
asks = sum(float(a[1]) for a in d[“asks”])
return {“bids”: bids, “asks”: asks, “bias”: “BUY” if bids > asks else “SELL”}
except:
return None

# ===== SMC ENGINE =====

def find_swings(candles, lookback=5):
highs, lows = [], []
for i in range(lookback, len(candles) - lookback):
wh = [c[“high”] for c in candles[i-lookback:i+lookback+1]]
wl = [c[“low”] for c in candles[i-lookback:i+lookback+1]]
if candles[i][“high”] == max(wh):
highs.append((i, candles[i][“high”]))
if candles[i][“low”] == min(wl):
lows.append((i, candles[i][“low”]))
return highs, lows

def classify_swings(highs, lows):
result = []
for i, (idx, price) in enumerate(highs):
kind = “HH” if i == 0 or price > highs[i-1][1] else “LH”
result.append({“idx”: idx, “price”: price, “kind”: kind})
for i, (idx, price) in enumerate(lows):
kind = “HL” if i == 0 or price > lows[i-1][1] else “LL”
result.append({“idx”: idx, “price”: price, “kind”: kind})
return sorted(result, key=lambda x: x[“idx”])

def detect_events(candles, classified):
last_close = candles[-1][“close”]
events = []
highs = [s for s in classified if s[“kind”] in (“HH”, “LH”)]
lows = [s for s in classified if s[“kind”] in (“HL”, “LL”)]
if highs and last_close > highs[-1][“price”]:
etype = “CHoCH” if highs[-1][“kind”] == “LH” else “BOS”
events.append({“type”: etype, “direction”: “BULLISH”, “level”: highs[-1][“price”]})
if lows and last_close < lows[-1][“price”]:
etype = “CHoCH” if lows[-1][“kind”] == “HL” else “BOS”
events.append({“type”: etype, “direction”: “BEARISH”, “level”: lows[-1][“price”]})
return events

def find_ob(candles, direction):
for i in range(len(candles) - 2, max(0, len(candles) - 25), -1):
c = candles[i]
if direction == “BULLISH” and c[“close”] < c[“open”]:
return {“top”: max(c[“open”], c[“close”]), “bottom”: min(c[“open”], c[“close”])}
if direction == “BEARISH” and c[“close”] > c[“open”]:
return {“top”: max(c[“open”], c[“close”]), “bottom”: min(c[“open”], c[“close”])}
return None

def find_fvg(candles, direction):
for i in range(len(candles) - 3, len(candles) - 20, -1):
if i < 1:
break
if direction == “BULLISH” and candles[i+1][“low”] > candles[i-1][“high”]:
return {“top”: candles[i+1][“low”], “bottom”: candles[i-1][“high”]}
if direction == “BEARISH” and candles[i+1][“high”] < candles[i-1][“low”]:
return {“top”: candles[i-1][“low”], “bottom”: candles[i+1][“high”]}
return None

def full_scan(symbol):
“”“Полный SMC анализ: 1H структура + 15m вход”””
try:
c1h = get_candles(symbol, “1h”, 100)
c15m = get_candles(symbol, “15m”, 150)
if len(c1h) < 20:
return None

```
    price = c1h[-1]["close"]
    highs, lows = find_swings(c1h)
    classified = classify_swings(highs, lows)
    events = detect_events(c1h, classified)

    if not events:
        return None

    event = events[0]
    direction = event["direction"]

    # Вход на 15m
    ob = find_ob(c15m if c15m else c1h, direction)
    fvg = find_fvg(c15m if c15m else c1h, direction)
    ob_data = get_orderbook(symbol)

    # Confluence
    confluence = [f"✅ {event['type']} на 1H"]
    if ob:
        confluence.append(f"✅ Order Block: {ob['bottom']:.4f}–{ob['top']:.4f}")
    if fvg:
        confluence.append(f"✅ FVG: {fvg['bottom']:.4f}–{fvg['top']:.4f}")
    if ob_data:
        match = (direction == "BULLISH" and ob_data["bias"] == "BUY") or \
                (direction == "BEARISH" and ob_data["bias"] == "SELL")
        if match:
            confluence.append(f"✅ OrderBook подтверждает {ob_data['bias']}")

    if len(confluence) < 2:
        return None

    # Уровни
    risk = price * 0.015
    if direction == "BULLISH":
        entry = ob["top"] if ob else price
        sl = round(entry - risk, 4)
        tp1 = round(entry + risk * 2, 4)
        tp2 = round(entry + risk * 3, 4)
        tp3 = round(entry + risk * 5, 4)
    else:
        entry = ob["bottom"] if ob else price
        sl = round(entry + risk, 4)
        tp1 = round(entry - risk * 2, 4)
        tp2 = round(entry - risk * 3, 4)
        tp3 = round(entry - risk * 5, 4)

    # Время отработки
    est_hours, confidence, win_rate = get_estimated_time(symbol, event["type"], "1h")
    if est_hours < 24:
        time_str = f"~{est_hours}ч"
    elif est_hours < 48:
        time_str = "~1-2 дня"
    else:
        time_str = f"~{est_hours // 24} дн"

    wr_str = f"{win_rate:.0f}% WR" if win_rate > 0 else "нет истории"

    save_signal(symbol, direction, event["type"], entry, tp1, tp2, tp3, sl)

    emoji = "🟢" if direction == "BULLISH" else "🔴"
    stars = "⭐" * min(5, len(confluence))
    conf_text = "\n".join(confluence)

    return (
        f"{'━'*26}\n"
        f"{emoji} <b>{symbol}</b> — {direction} [1H+15m]\n"
        f"{'━'*26}\n\n"
        f"📐 <b>Сетап:</b> {event['type']}  {stars}\n\n"
        f"💰 <b>Вход:</b> <code>{entry:.4f}</code>\n"
        f"🛑 <b>Стоп:</b> <code>{sl:.4f}</code>\n"
        f"🎯 <b>TP1:</b> <code>{tp1:.4f}</code> (+2R)\n"
        f"🎯 <b>TP2:</b> <code>{tp2:.4f}</code> (+3R)\n"
        f"🎯 <b>TP3:</b> <code>{tp3:.4f}</code> (+5R)\n\n"
        f"⏱ <b>Время отработки:</b> {time_str}\n"
        f"📊 <b>Точность по истории:</b> {wr_str}\n"
        f"🎯 <b>Уверенность:</b> {confidence}\n\n"
        f"📋 <b>Confluence ({len(confluence)}):</b>\n{conf_text}\n"
        f"{'━'*26}"
    )
except Exception as e:
    logging.error(f"Scan error {symbol}: {e}")
    return None
```

# ===== TAVILY SEARCH =====

def tavily_search(query, max_results=4):
if not TAVILY_KEY:
return “Поиск недоступен (нет TAVILY_API_KEY)”
try:
r = requests.post(
“https://api.tavily.com/search”,
json={
“api_key”: TAVILY_KEY,
“query”: query,
“search_depth”: “advanced”,
“max_results”: max_results,
“include_answer”: True
},
timeout=20
)
data = r.json()
results = []
if data.get(“answer”):
results.append(data[“answer”])
for item in data.get(“results”, []):
results.append(f”• {item.get(‘title’,’’)}: {item.get(‘content’,’’)[:200]}”)
return “\n\n”.join(results) if results else “Нет результатов”
except Exception as e:
logging.error(f”Tavily error: {e}”)
return “Поиск временно недоступен”

def save_news(query, content):
try:
conn = sqlite3.connect(“brain.db”)
conn.execute(“INSERT INTO news_cache VALUES (NULL,?,?,CURRENT_TIMESTAMP)”, (query, content[:1000]))
conn.commit()
conn.close()
except:
pass

def get_recent_news():
try:
conn = sqlite3.connect(“brain.db”)
rows = conn.execute(
“SELECT query, content FROM news_cache ORDER BY created_at DESC LIMIT 3”
).fetchall()
conn.close()
return “\n\n”.join([f”{r[0]}: {r[1]}” for r in rows])
except:
return “”

def save_knowledge(topic, content, source=“auto”):
try:
conn = sqlite3.connect(“brain.db”)
conn.execute(“INSERT INTO knowledge VALUES (NULL,?,?,?,CURRENT_TIMESTAMP)”, (topic, content, source))
conn.commit()
conn.close()
except:
pass

def get_knowledge(topic):
try:
conn = sqlite3.connect(“brain.db”)
rows = conn.execute(
“SELECT content FROM knowledge WHERE topic LIKE ? ORDER BY created_at DESC LIMIT 3”,
(f”%{topic}%”,)
).fetchall()
conn.close()
return “\n”.join([r[0] for r in rows])
except:
return “”

# ===== AI BRAIN =====

def ask_groq(prompt, max_tokens=800):
try:
r = groq_client.chat.completions.create(
model=“llama-3.3-70b-versatile”,
messages=[{“role”: “user”, “content”: prompt}],
max_tokens=max_tokens
)
return r.choices[0].message.content
except Exception as e:
logging.error(f”Groq error: {e}”)
return None

def ask_ai(user_id, user_name, user_message):
# Загружаем ВСЁ из БД
mem = get_user_memory(user_id)
history_rows = get_chat_history(user_id, limit=15)
knowledge = get_knowledge(user_message[:40])
recent_news = get_recent_news()
market = format_market()
now = datetime.now().strftime(”%Y-%m-%d %H:%M”)

```
# История в текст
history_text = ""
for row in history_rows:
    role_label = "Ты" if row[0] == "user" else "APEX"
    history_text += f"{role_label}: {row[1]}\n"

# Нужен ли поиск
search_kw = ["новост", "почему", "что случилось", "аналитик", "прогноз",
             "сегодня", "сейчас", "упал", "вырос", "новини"]
search_results = ""
if any(kw in user_message.lower() for kw in search_kw):
    search_results = tavily_search(f"crypto {user_message} 2025", max_results=3)

# Строим контекст о пользователе
user_context = ""
if mem["name"] or mem["profile"]:
    user_context = f"""
```

ЧТО Я ЗНАЮ ОБ ЭТОМ ПОЛЬЗОВАТЕЛЕ (запомни и используй!):
Имя: {mem[‘name’] or user_name}
Профиль: {mem[‘profile’] or ‘нет данных’}
Предпочтения: {mem[‘preferences’] or ‘нет данных’}
Монеты интереса: {mem[‘coins’] or ‘нет данных’}
Сообщений со мной: {mem[‘messages’]}
“””

```
prompt = f"""Ты APEX — дерзкий AI трейдер. Дата: {now}
```

{user_context}

ЖИВЫЕ ЦЕНЫ (Binance):
{market}

{f”ПОИСК В ИНТЕРНЕТЕ:{chr(10)}{search_results}” if search_results else “”}
{f”ПОСЛЕДНИЕ НОВОСТИ:{chr(10)}{recent_news}” if recent_news and not search_results else “”}
{f”МОИ ЗНАНИЯ:{chr(10)}{knowledge}” if knowledge else “”}

ИСТОРИЯ НАШЕГО РАЗГОВОРА:
{history_text}

ПРАВИЛА:

- Помни кто этот пользователь — используй его имя и историю
- Говори дерзко и по делу, без воды
- Используй живые цены, не придумывай
- Если спрашивают про сигнал — говори /scan
- Давай конкретные уровни при анализе

{user_name}: {user_message}
APEX:”””

```
return ask_groq(prompt, max_tokens=700)
```

# ===== AUTO TASKS =====

async def auto_research():
“”“Каждые 2 часа собирает знания”””
topics = [
“bitcoin price analysis today”,
“crypto market sentiment today”,
“ethereum news today”,
“altcoins to watch 2025”
]
for topic in topics:
try:
result = tavily_search(topic, max_results=3)
summary = ask_groq(
f”Сделай вывод для трейдера (2-3 предложения):\nТема: {topic}\nДанные: {result[:600]}”,
max_tokens=150
)
if summary:
save_knowledge(topic, summary, “tavily-auto”)
save_news(topic, summary)
await asyncio.sleep(15)
except Exception as e:
logging.error(f”Research error: {e}”)

async def auto_scan_job():
“”“Каждые 30 минут сканирует рынок и закрывает сигналы”””
# Проверяем старые сигналы
closed = check_pending_signals()
for c in closed:
if ADMIN_ID:
result_emoji = “✅” if c[“result”] == “tp1” else “❌” if c[“result”] == “sl” else “⏰”
result_text = {“tp1”: “TP1 достигнут!”, “sl”: “Стоп сработал”, “expired”: “Истёк”}.get(c[“result”], c[“result”])
try:
await bot.send_message(
ADMIN_ID,
f”{result_emoji} <b>{c[‘symbol’]}</b> — {result_text}\n”
f”⏱ Время в позиции: {c[‘hours’]}ч\n”
f”🧠 Данные сохранены для обучения”,
parse_mode=“HTML”
)
except:
pass

```
# Новые сигналы
signals = []
for symbol in PAIRS:
    sig = full_scan(symbol)
    if sig:
        signals.append((symbol, sig))
    await asyncio.sleep(2)

if signals and ADMIN_ID:
    await bot.send_message(ADMIN_ID, f"⚡️ Найдено сигналов: {len(signals)}", parse_mode="HTML")
    for symbol, sig in signals[:3]:
        try:
            await bot.send_message(ADMIN_ID, sig, parse_mode="HTML")
            await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Send error: {e}")
```

# ===== HANDLERS =====

@dp.message(Command(“start”))
async def cmd_start(message: types.Message):
user_id = message.from_user.id
name = message.from_user.first_name or “трейдер”
update_user_memory(user_id, name=name)
mem = get_user_memory(user_id)

```
greeting = f"С возвращением, {name}! 👊" if mem["messages"] > 1 else f"Привет, {name}!"

await message.answer(
    f"⚡️ <b>APEX — AI трейдер по SMC</b>\n\n"
    f"{greeting}\n\n"
    f"🔍 /scan — SMC сигналы (Binance Futures)\n"
    f"📊 /market — живые цены\n"
    f"📰 /news — новости крипты\n"
    f"🔎 /research BTC — глубокий анализ\n"
    f"📈 /stats — статистика + моя история\n\n"
    f"Или просто пиши — помню всё о тебе 🧠",
    parse_mode="HTML"
)
```

@dp.message(Command(“scan”))
async def cmd_scan(message: types.Message):
await message.answer(“🔍 Сканирую рынок по SMC…”)
found = []
for symbol in PAIRS[:6]:
sig = full_scan(symbol)
if sig:
found.append(sig)
await asyncio.sleep(1)

```
if found:
    await message.answer(f"⚡️ Найдено: {len(found)} сигналов")
    for sig in found:
        await message.answer(sig, parse_mode="HTML")
        await asyncio.sleep(0.5)
else:
    await message.answer(
        "😴 Чистый рынок — нет достаточного confluence.\n"
        "Следующий авто-скан через 30 мин."
    )
```

@dp.message(Command(“market”))
async def cmd_market(message: types.Message):
await message.answer(“📊 Получаю данные Binance…”)
market = format_market()
comment = ask_groq(
f”Дай короткий комментарий по рынку (2 предложения, по делу):\n{market}”,
max_tokens=150
)
await message.answer(
f”📊 <b>Рынок сейчас (Binance)</b>\n\n{market}\n\n”
f”💬 <b>APEX:</b> {comment or ‘Анализирую…’}”,
parse_mode=“HTML”
)

@dp.message(Command(“news”))
async def cmd_news(message: types.Message):
await message.answer(“📰 Ищу новости…”)
news = tavily_search(“crypto bitcoin news today latest”, max_results=5)
summary = ask_groq(
f”Сделай выжимку главных новостей (5 пунктов):\n{news[:1500]}”,
max_tokens=400
)
save_news(“crypto news”, summary or news[:500])
await message.answer(
f”📰 <b>Новости рынка</b>\n\n{summary or news[:800]}”,
parse_mode=“HTML”
)

@dp.message(Command(“research”))
async def cmd_research(message: types.Message):
args = message.text.split(maxsplit=1)
coin = args[1].strip() if len(args) > 1 else “bitcoin”
await message.answer(f”🔎 Исследую {coin.upper()}…”)

```
prices = get_live_prices()
symbol = coin.upper() + "USDT"
price_info = ""
if symbol in prices:
    p = prices[symbol]
    ps = f"${p['price']:,.4f}"
    price_info = f"Цена: {ps} ({p['change']:+.2f}% за 24ч)\n"

news = tavily_search(f"{coin} crypto news analysis today 2025", max_results=4)
est_hours, confidence, win_rate = get_estimated_time(symbol, "BOS", "1h")

report = ask_groq(
    f"""Полный анализ {coin} для трейдера:
```

{price_info}
НОВОСТИ И АНАЛИТИКА:
{news[:1000]}

Включи: текущая ситуация, ключевые уровни поддержки/сопротивления, прогноз на 1-7 дней, риски. Конкретно.”””,
max_tokens=600
)
save_knowledge(coin, report or “”, “research”)

```
time_note = ""
if win_rate > 0:
    time_note = f"\n\n📊 <b>История сигналов по {symbol}:</b> {win_rate:.0f}% WR, avg {est_hours}ч до TP"

await message.answer(
    f"🔎 <b>Анализ {coin.upper()}</b>\n\n{price_info}{report or 'Анализ временно недоступен.'}{time_note}",
    parse_mode="HTML"
)
```

@dp.message(Command(“stats”))
async def cmd_stats(message: types.Message):
user_id = message.from_user.id
mem = get_user_memory(user_id)

```
try:
    conn = sqlite3.connect("brain.db")
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
top_text = "\n".join([f"• {r[0]}: {r[1]:.0f}% WR, avg {r[3]:.0f}ч, {r[2]} сигналов" for r in top]) or "Нет данных"

profile_text = ""
if mem["profile"]:
    profile_text = f"\n\n👤 <b>Что я о тебе знаю:</b>\n{mem['profile']}"
    if mem["coins"]:
        profile_text += f"\n💎 Интересные монеты: {mem['coins']}"
    if mem["preferences"]:
        profile_text += f"\n⚙️ Стиль: {mem['preferences']}"

await message.answer(
    f"📈 <b>Статистика APEX</b>\n\n"
    f"Сигналов всего: {total}\n"
    f"✅ Выигрыши: {wins} | ❌ Стопы: {losses} | ⏳ Открыто: {pending}\n"
    f"🎯 Win Rate: {wr}%\n\n"
    f"🏆 <b>Топ монеты по WR:</b>\n{top_text}"
    f"{profile_text}",
    parse_mode="HTML"
)
```

@dp.message()
async def handle_text(message: types.Message):
user_id = message.from_user.id
user_name = message.from_user.first_name or “трейдер”
text = message.text
if not text:
return

```
# Сохраняем сообщение в БД
save_chat_log(user_id, "user", text)

thinking = await message.answer("⚡️")
reply = ask_ai(user_id, user_name, text)

try:
    await thinking.delete()
except:
    pass

if reply:
    save_chat_log(user_id, "assistant", reply)
    await message.answer(reply)
    # Обновляем профиль в фоне
    asyncio.create_task(
        asyncio.to_thread(extract_and_save_profile, user_id, user_name, text, reply)
    )
else:
    await message.answer("⚡️ Перегружен, попробуй через минуту.")
```

# ===== MAIN =====

async def main():
init_db()
threading.Thread(target=run_server, daemon=True).start()

```
scheduler = AsyncIOScheduler()
scheduler.add_job(auto_scan_job, "interval", minutes=30)
scheduler.add_job(auto_research, "interval", hours=2)
scheduler.start()

logging.info("⚡ APEX запущен!")
await dp.start_polling(bot)
```

if **name** == “**main**”:
asyncio.run(main())
