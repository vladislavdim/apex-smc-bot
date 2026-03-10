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
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
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
    timeframe TEXT, estimated_hours INTEGER, grade TEXT,
    result TEXT DEFAULT 'pending',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    closed_at TEXT)""")

c.execute("""CREATE TABLE IF NOT EXISTS knowledge (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic TEXT, content TEXT, source TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

c.execute("""CREATE TABLE IF NOT EXISTS user_memory (
    user_id INTEGER PRIMARY KEY,
    name TEXT, profile TEXT, preferences TEXT,
    coins_mentioned TEXT, deposit REAL DEFAULT 0,
    risk_percent REAL DEFAULT 1.0,
    total_messages INTEGER DEFAULT 0,
    first_seen TEXT, last_seen TEXT)""")

c.execute("""CREATE TABLE IF NOT EXISTS chat_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER, role TEXT, content TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

c.execute("""CREATE TABLE IF NOT EXISTS news_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query TEXT, content TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

c.execute("""CREATE TABLE IF NOT EXISTS signal_learning (
    symbol TEXT PRIMARY KEY,
    total INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    avg_hours_to_tp REAL DEFAULT 0,
    best_timeframe TEXT,
    worst_timeframe TEXT,
    win_rate REAL DEFAULT 0,
    last_analysis TEXT)""")

# Дневник сделок пользователя
c.execute("""CREATE TABLE IF NOT EXISTS journal (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER, symbol TEXT, direction TEXT,
    entry REAL, exit_price REAL, result TEXT,
    note TEXT, pnl_percent REAL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

# Алерты на пробой уровней
c.execute("""CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER, symbol TEXT,
    price_level REAL, direction TEXT,
    triggered INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

conn.commit()
conn.close()
```

# ===== USER MEMORY =====

def get_user_memory(user_id):
try:
conn = sqlite3.connect(“brain.db”)
row = conn.execute(
“SELECT name, profile, preferences, coins_mentioned, total_messages, deposit, risk_percent FROM user_memory WHERE user_id=?”,
(user_id,)
).fetchone()
conn.close()
if row:
return {
“name”: row[0] or “”, “profile”: row[1] or “”,
“preferences”: row[2] or “”, “coins”: row[3] or “”,
“messages”: row[4] or 0, “deposit”: row[5] or 0,
“risk”: row[6] or 1.0
}
return {“name”: “”, “profile”: “”, “preferences”: “”, “coins”: “”, “messages”: 0, “deposit”: 0, “risk”: 1.0}
except:
return {“name”: “”, “profile”: “”, “preferences”: “”, “coins”: “”, “messages”: 0, “deposit”: 0, “risk”: 1.0}

def update_user_memory(user_id, name=””, profile=None, preferences=None, coins=None, deposit=None, risk=None):
try:
conn = sqlite3.connect(“brain.db”)
now = datetime.now().isoformat()
existing = conn.execute(“SELECT user_id FROM user_memory WHERE user_id=?”, (user_id,)).fetchone()
if existing:
updates = [“total_messages = total_messages + 1”, “last_seen = ?”]
params = [now]
if name:
updates.append(“name = ?”); params.append(name)
if profile:
updates.append(“profile = ?”); params.append(profile)
if preferences:
updates.append(“preferences = ?”); params.append(preferences)
if coins:
updates.append(“coins_mentioned = ?”); params.append(coins)
if deposit is not None:
updates.append(“deposit = ?”); params.append(deposit)
if risk is not None:
updates.append(“risk_percent = ?”); params.append(risk)
params.append(user_id)
conn.execute(f”UPDATE user_memory SET {’, ’.join(updates)} WHERE user_id=?”, params)
else:
conn.execute(
“INSERT INTO user_memory VALUES (?,?,?,?,?,?,1,?,?)”,
(user_id, name, profile or “”, preferences or “”, coins or “”, deposit or 0, now, now)
)
conn.commit()
conn.close()
except Exception as e:
logging.error(f”Memory error: {e}”)

def save_chat_log(user_id, role, content):
try:
conn = sqlite3.connect(“brain.db”)
conn.execute(“INSERT INTO chat_log VALUES (NULL,?,?,?,CURRENT_TIMESTAMP)”, (user_id, role, content[:2000]))
conn.commit()
conn.close()
except:
pass

def get_chat_history(user_id, limit=15):
try:
conn = sqlite3.connect(“brain.db”)
rows = conn.execute(
“SELECT role, content FROM chat_log WHERE user_id=? ORDER BY id DESC LIMIT ?”,
(user_id, limit)
).fetchall()
conn.close()
return list(reversed(rows))
except:
return []

def extract_and_save_profile(user_id, user_name, message, ai_response):
try:
mem = get_user_memory(user_id)
prompt = f””“Извлеки факты о трейдере из сообщения. Верни только JSON:
Текущий профиль: {mem[“profile”] or “пустой”}
Сообщение: {message}
{{“profile”: “1-2 предложения о стиле торговли”, “coins”: “монеты через запятую”, “preferences”: “таймфрейм, стиль, риск”}}”””
r = groq_client.chat.completions.create(
model=“llama-3.3-70b-versatile”,
messages=[{“role”: “user”, “content”: prompt}],
max_tokens=200
)
text = r.choices[0].message.content.strip()
start = text.find(”{”)
end = text.rfind(”}”) + 1
if start >= 0 and end > start:
data = json.loads(text[start:end])
update_user_memory(user_id, name=user_name,
profile=data.get(“profile”),
coins=data.get(“coins”),
preferences=data.get(“preferences”))
except Exception as e:
logging.error(f”Profile extract error: {e}”)
update_user_memory(user_id, name=user_name)

# ===== BINANCE DATA =====

BINANCE = “https://api.binance.com”
BINANCE_F = “https://fapi.binance.com”

PAIRS = [“BTCUSDT”, “ETHUSDT”, “SOLUSDT”, “BNBUSDT”,
“XRPUSDT”, “TONUSDT”, “DOGEUSDT”, “AVAXUSDT”,
“LINKUSDT”, “ARBUSDT”]

price_cache = {}
last_price_update = 0

def get_live_prices():
global price_cache, last_price_update
if time.time() - last_price_update < 20 and price_cache:
return price_cache
market = {}
for pair in PAIRS:
try:
r = requests.get(f”{BINANCE}/api/v3/ticker/24hr”, params={“symbol”: pair}, timeout=8)
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
f”{BINANCE_F}/fapi/v1/klines”,
params={“symbol”: symbol, “interval”: interval, “limit”: limit},
timeout=10
)
return [{“open”: float(c[1]), “high”: float(c[2]),
“low”: float(c[3]), “close”: float(c[4]),
“volume”: float(c[5])} for c in r.json()]
except:
# Fallback to spot
try:
r = requests.get(
f”{BINANCE}/api/v3/klines”,
params={“symbol”: symbol, “interval”: interval, “limit”: limit},
timeout=10
)
return [{“open”: float(c[1]), “high”: float(c[2]),
“low”: float(c[3]), “close”: float(c[4]),
“volume”: float(c[5])} for c in r.json()]
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
for i in range(len(candles) - 3, max(1, len(candles) - 20), -1):
if direction == “BULLISH” and candles[i+1][“low”] > candles[i-1][“high”]:
return {“top”: candles[i+1][“low”], “bottom”: candles[i-1][“high”]}
if direction == “BEARISH” and candles[i+1][“high”] < candles[i-1][“low”]:
return {“top”: candles[i-1][“low”], “bottom”: candles[i+1][“high”]}
return None

def smc_on_tf(symbol, interval):
“”“SMC анализ на одном таймфрейме — возвращает направление или None”””
candles = get_candles(symbol, interval, 150)
if len(candles) < 20:
return None
highs, lows = find_swings(candles)
classified = classify_swings(highs, lows)
events = detect_events(candles, classified)
if not events:
return None
return events[0][“direction”]  # BULLISH / BEARISH

# ===== МУЛЬТИТАЙМФРЕЙМНЫЙ АНАЛИЗ =====

TF_LABELS = {
“5m”: “5 мин”,
“15m”: “15 мин”,
“1h”: “1 час”,
“4h”: “4 часа”,
“1d”: “1 день”
}

TF_HOURS = {
“5m”: 1,
“15m”: 4,
“1h”: 12,
“4h”: 48,
“1d”: 120
}

def multi_tf_analysis(symbol, timeframes=None):
“””
Анализ по нескольким таймфреймам.
Возвращает направление, список совпадений и GRADE сигнала.
“””
if timeframes is None:
timeframes = [“15m”, “1h”, “4h”]

```
results = {}
for tf in timeframes:
    direction = smc_on_tf(symbol, tf)
    results[tf] = direction

bullish = [tf for tf, d in results.items() if d == "BULLISH"]
bearish = [tf for tf, d in results.items() if d == "BEARISH"]

total = len(timeframes)
if len(bullish) == total:
    direction = "BULLISH"
    matched = bullish
elif len(bearish) == total:
    direction = "BEARISH"
    matched = bearish
elif len(bullish) > len(bearish):
    direction = "BULLISH"
    matched = bullish
elif len(bearish) > len(bullish):
    direction = "BEARISH"
    matched = bearish
else:
    return None  # Нет ясности

match_count = len(matched)

# GRADE системы
if match_count == total and total >= 3:
    grade = "МЕГА ТОП"
    grade_emoji = "🔥🔥🔥"
    stars = "⭐⭐⭐⭐⭐"
elif match_count >= 3:
    grade = "ТОП СДЕЛКА"
    grade_emoji = "🔥🔥"
    stars = "⭐⭐⭐⭐"
elif match_count == 2:
    grade = "ХОРОШАЯ"
    grade_emoji = "✅"
    stars = "⭐⭐⭐"
else:
    grade = "СЛАБАЯ"
    grade_emoji = "⚠️"
    stars = "⭐⭐"

tf_status = ""
for tf in timeframes:
    d = results.get(tf)
    if d == "BULLISH":
        icon = "🟢"
    elif d == "BEARISH":
        icon = "🔴"
    else:
        icon = "⚪️"
    tf_status += f"{icon} {TF_LABELS.get(tf, tf)}: {d or 'нет сигнала'}\n"

return {
    "direction": direction,
    "matched": matched,
    "match_count": match_count,
    "total": total,
    "grade": grade,
    "grade_emoji": grade_emoji,
    "stars": stars,
    "tf_status": tf_status,
    "results": results
}
```

def full_scan(symbol, timeframe=“1h”):
“”“Полный SMC анализ с мультитаймфреймом”””
try:
# Мультитаймфрейм
mtf = multi_tf_analysis(symbol, [“15m”, “1h”, “4h”])
if not mtf:
return None

```
    direction = mtf["direction"]

    # Детальный вход на выбранном ТФ
    candles = get_candles(symbol, timeframe, 150)
    if len(candles) < 20:
        return None

    price = candles[-1]["close"]
    ob = find_ob(candles, direction)
    fvg = find_fvg(candles, direction)
    ob_data = get_orderbook(symbol)

    confluence = []
    confluence.append(f"✅ {mtf['match_count']}/{mtf['total']} таймфреймов совпали")
    if ob:
        confluence.append(f"✅ Order Block: {ob['bottom']:.4f}–{ob['top']:.4f}")
    if fvg:
        confluence.append(f"✅ FVG: {fvg['bottom']:.4f}–{fvg['top']:.4f}")
    if ob_data:
        match = (direction == "BULLISH" and ob_data["bias"] == "BUY") or \
                (direction == "BEARISH" and ob_data["bias"] == "SELL")
        if match:
            confluence.append(f"✅ OrderBook: {ob_data['bias']} PRESSURE")

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
    est_hours, confidence, win_rate = get_estimated_time(symbol, timeframe)
    if est_hours < 24:
        time_str = f"~{est_hours}ч"
    elif est_hours < 48:
        time_str = "~1-2 дня"
    else:
        time_str = f"~{est_hours // 24} дн"

    wr_str = f"{win_rate:.0f}% WR" if win_rate > 0 else "нет истории"

    # Сохраняем сигнал
    save_signal_db(symbol, direction, "MTF", entry, tp1, tp2, tp3, sl, timeframe, est_hours, mtf["grade"])

    emoji = "🟢" if direction == "BULLISH" else "🔴"
    conf_text = "\n".join(confluence)

    return (
        f"{'━'*26}\n"
        f"{mtf['grade_emoji']} <b>{mtf['grade']}</b>\n"
        f"{emoji} <b>{symbol}</b> — {direction}\n"
        f"{'━'*26}\n\n"
        f"📐 <b>Таймфреймы:</b>\n{mtf['tf_status']}\n"
        f"{mtf['stars']}\n\n"
        f"💰 <b>Вход:</b> <code>{entry:.4f}</code>\n"
        f"🛑 <b>Стоп:</b> <code>{sl:.4f}</code>\n"
        f"🎯 <b>TP1:</b> <code>{tp1:.4f}</code> (+2R)\n"
        f"🎯 <b>TP2:</b> <code>{tp2:.4f}</code> (+3R)\n"
        f"🎯 <b>TP3:</b> <code>{tp3:.4f}</code> (+5R)\n\n"
        f"⏱ <b>Время отработки:</b> {time_str}\n"
        f"📊 <b>Точность:</b> {wr_str} | {confidence}\n\n"
        f"📋 <b>Confluence:</b>\n{conf_text}\n"
        f"{'━'*26}"
    )
except Exception as e:
    logging.error(f"Scan error {symbol}: {e}")
    return None
```

def save_signal_db(symbol, direction, signal_type, entry, tp1, tp2, tp3, sl, timeframe, est_hours, grade):
try:
conn = sqlite3.connect(“brain.db”)
conn.execute(
“INSERT INTO signals VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,‘pending’,CURRENT_TIMESTAMP,NULL)”,
(symbol, direction, signal_type, entry, tp1, tp2, tp3, sl, timeframe, est_hours, grade)
)
conn.commit()
conn.close()
except Exception as e:
logging.error(f”Save signal error: {e}”)

# ===== САМООБУЧЕНИЕ =====

def get_estimated_time(symbol, timeframe):
try:
conn = sqlite3.connect(“brain.db”)
row = conn.execute(
“SELECT avg_hours_to_tp, win_rate, total FROM signal_learning WHERE symbol=?”,
(symbol,)
).fetchone()
conn.close()
base = TF_HOURS.get(timeframe, 24)
if row and row[0] and row[2] > 5:
wr = row[1]
confidence = “высокая” if wr > 60 else “средняя” if wr > 45 else “низкая”
return int(row[0]), confidence, wr
return base, “нет данных”, 0
except:
return 24, “нет данных”, 0

def check_pending_signals():
“”“Проверяем открытые сигналы — сработал ли TP/SL”””
try:
conn = sqlite3.connect(“brain.db”)
pending = conn.execute(
“SELECT id, symbol, direction, entry, tp1, tp2, tp3, sl, timeframe, grade, created_at FROM signals WHERE result=‘pending’”
).fetchall()
conn.close()

```
    closed = []
    for row in pending:
        sig_id, symbol, direction, entry, tp1, tp2, tp3, sl, timeframe, grade, created_at = row
        prices = get_live_prices()
        if symbol not in prices:
            continue
        current = prices[symbol]["price"]
        created = datetime.fromisoformat(created_at)
        hours_elapsed = (datetime.now() - created).total_seconds() / 3600

        result = None
        hit_tp = None
        if direction == "BULLISH":
            if current >= tp3:
                result, hit_tp = "tp3", 3
            elif current >= tp2:
                result, hit_tp = "tp2", 2
            elif current >= tp1:
                result, hit_tp = "tp1", 1
            elif current <= sl:
                result = "sl"
        else:
            if current <= tp3:
                result, hit_tp = "tp3", 3
            elif current <= tp2:
                result, hit_tp = "tp2", 2
            elif current <= tp1:
                result, hit_tp = "tp1", 1
            elif current >= sl:
                result = "sl"

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

            is_win = result in ("tp1", "tp2", "tp3")
            update_signal_learning(symbol, hours_elapsed, is_win, timeframe, result)

            # Анализ проигрышных сделок для самообучения
            if not is_win:
                asyncio.create_task(analyze_loss(symbol, direction, entry, sl, timeframe))

            closed.append({
                "signal_id": sig_id,
                "symbol": symbol,
                "result": result,
                "hours": round(hours_elapsed, 1),
                "grade": grade,
                "is_win": is_win
            })

    return closed
except Exception as e:
    logging.error(f"Check signals error: {e}")
    return []
```

async def analyze_loss(symbol, direction, entry, sl, timeframe):
“”“Анализирует проигрышную сделку и улучшает стратегию”””
try:
candles = get_candles(symbol, timeframe, 100)
if not candles:
return
price_now = candles[-1][“close”]
prompt = f””“Ты SMC трейдер. Разбери проигрышную сделку и дай вывод для улучшения стратегии:
Монета: {symbol}
Направление: {direction}
Вход: {entry}
Стоп: {sl}
Цена сейчас: {price_now}
Таймфрейм: {timeframe}

Что пошло не так? Как избежать в будущем? (2-3 предложения)”””
r = groq_client.chat.completions.create(
model=“llama-3.3-70b-versatile”,
messages=[{“role”: “user”, “content”: prompt}],
max_tokens=200
)
analysis = r.choices[0].message.content
save_knowledge(f”loss_analysis_{symbol}”, analysis, “self-learning”)
logging.info(f”Loss analysis saved for {symbol}: {analysis[:100]}”)
except Exception as e:
logging.error(f”Analyze loss error: {e}”)

def update_signal_learning(symbol, hours_to_close, is_win, timeframe, result):
try:
conn = sqlite3.connect(“brain.db”)
existing = conn.execute(
“SELECT total, wins, losses, avg_hours_to_tp FROM signal_learning WHERE symbol=?”,
(symbol,)
).fetchone()
now = datetime.now().isoformat()
if existing:
total = existing[0] + 1
wins = existing[1] + (1 if is_win else 0)
losses = existing[2] + (0 if is_win else 1)
avg_h = (existing[3] * existing[0] + hours_to_close) / total
wr = round(wins / total * 100, 1)
conn.execute(
“UPDATE signal_learning SET total=?, wins=?, losses=?, avg_hours_to_tp=?, win_rate=?, last_analysis=? WHERE symbol=?”,
(total, wins, losses, round(avg_h, 1), wr, now, symbol)
)
else:
conn.execute(
“INSERT INTO signal_learning VALUES (?,1,?,?,?,?,?,?,?)”,
(symbol, 1 if is_win else 0, 0 if is_win else 1,
float(hours_to_close), timeframe, None,
100.0 if is_win else 0.0, now)
)
conn.commit()
conn.close()
except Exception as e:
logging.error(f”Learning update error: {e}”)

# ===== BACKTESTING =====

def backtest(symbol, timeframe=“1h”, periods=500):
“”“Прогон SMC стратегии на исторических данных”””
try:
candles = get_candles(symbol, timeframe, periods)
if len(candles) < 100:
return None

```
    results = {"total": 0, "wins": 0, "losses": 0, "expired": 0}
    trades = []
    lookback = 50

    for i in range(lookback, len(candles) - 20):
        window = candles[i-lookback:i]
        highs, lows = find_swings(window)
        classified = classify_swings(highs, lows)
        events = detect_events(window, classified)

        if not events:
            continue

        event = events[0]
        direction = event["direction"]
        price = candles[i]["close"]
        risk = price * 0.015

        if direction == "BULLISH":
            entry = price
            sl = round(entry - risk, 4)
            tp1 = round(entry + risk * 2, 4)
        else:
            entry = price
            sl = round(entry + risk, 4)
            tp1 = round(entry - risk * 2, 4)

        # Проверяем следующие 20 свечей
        result = "expired"
        for j in range(i+1, min(i+21, len(candles))):
            c = candles[j]
            if direction == "BULLISH":
                if c["low"] <= sl:
                    result = "loss"
                    break
                if c["high"] >= tp1:
                    result = "win"
                    break
            else:
                if c["high"] >= sl:
                    result = "loss"
                    break
                if c["low"] <= tp1:
                    result = "win"
                    break

        results["total"] += 1
        if result == "win":
            results["wins"] += 1
        elif result == "loss":
            results["losses"] += 1
        else:
            results["expired"] += 1

        if len(trades) < 5:
            trades.append({"direction": direction, "entry": entry, "result": result})

    if results["total"] == 0:
        return None

    wr = round(results["wins"] / results["total"] * 100, 1)
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "total": results["total"],
        "wins": results["wins"],
        "losses": results["losses"],
        "win_rate": wr,
        "periods": periods,
        "trades": trades
    }
except Exception as e:
    logging.error(f"Backtest error: {e}")
    return None
```

# ===== РИСК КАЛЬКУЛЯТОР =====

def calc_risk(deposit, risk_percent, entry, sl):
“”“Считаем размер позиции”””
risk_amount = deposit * (risk_percent / 100)
sl_distance_pct = abs(entry - sl) / entry * 100
if sl_distance_pct == 0:
return None
position_size = risk_amount / (sl_distance_pct / 100)
leverage = round(position_size / deposit, 1)
return {
“risk_amount”: round(risk_amount, 2),
“position_size”: round(position_size, 2),
“sl_distance”: round(sl_distance_pct, 2),
“leverage”: min(leverage, 20),
“contracts”: round(position_size / entry, 4)
}

# ===== АЛЕРТЫ =====

async def check_alerts():
“”“Проверяем алерты каждые 5 минут”””
try:
conn = sqlite3.connect(“brain.db”)
alerts = conn.execute(
“SELECT id, user_id, symbol, price_level, direction FROM alerts WHERE triggered=0”
).fetchall()
conn.close()

```
    prices = get_live_prices()
    for alert_id, user_id, symbol, level, direction in alerts:
        if symbol not in prices:
            continue
        current = prices[symbol]["price"]
        triggered = False
        if direction == "above" and current >= level:
            triggered = True
        elif direction == "below" and current <= level:
            triggered = True

        if triggered:
            conn2 = sqlite3.connect("brain.db")
            conn2.execute("UPDATE alerts SET triggered=1 WHERE id=?", (alert_id,))
            conn2.commit()
            conn2.close()
            arrow = "⬆️" if direction == "above" else "⬇️"
            try:
                await bot.send_message(
                    user_id,
                    f"🔔 <b>АЛЕРТ СРАБОТАЛ!</b>\n\n"
                    f"{arrow} <b>{symbol}</b> достиг уровня <code>{level}</code>\n"
                    f"Текущая цена: <code>{current:.4f}</code>",
                    parse_mode="HTML"
                )
            except:
                pass
except Exception as e:
    logging.error(f"Alert check error: {e}")
```

# ===== TAVILY =====

def tavily_search(query, max_results=4):
if not TAVILY_KEY:
return “Поиск недоступен”
try:
r = requests.post(
“https://api.tavily.com/search”,
json={“api_key”: TAVILY_KEY, “query”: query, “max_results”: max_results, “include_answer”: True},
timeout=20
)
data = r.json()
results = []
if data.get(“answer”):
results.append(data[“answer”])
for item in data.get(“results”, []):
results.append(f”• {item.get(‘title’,’’)}: {item.get(‘content’,’’)[:200]}”)
return “\n\n”.join(results) if results else “Нет результатов”
except:
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
rows = conn.execute(“SELECT query, content FROM news_cache ORDER BY created_at DESC LIMIT 3”).fetchall()
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

# ===== AI =====

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
mem = get_user_memory(user_id)
history_rows = get_chat_history(user_id, limit=15)
knowledge = get_knowledge(user_message[:40])
recent_news = get_recent_news()
market = format_market()
now = datetime.now().strftime(”%Y-%m-%d %H:%M”)

```
history_text = ""
for row in history_rows:
    role_label = "Ты" if row[0] == "user" else "APEX"
    history_text += f"{role_label}: {row[1]}\n"

search_kw = ["новост", "почему", "что случилось", "прогноз", "сегодня", "упал", "вырос"]
search_results = ""
if any(kw in user_message.lower() for kw in search_kw):
    search_results = tavily_search(f"crypto {user_message} 2025", max_results=3)

user_context = ""
if mem["name"] or mem["profile"]:
    user_context = f"""ЧТО Я ЗНАЮ О ПОЛЬЗОВАТЕЛЕ:
```

Имя: {mem[“name”] or user_name}
Профиль: {mem[“profile”] or “нет”}
Предпочтения: {mem[“preferences”] or “нет”}
Монеты: {mem[“coins”] or “нет”}
Депозит: ${mem[“deposit”]} | Риск: {mem[“risk”]}%”””

```
prompt = f"""Ты APEX — дерзкий AI трейдер. Дата: {now}
```

{user_context}

ЖИВЫЕ ЦЕНЫ (Binance):
{market}

{f”ПОИСК:{chr(10)}{search_results}” if search_results else “”}
{f”НОВОСТИ:{chr(10)}{recent_news}” if recent_news and not search_results else “”}
{f”ЗНАНИЯ:{chr(10)}{knowledge}” if knowledge else “”}

ИСТОРИЯ:
{history_text}

ПРАВИЛА: Помни пользователя. Говори дерзко и кратко. Используй живые цены.

{user_name}: {user_message}
APEX:”””

```
return ask_groq(prompt, max_tokens=700)
```

# ===== KEYBOARDS =====

def main_menu():
return InlineKeyboardMarkup(inline_keyboard=[
[InlineKeyboardButton(text=“🔍 Сканировать рынок”, callback_data=“menu_scan”),
InlineKeyboardButton(text=“📊 Рынок сейчас”, callback_data=“menu_market”)],
[InlineKeyboardButton(text=“⏱ Выбрать таймфрейм”, callback_data=“menu_tf”),
InlineKeyboardButton(text=“🔬 Бектест”, callback_data=“menu_backtest”)],
[InlineKeyboardButton(text=“💰 Риск калькулятор”, callback_data=“menu_risk”),
InlineKeyboardButton(text=“📓 Дневник сделок”, callback_data=“menu_journal”)],
[InlineKeyboardButton(text=“🔔 Алерты”, callback_data=“menu_alerts”),
InlineKeyboardButton(text=“📈 Статистика”, callback_data=“menu_stats”)],
[InlineKeyboardButton(text=“📰 Новости”, callback_data=“menu_news”)]
])

def tf_keyboard():
return InlineKeyboardMarkup(inline_keyboard=[
[InlineKeyboardButton(text=“5 мин”, callback_data=“tf_5m”),
InlineKeyboardButton(text=“15 мин”, callback_data=“tf_15m”),
InlineKeyboardButton(text=“1 час”, callback_data=“tf_1h”)],
[InlineKeyboardButton(text=“4 часа”, callback_data=“tf_4h”),
InlineKeyboardButton(text=“1 день”, callback_data=“tf_1d”)],
[InlineKeyboardButton(text=“🔙 Назад”, callback_data=“menu_back”)]
])

def pairs_keyboard(action=“scan”):
buttons = []
row = []
for i, pair in enumerate(PAIRS):
row.append(InlineKeyboardButton(
text=pair.replace(“USDT”, “”),
callback_data=f”{action}_{pair}”
))
if len(row) == 3:
buttons.append(row)
row = []
if row:
buttons.append(row)
buttons.append([InlineKeyboardButton(text=“🔙 Назад”, callback_data=“menu_back”)])
return InlineKeyboardMarkup(inline_keyboard=buttons)

def backtest_tf_keyboard():
return InlineKeyboardMarkup(inline_keyboard=[
[InlineKeyboardButton(text=“15 мин”, callback_data=“bt_15m”),
InlineKeyboardButton(text=“1 час”, callback_data=“bt_1h”),
InlineKeyboardButton(text=“4 часа”, callback_data=“bt_4h”)],
[InlineKeyboardButton(text=“🔙 Назад”, callback_data=“menu_back”)]
])

# Хранилище состояний пользователей

user_states = {}

# ===== HANDLERS =====

@dp.message(Command(“start”))
async def cmd_start(message: types.Message):
user_id = message.from_user.id
name = message.from_user.first_name or “трейдер”
update_user_memory(user_id, name=name)
mem = get_user_memory(user_id)
greeting = f”С возвращением, {name}! 👊” if mem[“messages”] > 1 else f”Привет, {name}!”
await message.answer(
f”⚡️ <b>APEX — AI трейдер по SMC</b>\n\n{greeting}\n\nВыбирай что нужно 👇”,
parse_mode=“HTML”,
reply_markup=main_menu()
)

@dp.message(Command(“menu”))
async def cmd_menu(message: types.Message):
await message.answer(“Главное меню 👇”, reply_markup=main_menu())

@dp.message(Command(“scan”))
async def cmd_scan(message: types.Message):
await message.answer(“Выбери монету для скана:”, reply_markup=pairs_keyboard(“scan”))

@dp.message(Command(“backtest”))
async def cmd_backtest(message: types.Message):
args = message.text.split()
if len(args) == 3:
symbol = args[1].upper()
tf = args[2].lower()
await run_backtest(message, symbol, tf)
else:
await message.answer(
“Выбери таймфрейм для бектеста:\n(монета выбирается на следующем шаге)”,
reply_markup=backtest_tf_keyboard()
)

@dp.message(Command(“risk”))
async def cmd_risk(message: types.Message):
args = message.text.split()
mem = get_user_memory(message.from_user.id)
if len(args) == 2:
try:
deposit = float(args[1])
update_user_memory(message.from_user.id, deposit=deposit)
await message.answer(
f”✅ Депозит сохранён: <b>${deposit:,.2f}</b>\n\n”
f”Теперь при каждом сигнале я буду считать размер позиции.\n”
f”Риск на сделку: {mem[‘risk’]}%\n\n”
f”Изменить риск: /setrisk 2”,
parse_mode=“HTML”
)
except:
await message.answer(“Использование: /risk 1000 (размер депозита в $)”)
else:
deposit = mem[“deposit”]
if deposit > 0:
await message.answer(
f”💰 <b>Риск калькулятор</b>\n\n”
f”Твой депозит: <b>${deposit:,.2f}</b>\n”
f”Риск на сделку: <b>{mem[‘risk’]}%</b>\n”
f”Риск в $: <b>${deposit * mem[‘risk’] / 100:.2f}</b>\n\n”
f”Изменить депозит: /risk 5000\n”
f”Изменить риск %: /setrisk 2”,
parse_mode=“HTML”
)
else:
await message.answer(
“💰 <b>Риск калькулятор</b>\n\nУкажи свой депозит:\n/risk 1000”,
parse_mode=“HTML”
)

@dp.message(Command(“setrisk”))
async def cmd_setrisk(message: types.Message):
args = message.text.split()
if len(args) == 2:
try:
risk = float(args[1])
if 0.1 <= risk <= 10:
update_user_memory(message.from_user.id, risk=risk)
await message.answer(f”✅ Риск на сделку: <b>{risk}%</b>”, parse_mode=“HTML”)
else:
await message.answer(“Риск должен быть от 0.1% до 10%”)
except:
await message.answer(“Использование: /setrisk 2”)

@dp.message(Command(“alert”))
async def cmd_alert(message: types.Message):
args = message.text.split()
if len(args) == 3:
symbol = args[1].upper()
try:
level = float(args[2])
prices = get_live_prices()
current = prices.get(symbol, {}).get(“price”, 0)
direction = “above” if level > current else “below”
conn = sqlite3.connect(“brain.db”)
conn.execute(
“INSERT INTO alerts VALUES (NULL,?,?,?,?,0,CURRENT_TIMESTAMP)”,
(message.from_user.id, symbol, level, direction)
)
conn.commit()
conn.close()
arrow = “⬆️” if direction == “above” else “⬇️”
await message.answer(
f”🔔 Алерт установлен!\n{arrow} <b>{symbol}</b> → <code>{level}</code>\nТекущая цена: <code>{current:.4f}</code>”,
parse_mode=“HTML”
)
except:
await message.answer(“Использование: /alert BTCUSDT 70000”)
else:
await message.answer(
“🔔 <b>Алерты на пробой уровня</b>\n\nКогда цена достигает твоего уровня — пишу сразу.\n\nУстановить: /alert BTCUSDT 70000”,
parse_mode=“HTML”
)

@dp.message(Command(“journal”))
async def cmd_journal(message: types.Message):
args = message.text.split(maxsplit=1)
user_id = message.from_user.id

```
if len(args) == 1:
    # Показываем дневник
    conn = sqlite3.connect("brain.db")
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
        symbol = parts[0].upper()
        direction = parts[1].upper()
        entry = float(parts[2])
        exit_price = float(parts[3])
        result = parts[4].lower()
        note = " ".join(parts[5:]) if len(parts) > 5 else ""

        pnl = (exit_price - entry) / entry * 100
        if direction == "SHORT":
            pnl = -pnl

        conn = sqlite3.connect("brain.db")
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
    except:
        await message.answer(
            "Формат: /journal BTC LONG 65000 67000 win [заметка]\n"
            "Пример: /journal ETH SHORT 3200 3050 win взял на OB"
        )
```

@dp.message(Command(“stats”))
async def cmd_stats(message: types.Message):
user_id = message.from_user.id
mem = get_user_memory(user_id)
try:
conn = sqlite3.connect(“brain.db”)
total = conn.execute(“SELECT COUNT(*) FROM signals”).fetchone()[0]
wins = conn.execute(“SELECT COUNT(*) FROM signals WHERE result LIKE ‘tp%’”).fetchone()[0]
losses = conn.execute(“SELECT COUNT(*) FROM signals WHERE result=‘sl’”).fetchone()[0]
pending = conn.execute(“SELECT COUNT(*) FROM signals WHERE result=‘pending’”).fetchone()[0]
top = conn.execute(
“SELECT symbol, win_rate, total, avg_hours_to_tp FROM signal_learning ORDER BY win_rate DESC LIMIT 5”
).fetchall()
conn.close()
except:
total = wins = losses = pending = 0
top = []

```
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
```

async def run_backtest(target, symbol, timeframe):
“”“Запуск бектеста”””
send = target.message.answer if hasattr(target, “message”) else target.answer
await send(f”🔬 Запускаю бектест {symbol} {TF_LABELS.get(timeframe, timeframe)}…”)
result = backtest(symbol, timeframe)
if not result:
await send(“Недостаточно данных для бектеста”)
return

```
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
```

# ===== CALLBACK HANDLERS =====

@dp.callback_query()
async def handle_callback(callback: CallbackQuery):
data = callback.data
user_id = callback.from_user.id
await callback.answer()

```
if data == "menu_back":
    await callback.message.edit_text("Главное меню 👇", reply_markup=main_menu())

elif data == "menu_scan":
    await callback.message.edit_text("Выбери монету:", reply_markup=pairs_keyboard("scan"))

elif data == "menu_market":
    market = format_market()
    comment = ask_groq(f"Короткий комментарий по рынку (2 предложения):\n{market}", max_tokens=150)
    await callback.message.edit_text(
        f"📊 <b>Рынок (Binance)</b>\n\n{market}\n\n💬 {comment or ''}",
        parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
        ])
    )

elif data == "menu_tf":
    await callback.message.edit_text(
        "⏱ <b>Выбери таймфрейм для анализа</b>\n\nПосле выбора бот просканирует все монеты на этом ТФ:",
        parse_mode="HTML", reply_markup=tf_keyboard()
    )

elif data.startswith("tf_"):
    tf = data.replace("tf_", "")
    await callback.message.edit_text(f"🔍 Сканирую все монеты на {TF_LABELS.get(tf, tf)}...")
    found = []
    for symbol in PAIRS:
        try:
            candles = get_candles(symbol, tf, 100)
            if len(candles) < 20:
                continue
            highs, lows = find_swings(candles)
            classified = classify_swings(highs, lows)
            events = detect_events(candles, classified)
            if events:
                direction = events[0]["direction"]
                price = candles[-1]["close"]
                emoji = "🟢" if direction == "BULLISH" else "🔴"
                found.append(f"{emoji} {symbol.replace('USDT','')}: {direction} @ {price:.4f}")
            await asyncio.sleep(0.5)
        except:
            pass

    if found:
        text = f"⏱ <b>Сигналы на {TF_LABELS.get(tf, tf)}:</b>\n\n" + "\n".join(found)
    else:
        text = f"😴 На таймфрейме {TF_LABELS.get(tf, tf)} сигналов нет"

    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_tf")]
    ]))

elif data.startswith("scan_"):
    symbol = data.replace("scan_", "")
    await callback.message.edit_text(f"🔍 Анализирую {symbol}...")
    sig = full_scan(symbol)

    # Добавляем риск-расчёт если есть депозит
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
        await callback.message.edit_text(
            f"😴 {symbol} — нет сигнала\nНедостаточный confluence",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 К монетам", callback_data="menu_scan")]
            ])
        )

elif data == "menu_backtest":
    await callback.message.edit_text(
        "🔬 <b>Бектест стратегии</b>\n\nВыбери таймфрейм:",
        parse_mode="HTML", reply_markup=backtest_tf_keyboard()
    )

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
    mem = get_user_memory(user_id)
    try:
        conn = sqlite3.connect("brain.db")
        total = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
        wins = conn.execute("SELECT COUNT(*) FROM signals WHERE result LIKE 'tp%'").fetchone()[0]
        losses = conn.execute("SELECT COUNT(*) FROM signals WHERE result='sl'").fetchone()[0]
        conn.close()
    except:
        total = wins = losses = 0
    wr = round(wins / total * 100, 1) if total > 0 else 0
    await callback.message.edit_text(
        f"📈 <b>Статистика</b>\n\n"
        f"Сигналов: {total} | ✅ {wins} | ❌ {losses}\n"
        f"Win Rate: <b>{wr}%</b>",
        parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
        ])
    )

elif data == "menu_news":
    await callback.message.edit_text("📰 Собираю новости...")
    news = tavily_search("crypto bitcoin news today", max_results=4)
    summary = ask_groq(f"Выжимка новостей (5 пунктов):\n{news[:1200]}", max_tokens=350)
    save_news("crypto news", summary or news[:500])
    await callback.message.edit_text(
        f"📰 <b>Новости рынка</b>\n\n{summary or news[:800]}",
        parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
        ])
    )
```

# ===== TEXT HANDLER =====

@dp.message()
async def handle_text(message: types.Message):
user_id = message.from_user.id
user_name = message.from_user.first_name or “трейдер”
text = message.text
if not text:
return

```
# Проверяем состояние (ожидаем ввод монеты для бектеста)
if user_id in user_states:
    state = user_states.pop(user_id)
    if state.get("action") == "backtest":
        symbol = text.upper().replace("USDT", "") + "USDT"
        tf = state.get("tf", "1h")
        await message.answer(f"🔬 Запускаю бектест {symbol} {TF_LABELS.get(tf, tf)}...")
        result = backtest(symbol, tf)
        if result:
            grade = "🔥 Отличная" if result["win_rate"] >= 60 else "✅ Рабочая" if result["win_rate"] >= 50 else "⚠️ Слабая"
            await message.answer(
                f"🔬 <b>Бектест {symbol} [{TF_LABELS.get(tf, tf)}]</b>\n\n"
                f"Сигналов: {result['total']}\n"
                f"✅ Выигрыши: {result['wins']}\n"
                f"❌ Проигрыши: {result['losses']}\n"
                f"🎯 Win Rate: <b>{result['win_rate']}%</b>\n"
                f"Оценка: {grade}",
                parse_mode="HTML"
            )
        else:
            await message.answer("Недостаточно данных для бектеста")
        return

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
    asyncio.create_task(
        asyncio.to_thread(extract_and_save_profile, user_id, user_name, text, reply)
    )
else:
    await message.answer("⚡️ Перегружен, попробуй через минуту.")
```

# ===== AUTO TASKS =====

async def auto_research():
topics = [“bitcoin analysis today”, “crypto market today”, “altcoins 2025”]
for topic in topics:
try:
result = tavily_search(topic, max_results=3)
summary = ask_groq(f”Вывод для трейдера (2 предложения):\n{result[:600]}”, max_tokens=150)
if summary:
save_knowledge(topic, summary, “auto”)
save_news(topic, summary)
await asyncio.sleep(15)
except:
pass

async def auto_scan_job():
closed = check_pending_signals()
# Отправляем только выигрышные — проигрышные уходят в анализ тихо
for c in closed:
if c[“is_win”] and ADMIN_ID:
tp_icons = {“tp1”: “🎯”, “tp2”: “🎯🎯”, “tp3”: “🎯🎯🎯”}
icon = tp_icons.get(c[“result”], “✅”)
try:
await bot.send_message(
ADMIN_ID,
f”{icon} <b>{c[‘symbol’]}</b> — {c[‘result’].upper()}!\n”
f”⏱ Закрыто за {c[‘hours’]}ч\n”
f”Оценка сигнала: {c.get(‘grade’, ‘-’)}”,
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
        signals.append(sig)
    await asyncio.sleep(2)

if signals and ADMIN_ID:
    for sig in signals[:3]:
        try:
            await bot.send_message(ADMIN_ID, sig, parse_mode="HTML")
            await asyncio.sleep(1)
        except:
            pass
```

# ===== MAIN =====

async def main():
init_db()
threading.Thread(target=run_server, daemon=True).start()
scheduler = AsyncIOScheduler()
scheduler.add_job(auto_scan_job, “interval”, minutes=30)
scheduler.add_job(auto_research, “interval”, hours=2)
scheduler.add_job(check_alerts, “interval”, minutes=5)
scheduler.start()
logging.info(“APEX запущен!”)
await dp.start_polling(bot)

if **name** == “**main**”:
asyncio.run(main())
