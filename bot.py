import asyncio
import logging
import os
import requests
import sqlite3
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import google.generativeai as genai
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler

TOKEN = os.environ.get("TELEGRAM_TOKEN")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
GEMINI_KEY = os.environ.get("GEMINI_API_KEY")

bot = Bot(token=TOKEN)
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)

genai.configure(api_key=GEMINI_KEY)
model = genai.GenerativeModel("gemini-2.0-flash")

chat_history = {}

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

BYBIT_URL = "https://api.bybit.com/v5/market/kline"

def get_klines(symbol, interval="60", limit=100):
    params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
    r = requests.get(BYBIT_URL, params=params, timeout=10)
    data = r.json()["result"]["list"]
    candles = []
    for row in reversed(data):
        candles.append({
            "time": int(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5])
        })
    return candles

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

def init_db():
    conn = sqlite3.connect("signals.db")
    conn.execute("""CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, direction TEXT, signal_type TEXT,
        entry REAL, tp REAL, sl REAL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
    conn.commit()
    conn.close()

def save_signal(symbol, direction, signal_type, entry, tp, sl):
    conn = sqlite3.connect("signals.db")
    conn.execute("INSERT INTO signals VALUES (NULL,?,?,?,?,?,?,CURRENT_TIMESTAMP)",
                 (symbol, direction, signal_type, entry, tp, sl))
    conn.commit()
    conn.close()

SYMBOLS = ["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT",
           "TONUSDT","DOGEUSDT","AVAXUSDT","LINKUSDT","ARBUSDT"]

def scan_market():
    results = []
    for symbol in SYMBOLS:
        for interval in ["60", "240"]:
            try:
                r = analyze(symbol, interval)
                if not r["signals"] or r["score"] < 3:
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
                       f"🎯 TP: {tp}\n"
                       f"🛡 SL: {sl}\n"
                       f"⚡️ Сила: {r['score']}/4")
                results.append(msg)
            except Exception as e:
                print(f"Error {symbol}: {e}")
    return results

SYSTEM_PROMPT = """Ты APEX — дерзкий AI трейдер.
Специализируешься на SMC: BOS, CHoCH, Order Blocks, FVG.
Отвечаешь коротко и по делу. Знаешь всё о крипте и фьючерсах.
Для сигналов говори использовать /scan."""

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(
        "⚡️ <b>APEX SMC BOT</b>\n\nГотов. Спрашивай или используй:\n/scan — сигналы\n/help — справка",
        parse_mode="HTML")

@dp.message(Command("scan"))
async def cmd_scan(message: types.Message):
    await message.answer("🔍 Сканирую...")
    signals = scan_market()
    if signals:
        for s in signals:
            await message.answer(s, parse_mode="HTML")
    else:
        await message.answer("😴 Сигналов нет.")

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    await message.answer("📖 BOS — продолжение тренда\nCHoCH — разворот\n🟢 LONG\n🔴 SHORT")

@dp.message()
async def handle_text(message: types.Message):
    user_id = message.from_user.id
    text = message.text
    if user_id not in chat_history:
        chat_history[user_id] = []
    chat_history[user_id].append(f"Пользователь: {text}")
    if len(chat_history[user_id]) > 20:
        chat_history[user_id] = chat_history[user_id][-20:]
    history_text = "\n".join(chat_history[user_id][-10:])
    prompt = f"{SYSTEM_PROMPT}\n\nИстория:\n{history_text}\n\nОтветь:"
    try:
        response = model.generate_content(prompt)
        reply = response.text if hasattr(response, 'text') else str(response.candidates[0].content.parts[0].text)
        chat_history[user_id].append(f"APEX: {reply}")
        await message.answer(reply)
    except Exception as e:
        await message.answer(f"Ошибка: {str(e)}")

async def auto_scan():
    signals = scan_market()
    if signals:
        for s in signals:
            await bot.send_message(ADMIN_ID, s, parse_mode="HTML")

async def main():
    init_db()
    threading.Thread(target=run_server, daemon=True).start()
    scheduler = AsyncIOScheduler()
    scheduler.add_job(auto_scan, "interval", minutes=30)
    scheduler.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
