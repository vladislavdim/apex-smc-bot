pythonimport asyncio
import logging
import os
import requests
import pandas as pd
import sqlite3
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
model = genai.GenerativeModel("gemini-1.5-flash")

chat_history = {}

BYBIT_URL = "https://api.bybit.com/v5/market/kline"

def get_klines(symbol, interval="60", limit=200):
    params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
    r = requests.get(BYBIT_URL, params=params, timeout=10)
    data = r.json()["result"]["list"]
    df = pd.DataFrame(data, columns=["time","open","high","low","close","volume","turnover"])
    df = df.astype({"open": float, "high": float, "low": float, "close": float, "volume": float})
    return df.iloc[::-1].reset_index(drop=True)

def find_swings(df, lookback=5):
    highs, lows = [], []
    for i in range(lookback, len(df) - lookback):
        if df["high"][i] == df["high"][i-lookback:i+lookback+1].max():
            highs.append((i, df["high"][i]))
        if df["low"][i] == df["low"][i-lookback:i+lookback+1].min():
            lows.append((i, df["low"][i]))
    return highs, lows

def detect_bos_choch(df, highs, lows):
    signals = []
    if len(highs) < 2 or len(lows) < 2:
        return signals
    last_close = df["close"].iloc[-1]
    if last_close > highs[-1][1] and highs[-1][1] > highs[-2][1]:
        signals.append({"type": "BOS", "direction": "BULLISH"})
    if last_close < lows[-1][1] and lows[-1][1] < lows[-2][1]:
        signals.append({"type": "BOS", "direction": "BEARISH"})
    if last_close > highs[-1][1] and highs[-1][1] < highs[-2][1]:
        signals.append({"type": "CHoCH", "direction": "BULLISH"})
    if last_close < lows[-1][1] and lows[-1][1] > lows[-2][1]:
        signals.append({"type": "CHoCH", "direction": "BEARISH"})
    return signals

def find_fvg(df):
    fvgs = []
    for i in range(1, len(df) - 1):
        if df["low"][i+1] > df["high"][i-1]:
            fvgs.append("BULL")
        if df["high"][i+1] < df["low"][i-1]:
            fvgs.append("BEAR")
    return fvgs

def analyze(symbol, interval="60"):
    df = get_klines(symbol, interval)
    highs, lows = find_swings(df)
    signals = detect_bos_choch(df, highs, lows)
    fvgs = find_fvg(df)
    score = 0
    if signals:
        score += 2
        if signals[0]["type"] == "CHoCH":
            score += 1
    if fvgs:
        score += 1
    return {"symbol": symbol, "interval": interval, "price": df["close"].iloc[-1],
            "signals": signals, "score": score}

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
    conn.execute("INSERT INTO signals (symbol,direction,signal_type,entry,tp,sl) VALUES (?,?,?,?,?,?)",
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
                       f"⚡️ Сила: {r['score']}/5")
                results.append(msg)
            except Exception as e:
                print(f"Error {symbol}: {e}")
    return results

SYSTEM_PROMPT = """Ты APEX — дерзкий AI трейдер и ассистент. 
Специализируешься на SMC (Smart Money Concepts): BOS, CHoCH, Order Blocks, FVG, ликвидность.
Отвечаешь коротко, по делу, без воды. Можешь материться если пользователь так общается.
Знаешь всё о крипте, фьючерсах, Bybit, TON, NFT.
Если спрашивают про сигналы — говори использовать /scan."""

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(
        "⚡️ <b>APEX SMC BOT</b>\n\nГотов. Спрашивай что угодно или используй:\n/scan — сигналы\n/help — справка",
        parse_mode="HTML"
    )

@dp.message(Command("scan"))
async def cmd_scan(message: types.Message):
    await message.answer("🔍 Сканирую...")
    signals = scan_market()
    if signals:
        for s in signals:
            await message.answer(s, parse_mode="HTML")
    else:
        await message.answer("😴 Чисто. Сигналов нет.")

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    await message.answer(
        "📖 BOS — продолжение тренда\nCHoCH — разворот\n🟢 BULLISH = лонг\n🔴 BEARISH = шорт",
        parse_mode="HTML"
    )

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
    prompt = f"{SYSTEM_PROMPT}\n\nИстория:\n{history_text}\n\nОтветь на последнее сообщение:"

    try:
        response = model.generate_content(prompt)
        reply = response.text
        chat_history[user_id].append(f"APEX: {reply}")
        await message.answer(reply)
    except Exception as e:
        await message.answer("⚡️ Мозг временно недоступен. Попробуй позже.")

async def auto_scan():
    signals = scan_market()
    if signals:
        for s in signals:
            await bot.send_message(ADMIN_ID, s, parse_mode="HTML")

async def main():
    init_db()
    scheduler = AsyncIOScheduler()
    scheduler.add_job(auto_scan, "interval", minutes=30)
    scheduler.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
