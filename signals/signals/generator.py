import sys
sys.path.append("..")
from core.smc_engine import analyze
from core.learning import get_min_confluence, save_signal

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "TONUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "MATICUSDT",
    "ARBUSDT", "OPUSDT", "APTUSDT", "SUIUSDT", "INJUSDT"
]

INTERVALS = ["60", "240"]

def score_signal(result: dict) -> int:
    score = 0
    if result["signals"]:
        score += 2
        sig = result["signals"][0]
        if sig["type"] == "CHoCH":
            score += 1
    if result["order_blocks"]:
        score += 1
    if result["fvg"]:
        score += 1
    return score

def generate_signals() -> list:
    all_signals = []
    for symbol in SYMBOLS:
        for interval in INTERVALS:
            try:
                result = analyze(symbol, interval)
                if not result["signals"]:
                    continue
                score = score_signal(result)
                min_conf = get_min_confluence(symbol)
                if score < min_conf:
                    continue
                sig = result["signals"][0]
                price = result["price"]
                direction = sig["direction"]
                if direction == "BULLISH":
                    tp = round(price * 1.03, 4)
                    sl = round(price * 0.985, 4)
                else:
                    tp = round(price * 0.97, 4)
                    sl = round(price * 1.015, 4)
                save_signal(symbol, interval, direction, sig["type"], price, tp, sl)
                all_signals.append({
                    "symbol": symbol,
                    "interval": "1H" if interval == "60" else "4H",
                    "direction": direction,
                    "type": sig["type"],
                    "price": price,
                    "tp": tp,
                    "sl": sl,
                    "score": score
                })
            except Exception as e:
                print(f"Error {symbol} {interval}: {e}")
    return all_signals
