import requests
import pandas as pd
import numpy as np

BYBIT_URL = "https://api.bybit.com/v5/market/kline"

def get_klines(symbol: str, interval: str = "60", limit: int = 200) -> pd.DataFrame:
    params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
    r = requests.get(BYBIT_URL, params=params, timeout=10)
    data = r.json()["result"]["list"]
    df = pd.DataFrame(data, columns=["time","open","high","low","close","volume","turnover"])
    df = df.astype({"open": float, "high": float, "low": float, "close": float, "volume": float})
    df = df.iloc[::-1].reset_index(drop=True)
    return df

def find_swings(df: pd.DataFrame, lookback: int = 5):
    highs, lows = [], []
    for i in range(lookback, len(df) - lookback):
        if df["high"][i] == df["high"][i-lookback:i+lookback+1].max():
            highs.append((i, df["high"][i]))
        if df["low"][i] == df["low"][i-lookback:i+lookback+1].min():
            lows.append((i, df["low"][i]))
    return highs, lows

def detect_bos_choch(df: pd.DataFrame, highs: list, lows: list):
    signals = []
    if len(highs) < 2 or len(lows) < 2:
        return signals
    last_close = df["close"].iloc[-1]
    last_high = highs[-1][1]
    last_low = lows[-1][1]
    prev_high = highs[-2][1]
    prev_low = lows[-2][1]
    if last_close > last_high and last_high > prev_high:
        signals.append({"type": "BOS", "direction": "BULLISH", "level": last_high})
    if last_close < last_low and last_low < prev_low:
        signals.append({"type": "BOS", "direction": "BEARISH", "level": last_low})
    if last_close > last_high and last_high < prev_high:
        signals.append({"type": "CHoCH", "direction": "BULLISH", "level": last_high})
    if last_close < last_low and last_low > prev_low:
        signals.append({"type": "CHoCH", "direction": "BEARISH", "level": last_low})
    return signals

def find_order_blocks(df: pd.DataFrame, direction: str):
    obs = []
    for i in range(1, len(df) - 1):
        if direction == "BULLISH":
            if df["close"][i] < df["open"][i] and df["close"][i+1] > df["open"][i+1]:
                obs.append({"top": df["open"][i], "bottom": df["close"][i], "index": i})
        else:
            if df["close"][i] > df["open"][i] and df["close"][i+1] < df["open"][i+1]:
                obs.append({"top": df["close"][i], "bottom": df["open"][i], "index": i})
    return obs[-3:] if obs else []

def find_fvg(df: pd.DataFrame):
    fvgs = []
    for i in range(1, len(df) - 1):
        bull_fvg = df["low"][i+1] > df["high"][i-1]
        bear_fvg = df["high"][i+1] < df["low"][i-1]
        if bull_fvg:
            fvgs.append({"type": "BULL", "top": df["low"][i+1], "bottom": df["high"][i-1]})
        if bear_fvg:
            fvgs.append({"type": "BEAR", "top": df["low"][i-1], "bottom": df["high"][i+1]})
    return fvgs[-5:] if fvgs else []

def analyze(symbol: str, interval: str = "60"):
    df = get_klines(symbol, interval)
    highs, lows = find_swings(df)
    signals = detect_bos_choch(df, highs, lows)
    result = {"symbol": symbol, "interval": interval, "price": df["close"].iloc[-1],
              "signals": signals, "order_blocks": [], "fvg": find_fvg(df)}
    for s in signals:
        result["order_blocks"] = find_order_blocks(df, s["direction"])
    return result
