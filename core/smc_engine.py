"""
APEX SMC Engine v3 — Умный обход барьеров + самообучение источников
"""
import requests, sqlite3, time, logging, json
from datetime import datetime

# ── WAL патч ──

# ── WAL патч ──


BINANCE_INTERVALS = {
    "1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m",
    "1h":"1h","2h":"2h","4h":"4h","1d":"1d","1w":"1w"
}
CC_INTERVALS = {
    "1m":("histominute",1),"3m":("histominute",3),"5m":("histominute",5),
    "15m":("histominute",15),"30m":("histominute",30),
    "1h":("histohour",1),"2h":("histohour",2),"4h":("histohour",4),
    "1d":("histoday",1),
}
KRAKEN_INTERVALS = {"1m":1,"5m":5,"15m":15,"30m":30,"1h":60,"4h":240,"1d":1440}
BINANCE_F = "https://fapi.binance.com"
BINANCE_S = "https://api.binance.com"

COINGECKO_IDS = {
    "BTCUSDT":"bitcoin","ETHUSDT":"ethereum","SOLUSDT":"solana",
    "BNBUSDT":"binancecoin","XRPUSDT":"ripple","DOGEUSDT":"dogecoin",
    "AVAXUSDT":"avalanche-2","LINKUSDT":"chainlink","TONUSDT":"toncoin",
    "ARBUSDT":"arbitrum","SUIUSDT":"sui","NEARUSDT":"near",
    "INJUSDT":"injective-protocol","APTUSDT":"aptos",
    "DOTUSDT":"polkadot","ADAUSDT":"cardano","MATICUSDT":"matic-network",
    "LTCUSDT":"litecoin","ATOMUSDT":"cosmos","UNIUSDT":"uniswap",
    "XLMUSDT":"stellar","TRXUSDT":"tron","HBARUSDT":"hedera-hashgraph",
    "OPUSDT":"optimism","WIFUSDT":"dogwifcoin","PEPEUSDT":"pepe",
    "SHIBUSDT":"shiba-inu","BONKUSDT":"bonk","FETUSDT":"fetch-ai",
}

KRAKEN_SYMBOLS = {
    "BTCUSDT":"XBTUSD","ETHUSDT":"ETHUSD","SOLUSDT":"SOLUSD",
    "XRPUSDT":"XRPUSD","DOGEUSDT":"DOGEUSD","AVAXUSDT":"AVAXUSD",
    "LINKUSDT":"LINKUSD","ADAUSDT":"ADAUSD","DOTUSDT":"DOTUSD",
    "LTCUSDT":"LTCUSD","ATOMUSDT":"ATOMUSD","BNBUSDT":"BNBUSD",
}

_candle_cache: dict = {}
_CACHE_TTL = {"1m":30,"5m":60,"15m":120,"30m":180,"1h":300,"2h":450,"4h":600,"1d":1800}
_reliability: dict = {}
_reliability_loaded = False
import os as _os
DB_PATH = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "brain.db")
if not _os.path.exists(_os.path.dirname(DB_PATH)):
    DB_PATH = "brain.db"

def _init_tables():
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""CREATE TABLE IF NOT EXISTS source_reliability (
            source TEXT PRIMARY KEY, ok INTEGER DEFAULT 0, fail INTEGER DEFAULT 0,
            avg_candles REAL DEFAULT 0, last_ok TEXT, last_fail TEXT, notes TEXT DEFAULT '')""")
        conn.execute("""CREATE TABLE IF NOT EXISTS barrier_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT DEFAULT CURRENT_TIMESTAMP,
            symbol TEXT, interval TEXT, source TEXT, success INTEGER,
            candles INTEGER DEFAULT 0, error TEXT DEFAULT '')""")
        conn.execute("""CREATE TABLE IF NOT EXISTS source_knowledge (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT DEFAULT CURRENT_TIMESTAMP,
            fact TEXT, context TEXT)""")
        conn.commit(); conn.close()
    except Exception as e:
        logging.debug(f"_init_tables: {e}")

def _load_reliability():
    global _reliability, _reliability_loaded
    if _reliability_loaded: return
    _init_tables()
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("SELECT source, ok, fail FROM source_reliability").fetchall()
        conn.close()
        for src, ok, fail in rows:
            total = ok + fail
            _reliability[src] = ok / total if total > 0 else 0.5
    except: pass
    _reliability_loaded = True

def _record(source, symbol, interval, success, candles=0, error=""):
    try:
        conn = sqlite3.connect(DB_PATH)
        now = datetime.now().isoformat()
        ex = conn.execute("SELECT ok,fail,avg_candles FROM source_reliability WHERE source=?", (source,)).fetchone()
        if ex:
            ok = ex[0]+(1 if success else 0); fail = ex[1]+(0 if success else 1)
            avg = (ex[2]*ex[0]+candles)/ok if (success and ok>0) else ex[2]
            conn.execute("""UPDATE source_reliability SET ok=?,fail=?,avg_candles=?,
                last_ok=CASE WHEN ? THEN ? ELSE last_ok END,
                last_fail=CASE WHEN ? THEN ? ELSE last_fail END WHERE source=?""",
                (ok,fail,avg,success,now,not success,now,source))
        else:
            conn.execute("INSERT INTO source_reliability VALUES (?,?,?,?,?,?,?)",
                (source,1 if success else 0,0 if success else 1,
                 float(candles) if success else 0.0,
                 now if success else None, None if success else now, ""))
        conn.execute("INSERT INTO barrier_log (symbol,interval,source,success,candles,error) VALUES (?,?,?,?,?,?)",
            (symbol,interval,source,1 if success else 0,candles,error[:120]))
        conn.commit(); conn.close()
        cur = _reliability.get(source, 0.5)
        _reliability[source] = min(1.0,cur+0.04) if success else max(0.0,cur-0.08)
    except Exception as e:
        logging.debug(f"_record: {e}")

def _learn_fact(fact, context=""):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("INSERT INTO source_knowledge (fact,context) VALUES (?,?)", (fact,context))
        conn.commit(); conn.close()
    except: pass

def _ordered_sources(symbol):
    _load_reliability()
    defaults = {
        "bybit":0.82, "cryptocompare":0.80, "binance_futures":0.75, "binance_spot":0.70,
        "mexc":0.60, "kraken":0.58, "gate_io":0.55, "coingecko":0.50, "synthetic":0.10
    }
    scores = {src: _reliability.get(src, defaults.get(src,0.5)) for src in defaults}
    return sorted(scores, key=lambda s: scores[s], reverse=True)

def _ordered_sources_for_interval(symbol, interval):
    """Для коротких TF (1m/5m) ставим Binance Spot первым — он надёжнее CryptoCompare для альткоинов"""
    base_order = _ordered_sources(symbol)
    if interval in ("1m", "5m", "3m"):
        priority = ["bybit", "binance_spot", "binance_futures", "mexc", "gate_io", "cryptocompare", "kraken", "coingecko", "synthetic"]
        return priority
    return base_order

# ─── Фетчеры ─────────────────────────────────────────────────────────────────

def _fetch_bybit(symbol, interval, limit):
    bi_map = {"1m":"1","3m":"3","5m":"5","15m":"15","30m":"30",
              "1h":"60","2h":"120","4h":"240","1d":"D","1w":"W"}
    r = requests.get("https://api.bybit.com/v5/market/kline",
        params={"category":"linear","symbol":symbol,
                "interval":bi_map.get(interval,"60"),"limit":limit},
        headers={"User-Agent":"Mozilla/5.0"}, timeout=10)
    if r.status_code != 200: raise ValueError(f"Bybit HTTP {r.status_code}")
    data = r.json()
    rows = data.get("result",{}).get("list",[])
    if not rows: raise ValueError("Empty Bybit")
    # Bybit возвращает в обратном порядке [time,open,high,low,close,volume,...]
    rows = list(reversed(rows))
    return [{"open":float(c[1]),"high":float(c[2]),"low":float(c[3]),
             "close":float(c[4]),"volume":float(c[5])} for c in rows]


def _fetch_cryptocompare(symbol, interval, limit):
    base = symbol.replace("USDT","").replace("BUSD","")
    ep, agg = CC_INTERVALS.get(interval, ("histohour",1))
    # v2 API для минутных данных — надёжнее для альткоинов
    if ep == "histominute":
        r = requests.get(f"https://min-api.cryptocompare.com/data/v2/{ep}",
            params={"fsym":base,"tsym":"USD","limit":limit,"aggregate":agg},
            headers={"User-Agent":"Mozilla/5.0"}, timeout=10)
        raw = r.json()
        data = raw.get("Data",{}).get("Data",[])
        if not data:
            data = raw.get("Data",[])
    else:
        r = requests.get(f"https://min-api.cryptocompare.com/data/{ep}",
            params={"fsym":base,"tsym":"USD","limit":limit,"aggregate":agg},
            headers={"User-Agent":"Mozilla/5.0"}, timeout=10)
        data = r.json().get("Data",[])
    if not data: raise ValueError("Empty CC")
    return [{"open":float(c["open"]),"high":float(c["high"]),"low":float(c["low"]),
             "close":float(c["close"]),"volume":float(c.get("volumeto",0))} for c in data if c.get("close")]

def _fetch_binance_futures(symbol, interval, limit):
    bi = BINANCE_INTERVALS.get(interval,"1h")
    r = requests.get(f"{BINANCE_F}/fapi/v1/klines",
        params={"symbol":symbol,"interval":bi,"limit":limit},
        headers={"User-Agent":"Mozilla/5.0"}, timeout=10)
    if r.status_code != 200: raise ValueError(f"HTTP {r.status_code}")
    data = r.json()
    if not isinstance(data,list) or not data: raise ValueError("Empty BF")
    return [{"open":float(c[1]),"high":float(c[2]),"low":float(c[3]),"close":float(c[4]),"volume":float(c[5])} for c in data]

def _fetch_binance_spot(symbol, interval, limit):
    bi = BINANCE_INTERVALS.get(interval,"1h")
    r = requests.get(f"{BINANCE_S}/api/v3/klines",
        params={"symbol":symbol,"interval":bi,"limit":limit},
        headers={"User-Agent":"Mozilla/5.0"}, timeout=10)
    if r.status_code != 200: raise ValueError(f"HTTP {r.status_code}")
    data = r.json()
    if not isinstance(data,list) or not data: raise ValueError("Empty BS")
    return [{"open":float(c[1]),"high":float(c[2]),"low":float(c[3]),"close":float(c[4]),"volume":float(c[5])} for c in data]

def _fetch_mexc(symbol, interval, limit):
    mi_map = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m","1h":"1h","4h":"4h","1d":"1d"}
    r = requests.get("https://api.mexc.com/api/v3/klines",
        params={"symbol":symbol,"interval":mi_map.get(interval,"1h"),"limit":limit},
        headers={"User-Agent":"Mozilla/5.0"}, timeout=10)
    if r.status_code != 200: raise ValueError(f"MEXC HTTP {r.status_code}")
    data = r.json()
    if not isinstance(data,list) or not data: raise ValueError("Empty MEXC")
    return [{"open":float(c[1]),"high":float(c[2]),"low":float(c[3]),"close":float(c[4]),"volume":float(c[5])} for c in data]

def _fetch_kraken(symbol, interval, limit):
    ksym = KRAKEN_SYMBOLS.get(symbol)
    if not ksym: raise ValueError(f"No Kraken pair for {symbol}")
    ki = KRAKEN_INTERVALS.get(interval, 60)
    r = requests.get("https://api.kraken.com/0/public/OHLC",
        params={"pair":ksym,"interval":ki},
        headers={"User-Agent":"Mozilla/5.0"}, timeout=10)
    data = r.json()
    if data.get("error"): raise ValueError(str(data["error"]))
    result_key = [k for k in data["result"] if k != "last"][0]
    rows = data["result"][result_key]
    return [{"open":float(c[1]),"high":float(c[2]),"low":float(c[3]),
             "close":float(c[4]),"volume":float(c[6])} for c in rows[-limit:]]

def _fetch_gate(symbol, interval, limit):
    gi_map = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m","1h":"1h","4h":"4h","1d":"1d"}
    gate_sym = symbol if "_" in symbol else symbol.replace("USDT","_USDT")
    r = requests.get("https://api.gateio.ws/api/v4/futures/usdt/candlesticks",
        params={"contract":gate_sym,"interval":gi_map.get(interval,"1h"),"limit":limit},
        headers={"User-Agent":"Mozilla/5.0"}, timeout=10)
    if r.status_code != 200: raise ValueError(f"Gate HTTP {r.status_code}")
    data = r.json()
    if not isinstance(data,list) or not data: raise ValueError("Empty Gate")
    return [{"open":float(c.get("o",0)),"high":float(c.get("h",0)),
             "low":float(c.get("l",0)),"close":float(c.get("c",0)),"volume":float(c.get("v",0))} for c in data]

def _fetch_coingecko(symbol, interval, limit):
    cg_id = COINGECKO_IDS.get(symbol)
    if not cg_id: raise ValueError(f"No CG ID for {symbol}")
    days_map = {"1m":1,"5m":1,"15m":1,"30m":1,"1h":7,"4h":30,"1d":90}
    r = requests.get(f"https://api.coingecko.com/api/v3/coins/{cg_id}/ohlc",
        params={"vs_currency":"usd","days":days_map.get(interval,7)},
        headers={"User-Agent":"Mozilla/5.0"}, timeout=12)
    if r.status_code == 429: raise ValueError("CG rate limit")
    if r.status_code != 200: raise ValueError(f"CG HTTP {r.status_code}")
    data = r.json()
    if not isinstance(data,list) or len(data)<5: raise ValueError("CG: not enough data")
    return [{"open":float(c[1]),"high":float(c[2]),"low":float(c[3]),"close":float(c[4]),"volume":0.0} for c in data[-limit:]]

def _fetch_synthetic(symbol, interval, limit):
    """Последний резерв: строим свечи из market_chart (тики → группировка)"""
    cg_id = COINGECKO_IDS.get(symbol, "bitcoin")
    days_map = {"1m":1,"5m":1,"15m":1,"30m":1,"1h":7,"4h":14,"1d":30}
    r = requests.get(f"https://api.coingecko.com/api/v3/coins/{cg_id}/market_chart",
        params={"vs_currency":"usd","days":days_map.get(interval,7)},
        headers={"User-Agent":"Mozilla/5.0"}, timeout=14)
    if r.status_code != 200: raise ValueError(f"Synth HTTP {r.status_code}")
    pts = r.json().get("prices",[])
    if len(pts) < 10: raise ValueError("Synth: too few points")
    mins_map = {"1m":1,"5m":5,"15m":15,"30m":30,"1h":60,"4h":240,"1d":1440}
    ms_per = mins_map.get(interval,60) * 60 * 1000
    candles = []
    i = 0
    while i < len(pts) and len(candles) < limit:
        ts0 = pts[i][0]
        grp = [p[1] for p in pts if ts0 <= p[0] < ts0+ms_per]
        if grp:
            candles.append({"open":grp[0],"high":max(grp),"low":min(grp),
                            "close":grp[-1],"volume":0.0,"_synthetic":True})
        nxt = ts0 + ms_per
        while i < len(pts) and pts[i][0] < nxt: i += 1
    logging.info(f"[SMC] Синтетик {symbol}/{interval}: {len(candles)}св")
    return candles[-limit:]

_FETCHERS = {
    "bybit":_fetch_bybit, "cryptocompare":_fetch_cryptocompare, "binance_futures":_fetch_binance_futures,
    "binance_spot":_fetch_binance_spot, "mexc":_fetch_mexc,
    "kraken":_fetch_kraken, "gate_io":_fetch_gate,
    "coingecko":_fetch_coingecko, "synthetic":_fetch_synthetic,
}

# ═══════════════════════════════════════════════════════════════
# ГЛАВНАЯ ФУНКЦИЯ — умный обход барьеров
# ═══════════════════════════════════════════════════════════════

def get_candles_smart(symbol: str, interval: str = "1h", limit: int = 200) -> dict:
    ck = f"{symbol}_{interval}"
    ttl = _CACHE_TTL.get(interval, 300)
    if ck in _candle_cache:
        cached, ts = _candle_cache[ck]
        if time.time() - ts < ttl: return cached

    sources = _ordered_sources_for_interval(symbol, interval)
    attempts = 0
    all_errors = []

    for src in sources:
        fn = _FETCHERS.get(src)
        if not fn: continue
        attempts += 1
        try:
            candles = fn(symbol, interval, limit)
            if candles and len(candles) >= 15:
                _record(src, symbol, interval, True, len(candles))
                is_synth = src == "synthetic" or any(c.get("_synthetic") for c in candles[:1])
                quality = ("high"   if src in ("cryptocompare","binance_futures","binance_spot") else
                           "medium" if src in ("mexc","kraken","gate_io") else "low")
                res = {"candles":candles,"source":src,"attempts":attempts,
                       "quality":quality,"is_synthetic":is_synth,"error":"",
                       "symbol":symbol,"interval":interval}
                _candle_cache[ck] = (res, time.time())
                if attempts > 1:
                    _learn_fact(f"{symbol}/{interval}: нашли через {src} (попыток:{attempts})", "|".join(all_errors))
                    logging.info(f"[SMC] ✅ {symbol} {interval} → {src} ({attempts} попыток)")
                return res
            else:
                err = f"{src}:{len(candles) if candles else 0}св"
                _record(src, symbol, interval, False, 0, err); all_errors.append(err)
        except Exception as e:
            err = f"{src}:{str(e)[:60]}"
            _record(src, symbol, interval, False, 0, err); all_errors.append(err)
            logging.debug(f"[SMC] Барьер {src} {symbol}: {e}")

    _learn_fact(f"ПРОВАЛ {symbol}/{interval} ({attempts} ист.)", "|".join(all_errors[-3:]))
    logging.warning(f"[SMC] ❌ {symbol} {interval} — нет данных ({attempts} попыток)")
    return {"candles":[],"source":"none","attempts":attempts,"quality":"none",
            "is_synthetic":False,"error":"|".join(all_errors[-3:]),
            "symbol":symbol,"interval":interval}

# ═══════════════════════════════════════════════════════════════
# SMC АНАЛИЗ — pandas-free
# ═══════════════════════════════════════════════════════════════

def find_swings(candles: list, lookback: int = 5):
    highs, lows = [], []
    n = len(candles)
    for i in range(lookback, n-lookback):
        w = candles[i-lookback:i+lookback+1]
        if candles[i]["high"] == max(c["high"] for c in w): highs.append((i,candles[i]["high"]))
        if candles[i]["low"]  == min(c["low"]  for c in w): lows.append((i,candles[i]["low"]))
    return highs, lows

def classify_swings(highs, lows):
    result = []
    for i,(idx,price) in enumerate(highs):
        result.append({"idx":idx,"price":price,"kind":"HH" if i==0 or price>highs[i-1][1] else "LH"})
    for i,(idx,price) in enumerate(lows):
        result.append({"idx":idx,"price":price,"kind":"HL" if i==0 or price>lows[i-1][1] else "LL"})
    return sorted(result, key=lambda x: x["idx"])

def detect_events(candles: list, classified: list) -> list:
    if not classified or not candles: return []
    highs = [s for s in classified if s["kind"] in ("HH","LH")]
    lows  = [s for s in classified if s["kind"] in ("HL","LL")]
    last  = candles[-1]["close"]
    events = []
    if highs and last > highs[-1]["price"]:
        events.append({"type":"CHoCH" if highs[-1]["kind"]=="LH" else "BOS","direction":"BULLISH","level":highs[-1]["price"]})
    if lows and last < lows[-1]["price"]:
        events.append({"type":"CHoCH" if lows[-1]["kind"]=="HL" else "BOS","direction":"BEARISH","level":lows[-1]["price"]})
    if not events:
        hh = sum(1 for s in classified if s["kind"]=="HH")
        ll = sum(1 for s in classified if s["kind"]=="LL")
        if hh > ll:   events.append({"type":"TREND","direction":"BULLISH","level":0})
        elif ll > hh: events.append({"type":"TREND","direction":"BEARISH","level":0})
    return events

def find_ob(candles: list, direction: str):
    for i in range(len(candles)-2, max(0,len(candles)-30), -1):
        c = candles[i]
        if direction=="BULLISH" and c["close"]<c["open"]:
            return {"top":max(c["open"],c["close"]),"bottom":min(c["open"],c["close"])}
        if direction=="BEARISH" and c["close"]>c["open"]:
            return {"top":max(c["open"],c["close"]),"bottom":min(c["open"],c["close"])}
    return None

def find_fvg(candles: list, direction: str):
    for i in range(len(candles)-3, max(1,len(candles)-25), -1):
        if direction=="BULLISH" and candles[i+1]["low"]>candles[i-1]["high"]:
            return {"top":candles[i+1]["low"],"bottom":candles[i-1]["high"]}
        if direction=="BEARISH" and candles[i+1]["high"]<candles[i-1]["low"]:
            return {"top":candles[i-1]["low"],"bottom":candles[i+1]["high"]}
    return None

def smc_tf(symbol: str, interval: str) -> dict:
    res = get_candles_smart(symbol, interval, 150)
    candles = res["candles"]
    if len(candles) < 20:
        return {"direction":None,"source":res["source"],"quality":res["quality"],
                "error":res.get("error",""),"candles":[]}
    highs, lows = find_swings(candles)
    classified = classify_swings(highs, lows)
    events = detect_events(candles, classified)
    direction = events[0]["direction"] if events else None
    return {"direction":direction,"source":res["source"],"quality":res["quality"],
            "is_synthetic":res["is_synthetic"],"candles":candles,"candles_count":len(candles)}

def multi_tf_analysis(symbol: str, timeframes: list = None) -> dict | None:
    if timeframes is None: timeframes = ["15m","1h","4h"]
    TF_LABELS = {"1m":"1мин","5m":"5мин","15m":"15мин","30m":"30мин",
                 "1h":"1час","2h":"2ч","4h":"4ч","1d":"1д"}
    Q_ICON = {"high":"🟢","medium":"🟡","low":"🟠","none":"⚫"}

    tf_results = {tf: smc_tf(symbol, tf) for tf in timeframes}
    available  = [tf for tf,r in tf_results.items() if r["direction"] is not None]

    if not available:
        # Fallback на более широкие ТФ
        for tf in ["1h","4h","1d"]:
            if tf not in timeframes:
                r = smc_tf(symbol, tf)
                if r["direction"]:
                    tf_results[tf] = r; available.append(tf); timeframes = timeframes+[tf]
        if not available: return None

    bullish = [tf for tf in available if tf_results[tf]["direction"]=="BULLISH"]
    bearish = [tf for tf in available if tf_results[tf]["direction"]=="BEARISH"]

    if len(bullish) > len(bearish):
        direction, matched = "BULLISH", bullish
    elif len(bearish) > len(bullish):
        direction, matched = "BEARISH", bearish
    else:
        for tf in ["1d","4h","2h","1h","30m","15m"]:
            if tf in available:
                direction = tf_results[tf]["direction"]; matched = [tf]; break
        else: return None

    mc = len(matched); total = len(available)
    q_s = {"high":3,"medium":2,"low":1,"none":0}
    avg_q = sum(q_s.get(tf_results[tf]["quality"],0) for tf in matched) / mc

    if mc==total and total>=3 and avg_q>=2:
        grade,ge,stars = "МЕГА ТОП","🔥🔥🔥","⭐⭐⭐⭐⭐"
    elif mc>=3 or (mc==2 and avg_q>=2.5):
        grade,ge,stars = "ТОП СДЕЛКА","🔥🔥","⭐⭐⭐⭐"
    elif mc==2:
        grade,ge,stars = "ХОРОШАЯ","✅","⭐⭐⭐"
    else:
        grade,ge,stars = "СЛАБАЯ","⚠️","⭐⭐"

    tf_status = ""
    for tf in timeframes:
        r = tf_results[tf]; d = r["direction"]
        qi = Q_ICON.get(r["quality"],"⚫")
        src_s = r["source"][:8] if r["source"] != "none" else "нет"
        arrow = "🟢" if d=="BULLISH" else "🔴" if d=="BEARISH" else "⚪"
        synth = "[синт]" if r.get("is_synthetic") else ""
        tf_status += f"{arrow} {TF_LABELS.get(tf,tf)}: {d or 'нет'} {qi}{src_s}{synth}\n"

    return {"direction":direction,"matched":matched,"match_count":mc,"total":total,
            "grade":grade,"grade_emoji":ge,"stars":stars,"tf_status":tf_status,
            "available_tfs":available}

# ─── Диагностика ──────────────────────────────────────────────

def get_source_stats() -> str:
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("SELECT source,ok,fail,avg_candles,last_ok FROM source_reliability ORDER BY ok DESC").fetchall()
        barriers = conn.execute("SELECT ts,symbol,source,error FROM barrier_log WHERE success=0 ORDER BY id DESC LIMIT 5").fetchall()
        facts    = conn.execute("SELECT COUNT(*) FROM source_knowledge").fetchone()[0]
        conn.close()
        if not rows: return "📡 Статистики ещё нет — запусти несколько сканов"
        lines = [f"📡 <b>Надёжность источников</b> | Фактов в мозге: {facts}\n"]
        for src,ok,fail,avg_c,last in rows:
            total = ok+fail
            pct = round(ok/total*100) if total>0 else 0
            bar = "█"*(pct//10)+"░"*(10-pct//10)
            lines.append(f"<code>{src:<18}</code> [{bar}] {pct}% ({ok}/{total}) ~{avg_c:.0f}св")
        if barriers:
            lines.append("\n⛔ <b>Последние барьеры:</b>")
            for ts,sym,src,err in barriers:
                lines.append(f"<code>{ts[11:16]} {sym} {src}: {err[:50]}</code>")
        return "\n".join(lines)
    except Exception as e:
        return f"Ошибка: {e}"

def get_barrier_summary() -> str:
    try:
        conn = sqlite3.connect(DB_PATH)
        total  = conn.execute("SELECT COUNT(*) FROM barrier_log").fetchone()[0]
        fails  = conn.execute("SELECT COUNT(*) FROM barrier_log WHERE success=0").fetchone()[0]
        facts  = conn.execute("SELECT COUNT(*) FROM source_knowledge").fetchone()[0]
        conn.close()
        rate = round((total-fails)/total*100) if total>0 else 0
        return f"🧱 Барьеров: {fails} | Обходов: {total-fails} ({rate}%) | Фактов: {facts}"
    except: return ""

_init_tables()
_load_reliability()


# ═══════════════════════════════════════════════════════════════
# НОВЫЕ SMC МОДУЛИ v4
# ═══════════════════════════════════════════════════════════════

def detect_liquidity_sweep(candles: list, highs: list, lows: list) -> dict | None:
    """
    Liquidity Sweep — цена сметает ликвидность над/под свингами и разворачивается.
    Один из сильнейших SMC сигналов — часто предшествует резкому движению.
    """
    if len(candles) < 10 or not highs or not lows:
        return None
    last = candles[-1]
    prev = candles[-2] if len(candles) > 2 else candles[-1]

    # Бычий sweep: цена пробила лоу свинга (взяла ликвидность снизу) и закрылась выше
    if lows:
        last_low_price = lows[-1][1]
        if prev["low"] < last_low_price and last["close"] > last_low_price:
            wick = prev["low"]
            recovery = (last["close"] - wick) / (last["high"] - wick + 0.00001)
            if recovery > 0.5:  # Сильный отскок — не просто случайность
                return {
                    "type": "BULLISH_SWEEP",
                    "direction": "BULLISH",
                    "swept_level": last_low_price,
                    "wick_low": wick,
                    "recovery": round(recovery, 2),
                    "strength": "HIGH" if recovery > 0.75 else "MEDIUM"
                }

    # Медвежий sweep: пробила хай свинга и закрылась ниже
    if highs:
        last_high_price = highs[-1][1]
        if prev["high"] > last_high_price and last["close"] < last_high_price:
            wick = prev["high"]
            recovery = (wick - last["close"]) / (wick - last["low"] + 0.00001)
            if recovery > 0.5:
                return {
                    "type": "BEARISH_SWEEP",
                    "direction": "BEARISH",
                    "swept_level": last_high_price,
                    "wick_high": wick,
                    "recovery": round(recovery, 2),
                    "strength": "HIGH" if recovery > 0.75 else "MEDIUM"
                }
    return None


def find_imbalance_zones(candles: list) -> list:
    """
    Imbalance Zones — зоны где цена двигалась слишком быстро (gap между свечами).
    Шире чем FVG: включает любые зоны неэффективного движения.
    Цена ВСЕГДА возвращается их заполнить — используем как магниты.
    """
    zones = []
    for i in range(1, len(candles) - 1):
        c_prev = candles[i - 1]
        c_curr = candles[i]
        c_next = candles[i + 1]

        # Бычий имбаланс: между хаем предыдущей и лоем следующей — пустота
        if c_next["low"] > c_prev["high"]:
            gap = c_next["low"] - c_prev["high"]
            gap_pct = gap / c_prev["high"] * 100
            if gap_pct > 0.1:  # Минимум 0.1% разрыв
                zones.append({
                    "type": "BULL_IMBALANCE",
                    "top": c_next["low"],
                    "bottom": c_prev["high"],
                    "gap_pct": round(gap_pct, 3),
                    "idx": i,
                    "filled": False
                })

        # Медвежий имбаланс
        if c_next["high"] < c_prev["low"]:
            gap = c_prev["low"] - c_next["high"]
            gap_pct = gap / c_prev["low"] * 100
            if gap_pct > 0.1:
                zones.append({
                    "type": "BEAR_IMBALANCE",
                    "top": c_prev["low"],
                    "bottom": c_next["high"],
                    "gap_pct": round(gap_pct, 3),
                    "idx": i,
                    "filled": False
                })

    # Помечаем заполненные зоны (цена уже вернулась)
    current_price = candles[-1]["close"]
    for z in zones:
        if z["type"] == "BULL_IMBALANCE" and current_price <= z["top"]:
            z["filled"] = True
        elif z["type"] == "BEAR_IMBALANCE" and current_price >= z["bottom"]:
            z["filled"] = True

    # Возвращаем только незаполненные — они и есть магниты
    unfilled = [z for z in zones if not z["filled"]]
    return unfilled[-5:] if unfilled else []


def get_premium_discount(candles: list) -> dict:
    """
    Premium/Discount Zones — определяем где цена сейчас в диапазоне.
    Equilibrium (50%) = нейтраль.
    Выше 75% = Premium (дорого, лучше шортить/не покупать).
    Ниже 25% = Discount (дёшево, лучше покупать).
    """
    if len(candles) < 20:
        return {"zone": "UNKNOWN", "pct": 50, "bias": "NEUTRAL"}

    # Берём последние 50 свечей для диапазона
    recent = candles[-50:]
    high = max(c["high"] for c in recent)
    low  = min(c["low"]  for c in recent)
    price = candles[-1]["close"]

    if high == low:
        return {"zone": "EQUILIBRIUM", "pct": 50, "bias": "NEUTRAL"}

    pct = (price - low) / (high - low) * 100

    if pct >= 75:
        zone, bias = "PREMIUM", "BEARISH"      # Дорого — лучше шортить
    elif pct >= 55:
        zone, bias = "UPPER_EQ", "NEUTRAL"     # Немного выше середины
    elif pct >= 45:
        zone, bias = "EQUILIBRIUM", "NEUTRAL"  # Середина диапазона
    elif pct >= 25:
        zone, bias = "LOWER_EQ", "NEUTRAL"     # Немного ниже середины
    else:
        zone, bias = "DISCOUNT", "BULLISH"     # Дёшево — лучше покупать

    return {
        "zone": zone,
        "pct": round(pct, 1),
        "bias": bias,
        "range_high": high,
        "range_low": low
    }


def find_ob_fvg_chain(candles: list, direction: str) -> dict | None:
    """
    OB → FVG → OB цепочка — самый сильный SMC сетап.
    Когда Order Block подтверждён FVG который подтверждён следующим OB — 
    вероятность отработки значительно выше одиночного OB.
    """
    obs = []
    fvgs = []

    for i in range(1, len(candles) - 1):
        c = candles[i]
        # Собираем все OB
        if direction == "BULLISH" and c["close"] < c["open"]:
            if i + 1 < len(candles) and candles[i+1]["close"] > candles[i+1]["open"]:
                obs.append({"idx": i, "top": max(c["open"], c["close"]),
                            "bottom": min(c["open"], c["close"])})
        elif direction == "BEARISH" and c["close"] > c["open"]:
            if i + 1 < len(candles) and candles[i+1]["close"] < candles[i+1]["open"]:
                obs.append({"idx": i, "top": max(c["open"], c["close"]),
                            "bottom": min(c["open"], c["close"])})

        # Собираем все FVG
        if i + 1 < len(candles) - 1:
            if direction == "BULLISH" and candles[i+1]["low"] > candles[i-1]["high"]:
                fvgs.append({"idx": i, "top": candles[i+1]["low"],
                             "bottom": candles[i-1]["high"]})
            elif direction == "BEARISH" and candles[i+1]["high"] < candles[i-1]["low"]:
                fvgs.append({"idx": i, "top": candles[i-1]["low"],
                             "bottom": candles[i+1]["high"]})

    if len(obs) < 2 or not fvgs:
        return None

    # Ищем цепочку: OB1 → FVG → OB2 (в хронологическом порядке)
    for ob1 in obs[:-1]:
        for fvg in fvgs:
            if fvg["idx"] > ob1["idx"]:
                for ob2 in obs:
                    if ob2["idx"] > fvg["idx"]:
                        return {
                            "found": True,
                            "ob1": ob1,
                            "fvg": fvg,
                            "ob2": ob2,
                            "strength": "TRIPLE_CONFLUENCE",
                            "entry_zone_top": ob2["top"],
                            "entry_zone_bottom": ob2["bottom"]
                        }
    return None


def check_volume_on_structure(candles: list, structure_idx: int) -> dict:
    """
    Объём на структуре — сравниваем объём на BOS/CHoCH с средним.
    Пробой с высоким объёмом = настоящий.
    Пробой с низким объёмом = ложный, ловушка.
    """
    if len(candles) < 20 or structure_idx >= len(candles):
        return {"valid": True, "ratio": 1.0, "signal": "UNKNOWN"}

    # Средний объём за последние 20 свечей до пробоя
    lookback = candles[max(0, structure_idx-20):structure_idx]
    if not lookback:
        return {"valid": True, "ratio": 1.0, "signal": "UNKNOWN"}

    avg_vol = sum(c.get("volume", 0) for c in lookback) / len(lookback)
    break_vol = candles[structure_idx].get("volume", 0)

    if avg_vol == 0:
        return {"valid": True, "ratio": 1.0, "signal": "NO_VOLUME_DATA"}

    ratio = break_vol / avg_vol

    if ratio >= 1.5:
        signal = "STRONG_BREAK"     # Объём подтверждает пробой
        valid = True
    elif ratio >= 0.8:
        signal = "NORMAL_BREAK"
        valid = True
    else:
        signal = "WEAK_BREAK"       # Подозрительно — возможно ложный
        valid = False

    return {"valid": valid, "ratio": round(ratio, 2), "signal": signal}


def detect_divergence(candles: list, direction: str) -> dict | None:
    """
    Divergence — расхождение между ценой и RSI/объёмом.
    Бычья дивергенция: цена делает новый лоу, RSI — нет → разворот вверх.
    Медвежья дивергенция: цена делает новый хай, RSI — нет → разворот вниз.
    """
    if len(candles) < 20:
        return None

    # Считаем RSI без pandas
    def calc_rsi(closes, period=14):
        if len(closes) < period + 1:
            return None
        gains, losses = [], []
        for i in range(1, len(closes)):
            diff = closes[i] - closes[i-1]
            gains.append(max(0, diff))
            losses.append(max(0, -diff))
        if len(gains) < period:
            return None
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        if avg_loss == 0:
            return 100
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    closes = [c["close"] for c in candles]
    rsi_current = calc_rsi(closes)
    rsi_prev = calc_rsi(closes[:-5])  # RSI 5 свечей назад

    if rsi_current is None or rsi_prev is None:
        return None

    price_current = closes[-1]
    price_prev = closes[-6]

    # Бычья дивергенция: цена ниже, RSI выше
    if direction == "BULLISH":
        if price_current < price_prev and rsi_current > rsi_prev:
            strength = "STRONG" if rsi_current - rsi_prev > 5 else "WEAK"
            return {
                "type": "BULLISH_DIVERGENCE",
                "rsi_current": round(rsi_current, 1),
                "rsi_prev": round(rsi_prev, 1),
                "rsi_diff": round(rsi_current - rsi_prev, 1),
                "strength": strength
            }

    # Медвежья дивергенция: цена выше, RSI ниже
    elif direction == "BEARISH":
        if price_current > price_prev and rsi_current < rsi_prev:
            strength = "STRONG" if rsi_prev - rsi_current > 5 else "WEAK"
            return {
                "type": "BEARISH_DIVERGENCE",
                "rsi_current": round(rsi_current, 1),
                "rsi_prev": round(rsi_prev, 1),
                "rsi_diff": round(rsi_prev - rsi_current, 1),
                "strength": strength
            }

    return None


def get_market_profile(candles: list) -> dict:
    """
    Market Profile — где цена проводила больше всего времени.
    Point of Control (POC) = уровень с максимальным объёмом/временем.
    Цена часто возвращается к POC.
    """
    if len(candles) < 10:
        return {}

    # Делим диапазон на 20 уровней и считаем время/объём на каждом
    high = max(c["high"] for c in candles)
    low  = min(c["low"]  for c in candles)
    if high == low:
        return {}

    levels = 20
    step = (high - low) / levels
    profile = {}

    for c in candles:
        # Определяем в каких уровнях находилась эта свеча
        for i in range(levels):
            level_low  = low + i * step
            level_high = low + (i + 1) * step
            # Перекрытие свечи с уровнем
            overlap = min(c["high"], level_high) - max(c["low"], level_low)
            if overlap > 0:
                level_key = round(level_low + step / 2, 4)
                vol = c.get("volume", 1)
                profile[level_key] = profile.get(level_key, 0) + vol + overlap

    if not profile:
        return {}

    poc = max(profile, key=profile.get)
    sorted_levels = sorted(profile.items(), key=lambda x: x[1], reverse=True)

    # Value Area (70% объёма вокруг POC)
    total_vol = sum(profile.values())
    va_vol = 0
    va_levels = []
    for lvl, vol in sorted_levels:
        va_vol += vol
        va_levels.append(lvl)
        if va_vol >= total_vol * 0.7:
            break

    va_high = max(va_levels) if va_levels else poc
    va_low  = min(va_levels) if va_levels else poc

    current_price = candles[-1]["close"]
    distance_to_poc = abs(current_price - poc) / poc * 100

    return {
        "poc": poc,
        "va_high": va_high,
        "va_low": va_low,
        "distance_to_poc_pct": round(distance_to_poc, 2),
        "price_above_poc": current_price > poc
    }


def full_smc_analysis(symbol: str, interval: str = "1h") -> dict:
    """
    Полный SMC анализ с всеми новыми модулями.
    Возвращает словарь со всеми найденными структурами.
    Используется в full_scan для обогащения сигнала.
    """
    result = get_candles_smart(symbol, interval, 200)
    candles = result["candles"]
    if len(candles) < 20:
        return {"error": "no data", "source": result["source"]}

    highs, lows = find_swings(candles)
    classified = classify_swings(highs, lows)
    events = detect_events(candles, classified)
    direction = events[0]["direction"] if events else None

    ob  = find_ob(candles, direction) if direction else None
    fvg = find_fvg(candles, direction) if direction else None

    # Новые модули
    sweep    = detect_liquidity_sweep(candles, highs, lows)
    imb      = find_imbalance_zones(candles)
    pd_zone  = get_premium_discount(candles)
    chain    = find_ob_fvg_chain(candles, direction) if direction else None
    diverge  = detect_divergence(candles, direction) if direction else None
    profile  = get_market_profile(candles)

    # Объём на последнем структурном пробое
    vol_check = {"valid": True, "signal": "UNKNOWN"}
    if events and events[0].get("level", 0) > 0:
        # Ищем свечу пробоя
        break_price = events[0]["level"]
        for idx in range(len(candles)-1, max(0, len(candles)-15), -1):
            if abs(candles[idx]["close"] - break_price) / break_price < 0.005:
                vol_check = check_volume_on_structure(candles, idx)
                break

    return {
        "symbol": symbol,
        "interval": interval,
        "source": result["source"],
        "quality": result["quality"],
        "direction": direction,
        "ob": ob,
        "fvg": fvg,
        "liquidity_sweep": sweep,
        "imbalance_zones": imb,
        "premium_discount": pd_zone,
        "ob_fvg_chain": chain,
        "divergence": diverge,
        "market_profile": profile,
        "volume_check": vol_check,
        "candles_count": len(candles),
    }


# ═══════════════════════════════════════════════════════════════
# CVD — CUMULATIVE VOLUME DELTA (давление покупателей vs продавцов)
# ═══════════════════════════════════════════════════════════════

def calculate_cvd(candles: list) -> dict:
    """
    CVD = накопленная разница между объёмом покупок и продаж.
    Аппроксимация: если свеча зелёная — объём идёт в buy delta, красная — sell delta.
    Дополнительно считаем по upper/lower wick для точности.
    """
    if len(candles) < 10:
        return {"cvd": 0, "trend": "NEUTRAL", "divergence": None, "signal": "NEUTRAL"}

    deltas = []
    for c in candles:
        o, h, l, cl, v = c["open"], c["high"], c["low"], c["close"], c.get("volume", 0)
        if v == 0:
            deltas.append(0)
            continue
        # Оценка buy/sell давления через Body + Wicks
        body = abs(cl - o)
        total_range = h - l if h > l else 0.0001
        if cl >= o:  # бычья свеча
            # buy pressure пропорционально телу + нижней тени
            lower_wick = o - l
            buy_ratio = (body + lower_wick) / total_range / 2 + 0.5
        else:  # медвежья
            upper_wick = h - o
            buy_ratio = (h - cl) / total_range / 2  # меньше 0.5
        buy_vol = v * buy_ratio
        sell_vol = v * (1 - buy_ratio)
        deltas.append(buy_vol - sell_vol)

    # Накопленный CVD
    cvd_values = []
    acc = 0
    for d in deltas:
        acc += d
        cvd_values.append(acc)

    current_cvd = cvd_values[-1]
    prev_cvd = cvd_values[-10] if len(cvd_values) >= 10 else cvd_values[0]

    # Тренд CVD
    cvd_trend = "GROWING" if current_cvd > prev_cvd * 1.05 else \
                "FALLING" if current_cvd < prev_cvd * 0.95 else "FLAT"

    # Дивергенция CVD vs цена
    price_now = candles[-1]["close"]
    price_prev = candles[-10]["close"] if len(candles) >= 10 else candles[0]["close"]
    price_up = price_now > price_prev * 1.005
    cvd_up = current_cvd > prev_cvd

    divergence = None
    if price_up and not cvd_up:
        divergence = "BEARISH_DIV"  # цена растёт, давление покупок падает — слабость
    elif not price_up and cvd_up:
        divergence = "BULLISH_DIV"  # цена падает, давление покупок растёт — сила

    # Итоговый сигнал
    signal = "BULLISH" if cvd_trend == "GROWING" and not divergence == "BEARISH_DIV" else \
             "BEARISH" if cvd_trend == "FALLING" and not divergence == "BULLISH_DIV" else "NEUTRAL"

    return {
        "cvd": round(current_cvd, 2),
        "cvd_prev": round(prev_cvd, 2),
        "trend": cvd_trend,
        "divergence": divergence,
        "signal": signal,
        "buy_pressure_pct": round(
            sum(d for d in deltas[-20:] if d > 0) /
            (sum(abs(d) for d in deltas[-20:]) or 1) * 100, 1
        )
    }


# ═══════════════════════════════════════════════════════════════
# WHALE DETECTION — обнаружение аномальных объёмов китов
# ═══════════════════════════════════════════════════════════════

def detect_whale_candles(candles: list, lookback: int = 50) -> dict:
    """
    Находит свечи с аномальным объёмом (потенциальные следы китов).
    Аномалия = объём > avg * threshold.
    Определяет: кит накапливает (тихо, с малым движением цены)
    или сбрасывает (агрессивно, с большим движением).
    """
    if len(candles) < 20:
        return {"found": False, "spike": 0, "type": "NONE", "strength": 0}

    window = candles[-lookback:] if len(candles) >= lookback else candles
    volumes = [c.get("volume", 0) for c in window]
    avg_vol = sum(volumes) / len(volumes) if volumes else 1

    # Последние 3 свечи на аномалии
    recent = candles[-5:]
    max_spike = 0
    whale_candle = None

    for c in recent:
        v = c.get("volume", 0)
        if avg_vol > 0:
            spike = v / avg_vol
            if spike > max_spike:
                max_spike = spike
                whale_candle = c

    if max_spike < 1.8 or not whale_candle:
        return {"found": False, "spike": round(max_spike, 2), "type": "NONE", "strength": 0}

    # Тип активности кита
    o, h, l, cl = whale_candle["open"], whale_candle["high"], whale_candle["low"], whale_candle["close"]
    body = abs(cl - o)
    total_range = (h - l) if h > l else 0.0001
    body_ratio = body / total_range  # маленькое тело = аккумуляция/дистрибуция

    if body_ratio < 0.3:
        # Маленькое тело при огромном объёме = поглощение (кит аккумулирует)
        whale_type = "ACCUMULATION"
    elif cl > o:
        # Большая бычья свеча с объёмом = агрессивная покупка
        whale_type = "AGGRESSIVE_BUY"
    else:
        # Большая медвежья свеча с объёмом = агрессивная продажа / сброс
        whale_type = "AGGRESSIVE_SELL"

    # Сила сигнала (1-10)
    strength = min(10, int(max_spike * 2))

    # Направление вывода
    signal = "BULLISH" if whale_type in ("ACCUMULATION", "AGGRESSIVE_BUY") else "BEARISH"

    return {
        "found": True,
        "spike": round(max_spike, 2),
        "type": whale_type,
        "signal": signal,
        "strength": strength,
        "body_ratio": round(body_ratio, 2),
        "avg_volume": round(avg_vol, 0),
        "description": (
            f"🐋 Кит {'накапливает' if whale_type == 'ACCUMULATION' else 'агрессивно покупает' if whale_type == 'AGGRESSIVE_BUY' else 'сбрасывает'} "
            f"(объём x{max_spike:.1f} от среднего, сила {strength}/10)"
        )
    }


def get_volume_profile(candles: list, bins: int = 10) -> dict:
    """
    Упрощённый Volume Profile — где сосредоточен максимальный объём (POC).
    Возвращает зоны высокого объёма для определения поддержек/сопротивлений.
    """
    if len(candles) < 20:
        return {"poc": 0, "high_volume_zones": [], "current_zone": "UNKNOWN"}

    # Диапазон цен
    prices = [c["close"] for c in candles]
    low_price = min(c["low"] for c in candles)
    high_price = max(c["high"] for c in candles)
    if high_price <= low_price:
        return {"poc": 0, "high_volume_zones": [], "current_zone": "UNKNOWN"}

    # Распределяем объём по ценовым бинам
    bin_size = (high_price - low_price) / bins
    volume_bins = [0.0] * bins

    for c in candles:
        avg_price = (c["high"] + c["low"]) / 2
        vol = c.get("volume", 0)
        bin_idx = int((avg_price - low_price) / bin_size)
        bin_idx = max(0, min(bins - 1, bin_idx))
        volume_bins[bin_idx] += vol

    # POC — ценовой уровень с максимальным объёмом
    poc_bin = volume_bins.index(max(volume_bins))
    poc_price = low_price + (poc_bin + 0.5) * bin_size

    # High Volume Zones (топ 3 бина)
    sorted_bins = sorted(enumerate(volume_bins), key=lambda x: x[1], reverse=True)
    hvz = []
    for idx, vol in sorted_bins[:3]:
        zone_low = low_price + idx * bin_size
        zone_high = zone_low + bin_size
        hvz.append({"low": round(zone_low, 6), "high": round(zone_high, 6), "volume": round(vol, 0)})

    # Где сейчас цена относительно POC
    current = candles[-1]["close"]
    current_zone = "ABOVE_POC" if current > poc_price * 1.005 else \
                   "BELOW_POC" if current < poc_price * 0.995 else "AT_POC"

    return {
        "poc": round(poc_price, 6),
        "high_volume_zones": hvz,
        "current_zone": current_zone,
        "current_price": round(current, 6)
    }


# ===== SUPPLY / DEMAND ЗОНЫ =====
def find_supply_demand(candles: list, direction: str) -> dict | None:
    """
    Supply/Demand зоны — усиленная версия Order Block.
    Supply (предложение): зона откуда цена резко упала (медвежья зона).
    Demand (спрос): зона откуда цена резко выросла (бычья зона).
    Совпадение с OB = очень сильный уровень.
    """
    if len(candles) < 20:
        return None
    try:
        closes = [c["close"] for c in candles]
        # Ищем резкие движения (импульс > 1.5% за свечу)
        for i in range(len(candles)-10, max(0, len(candles)-50), -1):
            c = candles[i]
            move = abs(c["close"] - c["open"]) / c["open"] * 100
            if move < 1.5:
                continue
            is_bullish = c["close"] > c["open"]
            if direction == "BULLISH" and is_bullish:
                # Demand зона — основание импульса вверх
                zone_bottom = min(c["open"], c["low"])
                zone_top = max(c["open"], c["close"]) * 0.998
                if candles[-1]["close"] > zone_top:
                    continue  # цена уже выше зоны
                return {
                    "type": "DEMAND",
                    "bottom": round(zone_bottom, 6),
                    "top": round(zone_top, 6),
                    "strength": "STRONG" if move > 3 else "MODERATE",
                    "candle_idx": i
                }
            elif direction == "BEARISH" and not is_bullish:
                # Supply зона — вершина импульса вниз
                zone_bottom = min(c["open"], c["close"]) * 1.002
                zone_top = max(c["open"], c["high"])
                if candles[-1]["close"] < zone_bottom:
                    continue
                return {
                    "type": "SUPPLY",
                    "bottom": round(zone_bottom, 6),
                    "top": round(zone_top, 6),
                    "strength": "STRONG" if move > 3 else "MODERATE",
                    "candle_idx": i
                }
    except Exception:
        pass
    return None


# ===== WYCKOFF ФАЗЫ (расширенные) =====
def detect_wyckoff_phase(candles: list) -> dict:
    """
    Полные Wyckoff фазы:
    Accumulation → Markup → Distribution → Markdown
    Анализирует последние 50 свечей.
    """
    if len(candles) < 30:
        return {"phase": "UNKNOWN", "score": 0, "signals": []}

    try:
        last50 = candles[-50:] if len(candles) >= 50 else candles
        closes = [c["close"] for c in last50]
        vols = [c.get("volume", 0) for c in last50]
        highs = [c["high"] for c in last50]
        lows = [c["low"] for c in last50]

        price_now = closes[-1]
        avg_vol = sum(vols) / len(vols) if vols else 1
        recent_vol = sum(vols[-5:]) / 5 if len(vols) >= 5 else avg_vol

        # Разбиваем на трети
        third = len(closes) // 3
        first_third = closes[:third]
        last_third = closes[-third:]
        first_avg = sum(first_third) / len(first_third)
        last_avg = sum(last_third) / len(last_third)

        range_pct = (max(highs) - min(lows)) / min(lows) * 100 if min(lows) > 0 else 0
        trend_change = (last_avg - first_avg) / first_avg * 100

        signals = []
        score = 0

        # ACCUMULATION: боковик + низкий объём + потом всплеск
        if range_pct < 8 and recent_vol > avg_vol * 1.3 and trend_change > -2:
            score += 40
            signals.append("Боковик с всплеском объёма")
            if trend_change > 0:
                score += 20
                signals.append("Цена начинает расти")
            phase = "ACCUMULATION"

        # MARKUP: устойчивый рост + объём подтверждает
        elif trend_change > 5 and recent_vol >= avg_vol * 0.8:
            score += 60
            signals.append(f"Устойчивый рост +{trend_change:.1f}%")
            phase = "MARKUP"

        # DISTRIBUTION: боковик на хаях + объём падает
        elif range_pct < 8 and price_now >= max(highs) * 0.95 and recent_vol < avg_vol:
            score += 40
            signals.append("Боковик на хаях + объём падает")
            phase = "DISTRIBUTION"

        # MARKDOWN: устойчивое падение
        elif trend_change < -5:
            score += 60
            signals.append(f"Устойчивое падение {trend_change:.1f}%")
            phase = "MARKDOWN"

        else:
            phase = "TRANSITION"
            score = 20
            signals.append("Переходная фаза")

        return {
            "phase": phase,
            "score": min(score, 100),
            "signals": signals,
            "range_pct": round(range_pct, 1),
            "trend_change": round(trend_change, 1),
            "vol_ratio": round(recent_vol / avg_vol, 2) if avg_vol > 0 else 1.0
        }
    except Exception:
        return {"phase": "UNKNOWN", "score": 0, "signals": []}


# ===== МНОГОМОНЕТНАЯ КОРРЕЛЯЦИЯ =====
def check_multi_coin_correlation(symbol: str, direction: str, get_candles_fn) -> dict:
    """
    Проверяет согласованность движения похожих монет.
    Если SOL, AVAX, NEAR одновременно BULLISH — сигнал сильнее.
    """
    CORRELATED_GROUPS = {
        "SOLUSDT":  ["AVAXUSDT", "NEARUSDT", "SUIUSDT"],
        "AVAXUSDT": ["SOLUSDT", "NEARUSDT", "ATOMUSDT"],
        "NEARUSDT": ["SOLUSDT", "SUIUSDT", "INJUSDT"],
        "LINKUSDT": ["INJUSDT", "ARBUSDT"],
        "ARBUSDT":  ["LINKUSDT", "INJUSDT"],
        "BTCUSDT":  ["ETHUSDT"],
        "ETHUSDT":  ["BTCUSDT", "ARBUSDT"],
        "XRPUSDT":  ["ADAUSDT", "XLMUSDT"],
        "ADAUSDT":  ["XRPUSDT", "DOTUSDT"],
        "LTCUSDT":  ["BTCUSDT"],
    }
    corr_symbols = CORRELATED_GROUPS.get(symbol, [])
    if not corr_symbols:
        return {"confirmed": 0, "total": 0, "score": 0}

    confirmed = 0
    for sym in corr_symbols:
        try:
            c = get_candles_fn(sym, "1h", 5)
            if len(c) < 3:
                continue
            change = (c[-1]["close"] - c[-3]["close"]) / c[-3]["close"] * 100
            if direction == "BULLISH" and change > 0.3:
                confirmed += 1
            elif direction == "BEARISH" and change < -0.3:
                confirmed += 1
        except Exception:
            continue

    total = len(corr_symbols)
    ratio = confirmed / total if total > 0 else 0
    return {
        "confirmed": confirmed,
        "total": total,
        "score": int(ratio * 10),  # макс +10 к confluence
        "strong": ratio >= 0.6
    }


# ═══════════════════════════════════════════════════════════════
# FIBONACCI LEVELS
# ═══════════════════════════════════════════════════════════════

def get_fibonacci_levels(candles: list, direction: str) -> dict:
    """
    Fibonacci уровни от последнего значимого движения.
    Золотая зона 0.618-0.786 — лучшая точка входа по SMC.
    """
    if len(candles) < 20:
        return {}
    highs, lows = find_swings(candles, lookback=10)
    if not highs or not lows:
        return {}

    if direction == "BULLISH":
        # Ретрейсмент от хая к лою (ищем вход на откате)
        swing_high = max(highs, key=lambda x: x[1])[1]
        swing_low  = min(lows,  key=lambda x: x[1])[1]
    else:
        swing_high = max(highs, key=lambda x: x[1])[1]
        swing_low  = min(lows,  key=lambda x: x[1])[1]

    diff = swing_high - swing_low
    if diff <= 0:
        return {}

    ratios = [0.236, 0.382, 0.5, 0.618, 0.786]
    levels = {}
    for r in ratios:
        levels[r] = round(swing_high - diff * r, 6)

    current = candles[-1]["close"]
    golden_low  = levels[0.618]
    golden_high = levels[0.786] if direction == "BULLISH" else levels[0.382]

    in_golden = (min(golden_low, golden_high) <= current <= max(golden_low, golden_high))
    nearest = min(levels.values(), key=lambda x: abs(x - current))
    nearest_ratio = min(levels, key=lambda r: abs(levels[r] - current))

    return {
        "levels": levels,
        "swing_high": swing_high,
        "swing_low": swing_low,
        "current": current,
        "in_golden_zone": in_golden,
        "nearest_level": nearest,
        "nearest_ratio": nearest_ratio,
        "golden_zone": (golden_low, golden_high),
    }


# ═══════════════════════════════════════════════════════════════
# SESSION VOLUME PROFILE
# ═══════════════════════════════════════════════════════════════

def get_session_volume_profile(candles: list) -> dict:
    """
    Заглушка для обратной совместимости — заменена на detect_mm_accumulation.
    """
    return {"signal": "NEUTRAL", "asia_bias": "NEUTRAL", "london_bias": "NEUTRAL", "ny_bias": "NEUTRAL"}


def detect_mm_accumulation(candles: list) -> dict:
    """
    Market Maker Accumulation Detector — комбинированный детектор накопления ММ.

    Объединяет два алгоритма:
    1. Accumulation Score — узкий диапазон + объём + волатильность
    2. Pre-Pump Scanner — volume spike + tight range + higher lows

    Возвращает score 0-4 и описание сигнала.
    Score >= 2 — вероятность накопления высокая.
    Score >= 3 — очень сильный сигнал (фондовый паттерн).
    """
    if len(candles) < 20:
        return {"score": 0, "signal": "NEUTRAL", "signals": [], "pre_pump": False}

    signals = []
    score = 0

    # ── 1. ACCUMULATION SCORE ──────────────────────────────────
    last10 = candles[-10:]
    highs10 = [c["high"] for c in last10]
    lows10  = [c["low"]  for c in last10]
    range10 = max(highs10) - min(lows10)
    avg_candle_range = sum(c["high"] - c["low"] for c in last10) / 10

    # Узкий диапазон — цена зажата (range меньше 5 средних свечей)
    if range10 < avg_candle_range * 5:
        score += 1
        signals.append("📦 Узкий диапазон — цена зажата")

    # Объём растёт на последних 3 свечах при боковике
    vols = [c["volume"] for c in candles[-20:]]
    avg_vol = sum(vols[:-3]) / max(len(vols) - 3, 1)
    recent_vol = sum(vols[-3:]) / 3
    if recent_vol > avg_vol * 1.3 and range10 < avg_candle_range * 6:
        score += 1
        signals.append(f"📈 Объём растёт x{recent_vol/avg_vol:.1f} при боковике — накопление")

    # Волатильность падает (последние 5 свечей уже последних 10)
    range5 = max(highs10[-5:]) - min(lows10[-5:])
    if range5 < range10 * 0.55:
        score += 1
        signals.append("🔇 Волатильность сжимается — пружина перед выстрелом")

    # ── 2. PRE-PUMP SCANNER ────────────────────────────────────
    lookback = 20
    vol_lb   = [c["volume"] for c in candles[-lookback:]]
    range_lb = [c["high"] - c["low"] for c in candles[-lookback:]]
    avg_vol_lb   = sum(vol_lb)   / len(vol_lb)
    avg_range_lb = sum(range_lb) / len(range_lb)

    last = candles[-1]
    last_vol   = last["volume"]
    last_range = last["high"] - last["low"]

    volume_spike = last_vol > avg_vol_lb * 2.0
    tight_range  = last_range < avg_range_lb * 0.6
    lows5 = [c["low"] for c in candles[-5:]]
    higher_lows  = lows5[-1] > lows5[0]

    pre_pump = volume_spike and tight_range and higher_lows
    if pre_pump:
        score += 1
        signals.append("🚀 PRE-PUMP паттерн: объём↑ + диапазон↓ + лои↑")

    # Дополнительно: wick rejection снизу (MM защищает уровень)
    wicks_down = [c["open"] - c["low"] for c in candles[-5:] if c["close"] >= c["open"]]
    if wicks_down and sum(wicks_down) / len(wicks_down) > avg_candle_range * 0.4:
        signals.append("🪝 Wick rejection снизу — MM защищает уровень")

    # ── ИТОГОВЫЙ СИГНАЛ ───────────────────────────────────────
    if score >= 3:
        signal = "STRONG_ACCUMULATION"
    elif score >= 2:
        signal = "ACCUMULATION"
    elif score >= 1:
        signal = "WEAK_ACCUMULATION"
    else:
        signal = "NEUTRAL"

    return {
        "score": score,
        "signal": signal,
        "signals": signals,
        "pre_pump": pre_pump,
        "range10": round(range10, 6),
        "vol_ratio": round(recent_vol / avg_vol, 2) if avg_vol > 0 else 1.0,
    }


# ═══════════════════════════════════════════════════════════════
# SMART MONEY DIVERGENCE
# ═══════════════════════════════════════════════════════════════

def detect_smart_money_divergence(candles: list, ob: dict, fvg: dict, direction: str) -> dict:
    """
    Расхождение между Smart Money индикаторами и направлением сигнала.
    Если OB/FVG противоречат направлению — сигнал слабее.
    """
    if len(candles) < 10:
        return {"score": 0, "signals": []}

    score  = 0
    signals = []
    price  = candles[-1]["close"]

    # OB против направления
    if ob:
        if direction == "BULLISH" and price < ob.get("bottom", price):
            score -= 8
            signals.append("⚠️ Цена ниже OB — медвежья структура")
        elif direction == "BEARISH" and price > ob.get("top", price):
            score -= 8
            signals.append("⚠️ Цена выше OB — бычья структура")

    # FVG против направления
    if fvg:
        fvg_mid = (fvg.get("top", 0) + fvg.get("bottom", 0)) / 2
        if direction == "BULLISH" and price < fvg.get("bottom", price):
            score -= 5
            signals.append("⚠️ Цена под FVG — не заполнен")
        elif direction == "BEARISH" and price > fvg.get("top", price):
            score -= 5
            signals.append("⚠️ Цена над FVG — не заполнен")

    # Новый хай/лоу без объёма
    last_5 = candles[-5:]
    avg_vol = sum(c.get("volume", 0) for c in candles[-20:-5]) / 15 if len(candles) >= 20 else 1
    last_vol = candles[-1].get("volume", 0)

    if direction == "BULLISH":
        if candles[-1]["high"] == max(c["high"] for c in last_5) and last_vol < avg_vol * 0.6:
            score -= 7
            signals.append("⚠️ Новый хай на слабом объёме")
    else:
        if candles[-1]["low"] == min(c["low"] for c in last_5) and last_vol < avg_vol * 0.6:
            score -= 7
            signals.append("⚠️ Новый лоу на слабом объёме")

    # Усиление в направлении сигнала
    if last_vol > avg_vol * 1.5:
        is_bull_candle = candles[-1]["close"] > candles[-1]["open"]
        if (direction == "BULLISH" and is_bull_candle) or (direction == "BEARISH" and not is_bull_candle):
            score += 8
            signals.append("✅ Сильный объём подтверждает направление")

    return {"score": score, "signals": signals}


# ═══════════════════════════════════════════════════════════════
# INDUCEMENT (ложный пробой с возвратом)
# ═══════════════════════════════════════════════════════════════

def detect_inducement(candles: list, direction: str) -> dict | None:
    """
    Inducement — ложный пробой уровня с быстрым возвратом.
    Один из сильнейших SMC паттернов входа.
    """
    if len(candles) < 5:
        return None

    highs, lows = find_swings(candles[:-3], lookback=5)
    if not highs or not lows:
        return None

    avg_vol = sum(c.get("volume", 0) for c in candles[-20:-3]) / 17 if len(candles) >= 20 else 1

    if direction == "BULLISH" and lows:
        key_level = lows[-1][1]
        # Ищем свечу которая пробила лоу и вернулась
        for i in range(len(candles)-3, len(candles)):
            c = candles[i]
            if c["low"] < key_level and c["close"] > key_level:
                wick_size = (key_level - c["low"]) / (c["high"] - c["low"] + 0.0001)
                vol_spike = c.get("volume", 0) / avg_vol if avg_vol > 0 else 1
                if wick_size > 0.4:
                    return {
                        "type": "BULLISH_INDUCEMENT",
                        "level": key_level,
                        "wick_size": round(wick_size, 2),
                        "vol_spike": round(vol_spike, 2),
                        "strength": "STRONG" if vol_spike > 1.5 else "MEDIUM",
                        "weight": 18 if vol_spike > 1.5 else 10,
                    }

    elif direction == "BEARISH" and highs:
        key_level = highs[-1][1]
        for i in range(len(candles)-3, len(candles)):
            c = candles[i]
            if c["high"] > key_level and c["close"] < key_level:
                wick_size = (c["high"] - key_level) / (c["high"] - c["low"] + 0.0001)
                vol_spike = c.get("volume", 0) / avg_vol if avg_vol > 0 else 1
                if wick_size > 0.4:
                    return {
                        "type": "BEARISH_INDUCEMENT",
                        "level": key_level,
                        "wick_size": round(wick_size, 2),
                        "vol_spike": round(vol_spike, 2),
                        "strength": "STRONG" if vol_spike > 1.5 else "MEDIUM",
                        "weight": 18 if vol_spike > 1.5 else 10,
                    }
    return None


# ═══════════════════════════════════════════════════════════════
# RSI / MACD DIVERGENCE
# ═══════════════════════════════════════════════════════════════

def detect_rsi_macd_divergence(candles: list, direction: str) -> dict:
    """
    RSI и MACD дивергенция — расхождение цены и индикаторов.
    Bullish divergence: цена делает новый лоу, RSI/MACD нет → разворот вверх.
    Bearish divergence: цена делает новый хай, RSI/MACD нет → разворот вниз.
    """
    if len(candles) < 30:
        return {"found": False, "score": 0, "signals": []}

    closes = [c["close"] for c in candles]
    signals = []
    score = 0

    # ── RSI (14) ──────────────────────────────────────────────
    def calc_rsi(prices, period=14):
        if len(prices) < period + 1:
            return []
        gains, losses = [], []
        for i in range(1, len(prices)):
            d = prices[i] - prices[i-1]
            gains.append(max(d, 0))
            losses.append(max(-d, 0))
        rsi_vals = []
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period
        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period-1) + gains[i]) / period
            avg_loss = (avg_loss * (period-1) + losses[i]) / period
            rs = avg_gain / avg_loss if avg_loss > 0 else 100
            rsi_vals.append(100 - 100 / (1 + rs))
        return rsi_vals

    rsi = calc_rsi(closes)
    if len(rsi) >= 10:
        # Bullish divergence: цена падает, RSI растёт
        if direction == "BULLISH":
            price_low_now  = closes[-1] < closes[-5]
            rsi_low_now    = rsi[-1]  > rsi[-5]
            if price_low_now and rsi_low_now:
                score += 2
                signals.append(f"📈 RSI Bullish Divergence — цена↓ RSI↑ (+8)")
        # Bearish divergence: цена растёт, RSI падает
        elif direction == "BEARISH":
            price_high_now = closes[-1] > closes[-5]
            rsi_high_now   = rsi[-1]  < rsi[-5]
            if price_high_now and rsi_high_now:
                score += 2
                signals.append(f"📉 RSI Bearish Divergence — цена↑ RSI↓ (+8)")

        # Перекупленность/перепроданность
        last_rsi = rsi[-1]
        if last_rsi < 30 and direction == "BULLISH":
            score += 1
            signals.append(f"💚 RSI перепродан ({last_rsi:.0f}) — хорошая точка для лонга (+5)")
        elif last_rsi > 70 and direction == "BEARISH":
            score += 1
            signals.append(f"🔴 RSI перекуплен ({last_rsi:.0f}) — хорошая точка для шорта (+5)")

    # ── MACD (12,26,9) ────────────────────────────────────────
    def calc_ema(prices, period):
        if len(prices) < period:
            return []
        k = 2 / (period + 1)
        ema = [sum(prices[:period]) / period]
        for p in prices[period:]:
            ema.append(p * k + ema[-1] * (1 - k))
        return ema

    ema12 = calc_ema(closes, 12)
    ema26 = calc_ema(closes, 26)
    if len(ema12) > 0 and len(ema26) > 0:
        min_len = min(len(ema12), len(ema26))
        macd_line = [ema12[-min_len+i] - ema26[-min_len+i] for i in range(min_len)]
        if len(macd_line) >= 9:
            signal_line = calc_ema(macd_line, 9)
            if len(signal_line) >= 3:
                # MACD crossover — пересечение сигнальной линии
                macd_now  = macd_line[-1]
                macd_prev = macd_line[-2]
                sig_now   = signal_line[-1]
                sig_prev  = signal_line[-2]
                # Bullish crossover: MACD пересекает сигнальную снизу вверх
                if direction == "BULLISH" and macd_prev < sig_prev and macd_now > sig_now:
                    score += 2
                    signals.append(f"📈 MACD Bullish Crossover — подтверждение разворота (+8)")
                # Bearish crossover
                elif direction == "BEARISH" and macd_prev > sig_prev and macd_now < sig_now:
                    score += 2
                    signals.append(f"📉 MACD Bearish Crossover — подтверждение разворота (+8)")
                # MACD дивергенция с ценой
                if direction == "BULLISH" and closes[-1] < closes[-5] and macd_line[-1] > macd_line[-5]:
                    score += 1
                    signals.append(f"📈 MACD Bullish Divergence — цена↓ MACD↑ (+5)")
                elif direction == "BEARISH" and closes[-1] > closes[-5] and macd_line[-1] < macd_line[-5]:
                    score += 1
                    signals.append(f"📉 MACD Bearish Divergence — цена↑ MACD↓ (+5)")

    weight = score * 4  # score 1 = +4, score 4 = +16
    return {
        "found": score > 0,
        "score": score,
        "weight": weight,
        "signals": signals,
        "rsi": round(rsi[-1], 1) if rsi else 0,
    }


# ═══════════════════════════════════════════════════════════════
# VWAP — СРЕДНЕВЗВЕШЕННАЯ ЦЕНА ПО ОБЪЁМУ
# ═══════════════════════════════════════════════════════════════

def calculate_vwap(candles: list) -> dict:
    """
    VWAP — Volume Weighted Average Price.
    Институциональные игроки используют VWAP как точку входа.
    Цена выше VWAP = бычий рынок, ниже = медвежий.
    Отклонение ±2% от VWAP = зона интереса.
    """
    if len(candles) < 10:
        return {"vwap": 0, "signal": "NEUTRAL", "deviation_pct": 0}

    # Берём последние 50 свечей (один торговый день примерно)
    candles_slice = candles[-50:]
    total_vol = 0
    total_pv  = 0

    for c in candles_slice:
        typical_price = (c["high"] + c["low"] + c["close"]) / 3
        vol = c.get("volume", 0)
        total_pv  += typical_price * vol
        total_vol += vol

    if total_vol == 0:
        return {"vwap": 0, "signal": "NEUTRAL", "deviation_pct": 0}

    vwap = total_pv / total_vol
    current = candles[-1]["close"]
    deviation_pct = (current - vwap) / vwap * 100

    # Стандартное отклонение для полос
    variances = []
    for c in candles_slice:
        tp = (c["high"] + c["low"] + c["close"]) / 3
        vol = c.get("volume", 0)
        variances.append(vol * (tp - vwap) ** 2)
    std_dev = (sum(variances) / total_vol) ** 0.5 if total_vol > 0 else 0

    upper_band = vwap + 2 * std_dev
    lower_band = vwap - 2 * std_dev

    # Сигнал
    if current > upper_band:
        signal = "BEARISH"  # перекуплен выше VWAP+2σ
        desc = f"Цена выше VWAP+2σ — перекуплен, возможен откат"
    elif current < lower_band:
        signal = "BULLISH"  # перепродан ниже VWAP-2σ
        desc = f"Цена ниже VWAP-2σ — перепродан, возможен отскок"
    elif current > vwap:
        signal = "BULLISH"
        desc = f"Цена выше VWAP — бычий контроль"
    else:
        signal = "BEARISH"
        desc = f"Цена ниже VWAP — медвежий контроль"

    # Близко к VWAP — зона интереса (±0.5%)
    near_vwap = abs(deviation_pct) < 0.5

    return {
        "vwap": round(vwap, 4),
        "current": round(current, 4),
        "deviation_pct": round(deviation_pct, 2),
        "upper_band": round(upper_band, 4),
        "lower_band": round(lower_band, 4),
        "signal": signal,
        "desc": desc,
        "near_vwap": near_vwap,
        "std_dev": round(std_dev, 4),
    }


# ═══════════════════════════════════════════════════════════════
# HEATMAP УРОВНЕЙ — ГДЕ СКОНЦЕНТРИРОВАНЫ СТОПЫ
# ═══════════════════════════════════════════════════════════════

def get_liquidity_heatmap(candles: list) -> dict:
    """
    Heatmap ликвидности — где сконцентрированы стопы и ликвидность.
    Логика: свинг хаи = стопы шортистов (buy stops выше)
            свинг лои = стопы лонгистов (sell stops ниже)
    Чем больше свечей тестировали уровень — тем больше там ликвидности.
    """
    if len(candles) < 30:
        return {"levels": [], "nearest_buy_stops": None, "nearest_sell_stops": None}

    current = candles[-1]["close"]
    levels = {}

    # Считаем сколько раз цена тестировала каждый уровень
    for c in candles[-100:]:
        # Округляем до значимых цифр
        if current >= 1000:
            h_level = round(c["high"] / 100) * 100
            l_level = round(c["low"]  / 100) * 100
        elif current >= 100:
            h_level = round(c["high"] / 10) * 10
            l_level = round(c["low"]  / 10) * 10
        elif current >= 1:
            h_level = round(c["high"], 2)
            l_level = round(c["low"],  2)
        else:
            h_level = round(c["high"], 5)
            l_level = round(c["low"],  5)

        levels[h_level] = levels.get(h_level, 0) + 1
        levels[l_level] = levels.get(l_level, 0) + 1

    # Сортируем по количеству касаний
    sorted_levels = sorted(levels.items(), key=lambda x: x[1], reverse=True)

    # Топ-10 уровней с наибольшей ликвидностью
    top_levels = []
    for price, touches in sorted_levels[:10]:
        level_type = "buy_stops" if price > current else "sell_stops"
        dist_pct = abs(price - current) / current * 100
        top_levels.append({
            "price": price,
            "touches": touches,
            "type": level_type,
            "dist_pct": round(dist_pct, 2),
            "strength": "HIGH" if touches >= 5 else "MEDIUM" if touches >= 3 else "LOW",
        })

    # Ближайшие buy stops (выше цены) и sell stops (ниже цены)
    buy_stops  = [l for l in top_levels if l["type"] == "buy_stops"]
    sell_stops = [l for l in top_levels if l["type"] == "sell_stops"]

    nearest_buy  = min(buy_stops,  key=lambda x: x["dist_pct"]) if buy_stops  else None
    nearest_sell = min(sell_stops, key=lambda x: x["dist_pct"]) if sell_stops else None

    return {
        "levels": top_levels,
        "nearest_buy_stops":  nearest_buy,
        "nearest_sell_stops": nearest_sell,
        "current": current,
    }


# ═══════════════════════════════════════════════════════════════
# BREAKER BLOCK — ПРОБИТЫЙ OB КОТОРЫЙ СТАЛ НОВЫМ УРОВНЕМ
# ═══════════════════════════════════════════════════════════════

def detect_breaker_block(candles: list, direction: str) -> dict | None:
    """
    Breaker Block — Order Block который был пробит и стал новым уровнем.
    Логика SMC: когда цена пробивает OB, он превращается в Breaker.
    Bullish Breaker: медвежий OB пробит снизу вверх → теперь поддержка.
    Bearish Breaker: бычий OB пробит сверху вниз → теперь сопротивление.
    """
    if len(candles) < 30:
        return None

    current = candles[-1]["close"]

    if direction == "BULLISH":
        # Ищем медвежий OB который цена пробила снизу вверх
        for i in range(len(candles) - 20, len(candles) - 5):
            if i < 0:
                continue
            c = candles[i]
            # Медвежья свеча (потенциальный Bear OB)
            if c["close"] >= c["open"]:
                continue
            ob_top    = c["open"]
            ob_bottom = c["close"]
            # Проверяем пробой — цена прошла выше этого OB
            broke_above = any(
                candles[j]["close"] > ob_top
                for j in range(i + 1, min(i + 10, len(candles)))
            )
            if not broke_above:
                continue
            # Breaker должен быть ниже текущей цены (как поддержка)
            if ob_top >= current:
                continue
            dist_pct = (current - ob_top) / current * 100
            # Цена недалеко от Breaker (в пределах 3%)
            if dist_pct > 3.0:
                continue
            return {
                "type": "BULLISH_BREAKER",
                "top": round(ob_top, 6),
                "bottom": round(ob_bottom, 6),
                "dist_pct": round(dist_pct, 2),
                "strength": "HIGH" if dist_pct < 1.0 else "MEDIUM",
                "weight": 12 if dist_pct < 1.0 else 8,
                "desc": f"Bullish Breaker {ob_bottom:.4f}–{ob_top:.4f} (поддержка, {dist_pct:.1f}% ниже)",
            }

    elif direction == "BEARISH":
        # Ищем бычий OB который цена пробила сверху вниз
        for i in range(len(candles) - 20, len(candles) - 5):
            if i < 0:
                continue
            c = candles[i]
            # Бычья свеча (потенциальный Bull OB)
            if c["close"] <= c["open"]:
                continue
            ob_top    = c["close"]
            ob_bottom = c["open"]
            # Проверяем пробой — цена ушла ниже этого OB
            broke_below = any(
                candles[j]["close"] < ob_bottom
                for j in range(i + 1, min(i + 10, len(candles)))
            )
            if not broke_below:
                continue
            # Breaker должен быть выше текущей цены (как сопротивление)
            if ob_bottom <= current:
                continue
            dist_pct = (ob_bottom - current) / current * 100
            if dist_pct > 3.0:
                continue
            return {
                "type": "BEARISH_BREAKER",
                "top": round(ob_top, 6),
                "bottom": round(ob_bottom, 6),
                "dist_pct": round(dist_pct, 2),
                "strength": "HIGH" if dist_pct < 1.0 else "MEDIUM",
                "weight": 12 if dist_pct < 1.0 else 8,
                "desc": f"Bearish Breaker {ob_bottom:.4f}–{ob_top:.4f} (сопротивление, {dist_pct:.1f}% выше)",
            }

    return None


def detect_mega_trade(candles_4h: list, candles_1d: list, symbol: str = "") -> dict | None:
    """
    Детектор мега-сделок на 100-200%:
    - Долгий боковик (30+ свечей 1d в диапазоне < 15%)
    - Сжатие объёма во время боковика
    - Зоны ликвидности выше/ниже диапазона
    - Wyckoff фаза C/D (спринг или UTAD пройден)
    - MM Accumulation score 3+/4
    - Breakout с объёмом x2.5+
    """
    try:
        if len(candles_1d) < 30 or len(candles_4h) < 50:
            return None

        # 1. Определяем диапазон боковика на 1d
        lookback = min(60, len(candles_1d))
        recent = candles_1d[-lookback:]
        highs = [c["high"] for c in recent]
        lows  = [c["low"]  for c in recent]
        closes = [c["close"] for c in recent]
        vols  = [c.get("volume", 0) for c in recent]

        range_high = max(highs)
        range_low  = min(lows)
        range_mid  = (range_high + range_low) / 2
        range_pct  = (range_high - range_low) / range_low * 100
        current    = closes[-1]

        # Боковик должен быть < 20% диапазона
        if range_pct > 20:
            return None

        # 2. Считаем дни в боковике
        days_in_range = 0
        for c in reversed(recent):
            if range_low * 0.95 <= c["close"] <= range_high * 1.05:
                days_in_range += 1
            else:
                break

        if days_in_range < 20:
            return None

        # 3. Сжатие объёма — средний объём последних 10 свечей < среднего за 30
        avg_vol_30 = sum(vols[-30:]) / 30 if len(vols) >= 30 else sum(vols) / len(vols)
        avg_vol_10 = sum(vols[-10:]) / 10 if len(vols) >= 10 else avg_vol_30
        vol_compression = avg_vol_10 < avg_vol_30 * 0.8

        # 4. Breakout — последняя свеча пробивает диапазон
        last_close = closes[-1]
        last_vol   = vols[-1] if vols else 0
        breakout_up   = last_close > range_high * 1.005
        breakout_down = last_close < range_low  * 0.995
        vol_spike = last_vol > avg_vol_30 * 2.0 if avg_vol_30 > 0 else False

        # 5. MM Accumulation на 4h
        mm = detect_mm_accumulation(candles_4h)
        mm_score = mm.get("score", 0) if mm else 0

        # 6. Wyckoff фаза
        wyckoff = detect_wyckoff_phase(candles_1d)
        wyckoff_phase = wyckoff.get("phase", "") if wyckoff else ""
        wyckoff_bullish = wyckoff_phase in ("ACCUMULATION", "MARKUP", "TRANSITION")
        wyckoff_bearish = wyckoff_phase in ("DISTRIBUTION", "MARKDOWN")

        # 7. Зоны ликвидности
        heatmap = get_liquidity_heatmap(candles_4h)
        nearest_buy  = heatmap.get("nearest_buy_stops")
        nearest_sell = heatmap.get("nearest_sell_stops")

        # Оцениваем направление и силу сигнала
        score = 0
        signals = []
        direction = None

        # Боковик
        score += 20
        signals.append(f"📦 Боковик {days_in_range} дней (диапазон {range_pct:.1f}%)")

        # Сжатие объёма
        if vol_compression:
            score += 15
            signals.append(f"📉 Объём сжат до {round(avg_vol_10/avg_vol_30*100)}% от среднего")

        # MM Accumulation
        if mm_score >= 3:
            score += 20
            signals.append(f"🐋 MM Накопление СИЛЬНОЕ (score {mm_score}/4)")
        elif mm_score >= 2:
            score += 10
            signals.append(f"📦 MM Накопление (score {mm_score}/4)")

        # Wyckoff
        if wyckoff_bullish:
            score += 15
            signals.append(f"📊 Wyckoff: {wyckoff_phase}")
            direction = "BULLISH"
        elif wyckoff_bearish:
            score += 15
            signals.append(f"📊 Wyckoff: {wyckoff_phase}")
            direction = "BEARISH"

        # Breakout
        if breakout_up and vol_spike:
            score += 25
            signals.append(f"🚀 BREAKOUT вверх с объёмом x{round(last_vol/avg_vol_30, 1)}")
            direction = "BULLISH"
        elif breakout_down and vol_spike:
            score += 25
            signals.append(f"💥 BREAKOUT вниз с объёмом x{round(last_vol/avg_vol_30, 1)}")
            direction = "BEARISH"
        elif breakout_up:
            score += 10
            signals.append(f"⬆️ Пробой вверх (объём слабый)")
            direction = "BULLISH"
        elif breakout_down:
            score += 10
            signals.append(f"⬇️ Пробой вниз (объём слабый)")
            direction = "BEARISH"

        # Если нет направления — определяем по позиции в диапазоне
        if not direction:
            direction = "BULLISH" if current > range_mid else "BEARISH"

        # Ликвидность
        liq_note = ""
        if direction == "BULLISH" and nearest_sell:
            liq_note = f"💧 Ликвидность выше: ${nearest_sell:.4f}"
            signals.append(liq_note)
            score += 5
        elif direction == "BEARISH" and nearest_buy:
            liq_note = f"💧 Ликвидность ниже: ${nearest_buy:.4f}"
            signals.append(liq_note)
            score += 5

        # Минимальный порог
        if score < 45:
            return None

        # Расчёт уровней
        range_size = range_high - range_low
        entry = current

        if direction == "BULLISH":
            sl    = round(range_low * 0.97, 6)       # под нижней границей боковика -3%
            tp1   = round(entry + range_size * 0.5, 6)  # +50% диапазона
            tp2   = round(entry + range_size * 1.5, 6)  # +150% диапазона
            tp3   = round(entry + range_size * 2.5, 6)  # +250% диапазона
            sl_pct  = round((entry - sl) / entry * 100, 1)
            tp1_pct = round((tp1 - entry) / entry * 100, 1)
            tp2_pct = round((tp2 - entry) / entry * 100, 1)
            tp3_pct = round((tp3 - entry) / entry * 100, 1)
        else:
            sl    = round(range_high * 1.03, 6)
            tp1   = round(entry - range_size * 0.5, 6)
            tp2   = round(entry - range_size * 1.5, 6)
            tp3   = round(entry - range_size * 2.5, 6)
            sl_pct  = round((sl - entry) / entry * 100, 1)
            tp1_pct = round((entry - tp1) / entry * 100, 1)
            tp2_pct = round((entry - tp2) / entry * 100, 1)
            tp3_pct = round((entry - tp3) / entry * 100, 1)

        return {
            "symbol": symbol,
            "direction": direction,
            "score": score,
            "signals": signals,
            "days_in_range": days_in_range,
            "range_pct": round(range_pct, 1),
            "range_high": range_high,
            "range_low": range_low,
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "sl_pct": sl_pct,
            "tp1_pct": tp1_pct,
            "tp2_pct": tp2_pct,
            "tp3_pct": tp3_pct,
            "wyckoff": wyckoff_phase,
            "mm_score": mm_score,
            "vol_compression": vol_compression,
            "breakout": breakout_up or breakout_down,
        }

    except Exception as e:
        import logging
        logging.debug(f"detect_mega_trade {symbol}: {e}")
        return None
