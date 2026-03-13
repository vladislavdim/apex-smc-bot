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
