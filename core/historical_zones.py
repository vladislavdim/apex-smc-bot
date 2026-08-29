"""Continuous map of reaction zones from confirmed candles only."""
from __future__ import annotations
import hashlib, json, os, sqlite3, statistics, time
from typing import Any

DB_PATH = os.environ.get("APEX_DB_PATH", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "brain.db"))

def _connect(db_path=DB_PATH):
    conn = sqlite3.connect(db_path, timeout=20, check_same_thread=False); conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS historical_zones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,symbol TEXT NOT NULL,timeframe TEXT NOT NULL,zone_type TEXT NOT NULL,
        zone_low REAL NOT NULL,zone_high REAL NOT NULL,center REAL NOT NULL,strength REAL DEFAULT 0,
        touch_count INTEGER DEFAULT 0,reaction_count INTEGER DEFAULT 0,break_count INTEGER DEFAULT 0,
        status TEXT DEFAULT 'active',first_seen INTEGER NOT NULL,last_seen INTEGER NOT NULL,last_touch INTEGER)""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_historical_zones_lookup ON historical_zones(symbol,timeframe,status,center)")
    conn.execute("""CREATE TABLE IF NOT EXISTS historical_zone_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,zone_id INTEGER NOT NULL,event_key TEXT NOT NULL UNIQUE,
        event_type TEXT NOT NULL,candle_json TEXT NOT NULL,observed_at INTEGER NOT NULL)""")
    return conn

def _value(row, key):
    try: return float(row.get(key))
    except (TypeError, ValueError): return None

def _clusters(candles, tolerance):
    pivots = []
    for i in range(2, len(candles) - 2):
        high, low = _value(candles[i], "high"), _value(candles[i], "low")
        highs, lows = [_value(candles[j], "high") for j in range(i-2, i+3)], [_value(candles[j], "low") for j in range(i-2, i+3)]
        if high is not None and all(v is not None and high >= v for v in highs): pivots.append(("resistance", high))
        if low is not None and all(v is not None and low <= v for v in lows): pivots.append(("support", low))
    result = []
    for kind in ("support", "resistance"):
        groups = []
        for value in sorted(v for k, v in pivots if k == kind):
            if groups and abs(value - statistics.mean(groups[-1])) <= tolerance: groups[-1].append(value)
            else: groups.append([value])
        for group in groups:
            center = statistics.mean(group); result.append((kind, center-tolerance, center+tolerance, len(group)))
    return result

def refresh_zones(symbol: str, timeframe: str, candles: list[dict[str, Any]], db_path=DB_PATH):
    rows = [row for row in candles if isinstance(row, dict)]
    confirmed = rows[:-1] if len(rows) > 1 else []
    if len(confirmed) < 20: return {"status": "insufficient_candles", "zones": 0}
    ranges = [hi-lo for row in confirmed if (hi:=_value(row,"high")) is not None and (lo:=_value(row,"low")) is not None and hi>=lo]
    closes = [v for row in confirmed if (v:=_value(row,"close")) is not None]
    if not ranges or not closes: return {"status": "invalid_candles", "zones": 0}
    price = closes[-1]; tolerance = min(max(statistics.median(ranges)*0.35, price*0.001), price*0.01)
    now, latest = int(time.time()), confirmed[-1]
    candle_key = hashlib.sha256(json.dumps({k:latest.get(k) for k in ("open","high","low","close")}, sort_keys=True).encode()).hexdigest()[:16]
    conn, updated = _connect(db_path), 0
    for kind, low, high, count in _clusters(confirmed, tolerance):
        center=(low+high)/2
        existing=conn.execute("SELECT * FROM historical_zones WHERE symbol=? AND timeframe=? AND zone_type=? AND ABS(center-?)<=? ORDER BY ABS(center-?) LIMIT 1",(symbol,timeframe,kind,center,tolerance,center)).fetchone()
        if existing:
            zone_id=int(existing["id"]); merged_low=min(existing["zone_low"],low); merged_high=max(existing["zone_high"],high)
            conn.execute("UPDATE historical_zones SET zone_low=?,zone_high=?,center=?,strength=MAX(strength,?),last_seen=? WHERE id=?",(merged_low,merged_high,(merged_low+merged_high)/2,min(1.0,count/5),now,zone_id))
        else:
            zone_id=conn.execute("INSERT INTO historical_zones(symbol,timeframe,zone_type,zone_low,zone_high,center,strength,first_seen,last_seen) VALUES(?,?,?,?,?,?,?,?,?)",(symbol,timeframe,kind,low,high,center,min(1.0,count/5),now,now)).lastrowid
        candle_low,candle_high,candle_open,candle_close=(_value(latest,k) for k in ("low","high","open","close")); event=None
        if candle_close is not None and ((kind=="support" and candle_close<low-tolerance) or (kind=="resistance" and candle_close>high+tolerance)): event="break"
        elif candle_low is not None and candle_high is not None and candle_low<=high and candle_high>=low:
            event="reaction" if candle_open is not None and candle_close is not None and ((kind=="support" and candle_close>candle_open) or (kind=="resistance" and candle_close<candle_open)) else "touch"
        if event:
            cursor=conn.execute("INSERT OR IGNORE INTO historical_zone_events(zone_id,event_key,event_type,candle_json,observed_at) VALUES(?,?,?,?,?)",(zone_id,f"{symbol}:{timeframe}:{zone_id}:{candle_key}:{event}",event,json.dumps(latest,default=str),now))
            if cursor.rowcount:
                column={"touch":"touch_count","reaction":"reaction_count","break":"break_count"}[event]
                conn.execute(f"UPDATE historical_zones SET {column}={column}+1,status=?,last_touch=? WHERE id=?",("broken" if event=="break" else "active",now,zone_id))
        updated+=1
    conn.execute("UPDATE historical_zones SET status='expired' WHERE symbol=? AND timeframe=? AND last_seen<? AND status='active'",(symbol,timeframe,now-60*86400)); conn.commit(); conn.close()
    return {"status":"updated","zones":updated,"tolerance":tolerance}

def build_zone_context(symbol: str, current_price, timeframe="", limit=8, db_path=DB_PATH):
    try:
        conn=_connect(db_path); params=[symbol]
        sql="SELECT timeframe,zone_type,zone_low,zone_high,center,strength,touch_count,reaction_count,break_count,last_seen FROM historical_zones WHERE symbol=? AND status='active'"
        if timeframe: sql+=" AND timeframe IN (?, '4h', '1d')"; params.append(timeframe)
        if current_price: sql+=" ORDER BY ABS(center-?) LIMIT ?"; params.extend((float(current_price),limit))
        else: sql+=" ORDER BY strength DESC,last_seen DESC LIMIT ?"; params.append(limit)
        zones=[dict(row) for row in conn.execute(sql,params).fetchall()]; conn.close()
        return {"available":bool(zones),"symbol":symbol,"zones":zones,"rule":"historical zones are reaction context only; never replace APEX levels"}
    except Exception: return {"available":False,"symbol":symbol,"zones":[]}

def format_zone_context(context):
    return "HISTORICAL ZONE MAP:\n"+json.dumps(context,ensure_ascii=False,default=str,separators=(",",":"))
