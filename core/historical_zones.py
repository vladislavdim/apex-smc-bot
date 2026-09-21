"""Continuous map of reaction zones from confirmed candles only.

This remains a compatibility store while V3 is being migrated.  Its lifecycle
values come from :mod:`apex.market.levels`; the map is context-only and cannot
admit or reject a production candidate.
"""
from __future__ import annotations
import hashlib, json, sqlite3, statistics, time
from typing import Any

from apex.config.settings import ApexConfig
from apex.db.connection import connect_compatibility as _connect_compatibility_db
from apex.market.levels import LevelSide, LevelState, PriceLevel, advance_level

DB_PATH = ApexConfig.from_env().database.compatibility_db_path

def _connect(db_path=DB_PATH):
    conn = _connect_compatibility_db(db_path, timeout=20, check_same_thread=False); conn.row_factory = sqlite3.Row
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
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(historical_zones)")}
    if "lifecycle_state" not in columns:
        conn.execute("ALTER TABLE historical_zones ADD COLUMN lifecycle_state TEXT NOT NULL DEFAULT 'ACTIVE'")
    if "lifecycle_side" not in columns:
        conn.execute("ALTER TABLE historical_zones ADD COLUMN lifecycle_side TEXT")
    return conn

def _value(row, key):
    try: return float(row.get(key))
    except (TypeError, ValueError): return None

def _confirmed_rows(rows):
    """Prefer explicit venue closure; preserve legacy drop-last callers."""
    if any("is_closed" in row for row in rows):
        return [row for row in rows if row.get("is_closed") is True]
    return rows[:-1] if len(rows) > 1 else []

def _event_time(row, fallback):
    for key in ("close_time", "closed_at", "timestamp", "time", "t"):
        value = row.get(key)
        try:
            result = float(value)
        except (TypeError, ValueError):
            continue
        if result > 10_000_000_000:
            result /= 1000.0
        if result > 0:
            return result
    return float(fallback)

def _level_state(value):
    try:
        return LevelState(str(value or "ACTIVE").upper())
    except ValueError:
        return LevelState.ACTIVE

def _level_side(kind, value=None):
    try:
        return LevelSide(str(value).upper())
    except ValueError:
        return LevelSide.DEMAND if kind == "support" else LevelSide.SUPPLY

def _compat_status(state):
    return "expired" if state is LevelState.EXPIRED else "broken" if state is LevelState.BROKEN else "active"

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
    confirmed = _confirmed_rows(rows)
    if len(confirmed) < 20: return {"status": "insufficient_candles", "zones": 0}
    ranges = [hi-lo for row in confirmed if (hi:=_value(row,"high")) is not None and (lo:=_value(row,"low")) is not None and hi>=lo]
    closes = [v for row in confirmed if (v:=_value(row,"close")) is not None]
    if not ranges or not closes: return {"status": "invalid_candles", "zones": 0}
    price = closes[-1]; tolerance = min(max(statistics.median(ranges)*0.35, price*0.001), price*0.01)
    now, latest = int(time.time()), confirmed[-1]
    latest_event_time = _event_time(latest, now)
    lifecycle_candle = {
        **latest,
        "is_closed": True,
        "close_time": latest_event_time,
    }
    candle_key = hashlib.sha256(json.dumps({
        "close_time": latest_event_time,
        **{k: latest.get(k) for k in ("open", "high", "low", "close")},
    }, sort_keys=True).encode()).hexdigest()[:16]
    conn, updated, lifecycle_events = _connect(db_path), 0, []
    for kind, low, high, count in _clusters(confirmed, tolerance):
        center=(low+high)/2
        existing=conn.execute("SELECT * FROM historical_zones WHERE symbol=? AND timeframe=? AND zone_type=? AND ABS(center-?)<=? ORDER BY ABS(center-?) LIMIT 1",(symbol,timeframe,kind,center,tolerance,center)).fetchone()
        if existing:
            zone_id=int(existing["id"]); merged_low=min(existing["zone_low"],low); merged_high=max(existing["zone_high"],high)
            conn.execute("UPDATE historical_zones SET zone_low=?,zone_high=?,center=?,strength=MAX(strength,?),last_seen=? WHERE id=?",(merged_low,merged_high,(merged_low+merged_high)/2,min(1.0,count/5),now,zone_id))
        else:
            zone_id=conn.execute("INSERT INTO historical_zones(symbol,timeframe,zone_type,zone_low,zone_high,center,strength,first_seen,last_seen) VALUES(?,?,?,?,?,?,?,?,?)",(symbol,timeframe,kind,low,high,center,min(1.0,count/5),now,now)).lastrowid
        event_prefix = f"{symbol}:{timeframe}:{zone_id}:{candle_key}:"
        already_processed = conn.execute(
            "SELECT 1 FROM historical_zone_events WHERE zone_id=? AND event_key LIKE ? LIMIT 1",
            (zone_id, f"{event_prefix}%"),
        ).fetchone()
        if already_processed:
            updated += 1
            continue
        previous_state = _level_state(existing["lifecycle_state"] if existing else "CREATED")
        previous_side = _level_side(kind, existing["lifecycle_side"] if existing else None)
        level = PriceLevel(
            level_id=f"historical-zone:{zone_id}", symbol=symbol, kind=kind.upper(),
            side=previous_side, lower=merged_low if existing else low,
            upper=merged_high if existing else high, created_at=0,
            state=previous_state, last_event_at=(existing["last_touch"] if existing else None),
            touches=int(existing["touch_count"] if existing else 0),
            broken_from=previous_side if previous_state is LevelState.BROKEN else None,
        )
        advanced = advance_level(level, lifecycle_candle)
        event = advanced.state.value if advanced.state is not previous_state else None
        conn.execute(
            "UPDATE historical_zones SET lifecycle_state=?,lifecycle_side=?,status=? WHERE id=?",
            (advanced.state.value, advanced.side.value, _compat_status(advanced.state), zone_id),
        )
        if event:
            cursor=conn.execute("INSERT OR IGNORE INTO historical_zone_events(zone_id,event_key,event_type,candle_json,observed_at) VALUES(?,?,?,?,?)",(zone_id,f"{event_prefix}{event}",event,json.dumps(lifecycle_candle,default=str),int(latest_event_time)))
            if cursor.rowcount:
                column={"TOUCHED":"touch_count","SWEPT":"touch_count","REACTED":"reaction_count","BROKEN":"break_count"}.get(event)
                if column:
                    conn.execute(f"UPDATE historical_zones SET {column}={column}+1,last_touch=? WHERE id=?",(int(latest_event_time),zone_id))
                lifecycle_events.append({
                    "level_id": f"historical-zone:{zone_id}",
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "kind": kind.upper(),
                    "side": advanced.side.value,
                    "lower": advanced.lower,
                    "upper": advanced.upper,
                    "previous_state": previous_state.value,
                    "event_type": event,
                    "event_time": latest_event_time,
                    "touches": advanced.touches,
                })
        updated+=1
    conn.execute("UPDATE historical_zones SET status='expired' WHERE symbol=? AND timeframe=? AND last_seen<? AND status='active'",(symbol,timeframe,now-60*86400)); conn.commit(); conn.close()
    return {"status":"updated","zones":updated,"tolerance":tolerance,"lifecycle_events":lifecycle_events}

def build_zone_context(symbol: str, current_price, timeframe="", limit=8, db_path=DB_PATH):
    try:
        conn=_connect(db_path); params=[symbol]
        sql="SELECT timeframe,zone_type,zone_low,zone_high,center,strength,touch_count,reaction_count,break_count,last_seen,lifecycle_state,lifecycle_side FROM historical_zones WHERE symbol=? AND status='active'"
        if timeframe: sql+=" AND timeframe IN (?, '4h', '1d')"; params.append(timeframe)
        if current_price: sql+=" ORDER BY ABS(center-?) LIMIT ?"; params.extend((float(current_price),limit))
        else: sql+=" ORDER BY strength DESC,last_seen DESC LIMIT ?"; params.append(limit)
        zones=[dict(row) for row in conn.execute(sql,params).fetchall()]; conn.close()
        return {"available":bool(zones),"symbol":symbol,"zones":zones,"rule":"historical zones are LIVE_CONTEXT only; never replace APEX levels or strategy gates"}
    except Exception: return {"available":False,"symbol":symbol,"zones":[]}

def format_zone_context(context):
    return "HISTORICAL ZONE MAP:\n"+json.dumps(context,ensure_ascii=False,default=str,separators=(",",":"))
