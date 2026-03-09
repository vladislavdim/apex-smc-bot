import sqlite3
from datetime import datetime

DB_PATH = "signals.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, interval TEXT, direction TEXT,
            signal_type TEXT, entry_price REAL, tp REAL, sl REAL,
            result TEXT DEFAULT 'PENDING',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stats (
            symbol TEXT PRIMARY KEY,
            total INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            win_rate REAL DEFAULT 0.0
        )
    """)
    conn.commit()
    conn.close()

def save_signal(symbol, interval, direction, signal_type, entry, tp, sl):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO signals (symbol,interval,direction,signal_type,entry_price,tp,sl) VALUES (?,?,?,?,?,?,?)",
        (symbol, interval, direction, signal_type, entry, tp, sl)
    )
    conn.commit()
    conn.close()

def update_result(signal_id, result):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE signals SET result=? WHERE id=?", (result, signal_id))
    conn.commit()
    row = conn.execute("SELECT symbol FROM signals WHERE id=?", (signal_id,)).fetchone()
    if row:
        _recalc_stats(conn, row[0])
    conn.close()

def _recalc_stats(conn, symbol):
    rows = conn.execute(
        "SELECT result FROM signals WHERE symbol=? AND result != 'PENDING'", (symbol,)
    ).fetchall()
    total = len(rows)
    wins = sum(1 for r in rows if r[0] == "WIN")
    losses = total - wins
    win_rate = (wins / total * 100) if total > 0 else 0
    conn.execute(
        "INSERT OR REPLACE INTO stats (symbol,total,wins,losses,win_rate) VALUES (?,?,?,?,?)",
        (symbol, total, wins, losses, win_rate)
    )
    conn.commit()

def get_win_rate(symbol):
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute("SELECT win_rate, total FROM stats WHERE symbol=?", (symbol,)).fetchone()
    conn.close()
    return row if row else (0.0, 0)

def get_min_confluence(symbol):
    wr, total = get_win_rate(symbol)
    if total < 10:
        return 2
    if wr < 40:
        return 4
    if wr < 55:
        return 3
    return 2

def get_all_stats():
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT symbol, total, wins, win_rate FROM stats ORDER BY win_rate DESC").fetchall()
    conn.close()
    return rows

init_db()
