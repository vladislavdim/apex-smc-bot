"""Atomic Telegram signal delivery claims.

The worker can reach the same candidate from overlapping/manual scan paths.  A
claim is written before Telegram delivery so only one coroutine in the current
database generation may send it.  Failed/cancelled deliveries release their
claim; successful claims remain the normal cooldown record and are included in
the immediate durable brain checkpoint.
"""

from __future__ import annotations

import sqlite3


def signal_delivery_key(candidate: dict, strategy: str) -> str:
    """Build a stable key independent of display labels or quality grades."""
    return ":".join((
        str(candidate.get("symbol") or "").upper(),
        str(strategy or "MTF").upper(),
        str(candidate.get("direction") or "").upper(),
        str(candidate.get("timeframe") or "1h").lower(),
    ))


def claim_signal_delivery(
    db_path: str,
    cache_key: str,
    now_ts: float,
    cooldown_seconds: float,
) -> bool:
    """Atomically reserve a signal delivery in the existing cooldown table."""
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("""CREATE TABLE IF NOT EXISTS signal_cooldown (
            cache_key TEXT PRIMARY KEY,
            sent_at REAL
        )""")
        row = conn.execute(
            "SELECT sent_at FROM signal_cooldown WHERE cache_key=?",
            (cache_key,),
        ).fetchone()
        last_sent = float(row[0] or 0) if row else 0.0
        if last_sent > 0 and now_ts - last_sent < cooldown_seconds:
            conn.rollback()
            return False
        conn.execute(
            "INSERT OR REPLACE INTO signal_cooldown (cache_key, sent_at) VALUES (?, ?)",
            (cache_key, float(now_ts)),
        )
        conn.commit()
        return True
    finally:
        conn.close()


def release_signal_delivery_claim(db_path: str, cache_key: str, claim_ts: float) -> None:
    """Release only this attempt's claim after a confirmed delivery failure."""
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        conn.execute(
            "DELETE FROM signal_cooldown WHERE cache_key=? AND sent_at=?",
            (cache_key, float(claim_ts)),
        )
        conn.commit()
    finally:
        conn.close()
