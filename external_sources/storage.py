"""Isolated audit storage for external context; failures are non-fatal."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from typing import Any

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "brain.db")


def persist_context(context: dict[str, Any], strategy: str | None, used_in_groq: bool, result: str | None = None) -> None:
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""CREATE TABLE IF NOT EXISTS external_market_context (
          id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, strategy TEXT, timestamp TEXT,
          payload_json TEXT, bias TEXT, confidence REAL, data_age INTEGER, used_in_groq INTEGER,
          result TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS external_source_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, strategy TEXT, timestamp TEXT,
          source TEXT, status TEXT, payload_json TEXT, data_age INTEGER, error_message TEXT,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
        conn.execute("INSERT INTO external_market_context (symbol,strategy,timestamp,payload_json,bias,confidence,data_age,used_in_groq,result) VALUES (?,?,?,?,?,?,?,?,?)", (
          context.get("symbol"), strategy, context.get("timestamp"), json.dumps(context, default=str), context.get("external_bias"),
          context.get("external_confidence", 0), context.get("data_quality", {}).get("age_seconds"), int(used_in_groq), result))
        for source in context.get("_source_results", []):
            conn.execute("INSERT INTO external_source_events (symbol,strategy,timestamp,source,status,payload_json,data_age,error_message) VALUES (?,?,?,?,?,?,?,?)", (
              context.get("symbol"), strategy, context.get("timestamp"), source.get("source"), source.get("status"),
              json.dumps(source, default=str), source.get("age_seconds"), source.get("error")))
        conn.commit(); conn.close()
    except Exception as exc:
        logging.warning("[ExternalSources] persistence failed: %s", exc)


def persist_shadow_source(result: dict[str, Any]) -> None:
    """Separate shadow store: its values are never returned to live Groq callers."""
    from contextlib import closing
    path = os.getenv('APEX_DB_PATH') or os.getenv('APEX_BRAIN_DB_PATH') or DB_PATH
    with closing(sqlite3.connect(path, timeout=2)) as conn, conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS external_shadow_snapshots (
            source TEXT NOT NULL, symbol TEXT NOT NULL, payload_json TEXT NOT NULL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(source,symbol))''')
        conn.execute('''INSERT INTO external_shadow_snapshots(source,symbol,payload_json)
            VALUES(?,?,?) ON CONFLICT(source,symbol) DO UPDATE SET
            payload_json=excluded.payload_json,updated_at=CURRENT_TIMESTAMP''',
            (result.get('source','unknown'),result.get('symbol',''),json.dumps(result,default=str)))


def persist_gate_microstructure(symbol: str, features: dict[str, Any], db_path: str | None = None) -> None:
    """Persist bounded Gate WS features for read-only diagnostics.

    The table is intentionally separate from normalized external context so a
    WS imbalance can never become an entry gate or a Manager command by itself.
    """
    path = db_path or os.getenv('APEX_DB_PATH') or os.getenv('APEX_BRAIN_DB_PATH') or DB_PATH
    import hashlib
    import time
    payload = json.dumps(features or {}, ensure_ascii=False, sort_keys=True, default=str)
    update_id = str((features or {}).get('update_id') or int(time.time()))
    key = hashlib.sha256(f"{str(symbol).upper()}:{update_id}".encode()).hexdigest()
    with sqlite3.connect(path, timeout=2) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS gate_microstructure_shadow (
            event_key TEXT PRIMARY KEY, symbol TEXT NOT NULL, update_id TEXT,
            payload_json TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP)''')
        conn.execute('''INSERT OR IGNORE INTO gate_microstructure_shadow(event_key,symbol,update_id,payload_json)
                        VALUES(?,?,?,?)''', (key, str(symbol).upper(), update_id, payload))
