"""Append-only news context audit storage; existing brain.db tables are untouched."""

from __future__ import annotations

import json
import os
import sqlite3
from typing import Any


DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "brain.db")


def persist_news_context(context: dict[str, Any], strategy: str, decision: str | None) -> None:
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""CREATE TABLE IF NOT EXISTS news_market_context (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, strategy TEXT, risk_level TEXT, phase TEXT,
            payload_json TEXT, used_in_groq INTEGER DEFAULT 1,
            result TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        conn.execute(
            """INSERT INTO news_market_context
               (symbol, strategy, risk_level, phase, payload_json, used_in_groq, result)
               VALUES (?, ?, ?, ?, ?, 1, ?)""",
            (context.get("symbol"), strategy, context.get("risk_level"), context.get("phase"),
             json.dumps(context, ensure_ascii=False, default=str), decision),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass
