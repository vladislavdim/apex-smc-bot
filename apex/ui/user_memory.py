"""Compatibility-backed user profile and chat-history service."""

from __future__ import annotations

import logging
from datetime import datetime

from apex.db.compatibility_runtime import get_db_conn


_EMPTY_MEMORY = {
    "name": "", "profile": "", "preferences": "", "coins": "",
    "messages": 0, "deposit": 0, "risk": 1.0,
}


def get_user_memory(user_id: int) -> dict:
    try:
        conn = get_db_conn(timeout=30)
        row = conn.execute(
            """SELECT name,profile,preferences,coins_mentioned,total_messages,
                      deposit,risk_percent FROM user_memory WHERE user_id=?""",
            (user_id,),
        ).fetchone()
        conn.close()
        if row:
            return {
                "name": row[0] or "", "profile": row[1] or "",
                "preferences": row[2] or "", "coins": row[3] or "",
                "messages": row[4] or 0, "deposit": row[5] or 0,
                "risk": row[6] or 1.0,
            }
    except Exception:
        pass
    return dict(_EMPTY_MEMORY)


def update_user_memory(
    user_id: int, name: str = "", profile=None, preferences=None,
    coins=None, deposit=None, risk=None,
) -> None:
    try:
        conn = get_db_conn(timeout=30)
        now = datetime.now().isoformat()
        existing = conn.execute(
            "SELECT user_id FROM user_memory WHERE user_id=?", (user_id,),
        ).fetchone()
        if existing:
            updates = ["total_messages = total_messages + 1", "last_seen = ?"]
            params = [now]
            for column, value in (
                ("name", name), ("profile", profile),
                ("preferences", preferences), ("coins_mentioned", coins),
            ):
                if value:
                    updates.append(f"{column} = ?")
                    params.append(value)
            for column, value in (("deposit", deposit), ("risk_percent", risk)):
                if value is not None:
                    updates.append(f"{column} = ?")
                    params.append(value)
            params.append(user_id)
            conn.execute(
                f"UPDATE user_memory SET {', '.join(updates)} WHERE user_id=?", params,
            )
        else:
            conn.execute(
                """INSERT INTO user_memory(
                       user_id,name,profile,preferences,coins_mentioned,deposit,
                       risk_percent,total_messages,first_seen,last_seen
                   ) VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (
                    user_id, name, profile or "", preferences or "", coins or "",
                    deposit or 0, 1.0, 0, now, now,
                ),
            )
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.error("Memory error: %s", exc)


def save_chat_log(user_id: int, role: str, content: str) -> None:
    try:
        conn = get_db_conn(timeout=30)
        conn.execute(
            """INSERT INTO chat_log(user_id,role,content,created_at)
               VALUES(?,?,?,CURRENT_TIMESTAMP)""",
            (user_id, role, content[:2000]),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def get_chat_history(user_id: int, limit: int = 15) -> list:
    try:
        conn = get_db_conn(timeout=30)
        rows = conn.execute(
            """SELECT role,content FROM chat_log WHERE user_id=?
               ORDER BY id DESC LIMIT ?""",
            (user_id, limit),
        ).fetchall()
        conn.close()
        return list(reversed(rows))
    except Exception:
        return []


__all__ = [
    "get_chat_history", "get_user_memory", "save_chat_log", "update_user_memory",
]
