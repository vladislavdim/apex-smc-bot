"""Fail-soft compatibility storage for UI news and advisory knowledge."""

from __future__ import annotations

import logging

from apex.db.compatibility_runtime import get_db_conn


def save_news(query: str, content: str) -> None:
    conn = None
    try:
        conn = get_db_conn(timeout=30)
        conn.execute(
            "INSERT INTO news_cache VALUES(NULL,?,?,CURRENT_TIMESTAMP)",
            (query, content[:1000]),
        )
        conn.commit()
    except Exception as exc:
        logging.warning("UI context storage failed: %s", type(exc).__name__)
    finally:
        if conn is not None:
            conn.close()


def get_recent_news() -> str:
    conn = None
    try:
        conn = get_db_conn(timeout=30)
        rows = conn.execute(
            "SELECT query,content FROM news_cache ORDER BY created_at DESC LIMIT 3"
        ).fetchall()
        return "\n\n".join(f"{row[0]}: {row[1]}" for row in rows)
    except Exception as exc:
        logging.warning("UI context read failed: %s", type(exc).__name__)
        return ""
    finally:
        if conn is not None:
            conn.close()


def save_knowledge(topic: str, content: str, source: str = "auto") -> None:
    conn = None
    try:
        conn = get_db_conn(timeout=30)
        conn.execute(
            """INSERT INTO knowledge(topic,content,source,created_at)
               VALUES(?,?,?,CURRENT_TIMESTAMP)""",
            (topic, content, source),
        )
        conn.commit()
    except Exception as exc:
        logging.warning("UI context storage failed: %s", type(exc).__name__)
    finally:
        if conn is not None:
            conn.close()


def get_knowledge(topic: str) -> str:
    conn = None
    try:
        conn = get_db_conn(timeout=30)
        rows = conn.execute(
            """SELECT content FROM knowledge WHERE topic LIKE ?
               ORDER BY created_at DESC LIMIT 3""",
            (f"%{topic}%",),
        ).fetchall()
        return "\n".join(row[0] for row in rows)
    except Exception as exc:
        logging.warning("UI context read failed: %s", type(exc).__name__)
        return ""
    finally:
        if conn is not None:
            conn.close()


__all__ = ["get_knowledge", "get_recent_news", "save_knowledge", "save_news"]
