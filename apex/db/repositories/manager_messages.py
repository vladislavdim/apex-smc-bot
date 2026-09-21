"""Durable Telegram message identities for real Manager positions."""

from __future__ import annotations

import sqlite3
from typing import Any, Callable


class ManagerMessageRepository:
    def __init__(self, connection_factory: Callable[[], sqlite3.Connection]) -> None:
        self.connection_factory = connection_factory

    def load(self, signal_id: int, chat_id: int, thread_id: int = 0) -> dict[str, Any] | None:
        conn = self.connection_factory()
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """SELECT * FROM trade_manager_messages
                   WHERE signal_id=? AND chat_id=? AND thread_id=?""",
                (int(signal_id), int(chat_id), int(thread_id or 0)),
            ).fetchone()
            return dict(row) if row is not None else None
        finally:
            conn.close()

    def store(
        self, signal_id: int, chat_id: int, thread_id: int, message_id: int,
        content_hash: str, is_final: bool,
    ) -> None:
        conn = self.connection_factory()
        try:
            conn.execute(
                """INSERT INTO trade_manager_messages
                   (signal_id,chat_id,thread_id,message_id,content_hash,is_final,updated_at)
                   VALUES(?,?,?,?,?,?,CURRENT_TIMESTAMP)
                   ON CONFLICT(signal_id,chat_id,thread_id) DO UPDATE SET
                     message_id=excluded.message_id,
                     content_hash=excluded.content_hash,
                     is_final=MAX(trade_manager_messages.is_final,excluded.is_final),
                     updated_at=CURRENT_TIMESTAMP""",
                (
                    int(signal_id), int(chat_id), int(thread_id or 0),
                    int(message_id), str(content_hash), int(bool(is_final)),
                ),
            )
            conn.commit()
        finally:
            conn.close()


__all__ = ["ManagerMessageRepository"]
