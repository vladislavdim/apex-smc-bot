"""Explicit compatibility database connection and serialized writer boundary."""

from __future__ import annotations

import logging
import queue
import sqlite3
import threading

from apex.config.settings import ApexConfig
from apex.db.connection import connect_compatibility


_DB_PATH = ApexConfig.from_env().database.compatibility_db_path
_WRITE_QUEUE: queue.Queue = queue.Queue()
_WRITER_RUNNING = False
_WRITER_LOCK = threading.Lock()


def get_db_conn(path: str | None = None, timeout: int = 30) -> sqlite3.Connection:
    conn = connect_compatibility(
        path or _DB_PATH, timeout=timeout, check_same_thread=False,
    )
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _writer_loop() -> None:
    while True:
        try:
            task = _WRITE_QUEUE.get(timeout=1)
            if task is None:
                break
            sql, params, callback = task
            try:
                conn = get_db_conn()
                conn.execute(sql, params or [])
                conn.commit()
                conn.close()
                if callback:
                    callback(True)
            except Exception as exc:
                logging.warning("[DB Writer] %s", exc)
                if callback:
                    callback(False)
        except queue.Empty:
            continue
        except Exception as exc:
            logging.error("[DB Writer] Fatal: %s", exc)


def start_db_writer() -> None:
    global _WRITER_RUNNING
    with _WRITER_LOCK:
        if _WRITER_RUNNING:
            return
        threading.Thread(target=_writer_loop, daemon=True).start()
        _WRITER_RUNNING = True
    logging.info("[DB Writer] Запущен")


def db_write_async(sql: str, params: tuple | None = None) -> None:
    _WRITE_QUEUE.put((sql, params, None))


__all__ = ["db_write_async", "get_db_conn", "start_db_writer"]
