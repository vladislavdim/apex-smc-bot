"""Bounded legacy signal writer retained only during the State cutover."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class LegacySignalPersistence:
    connector: Callable
    database_path: str
    register_waiting: Callable
    lifecycle_available: bool = True
    sleeper: Callable[[float], None] = time.sleep

    def save(
        self, symbol, direction, signal_type, entry, tp1, tp2, tp3, sl,
        timeframe, estimated_hours, grade, confluence=0, regime="UNKNOWN",
    ):
        """Persist a delivered compatibility signal awaiting entry activation."""
        learning_id = None
        if not self.lifecycle_available:
            logging.error(
                "save_signal_db: lifecycle module unavailable — persistence blocked"
            )
            return None, None
        try:
            precheck = self.connector(
                self.database_path, timeout=10, check_same_thread=False,
            )
            existing = precheck.execute(
                "SELECT id FROM signals WHERE symbol=? AND timeframe=? "
                "AND direction=? AND result='pending' LIMIT 1",
                (symbol, timeframe, direction),
            ).fetchone()
            precheck.close()
            if existing:
                logging.info(
                    "save_signal_db: duplicate %s %s %s skipped",
                    symbol, timeframe, direction,
                )
                return None, None
        except Exception as error:
            logging.warning("save_signal_db precheck: %s", error)

        for attempt in range(5):
            try:
                conn = self.connector(
                    self.database_path, timeout=30, check_same_thread=False,
                )
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA busy_timeout=15000")
                existing = conn.execute(
                    "SELECT id FROM signals WHERE symbol=? AND timeframe=? "
                    "AND direction=? AND result='pending' LIMIT 1",
                    (symbol, timeframe, direction),
                ).fetchone()
                if existing:
                    conn.close()
                    logging.info(
                        "save_signal_db: duplicate %s %s %s skipped (id=%s)",
                        symbol, timeframe, direction, existing[0],
                    )
                    return None, learning_id
                cursor = conn.execute(
                    """INSERT INTO signals
                       (symbol, direction, signal_type, entry, tp1, tp2, tp3, sl,
                        timeframe, estimated_hours, grade, result, created_at,
                        closed_at, learning_id, confluence, regime)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending',
                               CURRENT_TIMESTAMP, NULL, ?, ?, ?)""",
                    (
                        symbol, direction, signal_type, entry, tp1, tp2, tp3,
                        sl, timeframe, estimated_hours, grade, learning_id,
                        confluence, regime,
                    ),
                )
                signal_id = cursor.lastrowid
                self.register_waiting(conn, signal_id)
                conn.commit()
                conn.close()
                logging.info(
                    "Signal saved awaiting entry: %s %s (ID: %s)",
                    symbol, direction, signal_id,
                )
                return signal_id, learning_id
            except Exception as error:
                if "locked" in str(error).lower() and attempt < 4:
                    logging.warning("save_signal locked, retry %s...", attempt + 1)
                    self.sleeper(1 + attempt)
                    continue
                logging.error("save_signal: %s", error)
                break
        return None, learning_id


_service: LegacySignalPersistence | None = None


def configure_signal_persistence(service: LegacySignalPersistence | None) -> None:
    global _service
    _service = service


def save_signal_db(
    symbol, direction, signal_type, entry, tp1, tp2, tp3, sl, timeframe,
    est_hours, grade, confluence=0, regime="UNKNOWN",
):
    service = _service
    if service is None:
        logging.error("save_signal_db: compatibility persistence is not configured")
        return None, None
    return service.save(
        symbol, direction, signal_type, entry, tp1, tp2, tp3, sl, timeframe,
        est_hours, grade, confluence, regime,
    )


__all__ = [
    "LegacySignalPersistence",
    "configure_signal_persistence",
    "save_signal_db",
]
