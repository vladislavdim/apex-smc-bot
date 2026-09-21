"""Persistent lifecycle telemetry for scheduler jobs."""

from __future__ import annotations

import sqlite3
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from apex.ops.resource_guard import memory_snapshot


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class JobRunRecorder:
    conn_factory: Callable[[], sqlite3.Connection]
    job_id: str
    run_id: str = ""
    started_at: str = ""
    started_monotonic: float = 0.0
    rss_before: int = 0
    cpu_before: float = 0.0
    _finished: bool = False
    _persisted: bool = False

    def __enter__(self) -> "JobRunRecorder":
        self.run_id = "job_" + uuid.uuid4().hex
        self.started_at = _now()
        self.started_monotonic = time.monotonic()
        self.rss_before = memory_snapshot().rss_bytes
        self.cpu_before = time.process_time()
        try:
            conn = self.conn_factory()
            try:
                conn.execute(
                    """INSERT INTO job_runs(run_id,job_id,started_at,rss_before,cpu_before,status)
                       VALUES(?,?,?,?,?,'RUNNING')""",
                    (self.run_id, self.job_id, self.started_at, self.rss_before, self.cpu_before),
                )
                conn.commit()
                self._persisted = True
            finally:
                conn.close()
        except Exception as exc:
            # Observability can never become authority over a production job.
            logging.warning("[JobMetrics] start persist failed safely: %s", type(exc).__name__)
        return self

    def finish(self, status: str = "OK", *, items_processed: int = 0, error_code: str = "") -> None:
        if self._finished:
            return
        rss_after = memory_snapshot().rss_bytes
        cpu_after = time.process_time()
        duration = max(0.0, time.monotonic() - self.started_monotonic) * 1000.0
        if not self._persisted:
            self._finished = True
            return
        try:
            conn = self.conn_factory()
            try:
                conn.execute(
                    """UPDATE job_runs SET finished_at=?,duration_ms=?,rss_after=?,cpu_after=?,
                              items_processed=?,status=?,error_code=? WHERE run_id=?""",
                    (_now(), duration, rss_after, cpu_after, max(0, int(items_processed)), str(status), str(error_code)[:120], self.run_id),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("[JobMetrics] finish persist failed safely: %s", type(exc).__name__)
        self._finished = True

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: Any) -> bool:
        del traceback
        self.finish(
            "ERROR" if exc is not None else "OK",
            error_code=type(exc).__name__ if exc is not None else "",
        )
        return False


_CONN_FACTORY: Callable[[], sqlite3.Connection] | None = None


def configure_job_metrics(conn_factory: Callable[[], sqlite3.Connection] | None) -> None:
    global _CONN_FACTORY
    _CONN_FACTORY = conn_factory


def recorder_for(job_id: str) -> JobRunRecorder | None:
    return JobRunRecorder(_CONN_FACTORY, str(job_id)) if _CONN_FACTORY is not None else None


__all__ = ["JobRunRecorder", "configure_job_metrics", "recorder_for"]
