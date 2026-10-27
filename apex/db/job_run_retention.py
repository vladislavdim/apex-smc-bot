"""Bound raw scheduler telemetry while preserving durable daily evidence."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone


def rollup_and_prune_job_runs(
    conn: sqlite3.Connection,
    *,
    raw_days: int = 7,
    now: datetime | None = None,
) -> dict[str, int]:
    """Roll up finished raw rows before *raw_days*, then delete them atomically.

    Safety/execution tables are never touched. Repeated calls are idempotent
    because rows are removed only after the same transaction updates rollups.
    """
    if int(raw_days) < 1:
        raise ValueError("raw_days must be positive")
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    cutoff = (current - timedelta(days=int(raw_days))).isoformat(timespec="seconds")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS job_run_daily (
               day TEXT NOT NULL,
               job_id TEXT NOT NULL,
               status TEXT NOT NULL,
               error_code TEXT NOT NULL,
               runs INTEGER NOT NULL,
               duration_ms REAL NOT NULL,
               items_processed INTEGER NOT NULL,
               max_rss_after INTEGER NOT NULL,
               max_cpu_after REAL NOT NULL,
               PRIMARY KEY(day,job_id,status,error_code)
           )"""
    )
    eligible = int(
        conn.execute(
            "SELECT COUNT(*) FROM job_runs WHERE finished_at IS NOT NULL AND finished_at<?",
            (cutoff,),
        ).fetchone()[0]
    )
    conn.execute(
        """INSERT INTO job_run_daily(
               day,job_id,status,error_code,runs,duration_ms,items_processed,
               max_rss_after,max_cpu_after)
           SELECT substr(started_at,1,10),job_id,status,error_code,COUNT(*),
                  COALESCE(SUM(duration_ms),0),COALESCE(SUM(items_processed),0),
                  COALESCE(MAX(rss_after),0),COALESCE(MAX(cpu_after),0)
             FROM job_runs
            WHERE finished_at IS NOT NULL AND finished_at<?
            GROUP BY substr(started_at,1,10),job_id,status,error_code
           ON CONFLICT(day,job_id,status,error_code) DO UPDATE SET
               runs=job_run_daily.runs+excluded.runs,
               duration_ms=job_run_daily.duration_ms+excluded.duration_ms,
               items_processed=job_run_daily.items_processed+excluded.items_processed,
               max_rss_after=MAX(job_run_daily.max_rss_after,excluded.max_rss_after),
               max_cpu_after=MAX(job_run_daily.max_cpu_after,excluded.max_cpu_after)""",
        (cutoff,),
    )
    deleted = conn.execute(
        "DELETE FROM job_runs WHERE finished_at IS NOT NULL AND finished_at<?",
        (cutoff,),
    ).rowcount
    return {
        "eligible": eligible,
        "deleted": int(deleted),
        "raw_remaining": int(conn.execute("SELECT COUNT(*) FROM job_runs").fetchone()[0]),
        "rollup_rows": int(conn.execute("SELECT COUNT(*) FROM job_run_daily").fetchone()[0]),
    }


__all__ = ["rollup_and_prune_job_runs"]
