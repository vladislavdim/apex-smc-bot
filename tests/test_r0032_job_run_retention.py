from datetime import datetime, timezone
import sqlite3

import pytest

from apex.db.job_run_retention import rollup_and_prune_job_runs


NOW = datetime(2026, 10, 27, 13, 4, tzinfo=timezone.utc)


def _db():
    conn = sqlite3.connect(":memory:")
    conn.execute("""CREATE TABLE job_runs(
        run_id TEXT PRIMARY KEY,job_id TEXT,started_at TEXT,finished_at TEXT,
        duration_ms REAL,rss_before INTEGER,rss_after INTEGER,cpu_before REAL,
        cpu_after REAL,items_processed INTEGER,status TEXT,error_code TEXT)""")
    return conn


def test_rollup_is_atomic_idempotent_and_keeps_recent_and_running_rows():
    conn = _db()
    rows = [
        ("old1","watch","2026-10-01T00:00:00+00:00","2026-10-01T00:00:01+00:00",100,1,4,1,3,2,"OK",""),
        ("old2","watch","2026-10-01T01:00:00+00:00","2026-10-01T01:00:01+00:00",200,1,5,1,4,3,"OK",""),
        ("recent","watch","2026-10-26T00:00:00+00:00","2026-10-26T00:00:01+00:00",300,1,6,1,5,4,"OK",""),
        ("running","scan","2026-10-01T00:00:00+00:00",None,None,1,None,1,None,0,"RUNNING",""),
    ]
    conn.executemany("INSERT INTO job_runs VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    first = rollup_and_prune_job_runs(conn, raw_days=7, now=NOW)
    assert first == {"eligible": 2, "deleted": 2, "raw_remaining": 2, "rollup_rows": 1}
    assert conn.execute("SELECT runs,duration_ms,items_processed,max_rss_after,max_cpu_after FROM job_run_daily").fetchone() == (2,300,5,5,4)
    assert {row[0] for row in conn.execute("SELECT run_id FROM job_runs")} == {"recent","running"}
    second = rollup_and_prune_job_runs(conn, raw_days=7, now=NOW)
    assert second == {"eligible": 0, "deleted": 0, "raw_remaining": 2, "rollup_rows": 1}
    assert conn.execute("SELECT runs FROM job_run_daily").fetchone()[0] == 2


def test_invalid_retention_fails_without_mutation():
    conn = _db()
    try:
        rollup_and_prune_job_runs(conn, raw_days=0, now=NOW)
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError")
    assert conn.execute("SELECT name FROM sqlite_master WHERE name='job_run_daily'").fetchone() is None


def test_rollup_and_delete_rollback_together_on_failure():
    conn = _db()
    conn.execute(
        "INSERT INTO job_runs VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        ("old", "watch", "2026-10-01T00:00:00+00:00", "2026-10-01T00:00:01+00:00",
         100, 1, 4, 1, 3, 2, "OK", ""),
    )
    conn.commit()
    conn.execute("""CREATE TRIGGER reject_job_delete BEFORE DELETE ON job_runs
                    BEGIN SELECT RAISE(ABORT,'delete rejected'); END""")
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError, match="delete rejected"):
        rollup_and_prune_job_runs(conn, raw_days=7, now=NOW)
    conn.rollback()
    assert conn.execute("SELECT COUNT(*) FROM job_runs").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM job_run_daily").fetchone()[0] == 0
