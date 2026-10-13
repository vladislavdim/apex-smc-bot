"""Small durable daily totals survive scanner-detail retention and redeploys."""
from __future__ import annotations

FIELDS = ("pairs_attempted", "data_ok", "data_failed", "filtered", "candidates",
          "groq_approve", "groq_wait", "groq_reject", "delivered")


def ensure_scan_statistics(conn):
    conn.execute("BEGIN IMMEDIATE")
    try:
        columns = ",".join(f"{name} INTEGER NOT NULL DEFAULT 0" for name in FIELDS)
        conn.execute(f"CREATE TABLE IF NOT EXISTS scan_statistics_daily(day TEXT NOT NULL,"
                     f"strategy TEXT NOT NULL,{columns},PRIMARY KEY(day,strategy))")
        conn.execute("CREATE TABLE IF NOT EXISTS scan_statistics_meta(id INTEGER PRIMARY KEY CHECK(id=1))")
        names = ",".join(FIELDS)
        if conn.execute("SELECT 1 FROM scan_statistics_meta WHERE id=1").fetchone() is None:
            totals = ",".join(f"SUM(COALESCE({name},0))" for name in FIELDS)
            conn.execute(f"INSERT INTO scan_statistics_daily(day,strategy,{names}) "
                         f"SELECT DATE(started_at),UPPER(strategy),{totals} FROM scan_runs "
                         "WHERE DATE(started_at) IS NOT NULL GROUP BY DATE(started_at),UPPER(strategy)")
            conn.execute("INSERT INTO scan_statistics_meta VALUES(1)")
        for operation in ("INSERT", "UPDATE"):
            values = ",".join(f"COALESCE(NEW.{name},0)" +
                              (f"-COALESCE(OLD.{name},0)" if operation == "UPDATE" else "") for name in FIELDS)
            updates = ",".join(f"{name}={name}+excluded.{name}" for name in FIELDS)
            of = " OF " + names if operation == "UPDATE" else ""
            conn.execute(f"CREATE TRIGGER IF NOT EXISTS scan_statistics_{operation.lower()} "
                         f"AFTER {operation}{of} ON scan_runs BEGIN "
                         f"INSERT INTO scan_statistics_daily(day,strategy,{names}) "
                         f"VALUES(DATE(NEW.started_at),UPPER(NEW.strategy),{values}) "
                         f"ON CONFLICT(day,strategy) DO UPDATE SET {updates}; END")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
