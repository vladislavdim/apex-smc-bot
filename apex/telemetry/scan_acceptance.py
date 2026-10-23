"""Compact proof of completed universe rounds, distinct from process readiness."""
STRATEGIES = ("FAST", "MTF", "ZONE", "SWING", "WYCKOFF")


def completed_rounds(conn):
    return {strategy: conn.execute(
        "SELECT MAX(completed_at) FROM scan_coverage_rounds WHERE strategy=? AND status='COMPLETED'",
        (strategy,),
    ).fetchone()[0] for strategy in STRATEGIES}
