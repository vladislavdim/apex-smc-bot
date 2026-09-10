"""Build or increment the BTC-only Research database used as a GitHub Release asset."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# ``python scripts/<file>.py`` puts only ``scripts/`` on sys.path.  Add the
# repository root explicitly so the same command works in GitHub Actions and
# in a local checkout without relying on an ambient PYTHONPATH.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.store import ResearchStore
from research.worker import ResearchWorker


def main() -> None:
    root = Path(os.environ.get("APEX_RESEARCH_OUTPUT_DIR", "research-data"))
    root.mkdir(parents=True, exist_ok=True)
    path = root / "BTCUSDT.research.db"
    os.environ.update({
        "APEX_RESEARCH_PAIRS": "BTCUSDT",
        "APEX_RESEARCH_PAIR_LIMIT": "1",
        "APEX_RESEARCH_HISTORY_DAYS": "365",
        "APEX_RESEARCH_FAST_PAIRS": "",
        "APEX_RESEARCH_MAX_RSS_MB": os.environ.get("APEX_RESEARCH_MAX_RSS_MB", "2048"),
        "APEX_RESEARCH_CPU_DUTY_PERCENT": os.environ.get("APEX_RESEARCH_CPU_DUTY_PERCENT", "80"),
    })
    store = ResearchStore(str(path))
    worker = ResearchWorker(store=store)
    worker.cycle()
    dashboard = store.dashboard()
    runs = [row for row in (dashboard.get("runs") or []) if isinstance(row, dict)]
    latest = runs[0] if runs else None
    if not latest:
        raise RuntimeError("BTC research produced no persisted run; snapshot is not publishable")
    if latest.get("status") != "COMPLETED" or float(latest.get("progress") or 0) < 100:
        raise RuntimeError(
            "BTC research did not complete: "
            f"status={latest.get('status')!s} progress={latest.get('progress')!s}"
        )
    candle_coverage = {}
    for row in dashboard.get("candles") or []:
        if not isinstance(row, dict) or not row.get("timeframe"):
            continue
        candle_coverage[str(row["timeframe"])] = {
            "candles": int(row.get("candles") or 0),
            "coverage_start": row.get("coverage_start"),
            "coverage_end": row.get("coverage_end"),
        }
    dashboard["storage"] = {
        "kind": "GITHUB_RELEASE_ASSET",
        "symbol": "BTCUSDT",
        "requested_history_days": 365,
        "timeframes": ["15m", "1h", "4h", "1d"],
        "candle_coverage": candle_coverage,
        "gate_recent_limit_points": 10000,
        "coverage_policy": "GATE_CANONICAL_ONLY_NO_VENUE_SUBSTITUTION",
        "incremental": True,
        "snapshot_version": "research-snapshot-v2",
        "research_run_id": latest.get("research_run_id"),
        "run_status": latest.get("status"),
        "run_progress": latest.get("progress"),
        "dataset_version": latest.get("dataset_version"),
        "feature_version": latest.get("feature_version"),
        "historical_order_book": "UNAVAILABLE",
        "live_order_book_collection": "SEPARATE_FORWARD_ONLY",
        "no_real_execution": True,
        "live_activation": "FORBIDDEN",
    }
    (root / "BTCUSDT.dashboard.json").write_text(
        json.dumps(dashboard, ensure_ascii=False, separators=(",", ":"), default=str),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
