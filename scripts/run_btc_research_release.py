"""Build or increment the BTC-only Research database used as a GitHub Release asset."""
from __future__ import annotations

import json
import os
from pathlib import Path

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
    dashboard["storage"] = {
        "kind": "GITHUB_RELEASE_ASSET",
        "symbol": "BTCUSDT",
        "history_days": 365,
        "timeframes": ["15m", "1h", "4h", "1d"],
        "incremental": True,
        "historical_order_book": "UNAVAILABLE",
        "live_order_book_collection": "SEPARATE_FORWARD_ONLY",
    }
    (root / "BTCUSDT.dashboard.json").write_text(
        json.dumps(dashboard, ensure_ascii=False, separators=(",", ":"), default=str),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
