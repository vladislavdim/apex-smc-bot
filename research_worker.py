"""Entry point for the isolated APEX Research / Shadow worker."""
from __future__ import annotations

import logging
import os
import signal
import time

from research.worker import ResearchWorker


logging.basicConfig(level=logging.INFO,format="%(asctime)s [%(levelname)s] %(message)s")
worker=None


def _stop(*_args):
    if worker is not None:
        worker.stop_requested=True


def main() -> None:
    global worker
    database_url=os.environ.get("APEX_MARKET_DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError("Dedicated APEX_MARKET_DATABASE_URL is required for the Research service")
    if database_url == os.environ.get("DATABASE_URL", "").strip():
        raise RuntimeError("Research database must not reuse the LIVE telemetry database")
    from research.store import ResearchStore
    worker=ResearchWorker(store=ResearchStore(database_url))
    signal.signal(signal.SIGTERM,_stop); signal.signal(signal.SIGINT,_stop)
    while not worker.stop_requested:
        try:
            worker.cycle()
        except Exception:
            logging.exception("[Research] cycle failed; checkpoint preserved")
        # Incremental refresh cadence; checkpoints make this restart-safe.
        for _ in range(180):
            if worker.stop_requested: break
            time.sleep(10)
    # Persist a final non-trading lifecycle marker. In-flight candle/feature
    # jobs already checkpoint inside the pair pipeline before this is reached.
    try:
        worker.store.set_meta("research_shutdown",{"status":"GRACEFUL","at":time.time()})
    except Exception:
        logging.exception("[Research] final shutdown marker failed")


if __name__=="__main__": main()
