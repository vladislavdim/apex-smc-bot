"""Entry point for the isolated APEX Research / Shadow worker."""
from __future__ import annotations

import logging
import signal
import time

from research.worker import ResearchWorker


logging.basicConfig(level=logging.INFO,format="%(asctime)s [%(levelname)s] %(message)s")
worker=ResearchWorker()


def _stop(*_args):
    worker.stop_requested=True


def main() -> None:
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


if __name__=="__main__": main()
