#!/usr/bin/env python3
"""Atomically capture all five V3 strategy fixtures from Gate."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apex.market.gate_client import GateMarketClient
from apex.market.provider import GateSnapshotProvider
from apex.strategies.capture import capture_gate_corpus


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--output-directory", required=True, type=Path)
    parser.add_argument("--candle-limit", type=int, default=500)
    args = parser.parse_args()
    cases = capture_gate_corpus(
        GateSnapshotProvider(
            GateMarketClient(), candle_limit=args.candle_limit,
        ),
        args.symbol,
        args.output_directory,
        as_of=datetime.now(timezone.utc),
    )
    print(
        f"captured corpus cases={len(cases)} symbol={cases[0].snapshot.symbol} "
        f"path={args.output_directory}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
