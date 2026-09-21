#!/usr/bin/env python3
"""Capture one create-only Gate fixture for the V3 parity corpus."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apex.domain.enums import Strategy
from apex.market.gate_client import GateMarketClient
from apex.market.provider import GateSnapshotProvider
from apex.strategies.capture import capture_gate_case


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", required=True, choices=[row.value for row in Strategy])
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--candle-limit", type=int, default=500)
    args = parser.parse_args()
    provider = GateSnapshotProvider(
        GateMarketClient(), candle_limit=args.candle_limit,
    )
    case = capture_gate_case(
        provider, Strategy(args.strategy), args.symbol, args.output,
        as_of=datetime.now(timezone.utc),
    )
    print(f"captured {case.case_id} sha256={case.sha256} path={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
