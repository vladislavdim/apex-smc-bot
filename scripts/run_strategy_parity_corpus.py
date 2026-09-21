#!/usr/bin/env python3
"""Run legacy-vs-snapshot parity over the pinned real-market corpus."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apex.strategies.parity_runner import run_corpus


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--corpus", type=Path,
        default=Path("tests/fixtures/apex_v3_strategy_parity"),
    )
    parser.add_argument(
        "--output", type=Path,
        default=Path("tests/fixtures/apex_v3_strategy_parity_verdict.json"),
    )
    args = parser.parse_args()
    from bot import _get_v3_live_strategy_registry

    result = run_corpus(args.corpus, args.output, _get_v3_live_strategy_registry())
    if not result.ready:
        print("parity corpus blocked: " + ",".join(result.reasons))
        return 1
    print(f"parity corpus ready: cases={len(result.reports)} verdict={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
