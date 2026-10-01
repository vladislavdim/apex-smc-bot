#!/usr/bin/env python3
"""Inspect a signal across downloaded State/Memory/brain snapshots; no trading I/O."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from apex.telemetry.signal_trace import signal_trace


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("signal_id", type=int)
    parser.add_argument("--state", required=True)
    parser.add_argument("--memory")
    parser.add_argument("--brain")
    args = parser.parse_args()
    print(json.dumps(signal_trace(args.state, args.signal_id, memory_path=args.memory,
                                  brain_path=args.brain), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
