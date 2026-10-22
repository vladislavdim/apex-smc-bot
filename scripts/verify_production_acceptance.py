#!/usr/bin/env python3
"""Validate actual same-release health observations over a 48–72h window.

Input is JSONL of health objects or Render log records with a message field.
Missing history is an incomplete acceptance, never evidence of a healthy run.
"""

from __future__ import annotations
import argparse
from datetime import datetime, timezone
from math import isfinite
import json
from pathlib import Path


class AcceptanceIncomplete(ValueError):
    pass


def health_records(lines):
    for line in lines:
        if not line.strip():
            continue
        row = json.loads(line)
        if "message" in row:
            message = row["message"]
            marker = "[APEX Release Ready] "
            if marker not in message:
                continue
            row = json.loads(message.split(marker, 1)[1])
        if "release_sha" in row:
            yield row


def verify_acceptance(records, sha, *, hours=48, max_gap_seconds=900):
    if len(sha) != 40 or any(c not in "0123456789abcdef" for c in sha):
        raise ValueError("full_release_sha_required")
    if not 48 <= hours <= 72:
        raise ValueError("acceptance_window_must_be_48_to_72_hours")
    rows = sorted(
        (r for r in records if r.get("release_sha") == sha),
        key=lambda r: r["observed_at"],
    )
    if len(rows) < 2:
        raise AcceptanceIncomplete("health_history_missing")
    times = []
    now = datetime.now(timezone.utc).timestamp()
    proof = None
    generations = {}
    for row in rows:
        timestamp = datetime.fromisoformat(row["observed_at"].replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            raise AcceptanceIncomplete("observation_timezone_missing")
        if timestamp.timestamp() > now + 60:
            raise AcceptanceIncomplete("observation_is_in_the_future")
        times.append(timestamp.timestamp())
        if row.get("ready") is not True or row.get("reason_codes"):
            raise AcceptanceIncomplete("unhealthy_observation:" + row["observed_at"])
        if row.get("auto_trading_enabled") is not False:
            raise AcceptanceIncomplete("unexpected_execution_mode")
        if row.get("strategy_snapshot_active") is not True or not row.get(
            "strategy_proof_sha256"
        ):
            raise AcceptanceIncomplete("snapshot_cutover_not_verified")
        if proof is not None and proof != row["strategy_proof_sha256"]:
            raise AcceptanceIncomplete("strategy_proof_changed")
        proof = row["strategy_proof_sha256"]
        owner = row.get("instance_id")
        generation = row.get("fencing_generation")
        if not owner or not isinstance(generation, int):
            raise AcceptanceIncomplete("ownership_observation_missing")
        if generation in generations and generations[generation] != owner:
            raise AcceptanceIncomplete("conflicting_fencing_owner")
        generations[generation] = owner
        memory = row.get("memory", {})
        used, limit = memory.get("working_set_bytes"), memory.get("limit_bytes")
        if (
            not isinstance(used, (int, float))
            or not isinstance(limit, (int, float))
            or not isfinite(used)
            or not isfinite(limit)
            or used < 0
            or limit <= 0
            or used / limit >= 0.9
        ):
            raise AcceptanceIncomplete("memory_acceptance_failed")
    gap = max(right - left for left, right in zip(times, times[1:]))
    if gap > max_gap_seconds:
        raise AcceptanceIncomplete("health_observation_gap:" + str(gap))
    duration = times[-1] - times[0]
    if duration < hours * 3600:
        raise AcceptanceIncomplete("acceptance_window_not_elapsed")
    return {
        "release_sha": sha,
        "status": "PASSED",
        "hours": round(duration / 3600, 3),
        "observations": len(rows),
        "max_gap_seconds": gap,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("observations", type=Path)
    parser.add_argument("--sha", required=True)
    parser.add_argument("--hours", type=float, default=48)
    args = parser.parse_args()
    try:
        result = verify_acceptance(
            health_records(args.observations.read_text().splitlines()),
            args.sha,
            hours=args.hours,
        )
    except AcceptanceIncomplete as exc:
        print(json.dumps({"status": "INCOMPLETE", "reason": str(exc)}))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
