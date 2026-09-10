"""Validate and/or manifest the bounded BTC Research release artifacts.

This command intentionally fails closed.  It is used before publication and
on restore, so a partial run cannot replace the last known-good snapshot.
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
import tempfile
from pathlib import Path

# Make the verifier usable as the exact workflow command from repository root.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.snapshot import (
    EXPECTED_SYMBOL,
    EXPECTED_TIMEFRAMES,
    SnapshotValidationError,
    build_manifest,
    sha256_file,
    validate_database,
    validate_dashboard,
    verify_compressed_assets,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path)
    parser.add_argument("--dashboard", type=Path)
    parser.add_argument("--db-gz", type=Path)
    parser.add_argument("--dashboard-gz", type=Path)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--verify-compressed", action="store_true")
    parser.add_argument("--history-days", type=int, default=365)
    return parser


def _decompress_bounded(source: Path, target: Path, *, max_bytes: int) -> None:
    """Decompress an untrusted release asset without an unbounded read."""
    written = 0
    with gzip.open(source, "rb") as src, target.open("wb") as dst:
        while True:
            chunk = src.read(min(1024 * 1024, max_bytes - written + 1))
            if not chunk:
                break
            written += len(chunk)
            if written > max_bytes:
                raise SnapshotValidationError("decompressed_asset_exceeds_limit")
            dst.write(chunk)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.verify_compressed:
            if not (args.db_gz and args.dashboard_gz):
                raise SnapshotValidationError("compressed_paths_required")
            manifest = verify_compressed_assets(args.db_gz, args.dashboard_gz, args.manifest)
            # Verify the decompressed payload as well.  Never trust a hash
            # alone to establish SQLite integrity or a completed run.
            with tempfile.TemporaryDirectory(prefix="apex-research-restore-") as tmp:
                db = Path(tmp) / "BTCUSDT.research.db"
                dashboard = Path(tmp) / "BTCUSDT.dashboard.json"
                try:
                    # One BTC year contains feature snapshots plus the check
                    # trail for five profiles and legitimately exceeds 256MB
                    # uncompressed.  Keep a hard ceiling, but size it for the
                    # measured single-pair workload; this runs in Actions, not
                    # in the live Render worker.
                    _decompress_bounded(args.db_gz, db, max_bytes=1024 * 1024 * 1024)
                    _decompress_bounded(args.dashboard_gz, dashboard, max_bytes=20 * 1024 * 1024)
                except (OSError, EOFError) as exc:
                    raise SnapshotValidationError("compressed_payload_invalid") from exc
                for key, path in (("database", db), ("dashboard", dashboard)):
                    expected = str((manifest.get(key) or {}).get("sha256") or "").lower()
                    if not expected or sha256_file(path).lower() != expected:
                        raise SnapshotValidationError(f"{key}_checksum_mismatch")
                validate_database(db, expected_symbol=EXPECTED_SYMBOL,
                                  expected_timeframes=EXPECTED_TIMEFRAMES)
                validate_dashboard(dashboard, expected_symbol=EXPECTED_SYMBOL,
                                   expected_timeframes=EXPECTED_TIMEFRAMES)
            print("BTC Research snapshot: compressed assets and payload verified")
            return 0

        if not (args.db and args.dashboard and args.db_gz and args.dashboard_gz):
            raise SnapshotValidationError("db_dashboard_and_compressed_paths_required")
        manifest = build_manifest(
            args.db,
            args.dashboard,
            db_gz_path=args.db_gz,
            dashboard_gz_path=args.dashboard_gz,
            expected_symbol=EXPECTED_SYMBOL,
            expected_timeframes=EXPECTED_TIMEFRAMES,
            history_days=max(1, args.history_days),
        )
        args.manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
                                 encoding="utf-8")
        print(json.dumps({"snapshot_version": manifest["snapshot_version"],
                          "run_id": (manifest.get("latest_run") or {}).get("research_run_id"),
                          "status": (manifest.get("latest_run") or {}).get("status"),
                          "timeframes": manifest["timeframes"]}, ensure_ascii=False))
        return 0
    except SnapshotValidationError as exc:
        print(f"BTC Research snapshot rejected: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
