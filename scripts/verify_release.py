"""Fail closed unless every APEX V3 completion-matrix workstream is DONE."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apex.strategies.parity_corpus import load_corpus_directory
from apex.strategies.parity_runner import load_activation_verdict


ROW = re.compile(
    r"^\|\s*(?P<ids>\d+(?:[–-]\d+)?)\s*\|[^|]+\|\s*(?P<status>[A-Z_]+)\s*\|",
    re.MULTILINE,
)


class CompletionError(RuntimeError):
    pass


def verify_completion(path: Path) -> tuple[str, ...]:
    text = path.read_text(encoding="utf-8")
    rows = tuple((match.group("ids"), match.group("status")) for match in ROW.finditer(text))
    if not rows:
        raise CompletionError("completion_matrix_has_no_rows")
    unfinished = tuple(ids for ids, status in rows if status != "DONE")
    if unfinished:
        raise CompletionError("v3_workstreams_not_done:" + ",".join(unfinished))
    if rows[0][0] != "1–3" or rows[-1][0] != "111":
        raise CompletionError("completion_matrix_range_incomplete")
    return tuple(ids for ids, _ in rows)


def verify_predeployment(path: Path) -> tuple[str, ...]:
    """Require every build workstream DONE while reserving 111 for acceptance."""
    text = path.read_text(encoding="utf-8")
    rows = tuple((match.group("ids"), match.group("status")) for match in ROW.finditer(text))
    if not rows:
        raise CompletionError("completion_matrix_has_no_rows")
    if rows[0][0] != "1–3" or rows[-1][0] != "111":
        raise CompletionError("completion_matrix_range_incomplete")
    unfinished = tuple(
        ids for ids, status in rows[:-1]
        if status != "DONE"
    )
    if unfinished:
        raise CompletionError("v3_workstreams_not_done:" + ",".join(unfinished))
    if rows[-1][1] not in {"PARTIAL", "DONE"}:
        raise CompletionError("v3_acceptance_status_invalid:" + rows[-1][1])
    return tuple(ids for ids, _ in rows)


def verify_corpus(path: Path) -> tuple[str, ...]:
    try:
        cases = load_corpus_directory(path)
    except (OSError, TypeError, ValueError) as exc:
        raise CompletionError(f"strategy_parity_corpus_invalid:{exc}") from exc
    return tuple(case.case_id for case in cases)


def verify_verdict(corpus_path: Path, verdict_path: Path) -> str:
    try:
        cases = load_corpus_directory(corpus_path)
        verdict = load_activation_verdict(verdict_path, cases)
    except (OSError, TypeError, ValueError) as exc:
        raise CompletionError(f"strategy_parity_verdict_invalid:{exc}") from exc
    return str(verdict["verdict_sha256"])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "path", nargs="?", default="docs/APEX_V3_COMPLETION_MATRIX.md",
        type=Path,
    )
    parser.add_argument(
        "--corpus", type=Path,
        default="tests/fixtures/apex_v3_strategy_parity",
    )
    parser.add_argument(
        "--verdict", type=Path,
        default="tests/fixtures/apex_v3_strategy_parity_verdict.json",
    )
    parser.add_argument(
        "--pre-deploy", action="store_true",
        help="Require code workstreams complete while acceptance row 111 remains pending",
    )
    args = parser.parse_args()
    rows = (
        verify_predeployment(args.path)
        if args.pre_deploy
        else verify_completion(args.path)
    )
    cases = verify_corpus(args.corpus)
    verdict_sha = verify_verdict(args.corpus, args.verdict)
    print(
        f"APEX V3 {'pre-deployment' if args.pre_deploy else 'completion'} verified: "
        f"{len(rows)} workstreams, "
        f"{len(cases)} real-market parity cases, verdict={verdict_sha[:12]}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
