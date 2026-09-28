"""Portable read-only production-authority audit used by CI and release gates."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TARGETS = (
    ROOT / "bot.py",
    ROOT / "apex" / "app" / "worker.py",
)
WORKFLOW_ROOT = ROOT / ".github" / "workflows"
FORBIDDEN = re.compile(
    r"contents\s*:\s*write|\bgit\s+push\b|requests\s*\.\s*(?:put|patch|delete)\s*\(",
    re.IGNORECASE,
)


def violations() -> tuple[str, ...]:
    paths = [*TARGETS, *(p for p in WORKFLOW_ROOT.glob("*.yml") if p.name != "ci.yml")]
    found: list[str] = []
    for path in paths:
        text = path.read_text(encoding="utf-8")
        for line_number, line in enumerate(text.splitlines(), 1):
            if FORBIDDEN.search(line):
                found.append(f"{path.relative_to(ROOT)}:{line_number}:{line.strip()}")
    return tuple(found)


def main() -> int:
    found = violations()
    if found:
        raise SystemExit("production_mutation_authority_forbidden:\n" + "\n".join(found))
    print("Production authority audit passed: runtime and release workflows are read-only")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
