"""Fresh-process frozen corpus comparison for R0028."""

import json
from pathlib import Path

from apex.domain.enums import Strategy
from apex.strategies.parity_corpus import evaluate_activation_corpus, load_corpus_directory
from apex.strategies.wyckoff import WyckoffStrategy


def main() -> None:
    from apex.app.worker import _get_v3_live_strategy_registry

    candidate_registry = _get_v3_live_strategy_registry()
    candidate_adapters = dict(candidate_registry.registered_adapters())
    proposal_wyckoff = candidate_adapters[Strategy.WYCKOFF]
    baseline_adapters = dict(candidate_adapters)
    baseline_adapters[Strategy.WYCKOFF] = WyckoffStrategy(
        proposal_wyckoff.detectors,
        proposal_wyckoff.snapshot_detectors,
        sequence_policy=None,
    )
    root = Path(__file__).resolve().parents[1]
    cases = load_corpus_directory(root / "tests/fixtures/apex_v3_strategy_parity")
    result = evaluate_activation_corpus(cases, baseline_adapters, candidate_adapters)
    print(json.dumps({
        "cases": len(cases),
        "ready": result.ready,
        "reasons": list(result.reasons),
        "matched": sum(report.matched for report in result.reports),
    }, sort_keys=True))


if __name__ == "__main__":
    main()
