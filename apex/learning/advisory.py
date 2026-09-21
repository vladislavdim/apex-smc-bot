"""Expected-edge view without execution or risk authority."""

from __future__ import annotations

from statistics import median
from typing import Any, Iterable, Mapping

from .confidence import confidence_label


def expected_edge(cases: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = list(cases)
    values = [float(row["net_r"]) for row in rows if row.get("net_r") is not None]
    mfes = [float(row["mfe_r"]) for row in rows if row.get("mfe_r") is not None]
    maes = [float(row["mae_r"]) for row in rows if row.get("mae_r") is not None]
    count = len(values)
    return {
        "authority": "ADVISORY",
        "samples": count,
        "confidence": confidence_label(count),
        "positive_rate": sum(value > 0 for value in values) / count if count else None,
        "avg_net_r": sum(values) / count if count else None,
        "median_mfe_r": median(mfes) if mfes else None,
        "median_mae_r": median(maes) if maes else None,
        "may_block_strategy": False,
        "may_change_strategy": False,
        "may_increase_risk": False,
    }


__all__ = ["expected_edge"]
