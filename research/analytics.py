"""Leakage-safe, descriptive research analytics and promotion proposals."""
from __future__ import annotations

import json
import math
import statistics
from collections import defaultdict
from typing import Any, Iterable, Mapping

from .replay import WORKING_TF, chronological_splits, metrics
from .store import ResearchStore


def wilson_interval(wins: int, total: int, z: float = 1.96) -> tuple[float | None, float | None]:
    if total <= 0:
        return None, None
    p = wins / total
    denominator = 1 + z * z / total
    centre = (p + z * z / (2 * total)) / denominator
    margin = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total)) / denominator
    return max(0.0, centre - margin), min(1.0, centre + margin)


def promotion_proposal(deltas: Iterable[float], *, baseline_drawdown: float,
                       candidate_drawdown: float, safety_violations: int = 0,
                       minimum: int = 30) -> dict[str, Any]:
    values = [float(x) for x in deltas if x is not None and math.isfinite(float(x))]
    mean = statistics.fmean(values) if values else None
    median = statistics.median(values) if values else None
    total = sum(values)
    largest_share = max((abs(x) for x in values), default=0.0) / max(abs(total), 1e-12)
    sorted_values = sorted(values)
    tail = statistics.fmean(sorted_values[:max(1, len(values)//10)]) if values else None
    reasons = []
    if len(values) < minimum: reasons.append(f"n<{minimum}")
    if mean is None or mean <= 0: reasons.append("mean_delta_not_positive")
    if median is None or median <= 0: reasons.append("median_delta_not_positive")
    if candidate_drawdown > baseline_drawdown: reasons.append("drawdown_deteriorated")
    if tail is not None and tail < -1.0: reasons.append("tail_loss_deteriorated")
    if largest_share > .5: reasons.append("single_outlier_dependence")
    if safety_violations: reasons.append("safety_violation")
    return {"eligible": len(values), "mean_delta_r": mean, "median_delta_r": median,
            "tail_mean_delta_r": tail, "largest_outlier_share": largest_share,
            "safety_violations": safety_violations,
            "promotion_proposed": not reasons, "auto_activate": False, "reasons": reasons}


def evaluate_profile(store: ResearchStore, run_id: str, profile_id: str) -> list[dict[str, Any]]:
    """Evaluate predefined point-in-time indicator segments.

    Results are descriptive evidence only.  Explicitly prefer PLAYBOOK_ONLY
    rows so an ACTUAL execution record cannot silently become the replay
    baseline.
    """
    rows = store.completed_trade_rows(run_id, profile_id)
    from .experiments import paired_report
    paired = paired_report(rows)
    store.save_feature_evaluation({
        "research_run_id": run_id, "profile_id": profile_id,
        "feature": "paired_policy_walk_forward", "segment": {},
        "sample_size": paired["overall"]["n"],
        "status": "FIXED_POLICY_RESEARCH_ONLY",
        "comparison_kind": paired["comparison_kind"], "metrics": paired,
        "uplift": paired["overall"]["mean_delta_r"],
    })
    analysis_rows = [row for row in rows if row.get("track") == "PLAYBOOK_ONLY"] or rows
    if not analysis_rows:
        return []
    timestamps = [int(row["decision_time"]) for row in analysis_rows]
    split = chronological_splits(timestamps)
    test_set = set(split["TEST"])
    baseline = metrics([row["net_r"] for row in analysis_rows if row.get("net_r") is not None])
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in analysis_rows:
        try:
            snapshot = json.loads(row.get("snapshot_json") or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            snapshot = {}
        feature = ((snapshot.get("candidate") or {}).get("feature_snapshot") or {})
        reference = snapshot.get("feature_ref") or {}
        candidate = snapshot.get("candidate") or {}
        if not feature and reference:
            timeframe = candidate.get("timeframe") or WORKING_TF.get(candidate.get("scan_type"))
            if timeframe:
                feature = store.feature_snapshot(reference["symbol"], timeframe,
                    as_of=int(reference["as_of"]),
                    feature_version=reference.get("feature_version")) or {}
        segment_values = {
            "direction": row.get("side") or "UNKNOWN",
            "session": (feature.get("session") or {}).get("name") or "UNKNOWN",
            "regime": (feature.get("regime") or {}).get("primary") or "UNKNOWN",
            "volatility": (feature.get("regime") or {}).get("volatility") or "UNKNOWN",
        }
        participation = feature.get("participation") or {}
        volatility = feature.get("volatility") or {}
        momentum = feature.get("momentum") or {}
        location = feature.get("location") or {}
        relative_volume = participation.get("relative_volume")
        if relative_volume is not None:
            rv = float(relative_volume)
            segment_values["indicator.relative_volume"] = (
                "<1.0" if rv < 1 else "1.0-1.6" if rv < 1.6 else "1.6-2.0" if rv < 2 else ">=2.0")
        atr_percentile = volatility.get("atr_percentile")
        if atr_percentile is not None:
            ap = float(atr_percentile)
            segment_values["indicator.atr_percentile"] = (
                "LOW_<30" if ap < 30 else "NORMAL_30_70" if ap < 70 else "HIGH_>=70")
        rsi_value = momentum.get("rsi")
        if rsi_value is not None:
            rsv = float(rsi_value)
            segment_values["indicator.rsi"] = (
                "OVERSOLD_<30" if rsv < 30 else "OVERBOUGHT_>=70" if rsv >= 70 else "MID_30_70")
        segment_values["indicator.ob_present"] = "YES" if location.get("ob") else "NO"
        segment_values["indicator.fvg_present"] = "YES" if location.get("fvg") else "NO"
        price = float(feature.get("price") or 0)
        vwap = float(location.get("vwap") or 0)
        segment_values["indicator.vwap_side"] = "ABOVE" if vwap and price > vwap else "BELOW" if vwap else "UNKNOWN"
        derivatives = feature.get("derivatives") or {}
        funding = derivatives.get("funding_rate") or {}
        rate = funding.get("rate")
        if rate is not None:
            rate=float(rate)
            segment_values["shadow.funding_rate"] = (
                "EXTREME_POSITIVE" if rate >= .001 else "POSITIVE" if rate > .0001
                else "EXTREME_NEGATIVE" if rate <= -.001 else "NEGATIVE" if rate < -.0001
                else "NEUTRAL")
        oi = derivatives.get("open_interest") or {}
        oi_change = oi.get("change_1h_pct")
        if oi_change is not None:
            change=float(oi_change)
            segment_values["shadow.open_interest_change"] = (
                "RISING_>=1%" if change >= 1 else "FALLING_<=-1%" if change <= -1 else "FLAT")
        ratio = derivatives.get("long_short_ratio") or {}
        account_ratio = ratio.get("accounts")
        if account_ratio is not None:
            value=float(account_ratio)
            segment_values["shadow.long_short_ratio"] = (
                "LONG_CROWDED" if value >= 1.2 else "SHORT_CROWDED" if value <= .8 else "BALANCED")
        liquidations = derivatives.get("liquidations") or {}
        long_liq=float(liquidations.get("long_usd") or 0); short_liq=float(liquidations.get("short_usd") or 0)
        if long_liq or short_liq:
            segment_values["shadow.liquidation_dominance"] = (
                "LONG_LIQ" if long_liq > short_liq*1.2
                else "SHORT_LIQ" if short_liq > long_liq*1.2 else "BALANCED")
        cvd = derivatives.get("trade_cvd_real") or {}
        imbalance = cvd.get("taker_imbalance")
        if imbalance is not None:
            value=float(imbalance)
            segment_values["shadow.trade_cvd"] = (
                "BUY_DOMINANT" if value >= .1 else "SELL_DOMINANT" if value <= -.1 else "BALANCED")
        book = derivatives.get("order_book_liquidity") or {}
        depth_imbalance = book.get("depth_imbalance")
        if depth_imbalance is not None:
            value=float(depth_imbalance)
            segment_values["shadow.order_book_imbalance"] = (
                "BID_HEAVY" if value >= .1 else "ASK_HEAVY" if value <= -.1 else "BALANCED")
        for name, value in segment_values.items():
            groups[(name, str(value))].append(row)
    results = []
    baseline_expectancy = baseline.get("expectancy")
    for (feature, value), members in groups.items():
        values = [float(x["net_r"]) for x in members if x.get("net_r") is not None]
        result = metrics(values)
        test_values = [float(x["net_r"]) for x in members if x.get("net_r") is not None and int(x["decision_time"]) in test_set]
        test_baseline = [float(x["net_r"]) for x in analysis_rows if x.get("net_r") is not None and int(x["decision_time"]) in test_set]
        low, high = wilson_interval(sum(1 for x in values if x > 0), len(values))
        evaluation = {"research_run_id": run_id, "profile_id": profile_id,
            "feature": feature, "segment": {feature: value}, "sample_size": len(values),
            "coverage": len(values)/len(analysis_rows), "win_rate": result.get("win_rate"),
            "expectancy": result.get("expectancy"), "profit_factor": result.get("profit_factor"),
            "max_drawdown": result.get("max_drawdown_r"),
            "uplift": (result.get("expectancy") - baseline_expectancy) if result.get("expectancy") is not None and baseline_expectancy is not None else None,
            "oos_uplift": (statistics.fmean(test_values)-statistics.fmean(test_baseline)) if test_values and test_baseline else None,
            "confidence_low": low, "confidence_high": high,
            "status": "DESCRIPTIVE_ONLY",
            "comparison_kind": "INDICATOR_SEGMENT_DESCRIPTIVE",
            "metrics": {**result, "split_sizes": {k: len(v) for k,v in split.items()},
                        "point_in_time": True, "baseline_track": "PLAYBOOK_ONLY" if any(x.get("track") == "PLAYBOOK_ONLY" for x in rows) else "LEGACY",
                        "promotion": {
                            "promotion_proposed": False, "auto_activate": False,
                            "reasons": ["paired_out_of_sample_experiment_required"],
                        }, "comparison_kind": "segment_vs_population_not_causal"}}
        store.save_feature_evaluation(evaluation)
        results.append(evaluation)
    return results


__all__ = ["evaluate_profile", "promotion_proposal", "wilson_interval"]
