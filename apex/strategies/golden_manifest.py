"""Frozen source-order manifests for the pre-V3 production detectors.

The manifests are a migration guard, not a new trading specification.  They
record every audited decision point in the current production functions so a
later snapshot-native implementation cannot silently drop or reorder a check.
Some checks are deliberately classified as context, legacy authority, or
post-candidate review rather than entry gates.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping


@dataclass(frozen=True)
class LegacyGateManifest:
    source_file: str
    function_name: str
    source_order: tuple[str, ...]
    context_only: frozenset[str] = frozenset()
    legacy_authority: frozenset[str] = frozenset()
    post_candidate_review: frozenset[str] = frozenset()

    def classified_ids(self) -> frozenset[str]:
        return self.context_only | self.legacy_authority | self.post_candidate_review

    def role_for(self, check_id: str) -> str:
        if check_id in self.context_only:
            return "LIVE_CONTEXT"
        if check_id in self.legacy_authority:
            return "LEGACY_AUTHORITY"
        if check_id in self.post_candidate_review:
            return "POST_CANDIDATE_REVIEW"
        if check_id in self.source_order:
            return "HARD_GATE"
        return "OBSERVED_CHECK"


_MANIFESTS = {
    "FAST": LegacyGateManifest(
        "market.py",
        "detect_fast_deal",
        (
            "FAST_DETECT_FAST_DEAL_G9138",
            "FAST_LTF_CONTEXT_DATA",
            "FAST_LTF_CONTEXT_STRUCTURE",
            "FAST_HTF_SUPPORT",
            "FAST_BTC_HARD_CONFLICT",
            "FAST_DETECT_FAST_DEAL_G9172",
            "FAST_DETECT_FAST_DEAL_G9192",
            "FAST_DETECT_FAST_DEAL_G9242",
            "FAST_IMPULSE_VOLUME_CONTEXT",
            "FAST_DETECT_FAST_DEAL_G9254",
            "FAST_LTF_ZONE",
            "FAST_LTF_RETEST",
            "FAST_DETECT_FAST_DEAL_G9303",
            "FAST_DETECT_FAST_DEAL_G9332",
            "FAST_DETECT_FAST_DEAL_G9344",
            "FAST_DETECT_FAST_DEAL_G9353",
            "FAST_DETECT_FAST_DEAL_G9356",
            "FAST_DETECT_FAST_DEAL_G9449",
        ),
        context_only=frozenset({"FAST_IMPULSE_VOLUME_CONTEXT"}),
        legacy_authority=frozenset({"FAST_DETECT_FAST_DEAL_G9172"}),
        post_candidate_review=frozenset({"FAST_DETECT_FAST_DEAL_G9449"}),
    ),
    "MTF": LegacyGateManifest(
        "bot.py",
        "full_scan_raw",
        (
            "MTF_FULL_SCAN_RAW_G4558",
            "MTF_FULL_SCAN_RAW_G4564",
            "MTF_FULL_SCAN_RAW_G4573",
            "MTF_FULL_SCAN_RAW_G4585",
            "MTF_FULL_SCAN_RAW_G4587",
            "MTF_FULL_SCAN_RAW_G4592",
            "MTF_FULL_SCAN_RAW_G4610",
            "MTF_FULL_SCAN_RAW_G4624",
            "MTF_FULL_SCAN_RAW_G4645",
            "MTF_FULL_SCAN_RAW_G4653",
            "MTF_FULL_SCAN_RAW_G4672",
            "MTF_FULL_SCAN_RAW_G4675",
            "MTF_FULL_SCAN_RAW_G4767",
            "MTF_FULL_SCAN_RAW_G4785",
            "MTF_FULL_SCAN_RAW_G4791",
            "MTF_FULL_SCAN_RAW_G4798",
            "MTF_FULL_SCAN_RAW_G4803",
            "MTF_FULL_SCAN_RAW_G4807",
            "MTF_FULL_SCAN_RAW_G4817",
            "MTF_FULL_SCAN_RAW_G4824",
            "MTF_FULL_SCAN_RAW_G4964",
            "MTF_PASSIVE_LTF_BOS",
            "MTF_FULL_SCAN_RAW_G5053",
        ),
        context_only=frozenset({"MTF_PASSIVE_LTF_BOS"}),
        post_candidate_review=frozenset({"MTF_FULL_SCAN_RAW_G4964"}),
    ),
    "SWING": LegacyGateManifest(
        "market.py",
        "detect_swing_setup",
        (
            "SWING_DETECT_SWING_SETUP_G7123",
            "SWING_DETECT_SWING_SETUP_G7142",
            "SWING_DETECT_SWING_SETUP_G7336",
            "SWING_DETECT_SWING_SETUP_G7341",
            "SWING_DETECT_SWING_SETUP_G7350",
            "SWING_4H_STRUCTURE_CONTEXT",
            "SWING_DETECT_SWING_SETUP_G7411",
            "SWING_LTF_DATA",
            "SWING_LTF_STRUCTURE",
            "SWING_LTF_ZONE",
            "SWING_LTF_RETEST",
            "SWING_LTF_DISPLACEMENT",
            "SWING_LTF_VOLUME",
            "SWING_LTF_NO_CHASE",
            "SWING_LTF_TARGET",
            "SWING_LTF_READY",
            "SWING_DETECT_SWING_SETUP_G7490",
            "SWING_DETECT_SWING_SETUP_G7495",
            "SWING_DETECT_SWING_SETUP_G7502",
            "SWING_DETECT_SWING_SETUP_G7508",
            "SWING_DETECT_SWING_SETUP_G7511",
            "SWING_DETECT_SWING_SETUP_G7515",
            "SWING_DETECT_SWING_SETUP_G7517",
            "SWING_DETECT_SWING_SETUP_G7528",
            "SWING_DETECT_SWING_SETUP_G7533",
            "SWING_DETECT_SWING_SETUP_G7583",
            "SWING_DETECT_SWING_SETUP_G7729",
            "SWING_DETECT_SWING_SETUP_G7741",
        ),
        context_only=frozenset({"SWING_4H_STRUCTURE_CONTEXT"}),
        legacy_authority=frozenset({"SWING_DETECT_SWING_SETUP_G7341"}),
    ),
    "ZONE": LegacyGateManifest(
        "market.py",
        "detect_zone_setup",
        (
            "ZONE_DETECT_ZONE_SETUP_G7796",
            "ZONE_DETECT_ZONE_SETUP_G7811",
            "ZONE_DETECT_ZONE_SETUP_G7818",
            "ZONE_DETECT_ZONE_SETUP_G7852",
            "ZONE_DETECT_ZONE_SETUP_G7866",
            "ZONE_DETECT_ZONE_SETUP_G7884",
            "ZONE_DETECT_ZONE_SETUP_G7899",
            "ZONE_DETECT_ZONE_SETUP_G7901",
            "ZONE_DETECT_ZONE_SETUP_G7907",
            "ZONE_DETECT_ZONE_SETUP_G7909",
            "ZONE_DETECT_ZONE_SETUP_G7915",
            "ZONE_DETECT_ZONE_SETUP_G7966",
            "ZONE_DETECT_ZONE_SETUP_G8050",
            "ZONE_DETECT_ZONE_SETUP_G8060",
            "ZONE_DETECT_ZONE_SETUP_G8068",
            "ZONE_DETECT_ZONE_SETUP_G8075",
            "ZONE_DETECT_ZONE_SETUP_G8078",
            "ZONE_DETECT_ZONE_SETUP_G8127",
        ),
        post_candidate_review=frozenset({"ZONE_DETECT_ZONE_SETUP_G8127"}),
    ),
    "WYCKOFF_SPRING": LegacyGateManifest(
        "market.py",
        "detect_wyckoff_spring",
        tuple(
            f"WYCKOFF_DETECT_WYCKOFF_SPRING_G{gate}"
            for gate in (8367, 8375, 8377, 8387, 8424, 8466, 8469, 8483, 8496, 8513, 8518, 8540, 8626, 8639)
        ),
        legacy_authority=frozenset({"WYCKOFF_DETECT_WYCKOFF_SPRING_G8367"}),
        post_candidate_review=frozenset({"WYCKOFF_DETECT_WYCKOFF_SPRING_G8626"}),
    ),
    "WYCKOFF_DISTRIBUTION": LegacyGateManifest(
        "market.py",
        "detect_wyckoff_distribution",
        (
            "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8675",
            "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8683",
            "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8685",
            "WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8695",
            "WYCKOFF_DIST_RANGE",
            *tuple(
                f"WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G{gate}"
                for gate in (8730, 8766, 8769, 8782, 8790, 8805, 8810, 8832, 8913, 8926)
            ),
        ),
        legacy_authority=frozenset({"WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8675"}),
        post_candidate_review=frozenset({"WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8913"}),
    ),
    "WYCKOFF_REACCUMULATION": LegacyGateManifest(
        "market.py",
        "detect_wyckoff_reaccumulation",
        tuple(
            f"WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G{gate}"
            for gate in (8961, 8968, 8969, 8977, 8985, 8995, 9002, 9018, 9023, 9034, 9089, 9096, 9099)
        ),
        legacy_authority=frozenset({"WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G8961"}),
        post_candidate_review=frozenset({"WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G9089"}),
    ),
}


GOLDEN_GATE_MANIFESTS: Mapping[str, LegacyGateManifest] = MappingProxyType(_MANIFESTS)


__all__ = ["GOLDEN_GATE_MANIFESTS", "LegacyGateManifest"]
