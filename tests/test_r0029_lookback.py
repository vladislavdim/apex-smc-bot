import copy
import pytest
from tests.wyckoff_sequence_expanded import expanded
from tests.test_r0028_wyckoff_registry_sequence import _snapshot
from apex.strategies.wyckoff_sequence import strict_sequence_policy


@pytest.mark.parametrize("kind", ["SPRING", "DISTRIBUTION"])
@pytest.mark.parametrize("scale", [0.5, 1.0, 2.0])
@pytest.mark.parametrize("padding", [0, 11, 12, 30])
def test_old_history_cannot_change_native_window_proof(kind, scale, padding):
    case = expanded(kind, scale, padding)
    before = copy.deepcopy(case)
    snap = _snapshot(case)
    proof = strict_sequence_policy(snap, kind)
    assert proof["sequence_complete"] and proof["status"] == "ORDERED"
    window_start = snap.candles["1d"][-61:][0]["open_time"]
    assert all(row["close_time"] >= window_start for row in proof["evidence"])
    assert case == before
