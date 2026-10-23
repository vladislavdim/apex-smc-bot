from datetime import datetime, timedelta, timezone
import pytest
from scripts.verify_production_acceptance import verify_acceptance, AcceptanceIncomplete

SHA = "a" * 40


def rows(hours=48):
    start = datetime(2026, 10, 19, tzinfo=timezone.utc)
    return [
        dict(
            release_sha=SHA,
            observed_at=(start + timedelta(minutes=5 * i)).isoformat(),
            ready=True,
            reason_codes=[],
            scanner_coverage={s:(start + timedelta(minutes=5*i)).isoformat()
                              for s in ("FAST","MTF","ZONE","SWING","WYCKOFF")},
            auto_trading_enabled=False,
            strategy_snapshot_active=True,
            strategy_proof_sha256="b" * 64,
            instance_id="one",
            fencing_generation=3,
            memory={"working_set_bytes": 250, "limit_bytes": 512},
        )
        for i in range(hours * 12 + 1)
    ]


def test_actual_window_required_and_gap_and_unhealthy_observations_fail():
    assert verify_acceptance(rows(), SHA)["status"] == "PASSED"
    with pytest.raises(AcceptanceIncomplete, match="window_not_elapsed"):
        verify_acceptance(rows(1), SHA)
    data = rows()
    del data[100:110]
    with pytest.raises(AcceptanceIncomplete, match="observation_gap"):
        verify_acceptance(data, SHA)
    data = rows()
    data[100]["ready"] = False
    with pytest.raises(AcceptanceIncomplete, match="unhealthy_observation"):
        verify_acceptance(data, SHA)


def test_old_sha_wrong_owner_or_missing_migration_cannot_pass():
    with pytest.raises(AcceptanceIncomplete, match="history_missing"):
        verify_acceptance(rows(), "c" * 40)
    data = rows()
    data[100]["instance_id"] = "two"
    with pytest.raises(AcceptanceIncomplete, match="conflicting_fencing_owner"):
        verify_acceptance(data, SHA)
    data = rows()
    data[100]["strategy_snapshot_active"] = False
    with pytest.raises(AcceptanceIncomplete, match="cutover_not_verified"):
        verify_acceptance(data, SHA)
