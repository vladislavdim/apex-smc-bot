from research.experiments import paired_report


def pair(key, start, end, a=1.0, b=1.2):
    return [{"attempt_id": key, "decision_time": start, "exit_time": end,
             "track": track, "net_r": value, "status": "CLOSED"}
            for track, value in [("NO_MANAGER", a), ("PLAYBOOK_ONLY", b)]]


def test_pairing_rejects_duplicates_and_missing_outcomes():
    rows = pair("ok", 0, 20) + pair("duplicate", 0, 30)
    rows += [dict(rows[-1])]
    rows += pair("missing", 0, None)
    result = paired_report(rows)
    assert result["overall"]["n"] == 1
    assert result["excluded"] == {"duplicate": 1, "incomplete": 1}
    assert result["auto_activate"] is False


def test_walk_forward_purges_training_outcome_unavailable_at_test_start():
    day = 86400
    rows = pair("late", 0, 3*day) + pair("early", day, day+1)
    rows += pair("test", 2*day, 2*day+1) + pair("extent", 4*day, 5*day)
    result = paired_report(rows, train_days=2, test_days=1)
    first = result["folds"][0]
    assert first["train"]["n"] == 1
    assert first["purged_late_train"] == 1
    assert first["test"]["n"] == 1
    assert result["promotion_proposed"] is False
