import sqlite3
import unittest
from datetime import datetime, timezone

from apex.db.memory_db import migrate_memory
from apex.domain.enums import Direction, Strategy
from apex.domain.models import Candidate, TradeOutcome
from apex.learning import (
    LiveMemoryError,
    LiveMemoryRepository,
    confidence_label,
    expected_edge,
    similar_cases,
    strategy_statistics,
)


class LiveLearningTests(unittest.TestCase):
    CANDIDATE_ID = "cand_" + "1" * 32
    SNAPSHOT_ID = "snap_" + "1" * 32
    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        migrate_memory(self.connection)
        self.repo = LiveMemoryRepository(lambda: self.connection)
        self.repo._conn_factory = lambda: _NonClosingConnection(self.connection)

    def _candidate(self, candidate_id=CANDIDATE_ID):
        return Candidate(
            candidate_id=candidate_id, symbol="BTCUSDT", strategy=Strategy.FAST,
            direction=Direction.LONG, entry=100, initial_sl=95, tp1=110,
            tp2=115, tp3=120, rr=2, snapshot_id=self.SNAPSHOT_ID,
            created_at=datetime(2026, 9, 11, tzinfo=timezone.utc),
        )

    def make_outcome(
        self,
        outcome_id="out_" + "2" * 32,
        position_id="pos_" + "3" * 32,
        net_r=1.5,
    ):
        return TradeOutcome(
            outcome_id=outcome_id, position_id=position_id, weighted_entry=100,
            weighted_exit=108, net_r=net_r, fees=0.1, funding=0.02,
            closed_at=datetime(2026, 9, 11, 1, tzinfo=timezone.utc),
        )

    def test_only_executed_confirmed_trade_can_become_learning_outcome(self):
        candidate = self._candidate()
        self.repo.record_candidate(candidate, signal_id="sig-1", release_sha="a" * 40)
        with self.assertRaisesRegex(LiveMemoryError, "unconfirmed_position_forbidden"):
            self.repo.record_outcome(
                self.make_outcome(), candidate_id=self.CANDIDATE_ID, execution_id="exec-1",
                strategy="FAST", symbol="BTCUSDT", direction="LONG",
                release_sha="a" * 40, confirmed_position=False, gross_r=1.7,
            )
        with self.assertRaisesRegex(LiveMemoryError, "executed_candidate_not_found"):
            self.repo.record_outcome(
                self.make_outcome(), candidate_id=self.CANDIDATE_ID, execution_id="exec-1",
                strategy="FAST", symbol="BTCUSDT", direction="LONG",
                release_sha="a" * 40, confirmed_position=True, gross_r=1.7,
            )

    def test_real_outcomes_feed_statistics_similarity_and_advisory_only(self):
        candidate = self._candidate()
        self.repo.record_candidate(
            candidate, signal_id="sig-1", release_sha="a" * 40, executed=True,
        )
        self.repo.record_outcome(
            self.make_outcome(), candidate_id=self.CANDIDATE_ID, execution_id="exec-1",
            strategy="FAST", symbol="BTCUSDT", direction="LONG",
            release_sha="a" * 40, confirmed_position=True, gross_r=1.7,
            context={
                "setup_type": "RETEST", "session": "LONDON", "volatility": "NORMAL",
                "btc_state": "UP", "mfe_r": 2.1, "mae_r": 0.3,
            },
        )
        stats = strategy_statistics(self.connection)[0]
        self.assertEqual(stats["samples"], 1)
        self.assertEqual(stats["net_expectancy_r"], 1.5)
        cases = similar_cases(self.connection, {
            "strategy": "FAST", "direction": "LONG", "setup_type": "RETEST",
            "session": "LONDON", "volatility": "NORMAL", "btc_state": "UP",
        })
        advisory = expected_edge(cases)
        self.assertEqual(cases[0]["outcome_id"], "out_" + "2" * 32)
        self.assertEqual(advisory["authority"], "ADVISORY")
        self.assertFalse(advisory["may_block_strategy"])
        self.assertFalse(advisory["may_increase_risk"])

    def test_confidence_thresholds_are_exact(self):
        self.assertEqual(confidence_label(9), "INSUFFICIENT")
        self.assertEqual(confidence_label(10), "LOW")
        self.assertEqual(confidence_label(30), "MEDIUM")
        self.assertEqual(confidence_label(100), "STRONGER")


class _NonClosingConnection:
    def __init__(self, connection):
        self._connection = connection

    def __getattr__(self, name):
        return getattr(self._connection, name)

    def close(self):
        return None


if __name__ == "__main__":
    unittest.main()
