from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest

from apex.db.repositories.correlation import CorrelationError, TradeCorrelationRepository
from apex.db.state_db import migrate_state
from apex.domain.ids import new_id


class CorrelationTests(unittest.TestCase):
    def setUp(self):
        handle, self.path = tempfile.mkstemp(suffix=".db")
        os.close(handle)
        conn = sqlite3.connect(self.path)
        migrate_state(conn)
        conn.close()
        self.repo = TradeCorrelationRepository(lambda: sqlite3.connect(self.path))
        self.versions = {"strategy_version": "v3", "manager_version": "v3"}
        self.candidate = new_id("candidate")
        self.signal = new_id("signal")
        self.execution = new_id("execution")
        self.position = new_id("position")
        self.outcome = new_id("outcome")

    def tearDown(self):
        os.unlink(self.path)

    def test_chain_is_ordered_and_idempotent(self):
        self.repo.create_candidate(self.candidate, release_sha="abc", versions=self.versions)
        self.repo.attach(self.candidate, "signal_id", self.signal)
        self.repo.attach(self.candidate, "signal_id", self.signal)
        self.repo.attach(self.candidate, "execution_id", self.execution)
        self.repo.attach(self.candidate, "position_id", self.position)
        self.repo.attach(self.candidate, "outcome_id", self.outcome)
        row = self.repo.get(self.candidate)
        self.assertEqual((row["signal_id"], row["execution_id"], row["position_id"], row["outcome_id"]), (self.signal, self.execution, self.position, self.outcome))
        self.assertEqual(row["versions"], self.versions)

    def test_cannot_skip_a_step_or_replace_an_immutable_id(self):
        self.repo.create_candidate(self.candidate, release_sha="abc", versions=self.versions)
        with self.assertRaisesRegex(CorrelationError, "missing_predecessor:signal_id"):
            self.repo.attach(self.candidate, "execution_id", self.execution)
        self.repo.attach(self.candidate, "signal_id", self.signal)
        with self.assertRaisesRegex(CorrelationError, "immutable_step_conflict:signal_id"):
            self.repo.attach(self.candidate, "signal_id", new_id("signal"))

    def test_entity_id_cannot_belong_to_two_candidates(self):
        candidates = (self.candidate, new_id("candidate"))
        for candidate in candidates:
            self.repo.create_candidate(candidate, release_sha="abc", versions=self.versions)
        self.repo.attach(candidates[0], "signal_id", self.signal)
        with self.assertRaisesRegex(CorrelationError, "duplicate_entity_id:signal_id"):
            self.repo.attach(candidates[1], "signal_id", self.signal)

    def test_candidate_versions_are_immutable(self):
        self.repo.create_candidate(self.candidate, release_sha="abc", versions=self.versions)
        self.repo.create_candidate(self.candidate, release_sha="abc", versions=self.versions)
        with self.assertRaisesRegex(CorrelationError, "candidate_identity_conflict"):
            self.repo.create_candidate(self.candidate, release_sha="def", versions=self.versions)

    def test_rejects_ambiguous_legacy_ids(self):
        with self.assertRaisesRegex(CorrelationError, "invalid_entity_id:candidate_id"):
            self.repo.create_candidate("cand_1", release_sha="abc", versions=self.versions)


if __name__ == "__main__":
    unittest.main()
