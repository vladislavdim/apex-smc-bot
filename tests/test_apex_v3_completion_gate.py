from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.verify_v3_completion import (
    CompletionError, verify_completion, verify_corpus, verify_verdict,
)


class CompletionGateTests(unittest.TestCase):
    def write(self, text: str) -> Path:
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        path = Path(folder.name) / "matrix.md"
        path.write_text(text, encoding="utf-8")
        return path

    def test_partial_matrix_blocks_release(self):
        path = self.write(
            "| IDs | Workstream | Status | Evidence |\n"
            "|---|---|---|---|\n"
            "| 1–3 | first | DONE | yes |\n"
            "| 111 | last | PARTIAL | no |\n"
        )
        with self.assertRaisesRegex(CompletionError, "v3_workstreams_not_done:111"):
            verify_completion(path)

    def test_complete_range_passes(self):
        path = self.write(
            "| IDs | Workstream | Status | Evidence |\n"
            "|---|---|---|---|\n"
            "| 1–3 | first | DONE | yes |\n"
            "| 111 | last | DONE | yes |\n"
        )
        self.assertEqual(verify_completion(path), ("1–3", "111"))

    def test_missing_boundary_rows_fail(self):
        path = self.write(
            "| IDs | Workstream | Status | Evidence |\n"
            "|---|---|---|---|\n"
            "| 4–7 | middle | DONE | yes |\n"
        )
        with self.assertRaisesRegex(CompletionError, "range_incomplete"):
            verify_completion(path)

    def test_missing_real_market_corpus_blocks_release(self):
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "corpus"
            with self.assertRaisesRegex(CompletionError, "corpus_invalid"):
                verify_corpus(missing)

    def test_missing_activation_verdict_blocks_release(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(CompletionError, "verdict_invalid"):
                verify_verdict(root / "corpus", root / "verdict.json")


if __name__ == "__main__":
    unittest.main()
