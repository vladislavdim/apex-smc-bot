import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import brain_builder


class EmptyJsonResponse:
    status_code = 200

    @staticmethod
    def raise_for_status():
        return None

    @staticmethod
    def json():
        raise ValueError("Expecting value: line 1 column 1")


class BrainBuilderResilienceTests(unittest.TestCase):
    def test_btc_dominance_uses_persisted_value_on_non_json_response(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = os.path.join(directory, "brain.db")
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """CREATE TABLE market_context (
                        id INTEGER PRIMARY KEY,
                        btc_dominance REAL
                    )"""
                )
                conn.execute(
                    "INSERT INTO market_context (btc_dominance) VALUES (57.25)"
                )

            with patch.object(brain_builder, "DB_PATH", db_path), patch.object(
                brain_builder.requests,
                "get",
                return_value=EmptyJsonResponse(),
            ), self.assertLogs(level="WARNING") as logs:
                dominance, total_market_cap = brain_builder.fetch_btc_dominance()

        self.assertEqual(dominance, 57.25)
        self.assertIsNone(total_market_cap)
        self.assertIn("returned non-JSON HTTP 200", "\n".join(logs.output))
        self.assertIn("using stored 57.25%", "\n".join(logs.output))

    def test_incomplete_coin_rules_are_skipped_without_partial_activation(self):
        incomplete = '[{"symbol":"BTCUSDT","best_setup":"FVG"}'
        with patch.object(brain_builder, "ask_groq", return_value=incomplete), patch.object(
            brain_builder, "save_coin_rule"
        ) as save_coin, patch.object(brain_builder, "save_self_rule") as save_rule, patch.object(
            brain_builder, "save_knowledge"
        ) as save_raw, self.assertLogs(level="WARNING") as logs:
            brain_builder.learn_coin_rules()

        save_coin.assert_not_called()
        save_rule.assert_not_called()
        save_raw.assert_called_once()
        self.assertIn("does not contain a complete JSON array", "\n".join(logs.output))


if __name__ == "__main__":
    unittest.main()
