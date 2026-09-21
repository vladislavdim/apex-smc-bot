from __future__ import annotations

import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class RuntimeBoundaryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = (ROOT / "bot.py").read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.source)

    def test_bot_does_not_replace_sqlite_connect(self):
        assignments = []
        for node in ast.walk(self.tree):
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Attribute) and target.attr == "connect":
                    assignments.append(target.lineno)
        self.assertEqual(assignments, [])

    def test_bot_does_not_replace_aiogram_message_methods(self):
        forbidden = {"edit_text", "edit_reply_markup"}
        assignments = []
        for node in ast.walk(self.tree):
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Attribute) and target.attr in forbidden:
                    assignments.append((target.attr, target.lineno))
        self.assertEqual(assignments, [])

    def test_new_brain_schema_does_not_create_autonomous_learning_tables(self):
        source = (ROOT / "market.py").read_text(encoding="utf-8")
        for table in (
            "signal_learning",
            "signal_stats",
            "bot_errors",
            "error_patterns",
            "auto_rules",
            "self_analysis",
            "pattern_history",
            "self_rules",
            "learning_history",
            "market_model",
            "symbol_stats",
        ):
            self.assertNotIn(f"CREATE TABLE IF NOT EXISTS {table}", source)


if __name__ == "__main__":
    unittest.main()
