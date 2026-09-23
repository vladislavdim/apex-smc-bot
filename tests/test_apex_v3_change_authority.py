from __future__ import annotations

import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ChangeAuthorityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = (ROOT / "apex/app/worker.py").read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.source)

    def test_runtime_has_no_direct_github_write(self):
        direct_writes = []
        for node in ast.walk(self.tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if (
                isinstance(node.func.value, ast.Name)
                and node.func.value.id == "requests"
                and node.func.attr in {"put", "patch", "delete"}
            ):
                direct_writes.append((node.func.attr, node.lineno))
        self.assertEqual(direct_writes, [])

    def test_runtime_has_no_ai_change_paths(self):
        functions = {
            node.name
            for node in self.tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        forbidden = {
            "_groq_write_extension",
            "github_get_file",
            "github_push_patch",
            "analyze_and_patch",
            "apply_patch",
        }
        self.assertTrue(forbidden.isdisjoint(functions))

    def test_runtime_has_no_error_to_ai_handler(self):
        classes = {node.name for node in self.tree.body if isinstance(node, ast.ClassDef)}
        self.assertNotIn("ErrorCapture", classes)


if __name__ == "__main__":
    unittest.main()
