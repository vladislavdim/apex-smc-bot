"""The retiring worker's final Memory snapshot must win a rollout race."""

import ast
import asyncio
import contextlib
import types
import unittest
from pathlib import Path


def checkpoint_function(namespace):
    source = Path(__file__).resolve().parents[1] / "apex/app/worker.py"
    function = next(
        node for node in ast.parse(source.read_text()).body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "_v3_memory_startup_checkpoint"
    )
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), "exec"), namespace)
    return namespace[function.name]


class MemoryHandoffCheckpointTests(unittest.TestCase):
    def test_remote_advanced_after_restore_is_reloaded_under_lease(self):
        calls = []
        backups = iter([{"status": "stale_remote"}, {"status": "unchanged"}])

        async def backup(reason):
            calls.append(("backup", reason))
            return next(backups)

        async def lease(*, acquire):
            calls.append(("lease", acquire))
            return True

        async def restore():
            calls.append(("restore",))
            return {"ready": True}

        @contextlib.contextmanager
        def closing(connection):
            yield connection

        ready = "READY"
        namespace = {
            "backup_memory_db_to_github": backup,
            "_v3_refresh_runtime_lease": lease,
            "restore_memory_db_from_github": restore,
            "_v3_connect_memory": lambda _: object(),
            "_v3_migrate_memory": lambda _: calls.append(("migrate",)),
            "_closing": closing,
            "_V3_CONFIG": object(),
            "_MEMORY_PERSISTENCE": types.SimpleNamespace(configured=True),
            "_V3_RUNTIME": types.SimpleNamespace(mark_component=lambda *args, **kw: calls.append(("mark", args[1]))),
            "_V3_COMPONENT_STATE": types.SimpleNamespace(READY=ready, DEGRADED="DEGRADED"),
            "logging": types.SimpleNamespace(info=lambda *args: None),
        }
        result = asyncio.run(checkpoint_function(namespace)())
        self.assertEqual(result["status"], "unchanged")
        self.assertEqual(calls[:5], [
            ("backup", "startup_verified"), ("lease", True), ("restore",),
            ("migrate",), ("backup", "startup_handoff_verified"),
        ])
        self.assertEqual(calls[-1], ("mark", ready))

    def test_no_lease_never_replaces_local_memory(self):
        async def backup(_):
            return {"status": "stale_remote"}

        async def lease(*, acquire):
            return False

        namespace = {
            "backup_memory_db_to_github": backup,
            "_v3_refresh_runtime_lease": lease,
            "restore_memory_db_from_github": lambda: self.fail("restore without lease"),
            "_MEMORY_PERSISTENCE": types.SimpleNamespace(configured=True),
            "_V3_RUNTIME": types.SimpleNamespace(mark_component=lambda *args, **kw: None),
            "_V3_COMPONENT_STATE": types.SimpleNamespace(READY="READY", DEGRADED="DEGRADED"),
        }
        self.assertEqual(asyncio.run(checkpoint_function(namespace)())["status"], "stale_remote")
