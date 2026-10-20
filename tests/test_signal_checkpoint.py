"""A delivered signal must not hold a market scan during remote checkpointing."""

import ast
import asyncio
import logging
import unittest
from pathlib import Path


def checkpoint_namespace(backup, runtime, incidents):
    source = Path(__file__).resolve().parents[1] / "apex/app/worker.py"
    names = {"_schedule_delivered_signal_checkpoint", "_checkpoint_delivered_signals"}
    functions = [node for node in ast.parse(source.read_text()).body
                 if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    namespace = {
        "asyncio": asyncio, "logging": logging,
        "_signal_checkpoint_task": None, "_signal_checkpoint_revision": 0,
        "backup_state_db_to_github": backup, "_V3_RUNTIME": runtime,
        "_v3_report_incident": lambda *args: incidents.append(args),
    }
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(source), "exec"), namespace)
    return namespace


class Runtime:
    def __init__(self):
        self.inhibits = []

    def inhibit_entries(self, code):
        self.inhibits.append(code)


class SignalCheckpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_scanner_returns_while_checkpoint_runs_and_coalesces_new_delivery(self):
        started, release = asyncio.Event(), asyncio.Event()
        calls, incidents = [], []

        async def backup(reason):
            calls.append(reason)
            started.set()
            await release.wait()
            return {"status": "saved"}

        runtime = Runtime()
        ns = checkpoint_namespace(backup, runtime, incidents)
        ns["_schedule_delivered_signal_checkpoint"]("signal_zone")
        await started.wait()
        self.assertFalse(ns["_signal_checkpoint_task"].done())
        ns["_schedule_delivered_signal_checkpoint"]("signal_zone")
        release.set()
        await ns["_signal_checkpoint_task"]
        self.assertEqual(calls, ["signal_zone", "signal_zone"])
        self.assertEqual(runtime.inhibits, [])
        self.assertEqual(incidents, [])

    async def test_failed_state_checkpoint_blocks_future_entries(self):
        incidents = []

        async def backup(_):
            return {"status": "stale_remote"}

        runtime = Runtime()
        ns = checkpoint_namespace(backup, runtime, incidents)
        ns["_schedule_delivered_signal_checkpoint"]("signal_zone")
        await ns["_signal_checkpoint_task"]
        self.assertEqual(runtime.inhibits, ["STATE_BACKUP_DEFERRED"])
        self.assertEqual(incidents[0][:3], ("STATE_BACKUP_DEFERRED", "backup", "CRITICAL"))


if __name__ == "__main__":
    unittest.main()
