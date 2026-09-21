from __future__ import annotations

import asyncio
import unittest

from apex.app.cutover import CutoverSpec, refresh_cutover, sync_cutover


class RuntimeStub:
    def __init__(self):
        self.marked = []
        self.inhibits = []
        self.cleared = []

    def mark_component(self, *args):
        self.marked.append(args)

    def inhibit_entries(self, code):
        self.inhibits.append(code)

    def clear_inhibit(self, code):
        self.cleared.append(code)


class CutoverTests(unittest.TestCase):
    def spec(self, importer, parity):
        return CutoverSpec(
            label="Execution",
            inhibit_code="STATE_DB_EXECUTION_MIRROR_FAILED",
            parity_error="execution_state_parity_failed",
            importer=importer,
            parity_report=parity,
            parity_counts=("executions", "actions"),
        )

    def test_sync_uses_refresh_and_returns_bounded_parity_counts(self):
        calls = []

        def importer(legacy, state, *, refresh=False):
            calls.append((legacy(), state(), refresh))
            return {"executions": 2}

        spec = self.spec(
            importer,
            lambda legacy, state: {
                "ok": True, "executions": 2, "actions": 1, "mismatches": [],
            },
        )
        result = sync_cutover(spec, lambda: "legacy", lambda: "state")
        self.assertEqual(calls, [("legacy", "state", True)])
        self.assertEqual(result["parity_executions"], 2)
        self.assertEqual(result["parity_actions"], 1)
        self.assertTrue(result["parity_ok"])

    def test_sync_rejects_parity_mismatch(self):
        spec = self.spec(
            lambda *_args, **_kwargs: {},
            lambda *_args: {"ok": False, "mismatches": ["execution:7:sl"]},
        )
        with self.assertRaisesRegex(
            RuntimeError, "execution_state_parity_failed:execution:7:sl",
        ):
            sync_cutover(spec, lambda: object(), lambda: object())

    def test_refresh_applies_shared_fail_closed_policy(self):
        runtime = RuntimeStub()
        incidents = []
        recovered = []
        spec = self.spec(lambda *_a, **_k: {}, lambda *_a: {"ok": True})

        async def run():
            with self.assertRaisesRegex(RuntimeError, "broken"):
                await refresh_cutover(
                    spec, lambda: (_ for _ in ()).throw(RuntimeError("broken")),
                    runtime=runtime, failed_state="FAILED",
                    report_incident=lambda *args: incidents.append(args),
                    recover_incident=lambda *args: recovered.append(args),
                )

        asyncio.run(run())
        self.assertEqual(runtime.inhibits, [spec.inhibit_code])
        self.assertEqual(incidents[0][:3], (spec.inhibit_code, "state_db", "CRITICAL"))
        self.assertEqual(recovered, [])

    def test_refresh_clears_inhibit_after_success(self):
        runtime = RuntimeStub()
        recovered = []
        spec = self.spec(lambda *_a, **_k: {}, lambda *_a: {"ok": True})
        result = asyncio.run(refresh_cutover(
            spec, lambda: {"parity_ok": True}, runtime=runtime,
            failed_state="FAILED", report_incident=lambda *_args: None,
            recover_incident=lambda *args: recovered.append(args),
        ))
        self.assertTrue(result["parity_ok"])
        self.assertEqual(runtime.cleared, [spec.inhibit_code])
        self.assertEqual(recovered, [(spec.inhibit_code, "state_db")])


if __name__ == "__main__":
    unittest.main()
