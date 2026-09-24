import importlib.util
import io
from pathlib import Path
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "render_controlled_release.py"
SPEC = importlib.util.spec_from_file_location("render_controlled_release", SCRIPT)
release = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(release)


class FakeClient:
    def __init__(self):
        self.events = []

    def service(self, service_id):
        names = {value[0]: value[1] for value in release.EXPECTED_SERVICES.values()}
        return {
            "id": service_id,
            "name": names[service_id],
            "branch": "main",
            "repo": release.EXPECTED_REPOSITORY,
            "suspended": "not_suspended",
            "autoDeploy": "yes",
        }

    def set_auto_deploy(self, service_id, enabled):
        self.events.append(("auto", service_id, enabled))
        return {"autoDeploy": "yes" if enabled else "no"}

    def trigger(self, service_id, sha):
        self.events.append(("trigger", service_id, sha))
        return {"id": f"deploy-{service_id}"}

    def wait_live(self, service_id, deploy_id):
        self.events.append(("live", service_id, deploy_id))
        return {"id": deploy_id, "status": "live"}


class ReadySession:
    def __init__(self, payload, status=200):
        self.payload = payload
        self.status = status
        self.urls = []

    def get(self, url, timeout):
        self.urls.append((url, timeout))
        payload, status = self.payload, self.status

        class Response:
            status_code = status

            def json(self):
                return payload

        return Response()


class ControlledRenderReleaseTests(unittest.TestCase):
    def test_disable_only_never_triggers_a_deploy(self):
        client = FakeClient()
        result = release.disable_auto_deploy(client)
        self.assertEqual(result, {"web": "no", "worker": "no"})
        self.assertFalse(any(event[0] == "trigger" for event in client.events))
        self.assertEqual(
            [event[1] for event in client.events if event[0] == "auto"],
            [release.EXPECTED_SERVICES["web"][0], release.EXPECTED_SERVICES["worker"][0]],
        )

    def test_preflight_rejects_wrong_service(self):
        with self.assertRaises(release.ReleaseError):
            release.verify_service(
                {"id": "legacy", "name": "apex-smc-bot", "branch": "main",
                 "repo": release.EXPECTED_REPOSITORY, "suspended": "suspended"},
                service_id=release.EXPECTED_SERVICES["worker"][0],
                expected_name=release.EXPECTED_SERVICES["worker"][1],
            )

    def test_web_is_live_and_healthy_before_worker_trigger(self):
        client = FakeClient()
        with patch.object(release, "check_health", return_value={"ok": True}), patch.object(
            release, "check_worker_ready", return_value={"ready": True, "release_sha": "a" * 12},
        ) as worker_ready:
            result = release.controlled_release(client, commit_sha="a" * 40,
                                                health_url="https://example.invalid/health")
        web_id = release.EXPECTED_SERVICES["web"][0]
        worker_id = release.EXPECTED_SERVICES["worker"][0]
        self.assertLess(client.events.index(("live", web_id, f"deploy-{web_id}")),
                        client.events.index(("trigger", worker_id, "a" * 40)))
        self.assertEqual(result["auto_deploy"], "disabled")
        worker_ready.assert_called_once_with(
            "https://example.invalid/health/worker", commit_sha="a" * 40,
        )
        self.assertNotIn("srv-d6qp98paae7s739kubcg", repr(client.events))

    def test_invalid_sha_causes_no_mutation(self):
        client = FakeClient()
        with self.assertRaises(release.ReleaseError):
            release.controlled_release(client, commit_sha="short", health_url="x")
        self.assertEqual(client.events, [])

    def test_worker_readiness_requires_exact_release(self):
        session = ReadySession({"ready": True, "status": "READY", "release_sha": "b" * 12})
        with self.assertRaisesRegex(release.ReleaseError, "unexpected release"):
            release.check_worker_ready(
                "https://example.invalid/health/worker", commit_sha="a" * 40,
                session=session, timeout_seconds=1,
            )

    def test_worker_readiness_accepts_exact_ready_heartbeat(self):
        session = ReadySession({"ready": True, "status": "READY", "release_sha": "a" * 12})
        result = release.check_worker_ready(
            "https://example.invalid/health/worker", commit_sha="a" * 40,
            session=session, timeout_seconds=1,
        )
        self.assertTrue(result["ready"])
        self.assertIn("sha=" + "a" * 40, session.urls[0][0])

    def test_worker_readiness_allows_deferred_checkpoint_recovery_window(self):
        defaults = release.check_worker_ready.__kwdefaults__
        self.assertEqual(defaults["timeout_seconds"], 600)

    def test_worker_diagnostics_redacts_credentials(self):
        class DiagnosticClient:
            def recent_logs(self, service_id):
                self.service_id = service_id
                return [{
                    "timestamp": "2026-09-23T13:00:00Z",
                    "message": (
                        "telegram=https://api.telegram.org/bot123:secret/getMe "
                        "api_key=render-secret Authorization: Bearer bearer-secret"
                    ),
                }]

        client = DiagnosticClient()
        output = io.StringIO()
        with redirect_stdout(output):
            release.print_worker_diagnostics(client)
        rendered = output.getvalue()
        self.assertEqual(client.service_id, release.EXPECTED_SERVICES["worker"][0])
        self.assertIn("[REDACTED]", rendered)
        self.assertNotIn("123:secret", rendered)
        self.assertNotIn("render-secret", rendered)
        self.assertNotIn("bearer-secret", rendered)


if __name__ == "__main__":
    unittest.main()
