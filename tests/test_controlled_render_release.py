import importlib.util
from pathlib import Path
import unittest
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


class ControlledRenderReleaseTests(unittest.TestCase):
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
        with patch.object(release, "check_health", return_value={"ok": True}):
            result = release.controlled_release(client, commit_sha="a" * 40,
                                                health_url="https://example.invalid/health")
        web_id = release.EXPECTED_SERVICES["web"][0]
        worker_id = release.EXPECTED_SERVICES["worker"][0]
        self.assertLess(client.events.index(("live", web_id, f"deploy-{web_id}")),
                        client.events.index(("trigger", worker_id, "a" * 40)))
        self.assertEqual(result["auto_deploy"], "disabled")
        self.assertNotIn("srv-d6qp98paae7s739kubcg", repr(client.events))

    def test_invalid_sha_causes_no_mutation(self):
        client = FakeClient()
        with self.assertRaises(release.ReleaseError):
            release.controlled_release(client, commit_sha="short", health_url="x")
        self.assertEqual(client.events, [])


if __name__ == "__main__":
    unittest.main()
