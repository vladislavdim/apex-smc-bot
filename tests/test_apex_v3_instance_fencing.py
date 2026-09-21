from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from apex.ops.instance_fencing import InstanceLeaseClient, derive_lease_url


class FakeLeaseServer:
    def __init__(self):
        self.generation = 7
        self.owner = None
        self.calls = []

    def __call__(self, url, payload, headers, timeout):
        self.calls.append(dict(payload))
        action = payload["action"]
        if action == "acquire":
            if self.owner not in (None, payload["instance_id"]):
                return {"granted": False, "reason": "LEASE_HELD", "generation": self.generation}
            self.owner = payload["instance_id"]
            return {"granted": True, "generation": self.generation, "expires_at": "soon"}
        if payload.get("generation") != self.generation or self.owner != payload["instance_id"]:
            return {"granted": False, "reason": "FENCING_TOKEN_MISMATCH", "generation": self.generation}
        if action == "release":
            self.owner = None
            return {"granted": False, "reason": "RELEASED", "generation": self.generation}
        return {"granted": True, "generation": self.generation, "expires_at": "later"}


class InstanceFencingTests(unittest.TestCase):
    def test_lease_url_derives_only_from_exact_ingest_path(self):
        self.assertEqual(
            derive_lease_url("", "https://stats.example/ingest"),
            "https://stats.example/runtime/lease",
        )
        self.assertEqual(derive_lease_url("", "https://stats.example/other"), "")
        self.assertEqual(derive_lease_url("https://lease.example/x", "ignored"), "https://lease.example/x")

    def test_acquire_renew_release_carry_same_generation(self):
        server = FakeLeaseServer()
        client = InstanceLeaseClient(
            "https://stats.example/runtime/lease", "secret", "instance-a", "a" * 40,
            post_json=server,
        )
        self.assertTrue(client.acquire().granted)
        self.assertTrue(client.renew().granted)
        released = client.release()
        self.assertFalse(released.granted)
        self.assertEqual([row.get("generation") for row in server.calls], [None, 7, 7])

    def test_renew_with_stale_generation_fails_closed(self):
        server = FakeLeaseServer()
        client = InstanceLeaseClient(
            "https://stats.example/runtime/lease", "secret", "instance-a", "a" * 40,
            post_json=server,
        )
        client.acquire()
        server.generation = 8
        state = client.renew()
        self.assertFalse(state.granted)
        self.assertEqual(state.reason, "FENCING_TOKEN_MISMATCH")

    def test_missing_shared_store_never_grants_a_local_lease(self):
        client = InstanceLeaseClient("", "", "instance-a", "a" * 40)
        state = client.acquire()
        self.assertFalse(state.granted)
        self.assertEqual(state.reason, "LEASE_NOT_CONFIGURED")

    def test_local_owner_remains_valid_only_until_shared_expiry(self):
        server = FakeLeaseServer()
        expires = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()

        def post(url, payload, headers, timeout):
            value = server(url, payload, headers, timeout)
            if value.get("granted"):
                value["expires_at"] = expires
            return value

        client = InstanceLeaseClient(
            "https://stats.example/runtime/lease", "secret", "instance-a", "a" * 40,
            post_json=post,
        )
        state = client.acquire()
        self.assertTrue(state.valid_at())
        self.assertFalse(state.valid_at(datetime.now(timezone.utc) + timedelta(seconds=31)))


if __name__ == "__main__":
    unittest.main()
