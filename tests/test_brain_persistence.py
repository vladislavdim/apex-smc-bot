import base64
import hashlib
import os
import sqlite3
import tempfile
import unittest

from core.brain_persistence import BrainPersistence


class _Response:
    def __init__(self, status_code=200, payload=None, content=b""):
        self.status_code = status_code
        self._payload = payload or {}
        self.content = content

    def json(self):
        return self._payload


class _GitHubSession:
    def __init__(self, content, sha="remote-v1"):
        self.content = content
        self.sha = sha
        self.puts = []

    def get(self, _url, *, params, headers, timeout):
        del params, timeout
        if "raw" in headers.get("Accept", ""):
            return _Response(content=self.content)
        return _Response(payload={"sha": self.sha, "size": len(self.content)})

    def put(self, _url, *, headers, json, timeout):
        del headers, timeout
        self.puts.append(json)
        if json.get("sha") != self.sha:
            return _Response(status_code=409)
        self.content = base64.b64decode(json["content"])
        self.sha = hashlib.sha1(self.content).hexdigest()
        return _Response(payload={"content": {"sha": self.sha}})


class _HistorySession(_GitHubSession):
    def __init__(self, good_content):
        super().__init__(b"corrupt-current-head", sha="corrupt-blob")
        self.good_content = good_content

    def get(self, url, *, params, headers, timeout):
        del timeout
        if url.endswith("/commits"):
            return _Response(payload=[{"sha": "head"}, {"sha": "good-commit"}])
        if params.get("ref") == "good-commit":
            if "raw" in headers.get("Accept", ""):
                return _Response(content=self.good_content)
            return _Response(payload={"sha": "good-blob", "size": len(self.good_content)})
        if "raw" in headers.get("Accept", ""):
            return _Response(content=self.content)
        return _Response(payload={"sha": self.sha, "size": len(self.content)})


class _TransientConflictSession(_GitHubSession):
    def put(self, _url, *, headers, json, timeout):
        if not self.puts:
            self.puts.append(json)
            return _Response(status_code=409)
        return super().put(_url, headers=headers, json=json, timeout=timeout)


def _make_db(path, knowledge_rows=1):
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY,
            topic TEXT,
            content TEXT,
            source TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE self_rules (
            id INTEGER PRIMARY KEY,
            rule TEXT
        );
        CREATE TABLE web_knowledge (
            id INTEGER PRIMARY KEY,
            content TEXT
        );
        """
    )
    connection.executemany(
        "INSERT INTO knowledge(topic, content, source) VALUES (?, ?, ?)",
        [(f"topic-{index}", "x" * 200, "test") for index in range(knowledge_rows)],
    )
    connection.commit()
    connection.close()


def _bytes(path):
    with open(path, "rb") as source:
        return source.read()


class BrainPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.local = os.path.join(self.temp.name, "brain.db")
        self.remote = os.path.join(self.temp.name, "remote.db")

    def tearDown(self):
        self.temp.cleanup()

    def _manager(self, session):
        return BrainPersistence(
            self.local,
            "owner/repository",
            "token",
            session=session,
        )

    def test_restore_always_uses_remote_even_when_local_is_larger(self):
        _make_db(self.local, knowledge_rows=200)
        _make_db(self.remote, knowledge_rows=3)
        manager = self._manager(_GitHubSession(_bytes(self.remote)))

        result = manager.restore()

        self.assertTrue(result["ready"])
        with sqlite3.connect(self.local) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM knowledge").fetchone()[0], 3)

    def test_invalid_remote_never_replaces_or_uploads_local_database(self):
        _make_db(self.local, knowledge_rows=7)
        session = _GitHubSession(b"not-a-database")
        manager = self._manager(session)

        restored = manager.restore()
        backed_up = manager.backup("must_not_write")

        self.assertEqual(restored["status"], "restore_failed")
        self.assertEqual(backed_up["status"], "blocked_unrestored")
        self.assertEqual(session.puts, [])
        with sqlite3.connect(self.local) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM knowledge").fetchone()[0], 7)

    def test_corrupt_head_recovers_previous_valid_commit_automatically(self):
        _make_db(self.remote, knowledge_rows=4)
        session = _HistorySession(_bytes(self.remote))
        manager = self._manager(session)

        restored = manager.restore()
        repaired = manager.backup("recover_corrupt_head")

        self.assertTrue(restored["ready"])
        self.assertEqual(restored["recovered_from"], "good-commit")
        self.assertTrue(repaired["saved"])
        self.assertEqual(repaired["generation"], 1)

    def test_backup_contains_committed_wal_rows_and_generation_metadata(self):
        _make_db(self.remote, knowledge_rows=1)
        session = _GitHubSession(_bytes(self.remote))
        manager = self._manager(session)
        self.assertTrue(manager.restore()["ready"])

        writer = sqlite3.connect(self.local)
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute(
            "INSERT INTO knowledge(topic, content, source) VALUES ('wal', 'committed', 'test')"
        )
        writer.commit()
        result = manager.backup("wal_test")
        writer.close()

        self.assertTrue(result["saved"])
        uploaded = os.path.join(self.temp.name, "uploaded.db")
        with open(uploaded, "wb") as target:
            target.write(session.content)
        with sqlite3.connect(uploaded) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM knowledge").fetchone()[0], 2)
            meta = connection.execute(
                "SELECT generation, reason FROM brain_persistence_meta WHERE id=1"
            ).fetchone()
        self.assertEqual(meta, (1, "wal_test"))

    def test_unchanged_database_does_not_create_another_commit(self):
        _make_db(self.remote, knowledge_rows=1)
        session = _GitHubSession(_bytes(self.remote))
        manager = self._manager(session)
        manager.restore()

        first = manager.backup("first")
        second = manager.backup("second")

        self.assertTrue(first["saved"])
        self.assertEqual(second["status"], "unchanged")
        self.assertEqual(len(session.puts), 1)

    def test_stale_instance_cannot_overwrite_newer_remote_blob(self):
        _make_db(self.remote, knowledge_rows=1)
        session = _GitHubSession(_bytes(self.remote), sha="base")
        manager = self._manager(session)
        manager.restore()
        with sqlite3.connect(self.local) as connection:
            connection.execute(
                "INSERT INTO knowledge(topic, content, source) VALUES ('local', 'stale', 'test')"
            )
            connection.commit()
        session.sha = "newer-instance-sha"

        result = manager.backup("stale")

        self.assertEqual(result["status"], "stale_remote")
        self.assertEqual(session.puts, [])

    def test_transient_github_conflict_retries_once_with_same_blob(self):
        _make_db(self.remote, knowledge_rows=1)
        session = _TransientConflictSession(_bytes(self.remote), sha="base")
        manager = self._manager(session)
        manager.restore()

        result = manager.backup("retry_conflict")

        self.assertTrue(result["saved"])
        self.assertEqual(len(session.puts), 2)
        self.assertEqual(session.puts[0]["sha"], "base")
        self.assertEqual(session.puts[1]["sha"], "base")

    def test_generation_survives_restart_and_advances_monotonically(self):
        _make_db(self.remote, knowledge_rows=1)
        session = _GitHubSession(_bytes(self.remote), sha="base")
        first = self._manager(session)
        first.restore()
        self.assertTrue(first.backup("first_generation")["saved"])

        second = self._manager(session)
        restored = second.restore()
        self.assertEqual(restored["generation"], 1)
        with sqlite3.connect(self.local) as connection:
            connection.execute(
                "INSERT INTO knowledge(topic, content, source) VALUES ('restart', 'kept', 'test')"
            )
            connection.commit()
        saved = second.backup("second_generation")

        self.assertTrue(saved["saved"])
        self.assertEqual(saved["generation"], 2)

    def test_restart_without_data_change_creates_no_commit(self):
        _make_db(self.remote, knowledge_rows=1)
        session = _GitHubSession(_bytes(self.remote), sha="base")
        first = self._manager(session)
        first.restore()
        self.assertTrue(first.backup("first_generation")["saved"])
        puts_after_first = len(session.puts)

        second = self._manager(session)
        self.assertTrue(second.restore()["ready"])
        result = second.backup("startup_verified")

        self.assertEqual(result["status"], "unchanged")
        self.assertEqual(len(session.puts), puts_after_first)

    def test_heartbeat_only_change_does_not_create_backup_commit(self):
        _make_db(self.remote, knowledge_rows=1)
        session = _GitHubSession(_bytes(self.remote), sha="base")
        manager = self._manager(session)
        manager.restore()
        self.assertTrue(manager.backup("baseline")["saved"])
        with sqlite3.connect(self.local) as connection:
            connection.execute("CREATE TABLE heartbeat(id INTEGER PRIMARY KEY, ts TEXT)")
            connection.execute("INSERT INTO heartbeat(ts) VALUES ('new-liveness-row')")
            connection.commit()

        result = manager.backup("safety_10m")

        self.assertEqual(result["status"], "unchanged")

    def test_runtime_has_periodic_and_sigterm_safety_paths(self):
        root = os.path.dirname(os.path.dirname(__file__))
        with open(os.path.join(root, "bot.py"), encoding="utf-8") as source:
            bot_source = source.read()
        self.assertEqual(bot_source.count('minutes=10, jitter=60'), 2)
        self.assertGreaterEqual(bot_source.count('backup_db_to_github("render_sigterm")'), 2)
        self.assertIn('backup_db_to_github("experience_transition")', bot_source)
        self.assertNotIn("github_size > local_size * 2", bot_source)


if __name__ == "__main__":
    unittest.main()
