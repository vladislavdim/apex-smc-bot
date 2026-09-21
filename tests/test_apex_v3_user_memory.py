from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from apex.ui.user_memory import (
    get_chat_history, get_user_memory, save_chat_log, update_user_memory,
)
from apex.ui.context_store import (
    get_knowledge, get_recent_news, save_knowledge, save_news,
)


class UserMemoryServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = str(Path(self.temp.name) / "legacy.db")
        with sqlite3.connect(self.path) as conn:
            conn.executescript("""
                CREATE TABLE user_memory(
                    user_id INTEGER PRIMARY KEY,name TEXT,profile TEXT,
                    preferences TEXT,coins_mentioned TEXT,deposit REAL,
                    risk_percent REAL,total_messages INTEGER,first_seen TEXT,last_seen TEXT
                );
                CREATE TABLE chat_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER,
                    role TEXT,content TEXT,created_at TEXT
                );
                CREATE TABLE news_cache(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,query TEXT,
                    content TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE knowledge(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,topic TEXT,
                    content TEXT,source TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
            """)
        self.connection_patch = patch(
            "apex.ui.user_memory.get_db_conn",
            side_effect=lambda **_kwargs: sqlite3.connect(self.path),
        )
        self.connection_patch.start()
        self.addCleanup(self.connection_patch.stop)
        self.context_connection_patch = patch(
            "apex.ui.context_store.get_db_conn",
            side_effect=lambda **_kwargs: sqlite3.connect(self.path),
        )
        self.context_connection_patch.start()
        self.addCleanup(self.context_connection_patch.stop)

    def test_profile_updates_and_history_are_bounded_services(self):
        update_user_memory(7, name="Trader", deposit=1000)
        update_user_memory(7, risk=0.5, preferences="swing")
        memory = get_user_memory(7)
        self.assertEqual(memory["name"], "Trader")
        self.assertEqual(memory["deposit"], 1000)
        self.assertEqual(memory["risk"], 0.5)
        self.assertEqual(memory["messages"], 1)
        save_chat_log(7, "user", "a" * 2100)
        save_chat_log(7, "assistant", "reply")
        history = get_chat_history(7)
        self.assertEqual([row[0] for row in history], ["user", "assistant"])
        self.assertEqual(len(history[0][1]), 2000)

    def test_missing_storage_fails_soft(self):
        self.assertEqual(get_user_memory(404)["risk"], 1.0)
        self.assertEqual(get_chat_history(404), [])

    def test_news_and_advisory_context_storage_is_bounded(self):
        save_news("market", "x" * 1200)
        self.assertEqual(len(get_recent_news().split(": ", 1)[1]), 1000)
        save_knowledge("btc", "first", "manual")
        save_knowledge("btc-update", "second", "manual")
        self.assertCountEqual(get_knowledge("btc").splitlines(), ["first", "second"])


if __name__ == "__main__":
    unittest.main()
