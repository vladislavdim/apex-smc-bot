import inspect
import unittest
from pathlib import Path

class StrategyStatsFinalTests(unittest.TestCase):
    def test_setup_audit_uses_versioned_state_db(self):
        src=Path("apex/telemetry/event_log.py").read_text(encoding="utf-8")
        self.assertIn("_CONFIG.database.state_db_path",src)
        self.assertIn("migrate_state as _migrate_state",src)
        self.assertNotIn("APEX_SETUP_AUDIT_DB_PATH",src)
        self.assertNotIn("setup_audit.db",src)
if __name__=="__main__": unittest.main()
