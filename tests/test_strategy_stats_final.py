import inspect
import unittest
from pathlib import Path
import stats_server

class StrategyStatsFinalTests(unittest.TestCase):
    def test_setup_audit_default_is_separate(self):
        src=Path("core/setup_audit.py").read_text(encoding="utf-8")
        self.assertIn("APEX_SETUP_AUDIT_DB_PATH",src)
        self.assertIn("setup_audit.db",src)
        self.assertNotIn("APEX_BRAIN_DB_PATH",src)
    def test_dashboard_exact_dates(self):
        self.assertIn("id=fromdate",stats_server.HTML); self.assertIn("id=todate",stats_server.HTML)
        p=inspect.signature(stats_server.build_dashboard).parameters
        self.assertIn("from_date",p); self.assertIn("to_date",p)
    def test_stats_server_does_not_import_trading(self):
        src=inspect.getsource(stats_server)
        self.assertNotIn("import bot",src); self.assertNotIn("import market",src); self.assertNotIn("trade_execution",src)
if __name__=="__main__": unittest.main()
