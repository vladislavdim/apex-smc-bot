import os
import unittest
from unittest.mock import patch

from core import runtime_observability as ro


class ReleaseCohortObservabilityTests(unittest.TestCase):
    def test_metadata_carries_release_and_runtime_identity(self):
        env = {
            "RENDER_GIT_COMMIT": "abc123",
            "RENDER_INSTANCE_ID": "instance-1",
            "RENDER_DEPLOY_ID": "deploy-1",
        }
        with patch.dict(os.environ, env, clear=False):
            meta = ro._metadata("2026-09-06T20:00:00+00:00")
        self.assertEqual(meta["release_sha"], "abc123")
        self.assertEqual(meta["service_instance"], "instance-1")
        self.assertEqual(meta["deploy_id"], "deploy-1")
        self.assertEqual(meta["started_at"], "2026-09-06T20:00:00+00:00")

    def test_fast_timing_contract(self):
        self.assertEqual(
            ro.FAST_TIMING_FIELDS,
            (
                "liquidity_ms",
                "context_15m_ms",
                "htf_ms",
                "btc_ms",
                "zone_4h_ms",
                "trigger_ms",
                "total_pair_ms",
            ),
        )

    def test_dashboard_defaults_to_current_release_and_keeps_history_tabs(self):
        source = (
            '<div class=tabs id=periods><button class="btn active" data-days=1>24 часа</button>'
            '<button class=btn data-days=7>7 дней</button><button class=btn data-days=30>30 дней</button>'
            '<button class=btn id=latestRelease>После последнего deploy</button></div>'
            "<script>let DAYS=1,STRATEGY='',PAGE=1,LAST=null,RELEASE='';"
            "latestRelease.onclick=()=>{RELEASE=RELEASE?'':'latest';latestRelease.classList.toggle('active',!!RELEASE);PAGE=1;load()};</script>"
        )
        rendered = ro._patch_stats_html(source)
        self.assertIn("id=currentRelease", rendered)
        self.assertIn("id=previousRelease", rendered)
        self.assertIn("id=last24", rendered)
        self.assertIn("id=allHistory", rendered)
        self.assertIn("RELEASE='current'", rendered)
        self.assertNotIn("latestRelease.onclick", rendered)

    def test_patch_is_observability_only(self):
        source = open("core/runtime_observability.py", encoding="utf-8").read()
        self.assertNotIn("select_structural_targets(", source)
        self.assertNotIn("RR >=", source)
        self.assertNotIn("_vol_threshold =", source)
        self.assertNotIn("max_break_age=", source)


if __name__ == "__main__":
    unittest.main()
