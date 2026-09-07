import os
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
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

    def test_dashboard_exposes_only_current_release(self):
        source = (
            '<div class=tabs id=periods><button class="btn active" data-days=1>24 часа</button>'
            '<button class=btn data-days=7>7 дней</button><button class=btn data-days=30>30 дней</button>'
            '<button class=btn id=latestRelease>После последнего deploy</button></div>'
            "<script>let DAYS=1,STRATEGY='',PAGE=1,LAST=null,RELEASE='';"
            "latestRelease.onclick=()=>{RELEASE=RELEASE?'':'latest';latestRelease.classList.toggle('active',!!RELEASE);PAGE=1;load()};</script>"
        )
        rendered = ro._patch_stats_html(source)
        self.assertIn("id=currentRelease", rendered)
        self.assertIn("Current release only", rendered)
        self.assertNotIn("id=previousRelease", rendered)
        self.assertNotIn("id=last24", rendered)
        self.assertNotIn("id=allHistory", rendered)
        self.assertNotIn("Previous release:", rendered)
        self.assertNotIn("mixed releases", rendered)
        self.assertIn("RELEASE='current'", rendered)
        self.assertNotIn("latestRelease.onclick", rendered)

    def test_patch_is_observability_only(self):
        source = open("core/runtime_observability.py", encoding="utf-8").read()
        self.assertNotIn("select_structural_targets(", source)
        self.assertNotIn("RR >=", source)
        self.assertNotIn("_vol_threshold =", source)
        self.assertNotIn("max_break_age=", source)

    def test_api_cannot_select_previous_or_mixed_history(self):
        calls = []

        def original(*args):
            calls.append(args)
            return {}

        module = SimpleNamespace(
            build_dashboard=original,
            HTML="",
            STATS_BASELINE_UTC=datetime(2026, 9, 1, tzinfo=timezone.utc),
            _connect=lambda: (_ for _ in ()).throw(RuntimeError("test")),
            _metric_summary=lambda values: {"count": len(values)},
            _num=lambda value: value if isinstance(value, (int, float)) else None,
        )
        releases = [{"sha": "latest-sha", "first_seen": "2026-09-07T12:00:00Z", "last_seen": "2026-09-07T13:00:00Z"}]
        with patch.object(ro, "_release_rows", return_value=releases), \
             patch.object(ro, "_release_sha", return_value="latest-sha"), \
             patch.object(ro, "_fast_timing_summary_db", return_value={}):
            ro._patch_stats_module(module)
            result = module.build_dashboard(days=1, release="all")

        assert calls[0][0] == 30
        assert calls[0][11] == "latest-sha"
        assert result["cohort_mode"] == "current"
        assert result["available_releases"] == ["latest-sha"]
        assert result["previous_release_sha"] == ""


if __name__ == "__main__":
    unittest.main()
