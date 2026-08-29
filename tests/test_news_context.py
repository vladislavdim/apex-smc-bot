import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from news_context.aggregator import collect_news_context, format_news_context, normalize_news_context
from news_context.sources import parse_calendar_payload, parse_calendar_xml, parse_rss_xml


class NewsContextTests(unittest.TestCase):
    def test_calendar_normalization_marks_pre_release_without_prediction(self):
        calendar = [{
            "title": "Consumer Price Index CPI", "country": "USD", "date": "08-27-2026",
            "time": "10:00pm", "impact": "High", "forecast": "3.0%", "previous": "2.9%",
            "actual": "", "source": "ForexFactory",
        }]
        now = datetime(2026, 8, 28, 1, 0, tzinfo=timezone.utc)  # 21:00 New York; one hour before
        context = normalize_news_context("BTCUSDT", calendar, [], now=now)
        self.assertEqual(context["risk_level"], "HIGH")
        self.assertEqual(context["phase"], "PRE_EVENT")
        self.assertEqual(context["prediction"], "not_available_pre_release")

    def test_release_window_is_high_risk(self):
        calendar = [{
            "title": "FOMC Statement", "country": "USD", "date": "08-27-2026",
            "time": "2:00pm", "impact": "High", "forecast": "", "previous": "", "actual": "",
        }]
        now = datetime(2026, 8, 27, 18, 10, tzinfo=timezone.utc)
        context = normalize_news_context("ETHUSDT", calendar, [], now=now)
        self.assertEqual((context["risk_level"], context["phase"]), ("HIGH", "RELEASE_WINDOW"))

    def test_global_scope_critical_event_is_included(self):
        calendar = [{
            "title": "ISM Manufacturing PMI", "country": "ALL", "date": "08-28-2026",
            "time": "10:00am", "impact": "High", "forecast": "", "previous": "",
            "actual": "",
        }]
        now = datetime(2026, 8, 28, 13, 15, tzinfo=timezone.utc)
        context = normalize_news_context("BTCUSDT", calendar, [], now=now)
        self.assertEqual(context["phase"], "PRE_EVENT")
        self.assertEqual(context["nearest_critical_event"]["title"], "ISM Manufacturing PMI")

    def test_old_unrelated_headline_is_removed(self):
        rows = [{"title": "Local company opens office", "source": "x", "age_seconds": 10}]
        context = normalize_news_context("SOLUSDT", [], rows, now=datetime.now(timezone.utc))
        self.assertEqual(context["headlines"], [])

    def test_parsers_and_prompt_block(self):
        calendar_xml = """<weeklyevents><event><title>GDP q/q</title><country>USD</country><date>08-28-2026</date><time>8:30am</time><impact>High</impact></event></weeklyevents>"""
        rss_xml = """<rss><channel><item><title>Bitcoin ETF update</title><pubDate>Thu, 27 Aug 2026 20:00:00 GMT</pubDate><link>https://example.test/a</link></item></channel></rss>"""
        self.assertEqual(parse_calendar_xml(calendar_xml)[0]["title"], "GDP q/q")
        self.assertEqual(parse_calendar_payload('[{"title":"CPI","country":"USD","date":"2026-08-28T12:30:00-04:00","impact":"High"}]')[0]["title"], "CPI")
        self.assertEqual(parse_rss_xml(rss_xml, "test")[0]["source"], "test")
        block = format_news_context(normalize_news_context("BTCUSDT", [], [], now=datetime.now(timezone.utc)))
        self.assertIn("NEWS RISK CONTEXT", block)
        self.assertIn("Never invent", block)


class NewsContextAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_sources_unavailable_is_fail_open_context(self):
        with patch("news_context.aggregator.collect_calendar", new=AsyncMock(side_effect=TimeoutError())), \
             patch("news_context.aggregator.collect_headlines", new=AsyncMock(side_effect=ConnectionError())), \
             patch("news_context.aggregator.collect_official_actuals", new=AsyncMock(side_effect=TimeoutError())):
            context = await collect_news_context("BTCUSDT")
        self.assertTrue(context["news_data_unavailable"])
        self.assertEqual(context["risk_level"], "LOW")
        self.assertEqual(len(context["data_quality"]["failed_sources"]), 3)
