import unittest
from unittest.mock import Mock, patch

from apex.market import macro_context


class MacroContextProviderTests(unittest.TestCase):
    def setUp(self):
        macro_context._fear_greed_history_cache = None
        macro_context._fear_greed_history_time = 0
        macro_context._dxy_cache = {}
        macro_context._dxy_cache_time = 0
        macro_context._economic_cache = None
        macro_context._economic_cache_time = 0

    def test_fear_greed_history_order_and_trend_are_preserved(self):
        response = Mock()
        response.json.return_value = {"data": [
            {"value": "40"}, {"value": "35"}, {"value": "30"},
        ]}
        with patch.object(macro_context, "_request_get", return_value=response):
            result = macro_context.get_fg_history()
        self.assertEqual(result, {
            "values": [40, 35, 30], "avg7": 35.0,
            "trend": "IMPROVING", "current": 40,
        })

    def test_dxy_threshold_and_cache_are_preserved(self):
        response = Mock()
        response.json.return_value = {"chart": {"result": [{
            "indicators": {"quote": [{"close": [100.0, None, 100.5]}]},
        }]}}
        with patch.object(macro_context, "_request_get", return_value=response) as get:
            first = macro_context.get_dxy_signal()
            second = macro_context.get_dxy_signal()
        self.assertEqual(first, {"value": 100.5, "change": 0, "signal": "NEUTRAL"})
        self.assertIs(first, second)
        get.assert_called_once()

    def test_economic_events_are_filtered_and_bounded(self):
        response = Mock(status_code=200)
        response.json.return_value = [
            {"date": "2026-01-01", "title": "CPI release"},
            {"date": "2026-01-02", "title": "Minor event"},
            {"date": "2026-01-03", "title": "FOMC decision"},
            {"date": "2026-01-04", "title": "GDP update"},
        ]
        with patch.object(macro_context, "_request_get", return_value=response):
            result = macro_context.get_upcoming_events()
        self.assertEqual(
            result,
            "2026-01-01: CPI release | 2026-01-03: FOMC decision",
        )

    def test_failures_remain_optional(self):
        with patch.object(macro_context, "_request_get", side_effect=RuntimeError("offline")):
            self.assertIsNone(macro_context.get_fg_history())
            self.assertIsNone(macro_context.get_dxy_signal())
            self.assertEqual(macro_context.get_upcoming_events(), "")


if __name__ == "__main__":
    unittest.main()
