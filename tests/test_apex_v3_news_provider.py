from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from apex.market.news_provider import parse_rss


class NewsProviderTests(unittest.TestCase):
    def test_rss_items_are_bounded_and_normalized(self):
        response = Mock()
        response.text = """<rss><channel>
            <item><title><![CDATA[First]]></title><link>https://one</link>
              <pubDate>Sun, 20 Sep 2026 10:00:00 +0000</pubDate></item>
            <item><title>Second</title><link>https://two</link></item>
        </channel></rss>"""
        with patch("apex.market.news_provider._request_get", return_value=response):
            items = parse_rss("https://feed", "Source", limit=1)
        self.assertEqual(items, [{
            "title": "First", "link": "https://one",
            "date": "20.09 10:00", "source": "Source",
        }])

    def test_rss_failure_is_non_authoritative(self):
        with patch(
            "apex.market.news_provider._request_get", side_effect=RuntimeError("offline"),
        ):
            self.assertEqual(parse_rss("https://feed", "Source"), [])


if __name__ == "__main__":
    unittest.main()
