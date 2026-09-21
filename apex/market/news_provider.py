"""Read-only RSS news provider for Telegram market context."""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime
from email.utils import parsedate_to_datetime



def _request_get(url: str, **kwargs):
    import requests
    return requests.get(url, **kwargs)


def parse_rss(url: str, source_name: str, limit: int = 5) -> list[dict]:
    try:
        response = _request_get(
            url,
            headers={
                "User-Agent":
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            },
            timeout=10,
        )
        response.encoding = "utf-8"
        entries = re.findall(r"<item>(.*?)</item>", response.text, re.DOTALL)
        if not entries:
            entries = re.findall(r"<entry>(.*?)</entry>", response.text, re.DOTALL)
        items = []
        for entry in entries[:limit]:
            title_match = re.search(
                r"<title[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>",
                entry, re.DOTALL,
            )
            title = title_match.group(1).strip() if title_match else ""
            link_match = re.search(
                r"<link[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</link>",
                entry, re.DOTALL,
            )
            if not link_match:
                link_match = re.search(r"<link[^>]*href=['\"]([^'\"]+)['\"]", entry)
            link = link_match.group(1).strip() if link_match else ""
            date_match = re.search(r"<pubDate>(.*?)</pubDate>", entry, re.DOTALL)
            if not date_match:
                date_match = re.search(r"<published>(.*?)</published>", entry, re.DOTALL)
            raw_date = date_match.group(1).strip() if date_match else ""
            date_text = ""
            try:
                date_text = parsedate_to_datetime(raw_date).strftime("%d.%m %H:%M")
            except Exception:
                try:
                    parsed = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
                    date_text = parsed.strftime("%d.%m %H:%M")
                except Exception:
                    date_text = raw_date[:16] if raw_date else ""
            if title:
                items.append({
                    "title": title, "link": link,
                    "date": date_text, "source": source_name,
                })
        return items
    except Exception as exc:
        logging.error("RSS parse error %s: %s", source_name, exc)
        return []


def get_crypto_news(limit: int = 15) -> list[dict]:
    sources = (
        ("https://cointelegraph.com/rss", "CoinTelegraph"),
        ("https://www.coindesk.com/arc/outboundfeeds/rss/", "CoinDesk"),
        ("https://cryptonews.com/news/feed/", "CryptoNews"),
        ("https://decrypt.co/feed", "Decrypt"),
        ("https://investing.com/rss/news_301.rss", "Investing.com"),
        ("https://www.forexfactory.com/ff_calendar_thisweek.xml", "ForexFactory"),
    )
    all_news = []
    for url, name in sources:
        try:
            all_news.extend(parse_rss(url, name, limit=4))
            time.sleep(0.3)
        except Exception:
            pass
    return all_news[:limit]


def get_market_impact_news() -> list[dict]:
    sources = (
        ("https://feeds.bloomberg.com/markets/news.rss", "Bloomberg"),
        ("https://investing.com/rss/news_301.rss", "Investing.com"),
        ("https://feeds.feedburner.com/streetinsider/crypto", "StreetInsider"),
    )
    all_news = []
    for url, name in sources:
        try:
            all_news.extend(parse_rss(url, name, limit=3))
        except Exception:
            pass
    return all_news[:8]


__all__ = ["get_crypto_news", "get_market_impact_news", "parse_rss"]
