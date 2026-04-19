import hashlib
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional

import feedparser
import requests
from bs4 import BeautifulSoup

from wp1_data_collection.collectors.base_collector import BaseCollector
from wp1_data_collection.config.settings import RSS_FEEDS, DISASTER_KEYWORDS
from wp1_data_collection.storage.schema import RawRecord, SourceType
from wp1_data_collection.utils.logger import logger
from wp1_data_collection.utils.rate_limiter import retry_on_rate_limit


class RSSCollector(BaseCollector):
    """
    Fetches entries from curated RSS feeds (NDTV, The Hindu, ReliefWeb, GDACS, IMD).
    Scrapes article body text and images from each entry URL.
    No API key required.
    """

    def __init__(self, feeds: dict = None, **kwargs):
        super().__init__("RSSCollector", **kwargs)
        self.feeds = feeds or RSS_FEEDS

    def authenticate(self) -> bool:
        # RSS feeds are public — no auth needed
        logger.info("[RSSCollector] No authentication required.")
        return True

    def _parse_date(self, entry) -> Optional[str]:
        for attr in ("published", "updated"):
            raw = getattr(entry, attr, None)
            if raw:
                try:
                    return parsedate_to_datetime(raw).astimezone(timezone.utc).isoformat()
                except Exception:
                    try:
                        return datetime.fromisoformat(raw).isoformat()
                    except Exception:
                        pass
        return None

    def _scrape_body(self, url: str) -> tuple[str, list[str]]:
        """
        Return (body_text, [og_image_url]).
        Uses Open Graph og:image — the article's primary image — instead of
        scraping every <img> tag on the page (which picks up logos, ads, icons).
        """
        try:
            resp = requests.get(url, timeout=10, headers={"User-Agent": "DisasterBot/1.0"})
            if resp.status_code != 200:
                return "", []
            soup = BeautifulSoup(resp.text, "html.parser")

            paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
            text = " ".join(paragraphs)[:3000]

            # Prefer og:image (article hero image) over arbitrary <img> tags
            og_image = soup.find("meta", property="og:image")
            if og_image and og_image.get("content", "").startswith("http"):
                return text, [og_image["content"]]

            # Fallback: twitter:image card
            tw_image = soup.find("meta", attrs={"name": "twitter:image"})
            if tw_image and tw_image.get("content", "").startswith("http"):
                return text, [tw_image["content"]]

            return text, []
        except Exception as e:
            logger.debug(f"[RSSCollector] Scrape failed for {url}: {e}")
            return "", []

    def _parse_entry(self, entry, feed_name: str) -> Optional[RawRecord]:
        title   = getattr(entry, "title", "") or ""
        summary = getattr(entry, "summary", "") or ""
        link    = getattr(entry, "link", "") or ""

        quick_text = f"{title} {summary}"
        keywords_hit = [kw for kw in DISASTER_KEYWORDS if kw.lower() in quick_text.lower()]
        if not keywords_hit:
            return None  # skip non-disaster entries

        body, img_urls = self._scrape_body(link) if link else ("", [])
        full_text = f"{title} {summary} {body}".strip()

        record_id = hashlib.md5(link.encode()).hexdigest() if link else hashlib.md5(quick_text.encode()).hexdigest()

        return RawRecord(
            source_type=SourceType.RSS,
            source_name=feed_name,
            record_id=record_id,
            collected_at=self.utc_now(),
            published_at=self._parse_date(entry),
            url=link,
            text=full_text,
            language="en",
            keywords_hit=keywords_hit,
            location=None,
            media_urls=img_urls,
            raw_payload={
                "title": title,
                "summary": summary,
                "link": link,
                "tags": [t.term for t in getattr(entry, "tags", [])],
            },
        )

    @retry_on_rate_limit(max_retries=2, base_wait=30.0)
    def collect_batch(self, **kwargs) -> list[RawRecord]:
        records = []
        seen_ids = set()

        for feed_name, feed_url in self.feeds.items():
            try:
                parsed = feedparser.parse(feed_url)
                if parsed.bozo and not parsed.entries:
                    logger.warning(f"[RSSCollector] Could not parse feed: {feed_name}")
                    continue

                for entry in parsed.entries:
                    record = self._parse_entry(entry, feed_name)
                    if record and record.record_id not in seen_ids:
                        seen_ids.add(record.record_id)
                        records.append(record)

                logger.debug(f"[RSSCollector] Feed '{feed_name}': {len(parsed.entries)} entries checked.")
            except Exception as e:
                logger.error(f"[RSSCollector] Feed '{feed_name}' failed: {e}")

        logger.info(f"[RSSCollector] Collected {len(records)} disaster-relevant articles from RSS.")
        return records
