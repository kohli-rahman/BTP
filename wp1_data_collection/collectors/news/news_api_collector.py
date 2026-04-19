from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

from wp1_data_collection.collectors.base_collector import BaseCollector
from wp1_data_collection.config.settings import (
    NEWS_API_KEY, DISASTER_KEYWORDS, BATCH_LOOKBACK_DAYS, INDIA_NEWS_DOMAINS
)
from wp1_data_collection.storage.schema import RawRecord, SourceType
from wp1_data_collection.utils.logger import logger
from wp1_data_collection.utils.rate_limiter import retry_on_rate_limit

NEWS_API_EVERYTHING   = "https://newsapi.org/v2/everything"
NEWS_API_TOP_HEADLINES = "https://newsapi.org/v2/top-headlines"


class NewsAPICollector(BaseCollector):
    """
    Pulls India-specific disaster news from NewsAPI.org.

    Strategy (free-tier compatible):
      1. Primary: query /everything restricted to INDIA_NEWS_DOMAINS so only
         Indian outlets appear — avoids irrelevant foreign results.
      2. Fallback on 426 (date-filter blocked by free tier): query
         /top-headlines with country=in for each keyword.
    """

    def __init__(self, **kwargs):
        super().__init__("NewsAPICollector", **kwargs)

    def authenticate(self) -> bool:
        if not NEWS_API_KEY:
            logger.warning("[NewsAPICollector] NEWS_API_KEY not set — skipping.")
            return False
        resp = requests.get(
            NEWS_API_TOP_HEADLINES,
            params={"country": "in", "pageSize": 1, "apiKey": NEWS_API_KEY},
            timeout=10,
        )
        if resp.status_code == 200:
            logger.info("[NewsAPICollector] API key validated.")
            return True
        logger.error(f"[NewsAPICollector] Auth probe failed: {resp.status_code} {resp.text[:200]}")
        return False

    def _parse_article(self, article: dict) -> Optional[RawRecord]:
        url = article.get("url", "")
        if not url:
            return None
        record_id = str(hash(url))

        text_parts = filter(None, [
            article.get("title"),
            article.get("description"),
            article.get("content"),
        ])
        text = " ".join(text_parts)
        keywords_hit = [kw for kw in DISASTER_KEYWORDS if kw.lower() in text.lower()]

        media_urls = []
        if article.get("urlToImage"):
            media_urls.append(article["urlToImage"])

        return RawRecord(
            source_type=SourceType.NEWS_API,
            source_name=article.get("source", {}).get("name", "newsapi"),
            record_id=record_id,
            collected_at=self.utc_now(),
            published_at=article.get("publishedAt"),
            url=url,
            text=text,
            language="en",
            keywords_hit=keywords_hit,
            location={"name": "India"},
            media_urls=media_urls,
            raw_payload=article,
        )

    def _fetch_everything(self, keyword: str, from_date: str) -> list[dict]:
        """Query /everything restricted to Indian news domains."""
        articles, page = [], 1
        while True:
            resp = requests.get(
                NEWS_API_EVERYTHING,
                params={
                    "q": keyword,
                    "domains": INDIA_NEWS_DOMAINS,
                    "language": "en",
                    "from": from_date,
                    "sortBy": "publishedAt",
                    "pageSize": 100,
                    "page": page,
                    "apiKey": NEWS_API_KEY,
                },
                timeout=15,
            )
            if resp.status_code == 429:
                raise Exception("rate limit")
            if resp.status_code == 426:
                logger.debug(f"[NewsAPICollector] 426 on /everything for '{keyword}' — falling back to top-headlines.")
                return []   # caller will use fallback
            if resp.status_code != 200:
                logger.warning(f"[NewsAPICollector] {resp.status_code} on /everything for '{keyword}'")
                break
            data = resp.json()
            batch = data.get("articles", [])
            if not batch:
                break
            articles.extend(batch)
            if page * 100 >= min(data.get("totalResults", 0), 200):
                break
            page += 1
        return articles

    def _fetch_top_headlines(self, keyword: str) -> list[dict]:
        """Fallback: /top-headlines country=in — always works on free tier."""
        resp = requests.get(
            NEWS_API_TOP_HEADLINES,
            params={
                "q": keyword,
                "country": "in",
                "pageSize": 100,
                "apiKey": NEWS_API_KEY,
            },
            timeout=15,
        )
        if resp.status_code == 429:
            raise Exception("rate limit")
        if resp.status_code != 200:
            logger.warning(f"[NewsAPICollector] {resp.status_code} on /top-headlines for '{keyword}'")
            return []
        return resp.json().get("articles", [])

    @retry_on_rate_limit(max_retries=3, base_wait=60.0)
    def collect_batch(self, lookback_days: int = BATCH_LOOKBACK_DAYS) -> list[RawRecord]:
        from_date = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        records, seen_ids = [], set()

        for keyword in DISASTER_KEYWORDS:
            try:
                articles = self._fetch_everything(keyword, from_date)
                if not articles:
                    articles = self._fetch_top_headlines(keyword)

                for article in articles:
                    record = self._parse_article(article)
                    if record and record.record_id not in seen_ids:
                        seen_ids.add(record.record_id)
                        records.append(record)

            except Exception as e:
                logger.error(f"[NewsAPICollector] Error for keyword '{keyword}': {e}")

        logger.info(f"[NewsAPICollector] Collected {len(records)} India-specific articles.")
        return records
