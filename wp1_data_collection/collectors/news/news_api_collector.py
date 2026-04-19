from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

from wp1_data_collection.collectors.base_collector import BaseCollector
from wp1_data_collection.config.settings import NEWS_API_KEY, DISASTER_KEYWORDS, BATCH_LOOKBACK_DAYS
from wp1_data_collection.storage.schema import RawRecord, SourceType
from wp1_data_collection.utils.logger import logger
from wp1_data_collection.utils.rate_limiter import retry_on_rate_limit

NEWS_API_URL = "https://newsapi.org/v2/everything"


class NewsAPICollector(BaseCollector):
    """
    Pulls English news articles from NewsAPI.org matching disaster keywords.
    Supports images embedded in articles (urlToImage field).
    """

    def __init__(self, **kwargs):
        super().__init__("NewsAPICollector", **kwargs)

    def authenticate(self) -> bool:
        if not NEWS_API_KEY:
            logger.warning("[NewsAPICollector] NEWS_API_KEY not set — skipping.")
            return False
        resp = requests.get(
            NEWS_API_URL,
            params={"q": "test", "apiKey": NEWS_API_KEY, "pageSize": 1},
            timeout=10,
        )
        if resp.status_code == 200:
            logger.info("[NewsAPICollector] API key validated.")
            return True
        logger.error(f"[NewsAPICollector] Auth probe failed: {resp.status_code} {resp.text[:200]}")
        return False

    def _parse_article(self, article: dict, keyword: str) -> Optional[RawRecord]:
        url = article.get("url", "")
        record_id = str(hash(url))

        text_parts = filter(None, [article.get("title"), article.get("description"), article.get("content")])
        text = " ".join(text_parts)
        keywords_hit = [kw for kw in DISASTER_KEYWORDS if kw.lower() in text.lower()]

        media_urls = []
        if article.get("urlToImage"):
            media_urls.append(article["urlToImage"])

        published = article.get("publishedAt")

        return RawRecord(
            source_type=SourceType.NEWS_API,
            source_name=article.get("source", {}).get("name", "newsapi"),
            record_id=record_id,
            collected_at=self.utc_now(),
            published_at=published,
            url=url,
            text=text,
            language="en",
            keywords_hit=keywords_hit,
            location=None,
            media_urls=media_urls,
            raw_payload=article,
        )

    @retry_on_rate_limit(max_retries=3, base_wait=60.0)
    def collect_batch(self, lookback_days: int = BATCH_LOOKBACK_DAYS) -> list[RawRecord]:
        from_date = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        records = []
        seen_ids = set()

        for keyword in DISASTER_KEYWORDS:
            try:
                page = 1
                while True:
                    resp = requests.get(
                        NEWS_API_URL,
                        params={
                            "q": keyword,
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
                    if resp.status_code != 200:
                        logger.warning(f"[NewsAPICollector] {resp.status_code} for '{keyword}'")
                        break

                    data = resp.json()
                    articles = data.get("articles", [])
                    if not articles:
                        break

                    for article in articles:
                        record = self._parse_article(article, keyword)
                        if record and record.record_id not in seen_ids:
                            seen_ids.add(record.record_id)
                            records.append(record)

                    total = data.get("totalResults", 0)
                    if page * 100 >= min(total, 300):  # cap at 300 per keyword
                        break
                    page += 1

            except Exception as e:
                logger.error(f"[NewsAPICollector] Error for keyword '{keyword}': {e}")

        logger.info(f"[NewsAPICollector] Collected {len(records)} articles.")
        return records
