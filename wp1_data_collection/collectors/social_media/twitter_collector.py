import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional

import tweepy

from wp1_data_collection.collectors.base_collector import BaseCollector
from wp1_data_collection.config.settings import (
    TWITTER_BEARER_TOKEN, DISASTER_KEYWORDS, BATCH_LOOKBACK_DAYS
)
from wp1_data_collection.storage.schema import RawRecord, SourceType
from wp1_data_collection.utils.logger import logger
from wp1_data_collection.utils.rate_limiter import retry_on_rate_limit


class TwitterCollector(BaseCollector):
    """
    Collects disaster-related tweets via Twitter API v2 (Academic / Basic tiers).

    Batch mode : searches recent tweets matching disaster keywords.
    Stream mode: filtered stream that runs continuously.
    """

    MAX_RESULTS_PER_PAGE = 100

    def __init__(self, **kwargs):
        super().__init__("TwitterCollector", **kwargs)
        self._client: Optional[tweepy.Client] = None
        self._stream: Optional[tweepy.StreamingClient] = None

    def authenticate(self) -> bool:
        if not TWITTER_BEARER_TOKEN:
            logger.warning("[TwitterCollector] TWITTER_BEARER_TOKEN not set — skipping.")
            return False
        try:
            self._client = tweepy.Client(bearer_token=TWITTER_BEARER_TOKEN, wait_on_rate_limit=True)
            self._client.get_recent_tweets_count("disaster")  # lightweight probe
            logger.info("[TwitterCollector] Authenticated via Bearer token.")
            return True
        except Exception as e:
            logger.error(f"[TwitterCollector] Auth failed: {e}")
            return False

    def _build_query(self, keywords: list[str]) -> str:
        """Build an OR query with lang:en filter, excluding retweets."""
        kw_part = " OR ".join(f'"{kw}"' for kw in keywords[:15])  # API max ~512 chars
        return f"({kw_part}) lang:en -is:retweet"

    def _parse_tweet(self, tweet, includes: dict) -> RawRecord:
        users = {u.id: u for u in includes.get("users", [])}
        media = {m.media_key: m for m in includes.get("media", [])}

        media_urls = []
        if tweet.attachments and tweet.attachments.get("media_keys"):
            for key in tweet.attachments["media_keys"]:
                m = media.get(key)
                if m and hasattr(m, "url") and m.url:
                    media_urls.append(m.url)
                elif m and hasattr(m, "preview_image_url") and m.preview_image_url:
                    media_urls.append(m.preview_image_url)

        keywords_hit = [kw for kw in DISASTER_KEYWORDS if kw.lower() in (tweet.text or "").lower()]

        geo = None
        if tweet.geo and tweet.geo.get("place_id"):
            geo = {"name": tweet.geo["place_id"]}

        return RawRecord(
            source_type=SourceType.TWITTER,
            source_name="twitter",
            record_id=str(tweet.id),
            collected_at=self.utc_now(),
            published_at=tweet.created_at.isoformat() if tweet.created_at else None,
            url=f"https://twitter.com/i/web/status/{tweet.id}",
            text=tweet.text,
            language="en",
            keywords_hit=keywords_hit,
            location=geo,
            media_urls=media_urls,
            raw_payload={
                "id": str(tweet.id),
                "text": tweet.text,
                "author_id": str(tweet.author_id) if tweet.author_id else None,
                "public_metrics": tweet.public_metrics,
                "created_at": str(tweet.created_at),
            },
        )

    @retry_on_rate_limit(max_retries=3, base_wait=60.0)
    def collect_batch(self, lookback_days: int = BATCH_LOOKBACK_DAYS, max_results: int = 500) -> list[RawRecord]:
        start_time = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        query = self._build_query(DISASTER_KEYWORDS)
        records = []
        fetched = 0

        try:
            paginator = tweepy.Paginator(
                self._client.search_recent_tweets,
                query=query,
                start_time=start_time,
                tweet_fields=["created_at", "author_id", "geo", "public_metrics", "attachments"],
                expansions=["author_id", "attachments.media_keys"],
                media_fields=["url", "preview_image_url", "type"],
                max_results=self.MAX_RESULTS_PER_PAGE,
            )
            for response in paginator:
                if not response.data:
                    continue
                includes = response.includes or {}
                for tweet in response.data:
                    records.append(self._parse_tweet(tweet, includes))
                    fetched += 1
                    if fetched >= max_results:
                        break
                if fetched >= max_results:
                    break
        except tweepy.TooManyRequests:
            logger.warning("[TwitterCollector] Rate limit reached during batch.")
        except Exception as e:
            logger.error(f"[TwitterCollector] collect_batch error: {e}")

        logger.info(f"[TwitterCollector] Collected {len(records)} tweets.")
        return records

    def stream(self, **kwargs):
        """Filtered stream using Twitter API v2 StreamingClient."""
        if not TWITTER_BEARER_TOKEN:
            logger.error("[TwitterCollector] Cannot stream: bearer token missing.")
            return

        class DisasterStreamClient(tweepy.StreamingClient):
            def __init__(self, bearer_token, outer):
                super().__init__(bearer_token, wait_on_rate_limit=True)
                self._outer = outer

            def on_tweet(self, tweet):
                record = self._outer._parse_tweet(tweet, {})
                self._outer.store.save(record)
                logger.debug(f"[Stream] Tweet saved: {tweet.id}")

            def on_errors(self, errors):
                logger.error(f"[TwitterStream] Errors: {errors}")

            def on_disconnect(self):
                logger.warning("[TwitterStream] Disconnected.")

        stream_client = DisasterStreamClient(TWITTER_BEARER_TOKEN, self)

        # Remove old rules and set fresh ones
        existing = stream_client.get_rules()
        if existing.data:
            ids = [r.id for r in existing.data]
            stream_client.delete_rules(ids)

        for kw in DISASTER_KEYWORDS[:5]:  # API allows limited rules on free tier
            stream_client.add_rules(tweepy.StreamRule(f'"{kw}" lang:en'))

        logger.info("[TwitterCollector] Starting filtered stream …")
        stream_client.filter(
            tweet_fields=["created_at", "author_id", "geo", "public_metrics"],
            expansions=["attachments.media_keys"],
            media_fields=["url", "preview_image_url"],
        )
