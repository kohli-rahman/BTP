import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional

import praw

from wp1_data_collection.collectors.base_collector import BaseCollector
from wp1_data_collection.config.settings import (
    REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT,
    DISASTER_SUBREDDITS, DISASTER_KEYWORDS, BATCH_LOOKBACK_DAYS
)
from wp1_data_collection.storage.schema import RawRecord, SourceType
from wp1_data_collection.utils.logger import logger
from wp1_data_collection.utils.rate_limiter import retry_on_rate_limit


class RedditCollector(BaseCollector):
    """
    Collects disaster-relevant posts and comments from Reddit using PRAW.
    Targets curated subreddits and keyword-filtered searches.
    """

    def __init__(self, **kwargs):
        super().__init__("RedditCollector", **kwargs)
        self._reddit: Optional[praw.Reddit] = None

    def authenticate(self) -> bool:
        if not all([REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET]):
            logger.warning("[RedditCollector] Reddit credentials not set — skipping.")
            return False
        try:
            self._reddit = praw.Reddit(
                client_id=REDDIT_CLIENT_ID,
                client_secret=REDDIT_CLIENT_SECRET,
                user_agent=REDDIT_USER_AGENT,
            )
            _ = self._reddit.subreddit("india").hot(limit=1)  # auth probe
            logger.info("[RedditCollector] Authenticated (read-only).")
            return True
        except Exception as e:
            logger.error(f"[RedditCollector] Auth failed: {e}")
            return False

    def _is_relevant(self, text: str) -> list[str]:
        """Return which disaster keywords appear in text."""
        text_lower = text.lower()
        return [kw for kw in DISASTER_KEYWORDS if kw.lower() in text_lower]

    def _parse_submission(self, post) -> Optional[RawRecord]:
        full_text = f"{post.title} {post.selftext}"
        keywords_hit = self._is_relevant(full_text)
        if not keywords_hit:
            return None

        return RawRecord(
            source_type=SourceType.REDDIT,
            source_name=f"reddit/r/{post.subreddit.display_name}",
            record_id=post.id,
            collected_at=self.utc_now(),
            published_at=datetime.fromtimestamp(post.created_utc, tz=timezone.utc).isoformat(),
            url=f"https://reddit.com{post.permalink}",
            text=full_text,
            language="en",
            keywords_hit=keywords_hit,
            location=None,
            media_urls=[post.url] if post.url and not post.is_self else [],
            raw_payload={
                "id": post.id,
                "title": post.title,
                "selftext": post.selftext,
                "score": post.score,
                "num_comments": post.num_comments,
                "subreddit": post.subreddit.display_name,
                "author": str(post.author) if post.author else "[deleted]",
                "created_utc": post.created_utc,
                "url": post.url,
            },
        )

    @retry_on_rate_limit(max_retries=3, base_wait=30.0)
    def collect_batch(self, lookback_days: int = BATCH_LOOKBACK_DAYS, posts_per_sub: int = 100) -> list[RawRecord]:
        cutoff_ts = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).timestamp()
        records = []

        for sub_name in DISASTER_SUBREDDITS:
            try:
                subreddit = self._reddit.subreddit(sub_name)
                for post in subreddit.new(limit=posts_per_sub):
                    if post.created_utc < cutoff_ts:
                        break
                    record = self._parse_submission(post)
                    if record:
                        records.append(record)
                logger.debug(f"[RedditCollector] Processed r/{sub_name}")
            except Exception as e:
                logger.error(f"[RedditCollector] Failed on r/{sub_name}: {e}")

        # Also run a global search for each keyword
        for keyword in DISASTER_KEYWORDS[:5]:
            try:
                for post in self._reddit.subreddit("all").search(keyword, sort="new", limit=50, time_filter="week"):
                    if post.created_utc < cutoff_ts:
                        continue
                    record = self._parse_submission(post)
                    if record:
                        records.append(record)
            except Exception as e:
                logger.error(f"[RedditCollector] Search failed for '{keyword}': {e}")

        # Deduplicate by record_id
        seen = set()
        unique = []
        for r in records:
            if r.record_id not in seen:
                seen.add(r.record_id)
                unique.append(r)

        logger.info(f"[RedditCollector] Collected {len(unique)} unique posts.")
        return unique

    def stream(self, **kwargs):
        """PRAW does not support true push streaming; poll every N seconds instead."""
        import time
        interval = kwargs.get("interval_seconds", 300)
        logger.info(f"[RedditCollector] Polling every {interval}s (pseudo-stream).")
        while True:
            try:
                self.run_batch(lookback_days=1, posts_per_sub=25)
            except Exception as e:
                logger.error(f"[RedditCollector] Stream poll error: {e}")
            time.sleep(interval)
