import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from wp1_data_collection.collectors import (
    TwitterCollector, RedditCollector,
    NewsAPICollector, RSSCollector,
    WeatherCollector, MediaCollector,
)
from wp1_data_collection.storage.data_store import DataStore
from wp1_data_collection.utils.logger import logger


class BatchPipeline:
    """
    Orchestrates a full historical / on-demand batch collection run.

    Execution order:
      Stage 1 (parallel): Twitter, Reddit, NewsAPI, RSS, Weather
      Stage 2 (sequential): MediaCollector — harvests images from Stage 1 records

    Each collector is optional: if credentials are missing it logs a warning and skips.
    """

    def __init__(self, lookback_days: int = 7, max_workers: int = 4):
        self.lookback_days = lookback_days
        self.max_workers   = max_workers
        self.store         = DataStore()
        self._results: dict[str, int] = {}

    def _run_collector(self, collector_cls, **kwargs) -> tuple[str, int]:
        collector = collector_cls(store=self.store)
        name = collector.name
        try:
            count = collector.run_batch(**kwargs)
            return name, count
        except Exception as e:
            logger.error(f"[BatchPipeline] {name} raised an exception: {e}")
            return name, 0

    def run(self) -> dict[str, int]:
        start = time.monotonic()
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        logger.info(f"[BatchPipeline] Starting full batch run — lookback={self.lookback_days}d, date={today}")

        # ── Stage 1: Parallel text/metadata collection ─────────────────────
        stage1_collectors = [
            (TwitterCollector,  {"lookback_days": self.lookback_days}),
            (RedditCollector,   {"lookback_days": self.lookback_days}),
            (NewsAPICollector,  {"lookback_days": self.lookback_days}),
            (RSSCollector,      {}),
            (WeatherCollector,  {}),
        ]

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {
                pool.submit(self._run_collector, cls, **kw): cls.__name__
                for cls, kw in stage1_collectors
            }
            for future in as_completed(futures):
                name, count = future.result()
                self._results[name] = count
                logger.info(f"[BatchPipeline] {name} → {count} records")

        # ── Stage 2: Media download from all Stage 1 sources ───────────────
        logger.info("[BatchPipeline] Stage 2 — downloading media assets …")
        media_collector = MediaCollector(store=self.store)
        media_collector.authenticate()

        total_media = 0
        for source_type in ("twitter", "reddit", "news_api", "rss"):
            records = media_collector.collect_batch(source_type=source_type, date_str=today)
            saved = self.store.save_batch(records)
            total_media += saved
        self._results["MediaCollector"] = total_media

        elapsed = round(time.monotonic() - start, 1)
        logger.info(f"[BatchPipeline] Run complete in {elapsed}s. Summary: {self._results}")
        return self._results

    def summary(self) -> str:
        lines = ["=== WP1 Batch Pipeline Summary ==="]
        total = 0
        for name, count in self._results.items():
            lines.append(f"  {name:<25} {count:>6} records")
            total += count
        lines.append(f"  {'TOTAL':<25} {total:>6} records")
        lines.append(f"  Storage stats: {self.store.stats()}")
        return "\n".join(lines)
