import signal
import threading
import time

from wp1_data_collection.collectors import (
    TwitterCollector, RedditCollector, WeatherCollector
)
from wp1_data_collection.storage.data_store import DataStore
from wp1_data_collection.utils.logger import logger


class StreamPipeline:
    """
    Runs real-time / near-real-time data collection in parallel threads.

    Streams:
      - Twitter filtered stream  (push, runs until interrupted)
      - Reddit poll stream       (every 5 min)
      - Weather poll stream      (every 30 min)

    Each stream runs in its own daemon thread. Graceful shutdown via SIGINT/SIGTERM.
    """

    def __init__(self):
        self.store   = DataStore()
        self._stop   = threading.Event()
        self._threads: list[threading.Thread] = []

    def _twitter_stream_worker(self):
        collector = TwitterCollector(store=self.store)
        ok = collector.authenticate()
        if not ok:
            logger.warning("[StreamPipeline] Twitter stream unavailable — skipping.")
            return
        logger.info("[StreamPipeline] Twitter stream thread started.")
        try:
            collector.stream()
        except Exception as e:
            logger.error(f"[StreamPipeline] Twitter stream error: {e}")

    def _reddit_poll_worker(self, interval: int = 300):
        collector = RedditCollector(store=self.store)
        ok = collector.authenticate()
        if not ok:
            logger.warning("[StreamPipeline] Reddit stream unavailable — skipping.")
            return
        logger.info(f"[StreamPipeline] Reddit poll thread started (interval={interval}s).")
        while not self._stop.is_set():
            try:
                collector.run_batch(lookback_days=1, posts_per_sub=25)
            except Exception as e:
                logger.error(f"[StreamPipeline] Reddit poll error: {e}")
            self._stop.wait(interval)

    def _weather_poll_worker(self, interval: int = 1800):
        collector = WeatherCollector(store=self.store)
        ok = collector.authenticate()
        if not ok:
            logger.warning("[StreamPipeline] Weather stream unavailable — skipping.")
            return
        logger.info(f"[StreamPipeline] Weather poll thread started (interval={interval}s).")
        while not self._stop.is_set():
            try:
                collector.run_batch()
            except Exception as e:
                logger.error(f"[StreamPipeline] Weather poll error: {e}")
            self._stop.wait(interval)

    def _register_signals(self):
        def _handler(sig, frame):
            logger.info("[StreamPipeline] Shutdown signal received. Stopping …")
            self._stop.set()

        signal.signal(signal.SIGINT,  _handler)
        signal.signal(signal.SIGTERM, _handler)

    def run(self, twitter: bool = True, reddit: bool = True, weather: bool = True):
        self._register_signals()
        logger.info("[StreamPipeline] Starting real-time collection …")

        workers = []
        if twitter:
            workers.append(threading.Thread(target=self._twitter_stream_worker, daemon=True, name="TwitterStream"))
        if reddit:
            workers.append(threading.Thread(target=self._reddit_poll_worker,   daemon=True, name="RedditPoll"))
        if weather:
            workers.append(threading.Thread(target=self._weather_poll_worker,  daemon=True, name="WeatherPoll"))

        for t in workers:
            t.start()
            self._threads.append(t)

        logger.info(f"[StreamPipeline] {len(workers)} stream workers running. Press Ctrl+C to stop.")

        try:
            while not self._stop.is_set():
                time.sleep(5)
        finally:
            self._stop.set()
            for t in self._threads:
                t.join(timeout=10)
            logger.info("[StreamPipeline] All stream workers stopped.")
