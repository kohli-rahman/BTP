from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional

from wp1_data_collection.storage.data_store import DataStore
from wp1_data_collection.utils.logger import logger


class BaseCollector(ABC):
    """
    Abstract base for all WP1 collectors.
    Subclasses implement collect_batch() and optionally stream().
    """

    def __init__(self, name: str, store: Optional[DataStore] = None):
        self.name  = name
        self.store = store or DataStore()
        self._is_authenticated = False

    def utc_now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    @abstractmethod
    def authenticate(self) -> bool:
        """Initialise API client / validate credentials. Return True on success."""

    @abstractmethod
    def collect_batch(self, **kwargs) -> list:
        """
        Fetch a batch of records (historical / on-demand).
        Returns list of schema objects (RawRecord, WeatherRecord, …).
        """

    def stream(self, **kwargs):
        """
        Override in collectors that support real-time streaming.
        Default: raise NotImplementedError so callers know streaming is unsupported.
        """
        raise NotImplementedError(f"{self.name} does not support streaming.")

    def run_batch(self, **kwargs) -> int:
        """Authenticate → collect → persist. Returns number of records saved."""
        if not self._is_authenticated:
            ok = self.authenticate()
            if not ok:
                logger.error(f"[{self.name}] Authentication failed. Skipping.")
                return 0
            self._is_authenticated = True

        logger.info(f"[{self.name}] Starting batch collection …")
        try:
            records = self.collect_batch(**kwargs)
        except Exception as e:
            logger.error(f"[{self.name}] collect_batch failed: {e}")
            return 0

        saved = self.store.save_batch(records)
        logger.info(f"[{self.name}] Batch done — {saved} records saved.")
        return saved
