import hashlib
import mimetypes
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import requests
from PIL import Image

from wp1_data_collection.collectors.base_collector import BaseCollector
from wp1_data_collection.config.settings import RAW_DIR
from wp1_data_collection.storage.schema import MediaRecord, MediaType
from wp1_data_collection.storage.data_store import DataStore
from wp1_data_collection.utils.logger import logger
from wp1_data_collection.utils.rate_limiter import retry_on_rate_limit

MEDIA_DIR = RAW_DIR / "media"

SUPPORTED_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".tiff"}
SUPPORTED_VIDEO_EXTS = {".mp4", ".avi", ".mov", ".mkv", ".webm"}

MAX_FILE_SIZE_MB = 50


class MediaCollector(BaseCollector):
    """
    Downloads images and videos referenced by previously collected RawRecords.
    Reads media_urls from existing records in the data store, downloads and
    validates each file, and stores a MediaRecord with local path reference.

    Can also accept a direct list of URLs for standalone use.
    """

    def __init__(self, **kwargs):
        super().__init__("MediaCollector", **kwargs)
        MEDIA_DIR.mkdir(parents=True, exist_ok=True)

    def authenticate(self) -> bool:
        # No auth — uses public URLs from collected records
        return True

    def _detect_media_type(self, url: str, content_type: str = "") -> Optional[MediaType]:
        ext = Path(urlparse(url).path).suffix.lower()
        if ext in SUPPORTED_IMAGE_EXTS or "image" in content_type:
            return MediaType.IMAGE
        if ext in SUPPORTED_VIDEO_EXTS or "video" in content_type:
            return MediaType.VIDEO
        return None

    def _local_path(self, url: str, media_type: MediaType) -> Path:
        url_hash = hashlib.md5(url.encode()).hexdigest()
        ext = Path(urlparse(url).path).suffix.lower() or (
            ".jpg" if media_type == MediaType.IMAGE else ".mp4"
        )
        sub = "images" if media_type == MediaType.IMAGE else "videos"
        dest = MEDIA_DIR / sub
        dest.mkdir(parents=True, exist_ok=True)
        return dest / f"{url_hash}{ext}"

    @retry_on_rate_limit(max_retries=2, base_wait=5.0)
    def _download(self, url: str) -> tuple[Optional[bytes], str]:
        resp = requests.get(
            url,
            timeout=30,
            stream=True,
            headers={"User-Agent": "DisasterBot/1.0"},
        )
        if resp.status_code != 200:
            return None, ""
        content_type = resp.headers.get("Content-Type", "")
        size = int(resp.headers.get("Content-Length", 0))
        if size > MAX_FILE_SIZE_MB * 1024 * 1024:
            logger.warning(f"[MediaCollector] Skipping oversized file: {url}")
            return None, content_type
        return resp.content, content_type

    def _get_image_dimensions(self, path: Path) -> tuple[Optional[int], Optional[int]]:
        try:
            with Image.open(path) as img:
                return img.width, img.height
        except Exception:
            return None, None

    def _download_one(self, url: str, source_record_id: str = None) -> Optional[MediaRecord]:
        content, content_type = self._download(url)
        if not content:
            return None

        media_type = self._detect_media_type(url, content_type)
        if not media_type:
            logger.debug(f"[MediaCollector] Unknown media type, skipping: {url}")
            return None

        local_path = self._local_path(url, media_type)
        if local_path.exists():
            logger.debug(f"[MediaCollector] Already downloaded: {local_path.name}")
        else:
            local_path.write_bytes(content)

        size_kb = round(local_path.stat().st_size / 1024, 2)
        w, h = self._get_image_dimensions(local_path) if media_type == MediaType.IMAGE else (None, None)

        return MediaRecord(
            source_url=url,
            local_path=str(local_path),
            media_type=media_type,
            collected_at=self.utc_now(),
            source_record_id=source_record_id,
            file_size_kb=size_kb,
            width=w,
            height=h,
        )

    def collect_batch(self, urls: list[str] = None, source_type: str = None, date_str: str = None, **kwargs) -> list[MediaRecord]:
        """
        Two modes:
          1. Pass urls= directly for standalone use.
          2. Pass source_type= to extract media_urls from stored records of that type.
        """
        if urls is None:
            urls = []

        if source_type:
            stored = self.store.load(source_type, date_str)
            for record in stored:
                urls.extend(record.get("media_urls", []))

        urls = list(set(u for u in urls if u))  # deduplicate
        logger.info(f"[MediaCollector] Attempting to download {len(urls)} media URLs.")

        records = []
        for url in urls:
            try:
                record = self._download_one(url)
                if record:
                    records.append(record)
            except Exception as e:
                logger.error(f"[MediaCollector] Download failed for {url}: {e}")

        logger.info(f"[MediaCollector] Downloaded {len(records)}/{len(urls)} media files.")
        return records
