import hashlib
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests
from PIL import Image

from wp1_data_collection.collectors.base_collector import BaseCollector
from wp1_data_collection.config.settings import RAW_DIR, INDIA_MEDIA_DOMAINS
from wp1_data_collection.storage.schema import MediaRecord, MediaType
from wp1_data_collection.utils.logger import logger

MEDIA_DIR = RAW_DIR / "media"

SUPPORTED_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".tiff"}
SUPPORTED_VIDEO_EXTS = {".mp4", ".avi", ".mov", ".mkv", ".webm"}

MAX_FILE_SIZE_MB = 10
MIN_IMAGE_WIDTH  = 300   # pixels — skip icons, thumbnails, logos
MIN_IMAGE_HEIGHT = 200

# URL patterns that reliably indicate non-article media (logos, trackers, icons)
_SKIP_URL_PATTERNS = re.compile(
    r"(logo|icon|avatar|banner|pixel|tracker|badge|button|sprite|widget|"
    r"gravatar|favicon|ads|advert|analytics|1x1|blank\.|spacer)",
    re.IGNORECASE,
)

DOWNLOAD_TIMEOUT  = 8    # seconds — fail fast rather than hang
MAX_WORKERS       = 4    # concurrent downloads


class MediaCollector(BaseCollector):
    """
    Downloads disaster-relevant images/videos from previously collected RawRecords.

    Relevance rules applied before downloading:
      1. Source record must have at least one disaster keyword hit.
      2. URL must not match known logo/icon/tracker patterns.
      3. After download, image must be >= MIN_IMAGE_WIDTH x MIN_IMAGE_HEIGHT pixels.
    """

    def __init__(self, **kwargs):
        super().__init__("MediaCollector", **kwargs)
        MEDIA_DIR.mkdir(parents=True, exist_ok=True)

    def authenticate(self) -> bool:
        return True

    # ── Filtering helpers ──────────────────────────────────────────────────

    def _is_relevant_url(self, url: str) -> bool:
        """
        Two-pass filter:
          1. Reject URLs matching logo/icon/tracker patterns.
          2. Accept only images hosted on known Indian news domains so foreign
             stock photos and international CDNs are excluded.
        """
        if _SKIP_URL_PATTERNS.search(url):
            return False
        hostname = urlparse(url).hostname or ""
        # strip www. prefix for matching
        hostname = hostname.removeprefix("www.")
        return any(hostname == d or hostname.endswith("." + d) for d in INDIA_MEDIA_DOMAINS)

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

    def _get_image_dimensions(self, path: Path) -> tuple[Optional[int], Optional[int]]:
        try:
            with Image.open(path) as img:
                return img.width, img.height
        except Exception:
            return None, None

    # ── Download ───────────────────────────────────────────────────────────

    def _download_one(self, url: str, source_record_id: str = None) -> Optional[MediaRecord]:
        if not self._is_relevant_url(url):
            logger.debug(f"[MediaCollector] Skipped (pattern filter): {url}")
            return None

        try:
            resp = requests.get(
                url,
                timeout=DOWNLOAD_TIMEOUT,
                stream=True,
                headers={"User-Agent": "DisasterBot/1.0"},
            )
        except Exception as e:
            logger.debug(f"[MediaCollector] Request failed: {url} — {e}")
            return None

        if resp.status_code != 200:
            return None

        content_type = resp.headers.get("Content-Type", "")
        media_type = self._detect_media_type(url, content_type)
        if not media_type:
            return None

        # Skip oversized files
        size_header = int(resp.headers.get("Content-Length", 0))
        if size_header > MAX_FILE_SIZE_MB * 1024 * 1024:
            logger.debug(f"[MediaCollector] Skipped (too large): {url}")
            return None

        try:
            content = resp.content
        except Exception:
            return None

        local_path = self._local_path(url, media_type)
        if not local_path.exists():
            local_path.write_bytes(content)

        size_kb = round(local_path.stat().st_size / 1024, 2)

        w, h = (None, None)
        if media_type == MediaType.IMAGE:
            w, h = self._get_image_dimensions(local_path)
            if w and h and (w < MIN_IMAGE_WIDTH or h < MIN_IMAGE_HEIGHT):
                local_path.unlink(missing_ok=True)
                logger.debug(f"[MediaCollector] Skipped (too small {w}x{h}): {url}")
                return None

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

    # ── Public API ─────────────────────────────────────────────────────────

    def collect_batch(
        self,
        urls: list[str] = None,
        source_type: str = None,
        date_str: str = None,
        **kwargs,
    ) -> list[MediaRecord]:
        """
        Two modes:
          1. urls=  — download a specific list of URLs directly.
          2. source_type= — load stored records for that source and extract
             media_urls only from records that had at least one keyword hit.
        """
        if urls is None:
            urls = []

        if source_type:
            stored = self.store.load(source_type, date_str)
            for record in stored:
                # Only pull images from disaster-relevant records
                if record.get("keywords_hit"):
                    urls.extend(record.get("media_urls", []))

        urls = list(set(u for u in urls if u))
        logger.info(f"[MediaCollector] Queued {len(urls)} URLs for download.")

        if not urls:
            return []

        records = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(self._download_one, url): url for url in urls}
            for future in as_completed(futures):
                try:
                    record = future.result()
                    if record:
                        records.append(record)
                except Exception as e:
                    logger.error(f"[MediaCollector] Unexpected error: {e}")

        logger.info(f"[MediaCollector] Saved {len(records)}/{len(urls)} relevant media files.")
        return records
