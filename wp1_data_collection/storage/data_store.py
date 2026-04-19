import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Union

from wp1_data_collection.config.settings import RAW_DIR
from wp1_data_collection.storage.schema import RawRecord, WeatherRecord, MediaRecord, SourceType
from wp1_data_collection.utils.logger import logger


class DataStore:
    """
    Writes collected records to partitioned JSON-Lines files.
    Directory layout:
        data/raw/<source_type>/YYYY-MM-DD.jsonl
    Each line is one JSON-serialised record.
    """

    def __init__(self):
        RAW_DIR.mkdir(parents=True, exist_ok=True)

    def _partition_path(self, source_type: str, date_str: str = None) -> Path:
        date_str = date_str or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        dest = RAW_DIR / source_type
        dest.mkdir(parents=True, exist_ok=True)
        return dest / f"{date_str}.jsonl"

    def save(self, record: Union[RawRecord, WeatherRecord, MediaRecord]) -> str:
        """Append record to its partition file. Returns the file path written."""
        if isinstance(record, RawRecord):
            source_type = record.source_type.value
        elif isinstance(record, WeatherRecord):
            source_type = SourceType.WEATHER.value
        elif isinstance(record, MediaRecord):
            source_type = SourceType.MEDIA.value
        else:
            raise TypeError(f"Unknown record type: {type(record)}")

        path = self._partition_path(source_type)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record.to_dict(), ensure_ascii=False, default=str) + "\n")
        return str(path)

    def save_batch(self, records: list) -> int:
        """Bulk-save a list of records. Returns count saved."""
        saved = 0
        for record in records:
            try:
                self.save(record)
                saved += 1
            except Exception as e:
                logger.error(f"Failed to save record: {e}")
        logger.info(f"Saved {saved}/{len(records)} records.")
        return saved

    def load(self, source_type: str, date_str: str = None) -> list[dict]:
        """Load all records for a source_type on a given date."""
        path = self._partition_path(source_type, date_str)
        if not path.exists():
            return []
        records = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def stats(self) -> dict:
        """Return record counts per source per date."""
        stats = {}
        for src_dir in RAW_DIR.iterdir():
            if not src_dir.is_dir():
                continue
            stats[src_dir.name] = {}
            for f in sorted(src_dir.glob("*.jsonl")):
                count = sum(1 for line in open(f) if line.strip())
                stats[src_dir.name][f.stem] = count
        return stats
