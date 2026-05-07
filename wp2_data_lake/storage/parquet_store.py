from pathlib import Path
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from wp2_data_lake.storage.schema import LakeRecord

LAKE_SCHEMA = pa.schema([
    pa.field("lake_id",           pa.string()),
    pa.field("source_type",       pa.string()),
    pa.field("source_name",       pa.string()),
    pa.field("record_id",         pa.string()),
    pa.field("collected_at",      pa.string()),
    pa.field("published_at",      pa.string()),
    pa.field("date_partition",    pa.string()),
    pa.field("url",               pa.string()),
    pa.field("text",              pa.string()),
    pa.field("language",          pa.string()),
    pa.field("keywords_hit",      pa.list_(pa.string())),
    pa.field("hazard_type",       pa.string()),
    pa.field("hazard_severity",   pa.string()),
    pa.field("location_name",     pa.string()),
    pa.field("lat",               pa.float64()),
    pa.field("lon",               pa.float64()),
    pa.field("media_urls",        pa.list_(pa.string())),
    pa.field("engagement_score",  pa.float64()),
    pa.field("temperature_c",     pa.float64()),
    pa.field("humidity_pct",      pa.float64()),
    pa.field("wind_speed_ms",     pa.float64()),
    pa.field("rainfall_mm",       pa.float64()),
    pa.field("pressure_hpa",      pa.float64()),
    pa.field("weather_condition", pa.string()),
    pa.field("media_type",        pa.string()),
    pa.field("local_path",        pa.string()),
    pa.field("file_size_kb",      pa.float64()),
])


class ParquetStore:
    def __init__(self, lake_dir: Path):
        self.silver_dir = lake_dir / "silver"
        self.silver_dir.mkdir(parents=True, exist_ok=True)

    def write_partition(self, records: List[LakeRecord], date_str: str, source_type: str) -> int:
        if not records:
            return 0

        out_dir = self.silver_dir / f"date={date_str}" / f"source={source_type}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "data.parquet"

        table = pa.Table.from_pylist(
            [r.to_dict() for r in records],
            schema=LAKE_SCHEMA,
        )
        pq.write_table(table, out_path, compression="snappy")
        logger.info(f"[parquet] {len(records):>5} records → {out_path.relative_to(self.silver_dir.parent.parent)}")
        return len(records)

    def list_partitions(self) -> List[Path]:
        return sorted(self.silver_dir.glob("date=*/source=*/data.parquet"))
