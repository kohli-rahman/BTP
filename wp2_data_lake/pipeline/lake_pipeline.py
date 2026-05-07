from collections import defaultdict
from typing import Optional

from loguru import logger

from wp2_data_lake.config.settings import LAKE_DIR, RAW_DIR, SQLITE_DB_PATH
from wp2_data_lake.ingestion.raw_loader import load_raw
from wp2_data_lake.storage.duckdb_catalog import DuckDBCatalog
from wp2_data_lake.storage.parquet_store import ParquetStore
from wp2_data_lake.storage.sqlite_store import SQLiteStore
from wp2_data_lake.transform.normalizer import normalize


class LakePipeline:
    def __init__(self):
        self.store   = ParquetStore(LAKE_DIR)
        self.catalog = DuckDBCatalog(LAKE_DIR)
        self.sqlite  = SQLiteStore(SQLITE_DB_PATH)

    def run(self, date_str: Optional[str] = None, source_type: Optional[str] = None) -> dict:
        logger.info(f"Lake pipeline starting (date={date_str or 'all'}, source={source_type or 'all'})")

        # Group normalized records by (date, source) before writing
        buckets: dict = defaultdict(list)
        n_loaded = n_normalized = n_skipped = 0

        for raw in load_raw(RAW_DIR, source_type=source_type, date_str=date_str):
            n_loaded += 1
            record = normalize(raw)
            if record and record.date_partition:
                buckets[(record.date_partition, record.source_type)].append(record)
                n_normalized += 1
            else:
                n_skipped += 1

        logger.info(f"Loaded {n_loaded} | Normalized {n_normalized} | Skipped {n_skipped}")

        n_written = 0
        for (date, src), records in sorted(buckets.items()):
            n_written += self.store.write_partition(records, date, src)
            self.sqlite.upsert_lake_batch(records)

        self.catalog.refresh()
        logger.info(f"Lake pipeline complete — {n_written} records in {len(buckets)} partition(s)")

        return {
            "loaded": n_loaded,
            "normalized": n_normalized,
            "skipped": n_skipped,
            "partitions_written": len(buckets),
            "records_written": n_written,
        }

    def stats(self):
        return self.catalog.stats()

    def hazard_summary(self):
        return self.catalog.hazard_summary()

    def query(self, sql: str):
        return self.catalog.query(sql)

    # ── SQLite-backed methods ─────────────────────────────────────────────────

    def db_stats(self):
        return self.sqlite.stats()

    def db_query(self, sql: str):
        return self.sqlite.query(sql)

    def search_text(self, query_str: str, limit: int = 50):
        return self.sqlite.search_text(query_str, limit)

    def query_bbox(self, min_lat: float, max_lat: float, min_lon: float, max_lon: float):
        return self.sqlite.query_bbox(min_lat, max_lat, min_lon, max_lon)

    def query_time_range(self, start: str, end: str, source_type=None, hazard_type=None):
        return self.sqlite.query_time_range(start, end, source_type, hazard_type)

    def export_ml(self, output_path, **kwargs):
        return self.sqlite.export_ml_dataset(output_path, **kwargs)

    def close(self):
        self.catalog.close()
        self.sqlite.close()
