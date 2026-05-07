from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow.parquet as pq
from loguru import logger

from wp2_data_lake.config.settings import LAKE_DIR, RAW_DIR
from wp2_data_lake.ingestion.raw_loader import load_raw
from wp2_data_lake.storage.schema import LakeRecord

if TYPE_CHECKING:
    from wp2_data_lake.storage.sqlite_store import SQLiteStore


def _detect_type(raw: dict) -> str:
    if "temperature_c" in raw and "location_name" in raw:
        return "weather"
    if "source_url" in raw and "local_path" in raw:
        return "media"
    return "raw"


def migrate_bronze(store: SQLiteStore, raw_dir: Path) -> dict:
    """Load all WP1 JSONL files into bronze SQLite tables (idempotent)."""
    counts: dict = {"raw": 0, "weather": 0, "media": 0, "error": 0}
    for raw in load_raw(raw_dir):
        try:
            rtype = _detect_type(raw)
            with store.con:
                if rtype == "weather":
                    store.upsert_weather(raw)
                    counts["weather"] += 1
                elif rtype == "media":
                    store.upsert_media(raw)
                    counts["media"] += 1
                else:
                    store.upsert_raw(raw)
                    counts["raw"] += 1
        except Exception as exc:
            logger.warning(f"Skipping bronze record: {exc}")
            counts["error"] += 1
    return counts


def migrate_silver(store: SQLiteStore, silver_dir: Path) -> int:
    """Load all WP2 Parquet files into lake_records (idempotent)."""
    if not silver_dir.exists():
        logger.warning(f"Silver dir not found: {silver_dir}")
        return 0

    total = 0
    for parquet_file in sorted(silver_dir.glob("**/*.parquet")):
        try:
            rows = pq.read_table(parquet_file).to_pylist()
            records = [
                LakeRecord(
                    lake_id=r.get("lake_id") or "",
                    source_type=r.get("source_type") or "",
                    source_name=r.get("source_name") or "",
                    record_id=r.get("record_id") or "",
                    collected_at=r.get("collected_at") or "",
                    published_at=r.get("published_at"),
                    date_partition=r.get("date_partition") or "",
                    url=r.get("url"),
                    text=r.get("text"),
                    language=r.get("language") or "en",
                    keywords_hit=r.get("keywords_hit") or [],
                    hazard_type=r.get("hazard_type"),
                    hazard_severity=r.get("hazard_severity"),
                    location_name=r.get("location_name"),
                    lat=r.get("lat"),
                    lon=r.get("lon"),
                    media_urls=r.get("media_urls") or [],
                    engagement_score=r.get("engagement_score"),
                    temperature_c=r.get("temperature_c"),
                    humidity_pct=r.get("humidity_pct"),
                    wind_speed_ms=r.get("wind_speed_ms"),
                    rainfall_mm=r.get("rainfall_mm"),
                    pressure_hpa=r.get("pressure_hpa"),
                    weather_condition=r.get("weather_condition"),
                    media_type=r.get("media_type"),
                    local_path=r.get("local_path"),
                    file_size_kb=r.get("file_size_kb"),
                )
                for r in rows
            ]
            inserted = store.upsert_lake_batch(records)
            logger.info(f"  {parquet_file.parent.name}: {inserted}/{len(records)} inserted")
            total += inserted
        except Exception as exc:
            logger.warning(f"Skipping {parquet_file.name}: {exc}")
    return total


def _populate_spatial(store: SQLiteStore) -> int:
    cur = store.con.execute("""
        INSERT OR IGNORE INTO lake_spatial (id, min_lat, max_lat, min_lon, max_lon)
        SELECT lr.rowid, lr.lat, lr.lat, lr.lon, lr.lon
        FROM lake_records lr
        LEFT JOIN lake_spatial ls ON lr.rowid = ls.id
        WHERE lr.lat IS NOT NULL AND lr.lon IS NOT NULL AND ls.id IS NULL
    """)
    store.con.commit()
    return cur.rowcount


def run_migration(
    db_path: Path = None,
    raw_dir: Path = None,
    lake_dir: Path = None,
) -> None:
    from wp2_data_lake.storage.sqlite_store import SQLiteStore

    db_path  = db_path  or LAKE_DIR / "disaster_lake.db"
    raw_dir  = raw_dir  or RAW_DIR
    lake_dir = lake_dir or LAKE_DIR

    store = SQLiteStore(db_path)

    print("Migrating WP1 bronze JSONL...")
    bronze = migrate_bronze(store, raw_dir)
    for k, v in bronze.items():
        print(f"  {k:<10} {v}")

    print("\nMigrating WP2 silver Parquet...")
    silver_total = migrate_silver(store, lake_dir / "silver")
    print(f"  {silver_total} lake records inserted")

    print("\nPopulating spatial index...")
    n_spatial = _populate_spatial(store)
    print(f"  {n_spatial} spatial entries added")

    store.close()
    db_size_kb = db_path.stat().st_size // 1024
    print(f"\nMigration complete. DB: {db_path} ({db_size_kb} KB)")
