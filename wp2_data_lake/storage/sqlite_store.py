from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from wp2_data_lake.storage.schema import LakeRecord
from wp2_data_lake.transform.text_cleaner import clean as clean_text, label_confidence

_DDL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

-- ── Bronze tables (WP1 raw records) ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS raw_records (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type    TEXT NOT NULL,
    source_name    TEXT NOT NULL,
    record_id      TEXT NOT NULL,
    collected_at   TEXT NOT NULL,
    published_at   TEXT,
    date_partition TEXT NOT NULL,
    url            TEXT,
    text           TEXT,
    language       TEXT DEFAULT 'en',
    keywords_hit   TEXT DEFAULT '[]',
    media_urls     TEXT DEFAULT '[]',
    location       TEXT DEFAULT '{}',
    raw_payload    TEXT DEFAULT '{}',
    UNIQUE(source_type, record_id)
);
CREATE INDEX IF NOT EXISTS idx_raw_source_date  ON raw_records(source_type, date_partition);
CREATE INDEX IF NOT EXISTS idx_raw_collected_at ON raw_records(collected_at);

CREATE TABLE IF NOT EXISTS weather_records (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    location_name  TEXT NOT NULL,
    lat            REAL,
    lon            REAL,
    collected_at   TEXT NOT NULL,
    date_partition TEXT NOT NULL,
    temperature_c  REAL,
    humidity_pct   REAL,
    wind_speed_ms  REAL,
    wind_dir_deg   REAL,
    rainfall_mm    REAL,
    pressure_hpa   REAL,
    condition      TEXT,
    raw_payload    TEXT DEFAULT '{}',
    UNIQUE(location_name, collected_at)
);
CREATE INDEX IF NOT EXISTS idx_weather_loc_date ON weather_records(location_name, date_partition);
CREATE INDEX IF NOT EXISTS idx_weather_rainfall ON weather_records(rainfall_mm);

CREATE VIRTUAL TABLE IF NOT EXISTS weather_spatial USING rtree(
    id, min_lat, max_lat, min_lon, max_lon
);

CREATE TABLE IF NOT EXISTS media_records (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    source_url       TEXT NOT NULL UNIQUE,
    local_path       TEXT NOT NULL,
    media_type       TEXT NOT NULL,
    collected_at     TEXT NOT NULL,
    date_partition   TEXT NOT NULL,
    source_record_id TEXT,
    file_size_kb     REAL,
    width            INTEGER,
    height           INTEGER
);
CREATE INDEX IF NOT EXISTS idx_media_date         ON media_records(date_partition);
CREATE INDEX IF NOT EXISTS idx_media_source_record ON media_records(source_record_id);

-- ── Silver table (WP2 unified LakeRecord) ───────────────────────────────────

CREATE TABLE IF NOT EXISTS lake_records (
    lake_id           TEXT PRIMARY KEY,
    source_type       TEXT NOT NULL,
    source_name       TEXT NOT NULL,
    record_id         TEXT NOT NULL,
    collected_at      TEXT NOT NULL,
    published_at      TEXT,
    date_partition    TEXT NOT NULL,
    url               TEXT,
    text              TEXT,
    language          TEXT DEFAULT 'en',
    keywords_hit      TEXT DEFAULT '[]',
    hazard_type       TEXT,
    hazard_severity   TEXT,
    location_name     TEXT,
    lat               REAL,
    lon               REAL,
    media_urls        TEXT DEFAULT '[]',
    engagement_score  REAL,
    temperature_c     REAL,
    humidity_pct      REAL,
    wind_speed_ms     REAL,
    rainfall_mm       REAL,
    pressure_hpa      REAL,
    weather_condition TEXT,
    media_type        TEXT,
    local_path        TEXT,
    file_size_kb      REAL,
    UNIQUE(source_type, record_id)
);
CREATE INDEX IF NOT EXISTS idx_lake_source_date  ON lake_records(source_type, date_partition);
CREATE INDEX IF NOT EXISTS idx_lake_hazard       ON lake_records(hazard_type, hazard_severity);
CREATE INDEX IF NOT EXISTS idx_lake_collected_at ON lake_records(collected_at);
CREATE INDEX IF NOT EXISTS idx_lake_severity     ON lake_records(hazard_severity);
CREATE INDEX IF NOT EXISTS idx_lake_location     ON lake_records(location_name);

-- FTS5: full-text search on text + metadata (content table — no duplication)
CREATE VIRTUAL TABLE IF NOT EXISTS lake_fts USING fts5(
    text,
    keywords_hit,
    hazard_type,
    location_name,
    content='lake_records',
    content_rowid='rowid'
);

CREATE TRIGGER IF NOT EXISTS lake_fts_ai AFTER INSERT ON lake_records BEGIN
    INSERT INTO lake_fts(rowid, text, keywords_hit, hazard_type, location_name)
    VALUES (new.rowid, new.text, new.keywords_hit, new.hazard_type, new.location_name);
END;
CREATE TRIGGER IF NOT EXISTS lake_fts_ad AFTER DELETE ON lake_records BEGIN
    INSERT INTO lake_fts(lake_fts, rowid, text, keywords_hit, hazard_type, location_name)
    VALUES ('delete', old.rowid, old.text, old.keywords_hit, old.hazard_type, old.location_name);
END;
CREATE TRIGGER IF NOT EXISTS lake_fts_au AFTER UPDATE ON lake_records BEGIN
    INSERT INTO lake_fts(lake_fts, rowid, text, keywords_hit, hazard_type, location_name)
    VALUES ('delete', old.rowid, old.text, old.keywords_hit, old.hazard_type, old.location_name);
    INSERT INTO lake_fts(rowid, text, keywords_hit, hazard_type, location_name)
    VALUES (new.rowid, new.text, new.keywords_hit, new.hazard_type, new.location_name);
END;

-- R*Tree: bounding-box spatial queries on lat/lon
CREATE VIRTUAL TABLE IF NOT EXISTS lake_spatial USING rtree(
    id, min_lat, max_lat, min_lon, max_lon
);
"""


class SQLiteStore:
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = sqlite3.connect(str(db_path), check_same_thread=False)
        self.con.row_factory = sqlite3.Row
        self._create_schema()
        logger.debug(f"SQLiteStore ready at {db_path}")

    def _create_schema(self) -> None:
        self.con.executescript(_DDL)
        self.con.commit()

    @staticmethod
    def _jlist(v) -> str:
        if isinstance(v, list):
            return json.dumps(v)
        return v if isinstance(v, str) else "[]"

    @staticmethod
    def _jdict(v) -> str:
        if isinstance(v, dict):
            return json.dumps(v)
        return v if isinstance(v, str) else "{}"

    # ── Bronze inserts ────────────────────────────────────────────────────────

    def upsert_raw(self, rec: dict) -> None:
        self.con.execute("""
            INSERT OR IGNORE INTO raw_records
                (source_type, source_name, record_id, collected_at, published_at,
                 date_partition, url, text, language, keywords_hit, media_urls,
                 location, raw_payload)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            (rec.get("source_type") or "").lower(),
            rec.get("source_name") or "",
            rec.get("record_id") or "",
            rec.get("collected_at") or "",
            rec.get("published_at"),
            (rec.get("collected_at") or "")[:10],
            rec.get("url"),
            rec.get("text"),
            rec.get("language") or "en",
            self._jlist(rec.get("keywords_hit")),
            self._jlist(rec.get("media_urls")),
            self._jdict(rec.get("location")),
            self._jdict(rec.get("raw_payload")),
        ))

    def upsert_weather(self, rec: dict) -> None:
        cur = self.con.execute("""
            INSERT OR IGNORE INTO weather_records
                (location_name, lat, lon, collected_at, date_partition,
                 temperature_c, humidity_pct, wind_speed_ms, wind_dir_deg,
                 rainfall_mm, pressure_hpa, condition, raw_payload)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            rec.get("location_name") or "",
            rec.get("lat"), rec.get("lon"),
            rec.get("collected_at") or "",
            (rec.get("collected_at") or "")[:10],
            rec.get("temperature_c"), rec.get("humidity_pct"),
            rec.get("wind_speed_ms"), rec.get("wind_dir_deg"),
            rec.get("rainfall_mm"), rec.get("pressure_hpa"),
            rec.get("condition"),
            self._jdict(rec.get("raw_payload")),
        ))
        lat, lon = rec.get("lat"), rec.get("lon")
        if cur.rowcount > 0 and lat is not None and lon is not None:
            self.con.execute(
                "INSERT OR IGNORE INTO weather_spatial VALUES (?,?,?,?,?)",
                (cur.lastrowid, lat, lat, lon, lon),
            )

    def upsert_media(self, rec: dict) -> None:
        self.con.execute("""
            INSERT OR IGNORE INTO media_records
                (source_url, local_path, media_type, collected_at, date_partition,
                 source_record_id, file_size_kb, width, height)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            rec.get("source_url") or "",
            rec.get("local_path") or "",
            str(rec.get("media_type") or ""),
            rec.get("collected_at") or "",
            (rec.get("collected_at") or "")[:10],
            rec.get("source_record_id"),
            rec.get("file_size_kb"), rec.get("width"), rec.get("height"),
        ))

    # ── Silver insert ─────────────────────────────────────────────────────────

    def upsert_lake(self, record: LakeRecord) -> bool:
        """Insert or ignore. Returns True if the row was newly inserted."""
        cur = self.con.execute("""
            INSERT OR IGNORE INTO lake_records
                (lake_id, source_type, source_name, record_id,
                 collected_at, published_at, date_partition,
                 url, text, language,
                 keywords_hit, hazard_type, hazard_severity,
                 location_name, lat, lon, media_urls,
                 engagement_score, temperature_c, humidity_pct,
                 wind_speed_ms, rainfall_mm, pressure_hpa, weather_condition,
                 media_type, local_path, file_size_kb)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            record.lake_id, record.source_type, record.source_name, record.record_id,
            record.collected_at, record.published_at, record.date_partition,
            record.url, record.text, record.language,
            self._jlist(record.keywords_hit),
            record.hazard_type, record.hazard_severity,
            record.location_name, record.lat, record.lon,
            self._jlist(record.media_urls),
            record.engagement_score,
            record.temperature_c, record.humidity_pct,
            record.wind_speed_ms, record.rainfall_mm, record.pressure_hpa,
            record.weather_condition,
            record.media_type, record.local_path, record.file_size_kb,
        ))
        if cur.rowcount > 0 and record.lat is not None and record.lon is not None:
            self.con.execute(
                "INSERT OR IGNORE INTO lake_spatial VALUES (?,?,?,?,?)",
                (cur.lastrowid, record.lat, record.lat, record.lon, record.lon),
            )
        return cur.rowcount > 0

    def upsert_lake_batch(self, records: List[LakeRecord]) -> int:
        """Insert a batch in a single transaction. Returns newly inserted count."""
        inserted = 0
        with self.con:
            for rec in records:
                if self.upsert_lake(rec):
                    inserted += 1
        return inserted

    # ── Queries ───────────────────────────────────────────────────────────────

    def query(self, sql: str, params: tuple = ()) -> pd.DataFrame:
        return pd.read_sql_query(sql, self.con, params=params)

    def search_text(self, query_str: str, limit: int = 50) -> pd.DataFrame:
        return pd.read_sql_query("""
            SELECT lr.*
            FROM lake_fts f
            JOIN lake_records lr ON lr.rowid = f.rowid
            WHERE lake_fts MATCH ?
            ORDER BY rank
            LIMIT ?
        """, self.con, params=(query_str, limit))

    def query_bbox(
        self,
        min_lat: float, max_lat: float,
        min_lon: float, max_lon: float,
    ) -> pd.DataFrame:
        return pd.read_sql_query("""
            SELECT lr.*
            FROM lake_records lr
            JOIN lake_spatial ls ON lr.rowid = ls.id
            WHERE ls.min_lat >= ? AND ls.max_lat <= ?
              AND ls.min_lon >= ? AND ls.max_lon <= ?
        """, self.con, params=(min_lat, max_lat, min_lon, max_lon))

    def query_time_range(
        self,
        start_date: str,
        end_date: str,
        source_type: Optional[str] = None,
        hazard_type: Optional[str] = None,
    ) -> pd.DataFrame:
        clauses = ["date_partition BETWEEN ? AND ?"]
        params: list = [start_date, end_date]
        if source_type:
            clauses.append("source_type = ?")
            params.append(source_type)
        if hazard_type:
            clauses.append("hazard_type = ?")
            params.append(hazard_type)
        sql = f"SELECT * FROM lake_records WHERE {' AND '.join(clauses)} ORDER BY collected_at"
        return pd.read_sql_query(sql, self.con, params=tuple(params))

    # ── ML export ─────────────────────────────────────────────────────────────

    def export_ml_dataset(
        self,
        output_path: Path,
        hazard_types: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        format: str = "parquet",
        train_ratio: float = 0.8,
        val_ratio: float = 0.1,
    ) -> int:
        """Export a labeled, ML-ready dataset with temporal train/val/test split.

        Columns: text, hazard_type (label), hazard_severity (label), keywords_hit,
                 source_type, lat, lon, engagement_score, weather features, split.
        Returns number of rows written.
        """
        clauses = ["text IS NOT NULL", "hazard_type IS NOT NULL"]
        params: list = []
        if hazard_types:
            placeholders = ",".join("?" * len(hazard_types))
            clauses.append(f"hazard_type IN ({placeholders})")
            params.extend(hazard_types)
        if start_date:
            clauses.append("date_partition >= ?")
            params.append(start_date)
        if end_date:
            clauses.append("date_partition <= ?")
            params.append(end_date)

        sql = f"""
            SELECT
                lake_id, source_type, date_partition,
                text, keywords_hit,
                hazard_type, hazard_severity,
                location_name, lat, lon,
                engagement_score, language,
                temperature_c, humidity_pct, wind_speed_ms,
                rainfall_mm, pressure_hpa
            FROM lake_records
            WHERE {" AND ".join(clauses)}
            ORDER BY collected_at
        """
        df = pd.read_sql_query(sql, self.con, params=tuple(params))

        if df.empty:
            logger.warning("No labeled records found for ML export")
            return 0

        # Deserialize JSON array strings → proper Python lists
        df["keywords_hit"] = df["keywords_hit"].apply(
            lambda v: json.loads(v) if isinstance(v, str) else []
        )

        # Clean text: strip URLs, HTML, non-printable chars
        df["text"] = df["text"].apply(clean_text)
        df = df[df["text"].notna()].reset_index(drop=True)

        if df.empty:
            logger.warning("All records had empty text after cleaning")
            return 0

        # Label confidence score (keyword density) — keep for model weighting
        df["label_confidence"] = df.apply(
            lambda r: label_confidence(r["text"], r["keywords_hit"]), axis=1
        )

        # Normalize engagement_score to [0, 1] per source_type to make scales comparable
        for src, grp in df.groupby("source_type"):
            max_e = grp["engagement_score"].max()
            if max_e and max_e > 0:
                df.loc[grp.index, "engagement_score"] = grp["engagement_score"] / max_e

        # Temporal train/val/test split (by date, not random — avoids data leakage)
        unique_dates = sorted(df["date_partition"].unique())
        n = len(unique_dates)
        train_end = unique_dates[max(0, int(n * train_ratio) - 1)]
        val_end   = unique_dates[max(0, int(n * (train_ratio + val_ratio)) - 1)]

        df["split"] = df["date_partition"].apply(
            lambda d: "train" if d <= train_end else ("val" if d <= val_end else "test")
        )

        # Print class distribution so user can spot label imbalance
        logger.info(f"Exporting {len(df)} ML records")
        for (ht, hs), grp in df.groupby(["hazard_type", "hazard_severity"]):
            logger.info(f"  {ht:<15} {hs:<10} {len(grp)}")
        sc = df["split"].value_counts()
        logger.info(f"  Split → train:{sc.get('train',0)}  val:{sc.get('val',0)}  test:{sc.get('test',0)}")

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if format == "parquet":
            pq.write_table(pa.Table.from_pandas(df), str(output_path), compression="snappy")
        else:
            df.to_csv(output_path, index=False)

        logger.info(f"ML dataset → {output_path}")
        return len(df)

    # ── Stats ─────────────────────────────────────────────────────────────────

    def stats(self) -> pd.DataFrame:
        return pd.read_sql_query("""
            SELECT
                source_type,
                date_partition,
                COUNT(*) AS records,
                COUNT(DISTINCT hazard_type) AS hazard_types,
                SUM(CASE WHEN hazard_severity='CRITICAL' THEN 1 ELSE 0 END) AS critical,
                SUM(CASE WHEN hazard_severity='WARNING'  THEN 1 ELSE 0 END) AS warning,
                COUNT(DISTINCT location_name) AS locations
            FROM lake_records
            GROUP BY source_type, date_partition
            ORDER BY date_partition DESC, source_type
        """, self.con)

    def hazard_summary(self) -> pd.DataFrame:
        return pd.read_sql_query("""
            SELECT
                hazard_type,
                hazard_severity,
                COUNT(*) AS records,
                MIN(date_partition) AS first_seen,
                MAX(date_partition) AS last_seen
            FROM lake_records
            WHERE hazard_type IS NOT NULL
            GROUP BY hazard_type, hazard_severity
            ORDER BY records DESC
        """, self.con)

    def export_sensor_features(
        self,
        output_path: Path,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        format: str = "parquet",
    ) -> int:
        """Compute rolling sensor features and export for the early-warning model."""
        from wp2_data_lake.transform.sensor_features import build_sensor_features
        return build_sensor_features(
            con=self.con,
            output_path=output_path,
            start_date=start_date,
            end_date=end_date,
            format=format,
        )

    def validate(self) -> dict:
        """Return a data-quality and training-readiness report."""
        report = {}

        # Total record counts
        totals = pd.read_sql_query("""
            SELECT
                COUNT(*)                                         AS total,
                SUM(CASE WHEN text IS NOT NULL THEN 1 ELSE 0 END)        AS with_text,
                SUM(CASE WHEN lat IS NOT NULL  THEN 1 ELSE 0 END)        AS with_geo,
                SUM(CASE WHEN hazard_type IS NOT NULL THEN 1 ELSE 0 END) AS labeled,
                SUM(CASE WHEN text IS NOT NULL
                          AND hazard_type IS NOT NULL THEN 1 ELSE 0 END) AS labeled_with_text
            FROM lake_records
        """, self.con).iloc[0]
        report["totals"] = totals.to_dict()

        # Class distribution
        report["class_distribution"] = pd.read_sql_query("""
            SELECT hazard_type, hazard_severity, COUNT(*) AS records
            FROM lake_records
            WHERE hazard_type IS NOT NULL
            GROUP BY hazard_type, hazard_severity
            ORDER BY hazard_type, hazard_severity
        """, self.con)

        # Source coverage
        report["source_coverage"] = pd.read_sql_query("""
            SELECT
                source_type,
                COUNT(*) AS records,
                SUM(CASE WHEN hazard_type IS NOT NULL THEN 1 ELSE 0 END) AS labeled,
                ROUND(100.0 * SUM(CASE WHEN text IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1)
                    AS text_pct
            FROM lake_records
            GROUP BY source_type
            ORDER BY records DESC
        """, self.con)

        # Date range
        report["date_range"] = pd.read_sql_query("""
            SELECT MIN(date_partition) AS first_date,
                   MAX(date_partition) AS last_date
            FROM lake_records
        """, self.con).iloc[0].to_dict()

        # Sensor coverage
        report["sensor_coverage"] = pd.read_sql_query("""
            SELECT
                location_name,
                COUNT(*) AS readings,
                MIN(date_partition) AS first_date,
                MAX(date_partition) AS last_date
            FROM weather_records
            GROUP BY location_name
            ORDER BY readings DESC
        """, self.con)

        # Low-sample hazard type warnings (< 20 samples is too few)
        class_totals = report["class_distribution"].groupby("hazard_type")["records"].sum()
        report["low_sample_warnings"] = class_totals[class_totals < 20].index.tolist()

        return report

    def close(self) -> None:
        self.con.close()
