# Database Design — Why We Chose What We Chose

This document explains every storage technology used in this project, the reasoning behind each choice, and how they work together. Written to be accessible whether you're revisiting this six months later or explaining it to a new team member.

---

## The Big Picture: Three Storage Zones

```
Bronze Zone (raw)          Silver Zone (unified)                Gold Zone (ML-ready)
──────────────────         ──────────────────────────────────   ──────────────────────
JSONL flat files    ──►    Parquet files                  ──►   training.parquet
data/raw/                  + SQLite DB (disaster_lake.db)        (labeled, split, clean)
                           + DuckDB (catalog.duckdb)
```

Think of it like a refinery:
- **Bronze** = crude oil (raw, unprocessed, just collected)
- **Silver** = refined product (normalized, typed, indexed, queryable)
- **Gold** = ready to use (cleaned text, labels, train/val/test split)

---

## Layer 1 — Bronze: JSONL Files

**Location:** `data/raw/<source_type>/YYYY-MM-DD.jsonl`  
**Example:** `data/raw/rss/2026-04-19.jsonl`

Each line in a JSONL file is one complete JSON record — one tweet, one news article, one weather reading.

```json
{"source_type": "rss", "record_id": "abc123", "text": "Uttarakhand landslide...", "keywords_hit": ["Uttarakhand landslide"]}
{"source_type": "rss", "record_id": "def456", "text": "Kerala flood warning...", "keywords_hit": ["Kerala flood"]}
```

### Why JSONL?

| Requirement | Why JSONL fits |
|---|---|
| Five different collectors writing independently | Each appends to its own file — no coordination needed |
| No fixed schema at collection time | JSON handles missing fields gracefully |
| Crash recovery | A partial write at line N doesn't corrupt lines 1 to N-1 |
| Replay / re-ingestion | WP2 just re-reads the same files. No state to reset |
| Zero infrastructure | Pure Python `open(..., "a")` — no database driver, no server |

### Trade-off accepted

JSONL is row-oriented. If you want to query only the `hazard_type` column across 100,000 records, it reads every byte of every line. That's why the Silver layer exists.

---

## Layer 2 — Silver: Parquet Files

**Location:** `data/lake/silver/date=YYYY-MM-DD/source=<type>/data.parquet`

Parquet is a **columnar binary format** written by Apache Arrow and compressed with Snappy. It's the industry-standard format for analytical datasets (used by Spark, Hadoop, BigQuery, DuckDB, etc.).

### Why Parquet?

**Columnar storage**: imagine your data as a spreadsheet. Row-oriented storage writes row 1 fully, then row 2, etc. Columnar storage writes the entire `hazard_type` column, then the entire `date_partition` column, etc.

```
Row storage (JSONL):     [{row1_all_fields}, {row2_all_fields}, ...]
Columnar (Parquet):      [all hazard_types][all dates][all texts][all lats]...
```

If you query `SELECT hazard_type, COUNT(*) FROM lake GROUP BY hazard_type`, Parquet only reads the `hazard_type` column from disk. JSONL would scan every field of every row. For analytical queries, columnar is 10–100× faster.

**Hive partitioning** (the `date=X/source=Y` folder structure): when DuckDB queries `WHERE date_partition = '2026-04-19'`, it physically skips every folder that doesn't match. No Twitter folders are touched when you query only RSS data.

**Snappy compression**: ~3–5× smaller than raw JSON, with fast decompression (speed > maximum compression for our use case).

**Typed schema enforced at write time**: `LAKE_SCHEMA` in `parquet_store.py` defines the exact column types. Type mismatches are caught when writing, not when running a model.

### Trade-off accepted

Parquet files are **immutable**. You can't update one row — you'd rewrite the whole partition. That's fine here because records are append-only by design.

---

## Layer 3a — Silver: SQLite (`disaster_lake.db`)

**Location:** `data/lake/disaster_lake.db`

SQLite is a single-file relational database. It's the most deployed database engine in the world (it ships inside Python itself), and for this project it's the **transactional, searchable, ML-export-ready** side of the silver layer.

### Why SQLite (not PostgreSQL or MySQL)?

This is the most common question. Here's the direct comparison:

| | SQLite | PostgreSQL |
|---|---|---|
| Setup | Zero — single `.db` file | Requires a server process, users, ports |
| Concurrent writes | Single writer (fine for us) | Multiple concurrent writers |
| Included in Python | Yes (`import sqlite3`) | No — needs installation + driver |
| Full-text search | FTS5 built-in | `pg_trgm` or Elasticsearch needed |
| Spatial indexing | R*Tree built-in | PostGIS extension needed |
| Maintenance | None | Vacuuming, backups, connection pooling |
| Best for | Single-writer research pipeline | Multi-user production web app |

For a single-writer academic data pipeline, PostgreSQL's advantages (concurrent writes, advanced locking) don't apply. SQLite gives us everything we need with zero infrastructure overhead.

### Why SQLite (not MongoDB or other NoSQL)?

MongoDB is document-oriented and schema-free — great for truly variable data. Our data has a **well-defined, stable schema** (LakeRecord with 27 fields). We also need:
- SQL joins between `lake_records` and `lake_spatial`
- FTS5 full-text search (MongoDB's text search is less capable and not built-in)
- R*Tree spatial index (geospatial in MongoDB requires Atlas or extra setup)
- Pandas DataFrames from query results in one call (`pd.read_sql_query`)

SQLite with FTS5 + R*Tree gives all of this in one file.

### WAL Mode — What It Does

WAL (Write-Ahead Logging) is enabled at startup:
```sql
PRAGMA journal_mode=WAL;
```

In default mode, a write **locks the entire database**. In WAL mode:
- **Readers never block writers** — analytics queries can run while the streaming collector is inserting
- **Writers never block readers** — no query gets "database is locked" errors
- Essential for the stream pipeline, where Twitter/Reddit push data continuously while you're running queries

### Tables

| Table | Type | Contents |
|---|---|---|
| `raw_records` | Regular table | WP1 Bronze: articles, tweets, Reddit posts |
| `weather_records` | Regular table | WP1 Bronze: weather readings per location per timestamp |
| `media_records` | Regular table | WP1 Bronze: downloaded image/video metadata |
| `lake_records` | Regular table | WP2 Silver: unified 27-field record (one row per event) |
| `lake_fts` | FTS5 virtual table | Full-text index over `text`, `keywords_hit`, `hazard_type`, `location_name` |
| `lake_spatial` | R*Tree virtual table | Bounding-box spatial index on `lat`/`lon` |
| `weather_spatial` | R*Tree virtual table | Spatial index for weather station locations |

### FTS5 — Full-Text Search

`lake_fts` is a **content table** — a special mode where FTS5 doesn't duplicate the text:

```sql
CREATE VIRTUAL TABLE lake_fts USING fts5(
    text, keywords_hit, hazard_type, location_name,
    content='lake_records',    -- reads from this table
    content_rowid='rowid'      -- links by this key
);
```

Text lives only in `lake_records`. FTS5 just stores its index. Three triggers (`AFTER INSERT`, `AFTER UPDATE`, `AFTER DELETE`) keep the index automatically in sync when rows change.

A full-text search query:
```sql
SELECT * FROM lake_fts WHERE lake_fts MATCH 'flood warning Kerala' ORDER BY rank;
```

This returns results ranked by relevance — articles most closely matching the query come first.

### R*Tree — Spatial Index

```sql
CREATE VIRTUAL TABLE lake_spatial USING rtree(id, min_lat, max_lat, min_lon, max_lon);
```

R*Tree is a spatial index structure that finds records within a geographic bounding box in O(log n) time. Without it, a bounding-box query would scan every row and compare coordinates. With it:

```python
# Find all records within Uttarakhand's bounding box
store.query_bbox(min_lat=28.7, max_lat=31.5, min_lon=77.5, max_lon=81.1)
```

This is how a real-time map view would work — as the user pans the map, the query changes, and R*Tree returns matching records instantly.

### `INSERT OR IGNORE` — Idempotent Writes

Every insert uses a `UNIQUE` constraint + `INSERT OR IGNORE`:

```sql
CREATE TABLE lake_records (
    ...
    UNIQUE(source_type, record_id)   -- same article cannot be inserted twice
);

INSERT OR IGNORE INTO lake_records ...  -- silently skips duplicates
```

This means the pipeline is **idempotent** — you can re-run WP1 or WP2 as many times as you want without creating duplicate records. This is critical when debugging or backfilling data.

### ML Export — Temporal Train/Val/Test Split

The `export_ml_dataset()` function splits records by **date order**, not randomly:

```python
unique_dates = sorted(df["date_partition"].unique())
train_end = unique_dates[int(n * 0.8)]   # first 80% of dates → train
val_end   = unique_dates[int(n * 0.9)]   # next 10% → validation
# remaining → test
```

**Why temporal split?** If you split randomly, a model trained on Wednesday's data might "see" Tuesday's data during validation — data leakage. With temporal split, the model is always evaluated on dates it has never seen, which reflects real-world deployment where you train on past events and predict future ones.

---

## Layer 3b — Silver: DuckDB (`catalog.duckdb`)

**Location:** `data/lake/catalog.duckdb`

DuckDB is an **in-process analytical database** — like SQLite but built specifically for fast analytical queries rather than transactional inserts. It stores no data of its own; it reads the Parquet files directly.

### Why DuckDB?

| Need | How DuckDB meets it |
|---|---|
| Fast GROUP BY on millions of rows | Vectorized columnar execution — processes 1000+ rows per CPU instruction |
| No ETL / import step | `read_parquet('**/*.parquet')` — queries files directly |
| Hive partition pruning | Automatically skips `date=` / `source=` folders that don't match WHERE clause |
| Simple Pandas integration | `con.execute(sql).df()` — one line to get a DataFrame back |
| No server to manage | In-process, like SQLite — just a `.duckdb` file + a Python package |

### How It Works

After each WP2 ingest, the catalog is refreshed:

```python
self.con.execute(f"""
    CREATE OR REPLACE VIEW lake AS
    SELECT * FROM read_parquet('{glob_path}', hive_partitioning = true)
""")
```

From that point, `SELECT * FROM lake WHERE date_partition = '2026-04-19'` transparently reads only the matching Parquet partition. Every time WP2 writes a new partition, one `refresh()` call makes it available.

### DuckDB vs SQLite — When to Use Which

| Use case | Use |
|---|---|
| Insert records (streaming or batch) | **SQLite** — DuckDB is not designed for frequent row-level inserts |
| Full-text search on article text | **SQLite** (FTS5) |
| Bounding-box geo lookup | **SQLite** (R*Tree) |
| Export labeled ML dataset | **SQLite** (`export_ml_dataset`) |
| Heavy GROUP BY / aggregations | **DuckDB** — vectorized, much faster |
| Ad-hoc analytical queries across all dates | **DuckDB** — reads all Parquet partitions efficiently |
| Quick stats for a dashboard | Either — both have a `stats()` method |

They are **complementary, not competing**. SQLite is the transactional store; DuckDB is the analytics engine. Both read from the same underlying data (Parquet files or SQLite rows).

---

## Complete Data Flow

```
[Twitter / Reddit / NewsAPI / RSS / OpenWeatherMap]
             │
             ▼
   WP1 Collectors (5 source types)
             │
             │  append one JSON line per record
             ▼
   data/raw/<source>/YYYY-MM-DD.jsonl         ◄── Bronze Zone
   data/raw/media/images/*.jpg                ◄── Raw image files
             │
             │  WP2 reads these files (no message queue needed)
             ▼
   WP2 Raw Loader → streams JSONL line by line
             │
             ▼
   WP2 Normalizer
   ├── detects record type (text / weather / media)
   ├── classifies hazard type (FLOOD / LANDSLIDE / ...)
   └── classifies severity (CRITICAL / WARNING / INFO)
             │
             │  LakeRecord objects (27 typed fields)
             ▼
   ┌──────────────────────────────────────────────────────┐
   │               Silver Zone                            │
   │                                                      │
   │  data/lake/silver/date=X/source=Y/data.parquet       │
   │       └─► DuckDB catalog registers as VIEW `lake`    │
   │                                                      │
   │  data/lake/disaster_lake.db  (SQLite)                │
   │       ├── lake_records  (all 27 fields)              │
   │       ├── lake_fts      (full-text index)            │
   │       └── lake_spatial  (R*Tree bounding-box)        │
   └──────────────────────────────────────────────────────┘
             │
             │  export-ml / export-sensors commands
             ▼
   data/ml/training.parquet                   ◄── Gold Zone
   ├── cleaned text
   ├── hazard_type + hazard_severity (labels)
   ├── label_confidence (keyword density score)
   ├── normalized engagement_score
   └── split: train / val / test (temporal)
             │
             ▼
   [WP3 AI/NLP Models — next phase]
```

---

## Summary Cheat Sheet

| Store | Format | Role | Strength |
|---|---|---|---|
| `data/raw/**/*.jsonl` | JSONL | Bronze: raw append-only | Zero overhead, crash-safe, replayable |
| `data/lake/silver/**/*.parquet` | Parquet + Snappy | Silver: columnar archive | Fast analytics, partition pruning, compact |
| `disaster_lake.db` | SQLite WAL | Silver: transactional + search | FTS5 text search, R*Tree spatial, ML export |
| `catalog.duckdb` | DuckDB | Silver: analytics catalog | Vectorized GROUP BY, reads Parquet directly |
| `data/ml/training.parquet` | Parquet | Gold: ML-ready | Labeled, cleaned, split, confidence-scored |

---

## Why Not Other Options?

| Alternative | Why not used |
|---|---|
| PostgreSQL | Requires a server daemon, user accounts, network config — overkill for a single-writer academic pipeline |
| MongoDB | Document store without built-in FTS5, R*Tree, or Pandas SQL integration; schema-free doesn't help us (we have a fixed schema) |
| MySQL | Same as PostgreSQL — server overhead without benefit |
| Redis | In-memory; great for caching but not persistent analytics or text search |
| Elasticsearch | Powerful FTS but requires Java, a running server, and significant memory — FTS5 covers our needs |
| Plain CSV | No indexing, no types, no append safety, no FTS, no spatial queries |
| HDF5 | Common in ML but not queryable with SQL, no FTS, awkward schema changes |
