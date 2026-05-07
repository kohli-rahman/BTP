# Multi-Hazard Early Warning System — IIT Patna

**DST Technology Development Programme**  
*Intelligent Responsive System for Multi-Hazard Early Warning and Management in Hilly Regions of India*

A collaborative project between NITK, IIT Roorkee, Amrita Vishwa Vidyapeetham, **IIT Patna (with TCS)**, and UPES Dehradun.

---

## What This Is

IIT Patna's role in the project is to build the **data intelligence layer** — collecting raw disaster-related data from across the web, structuring it into a searchable hazard data lake, and preparing it for AI/NLP model training.

This repository covers **Phase I**: the data collection and lake creation pipelines.

**Hazard types tracked:** `FLOOD` · `LANDSLIDE` · `EARTHQUAKE` · `CYCLONE` · `DROUGHT` · `HEATWAVE` · `COLD_WAVE` · `FIRE` · `CLOUDBURST`

**Geographic focus:** India — specifically hilly and disaster-prone states: Uttarakhand, Sikkim, Kerala, Himachal Pradesh, Assam, Manipur.

---

## Project Structure

```
BTP/
├── run_pipeline.py          # One command to run WP1 + WP2 end-to-end
├── wp1_data_collection/     # Data collection (social, news, weather, media)
├── wp2_data_lake/           # Data lake (normalize, store, index, ML export)
├── data/                    # All data — gitignored
│   ├── raw/                 # Bronze: JSONL files per source per date
│   ├── lake/                # Silver: Parquet + SQLite + DuckDB
│   └── ml/                  # Gold: cleaned, labeled, split training datasets
├── explain.md               # Deep-dive on database design choices
└── requirements.txt
```

---

## Quick Start

```bash
# 1. Install dependencies
pip3 install -r requirements.txt

# 2. Set up API keys
cp .env.example .env
# Fill in: OPENWEATHER_API_KEY, NEWS_API_KEY, REDDIT_CLIENT_ID, etc.

# 3. Run the full pipeline (collect + ingest)
python3 run_pipeline.py --days 7

# 4. Check what was collected
python3 -m wp1_data_collection.main stats

# 5. Query the lake
python3 -m wp2_data_lake.main search "flood warning"
```

---

## WP1 — Data Collection

Collects disaster-relevant content from five source types, all scoped to India.

| Source | Status | Notes |
|---|---|---|
| RSS Feeds | ✅ Live | NDTV, The Hindu, HT, India Today, NDMA, ReliefWeb-India, The Print |
| OpenWeatherMap | ✅ Live | 6 hilly Indian states, rainfall threshold alerts |
| NewsAPI | ✅ Ready | 21 Indian news domains; 100 requests/day on free tier |
| Reddit | ✅ Ready | Indian disaster subreddits; needs `REDDIT_CLIENT_ID` in `.env` |
| Twitter/X | ⚠️ Paid | Requires $100/month Basic API tier |
| Media (images) | ✅ Live | Hero images from articles — Indian CDN domains only |

### WP1 Commands

```bash
# Collect everything (7-day lookback)
python3 -m wp1_data_collection.main batch --days 7

# Collect a single source
python3 -m wp1_data_collection.main collect rss
python3 -m wp1_data_collection.main collect weather
python3 -m wp1_data_collection.main collect news
python3 -m wp1_data_collection.main collect reddit
python3 -m wp1_data_collection.main collect media

# Real-time streaming
python3 -m wp1_data_collection.main stream
python3 -m wp1_data_collection.main stream --no-twitter   # skip if no paid key

# Stats
python3 -m wp1_data_collection.main stats
```

---

## WP2 — Data Lake

Reads WP1's raw files, normalizes everything into a unified 27-field schema, classifies hazard type and severity, and writes to a queryable lake backed by **Parquet + SQLite + DuckDB**.

See [explain.md](explain.md) for a full breakdown of why these databases were chosen.

### WP2 Commands

```bash
# Full ingest (JSONL → Parquet + SQLite)
python3 -m wp2_data_lake.main ingest

# Ingest a specific date or source
python3 -m wp2_data_lake.main ingest --date 2026-04-19
python3 -m wp2_data_lake.main ingest --source rss

# Stats
python3 -m wp2_data_lake.main stats       # Parquet / DuckDB view
python3 -m wp2_data_lake.main db-stats    # SQLite view

# SQL queries
python3 -m wp2_data_lake.main query "SELECT hazard_type, COUNT(*) FROM lake GROUP BY 1"
python3 -m wp2_data_lake.main db-query "SELECT * FROM lake_records WHERE hazard_severity='CRITICAL'"

# Full-text search (FTS5)
python3 -m wp2_data_lake.main search "cyclone warning Kerala"

# Bounding-box spatial query (R*Tree)
python3 -m wp2_data_lake.main bbox --min-lat 28.7 --max-lat 31.5 --min-lon 77.5 --max-lon 81.1

# Export ML training dataset
python3 -m wp2_data_lake.main export-ml --output data/ml/training.parquet
python3 -m wp2_data_lake.main export-ml --output data/ml/floods.parquet --hazard FLOOD,LANDSLIDE

# Export rolling sensor features for early-warning model
python3 -m wp2_data_lake.main export-sensors --output data/ml/sensor_features.parquet

# Validate training readiness (class balance, coverage, warnings)
python3 -m wp2_data_lake.main validate

# One-time migration of existing data into SQLite
python3 -m wp2_data_lake.main migrate
```

---

## End-to-End Pipeline

```bash
# Collect + ingest in one shot
python3 run_pipeline.py --days 7

# WP1 only
python3 run_pipeline.py --collect-only --days 7

# WP2 only (re-ingest existing raw data)
python3 run_pipeline.py --lake-only
```

---

## Storage Layout

```
data/
├── raw/                                   # Bronze Zone (WP1 output)
│   ├── rss/2026-04-19.jsonl
│   ├── weather/2026-04-19.jsonl
│   ├── media/2026-04-19.jsonl
│   └── media/images/*.jpg
│
├── lake/                                  # Silver Zone (WP2 output)
│   ├── disaster_lake.db                   # SQLite: FTS5 + R*Tree + ML export
│   ├── catalog.duckdb                     # DuckDB: fast analytics over Parquet
│   └── silver/
│       └── date=2026-04-19/
│           ├── source=rss/data.parquet
│           ├── source=weather/data.parquet
│           └── source=media/data.parquet
│
└── ml/                                    # Gold Zone (model training)
    ├── training.parquet                   # text + labels + split column
    └── sensor_features.parquet            # rolling weather features + hazard label
```

---

## API Keys Required

| API | Free Tier | Used For |
|---|---|---|
| `OPENWEATHER_API_KEY` | Yes | Weather for 6 Indian locations |
| `NEWS_API_KEY` | Yes (100 req/day) | Indian news articles |
| `REDDIT_CLIENT_ID` + `REDDIT_CLIENT_SECRET` | Yes | Disaster posts from Indian subreddits |
| `TWITTER_BEARER_TOKEN` | No ($100/mo) | Geotagged Indian disaster tweets |

---

## Progress

### ✅ Done

- [x] **WP1 — RSS Collector** — 9 Indian feeds (NDTV, The Hindu, NDMA, ReliefWeb, HT, India Today, The Print, GDACS, DownToEarth)
- [x] **WP1 — Weather Collector** — OpenWeatherMap for Uttarakhand, Sikkim, Kerala, Himachal Pradesh, Assam, Manipur; rainfall threshold alerts (15 mm = WARNING, 35 mm = CRITICAL)
- [x] **WP1 — NewsAPI Collector** — 21 Indian news domains; free-tier fallback to `country=in`
- [x] **WP1 — Reddit Collector** — 10 Indian disaster/regional subreddits
- [x] **WP1 — Twitter Collector** — `place_country:IN` filtered stream (ready; gated on paid API key)
- [x] **WP1 — Media Collector** — Image download with Indian-domain allowlist (22 hostnames), size filter (≥300×200px), 4 concurrent workers
- [x] **WP1 — Batch Pipeline** — Parallel Stage-1 (all text/weather collectors) → sequential Stage-2 (media harvest)
- [x] **WP1 — Stream Pipeline** — Multi-threaded: Twitter push stream + Reddit/Weather polling loops, graceful SIGINT shutdown
- [x] **WP2 — Normalizer** — Auto-detects record type (text / weather / media); classifies 9 hazard types + 3 severity levels
- [x] **WP2 — Parquet Store** — Hive-partitioned Parquet with Snappy compression; enforced typed schema
- [x] **WP2 — DuckDB Catalog** — Live SQL `VIEW lake` over all Parquet partitions; refreshed after each ingest
- [x] **WP2 — SQLite Store** — WAL mode; FTS5 full-text search; R*Tree spatial index; idempotent upserts
- [x] **WP2 — ML Export** — Text cleaning (URL/HTML/non-printable strip), label confidence score, normalized engagement, temporal train/val/test split
- [x] **WP2 — Sensor Features** — Rolling 3h/24h/72h rainfall, pressure drop, consecutive rain count; supervised label aligned from lake_records (hazard within 24h)
- [x] **WP2 — Data Validator** — Class distribution, source coverage, low-sample warnings, geo coverage report
- [x] **WP2 — Lake Pipeline** — Full orchestration: load → normalize → Parquet → SQLite → DuckDB refresh
- [x] **WP2 — Migration** — One-time idempotent loader from existing JSONL + Parquet into SQLite

---

## Design Notes

A few non-obvious decisions worth noting:

- **JSONL for bronze**: append-only, no schema migration needed, survives partial writes, easy to replay into WP2
- **SQLite over PostgreSQL**: zero infrastructure for a single-writer academic pipeline; FTS5 and R*Tree are built-in
- **DuckDB alongside SQLite**: complementary — SQLite handles row-level inserts and text/spatial queries; DuckDB handles fast columnar analytics over Parquet
- **Temporal train/val/test split**: splits by date order (not randomly) to prevent data leakage from future events into the training set
- **`INSERT OR IGNORE`**: every write is idempotent — the entire pipeline can be re-run without creating duplicates

For the full database design rationale, see [explain.md](explain.md).
