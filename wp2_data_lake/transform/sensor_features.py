"""
Sensor time-series feature engineering for the early-warning model.

Reads raw weather_records from SQLite, computes rolling window features
per location, and aligns each reading with a supervised hazard label
derived from lake_records (text/social sources).

Output schema (one row per weather reading):
  ── Identity ─────────────────────────────────────────────────────────
  location_name, lat, lon, collected_at, date_partition
  ── Rolling rainfall (mm) ─────────────────────────────────────────────
  rainfall_3h_sum, rainfall_24h_sum, rainfall_72h_sum, rainfall_24h_max
  ── Rolling wind (m/s) ────────────────────────────────────────────────
  wind_24h_max
  ── Temperature ───────────────────────────────────────────────────────
  temperature_c, temp_change_24h   (current minus 24-h rolling mean)
  ── Pressure ──────────────────────────────────────────────────────────
  pressure_hpa, pressure_drop_3h   (negative = storm forming)
  ── Humidity ──────────────────────────────────────────────────────────
  humidity_pct, humidity_24h_max
  ── Categorical ───────────────────────────────────────────────────────
  month, hour, day_of_year
  consecutive_rain_readings        (count of consecutive non-zero readings)
  ── Supervised labels ─────────────────────────────────────────────────
  hazard_within_24h   (0/1 — is a hazard reported for this location within 24h?)
  upcoming_hazard_type             (e.g. "FLOOD", None if no event)
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger


def _consecutive_rain(series: pd.Series) -> list:
    """Count consecutive non-zero rainfall readings ending at each row."""
    result, count = [], 0
    for v in series:
        count = count + 1 if (v or 0) > 0.0 else 0
        result.append(count)
    return result


def _rolling_features(group: pd.DataFrame) -> pd.DataFrame:
    """Compute time-based rolling features for a single location group."""
    g = group.sort_values("collected_at").copy()
    g = g.set_index("collected_at")

    rain = g["rainfall_mm"].fillna(0.0)
    wind = g["wind_speed_ms"].fillna(0.0)
    temp = g["temperature_c"].fillna(method="ffill")
    pres = g["pressure_hpa"].fillna(method="ffill")
    humi = g["humidity_pct"].fillna(0.0)

    # Rainfall rolling windows
    g["rainfall_3h_sum"]  = rain.rolling("3h",  min_periods=1).sum()
    g["rainfall_24h_sum"] = rain.rolling("24h", min_periods=1).sum()
    g["rainfall_72h_sum"] = rain.rolling("72h", min_periods=1).sum()
    g["rainfall_24h_max"] = rain.rolling("24h", min_periods=1).max()

    # Wind
    g["wind_24h_max"] = wind.rolling("24h", min_periods=1).max()

    # Temperature change vs 24-h mean
    g["temp_change_24h"] = temp - temp.rolling("24h", min_periods=1).mean()

    # Pressure drop over 3 h (negative value = dropping fast = storm signal)
    g["pressure_drop_3h"] = pres - pres.shift(1).rolling("3h", min_periods=1).mean()

    # Humidity max over 24 h
    g["humidity_24h_max"] = humi.rolling("24h", min_periods=1).max()

    g = g.reset_index()

    # Consecutive non-zero rainfall readings
    g["consecutive_rain_readings"] = _consecutive_rain(g["rainfall_mm"].fillna(0))

    # Time features
    g["hour"]        = g["collected_at"].dt.hour
    g["month"]       = g["collected_at"].dt.month
    g["day_of_year"] = g["collected_at"].dt.day_of_year

    return g


def _add_event_labels(df: pd.DataFrame, con: sqlite3.Connection) -> pd.DataFrame:
    """Join each weather reading with upcoming hazard events (next 24 h, same location).

    Adds:
      hazard_within_24h   int  (0 or 1)
      upcoming_hazard_type str (hazard type or None)
    """
    # Load all hazard lake_records (text/social sources only — not weather)
    events = pd.read_sql_query("""
        SELECT location_name, collected_at, hazard_type
        FROM lake_records
        WHERE hazard_type IS NOT NULL
          AND source_type != 'weather'
        ORDER BY location_name, collected_at
    """, con)

    if events.empty:
        logger.warning("No hazard events in lake_records — all labels will be 0")
        df["hazard_within_24h"]    = 0
        df["upcoming_hazard_type"] = None
        return df

    events["collected_at"] = pd.to_datetime(events["collected_at"], utc=True, errors="coerce")

    labels_within = []
    labels_type   = []

    for _, row in df.iterrows():
        loc  = row["location_name"]
        t    = row["collected_at"]
        t_end = t + pd.Timedelta(hours=24)

        loc_events = events[events["location_name"] == loc]
        upcoming   = loc_events[
            (loc_events["collected_at"] > t) &
            (loc_events["collected_at"] <= t_end)
        ]

        if upcoming.empty:
            labels_within.append(0)
            labels_type.append(None)
        else:
            labels_within.append(1)
            labels_type.append(upcoming.iloc[0]["hazard_type"])

    df["hazard_within_24h"]    = labels_within
    df["upcoming_hazard_type"] = labels_type
    return df


def build_sensor_features(
    con: sqlite3.Connection,
    output_path: Path,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    format: str = "parquet",
) -> int:
    """Compute rolling sensor features and export as a training-ready file.

    Returns number of rows written.
    """
    clauses = ["1=1"]
    params: list = []
    if start_date:
        clauses.append("date_partition >= ?")
        params.append(start_date)
    if end_date:
        clauses.append("date_partition <= ?")
        params.append(end_date)

    df = pd.read_sql_query(
        f"SELECT * FROM weather_records WHERE {' AND '.join(clauses)} ORDER BY location_name, collected_at",
        con,
        params=tuple(params),
    )

    if df.empty:
        logger.warning("No weather records found — sensor export skipped")
        return 0

    df["collected_at"] = pd.to_datetime(df["collected_at"], utc=True, errors="coerce")
    df = df.dropna(subset=["collected_at"])

    # Compute rolling features per location
    location_frames = []
    for location, group in df.groupby("location_name"):
        enriched = _rolling_features(group)
        location_frames.append(enriched)
        logger.debug(f"  {location}: {len(enriched)} readings processed")

    if not location_frames:
        return 0

    result = pd.concat(location_frames, ignore_index=True)

    # Add supervised labels from text/social hazard events
    result = _add_event_labels(result, con)

    # Select final ML columns (drop raw_payload and raw id columns)
    keep_cols = [
        "location_name", "lat", "lon", "collected_at", "date_partition",
        "temperature_c", "temp_change_24h",
        "humidity_pct", "humidity_24h_max",
        "wind_speed_ms", "wind_dir_deg", "wind_24h_max",
        "rainfall_mm", "rainfall_3h_sum", "rainfall_24h_sum",
        "rainfall_72h_sum", "rainfall_24h_max",
        "pressure_hpa", "pressure_drop_3h",
        "consecutive_rain_readings",
        "month", "hour", "day_of_year",
        "hazard_within_24h", "upcoming_hazard_type",
    ]
    result = result[[c for c in keep_cols if c in result.columns]]

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if format == "parquet":
        # Convert datetime to string for clean Parquet serialization
        result["collected_at"] = result["collected_at"].astype(str)
        pq.write_table(pa.Table.from_pandas(result, preserve_index=False),
                       str(output_path), compression="snappy")
    else:
        result["collected_at"] = result["collected_at"].astype(str)
        result.to_csv(output_path, index=False)

    n_hazard = int(result["hazard_within_24h"].sum())
    logger.info(f"Sensor features → {output_path}  "
                f"({len(result)} rows, {n_hazard} hazard-precursor readings)")
    return len(result)
