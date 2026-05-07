import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DIR  = BASE_DIR / "data" / "raw"
LAKE_DIR = BASE_DIR / "data" / "lake"

SQLITE_DB_PATH    = LAKE_DIR / "disaster_lake.db"
SQLITE_BATCH_SIZE = int(os.getenv("SQLITE_BATCH_SIZE", "500"))
