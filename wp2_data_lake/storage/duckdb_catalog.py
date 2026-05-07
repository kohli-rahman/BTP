from pathlib import Path

import duckdb
import pandas as pd
from loguru import logger


class DuckDBCatalog:
    def __init__(self, lake_dir: Path):
        self.db_path = lake_dir / "catalog.duckdb"
        self.silver_dir = lake_dir / "silver"
        self.con = duckdb.connect(str(self.db_path))
        self.refresh()

    def refresh(self):
        """Re-register the lake view to pick up newly written partitions."""
        parquet_files = list(self.silver_dir.glob("**/*.parquet"))
        if not parquet_files:
            self.con.execute(
                "CREATE OR REPLACE VIEW lake AS "
                "SELECT NULL::VARCHAR lake_id WHERE FALSE"
            )
            logger.debug("Lake is empty — created stub view")
            return

        glob_path = str(self.silver_dir / "**" / "*.parquet").replace("\\", "/")
        self.con.execute(f"""
            CREATE OR REPLACE VIEW lake AS
            SELECT * FROM read_parquet('{glob_path}', hive_partitioning = true)
        """)
        logger.debug(f"Catalog refreshed — {len(parquet_files)} parquet file(s) registered")

    def query(self, sql: str) -> pd.DataFrame:
        return self.con.execute(sql).df()

    def stats(self) -> pd.DataFrame:
        return self.con.execute("""
            SELECT
                source_type,
                date_partition,
                COUNT(*)                                                          AS records,
                COUNT(DISTINCT hazard_type)                                       AS hazard_types,
                SUM(CASE WHEN hazard_severity = 'CRITICAL' THEN 1 ELSE 0 END)    AS critical,
                SUM(CASE WHEN hazard_severity = 'WARNING'  THEN 1 ELSE 0 END)    AS warning,
                COUNT(DISTINCT location_name)                                     AS locations
            FROM lake
            GROUP BY source_type, date_partition
            ORDER BY date_partition DESC, source_type
        """).df()

    def hazard_summary(self) -> pd.DataFrame:
        return self.con.execute("""
            SELECT
                hazard_type,
                hazard_severity,
                COUNT(*)              AS records,
                MIN(date_partition)   AS first_seen,
                MAX(date_partition)   AS last_seen
            FROM lake
            WHERE hazard_type IS NOT NULL
            GROUP BY hazard_type, hazard_severity
            ORDER BY records DESC
        """).df()

    def close(self):
        self.con.close()
