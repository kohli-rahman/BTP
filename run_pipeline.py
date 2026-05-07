"""
Full pipeline runner: WP1 (collect) → WP2 (ingest into lake).
Usage:
    python3 run_pipeline.py              # collect 7 days + ingest
    python3 run_pipeline.py --days 14   # 14-day lookback
    python3 run_pipeline.py --lake-only  # only re-ingest existing raw data
    python3 run_pipeline.py --collect-only
"""
import argparse
import sys
import time

from loguru import logger


def run_collection(days: int):
    from wp1_data_collection.pipeline.batch_pipeline import BatchPipeline
    logger.info(f"=== WP1: Data Collection (lookback={days}d) ===")
    pipeline = BatchPipeline(lookback_days=days)
    result = pipeline.run()
    print(pipeline.summary())
    return result


def run_lake(date_str: str = None):
    from wp2_data_lake.pipeline.lake_pipeline import LakePipeline
    logger.info("=== WP2: Data Lake Ingestion ===")
    pipeline = LakePipeline()
    result = pipeline.run(date_str=date_str)
    print("\n── Lake Stats ───────────────────────────────────")
    df = pipeline.stats()
    if not df.empty:
        print(df.to_string(index=False))
    print("\n── Hazard Summary ───────────────────────────────")
    hz = pipeline.hazard_summary()
    if not hz.empty:
        print(hz.to_string(index=False))
    pipeline.close()
    return result


def main():
    parser = argparse.ArgumentParser(description="WP1→WP2 Full Pipeline Runner")
    parser.add_argument("--days",         type=int, default=7, help="WP1 lookback days (default: 7)")
    parser.add_argument("--collect-only", action="store_true", help="Run WP1 only")
    parser.add_argument("--lake-only",    action="store_true", help="Run WP2 only")
    parser.add_argument("--date",         metavar="YYYY-MM-DD", help="Restrict WP2 ingestion to one date")
    args = parser.parse_args()

    t0 = time.monotonic()

    if not args.lake_only:
        run_collection(args.days)

    if not args.collect_only:
        run_lake(date_str=args.date)

    elapsed = round(time.monotonic() - t0, 1)
    logger.info(f"Full pipeline done in {elapsed}s")


if __name__ == "__main__":
    main()
