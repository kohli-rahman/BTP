"""
WP1 Data Collection Pipeline — Entry Point
IIT Patna | DST Multi-Hazard Early Warning System

Usage:
    python -m wp1_data_collection.main batch   [--days N]
    python -m wp1_data_collection.main stream  [--no-twitter] [--no-reddit] [--no-weather]
    python -m wp1_data_collection.main collect <source>  (twitter|reddit|news|rss|weather|media)
    python -m wp1_data_collection.main stats
"""

import argparse
import sys

from wp1_data_collection.pipeline.batch_pipeline import BatchPipeline
from wp1_data_collection.pipeline.stream_pipeline import StreamPipeline
from wp1_data_collection.storage.data_store import DataStore
from wp1_data_collection.utils.logger import logger


def cmd_batch(args):
    pipeline = BatchPipeline(lookback_days=args.days)
    pipeline.run()
    print(pipeline.summary())


def cmd_stream(args):
    pipeline = StreamPipeline()
    pipeline.run(
        twitter=not args.no_twitter,
        reddit=not args.no_reddit,
        weather=not args.no_weather,
    )


def cmd_collect(args):
    """Run a single collector by name."""
    from wp1_data_collection.collectors import (
        TwitterCollector, RedditCollector, NewsAPICollector,
        RSSCollector, WeatherCollector, MediaCollector,
    )
    collector_map = {
        "twitter": TwitterCollector,
        "reddit":  RedditCollector,
        "news":    NewsAPICollector,
        "rss":     RSSCollector,
        "weather": WeatherCollector,
        "media":   MediaCollector,
    }
    cls = collector_map.get(args.source)
    if cls is None:
        print(f"Unknown source '{args.source}'. Choose from: {list(collector_map)}")
        sys.exit(1)

    collector = cls()
    count = collector.run_batch()
    print(f"Collected {count} records from {args.source}.")


def cmd_stats(args):
    store = DataStore()
    stats = store.stats()
    print("\n=== WP1 Data Store Statistics ===")
    total = 0
    for src, dates in stats.items():
        src_total = sum(dates.values())
        total += src_total
        print(f"\n  [{src}]  ({src_total} total)")
        for date, count in sorted(dates.items()):
            print(f"    {date}: {count} records")
    print(f"\n  GRAND TOTAL: {total} records\n")


def main():
    parser = argparse.ArgumentParser(
        prog="wp1",
        description="WP1 Multi-Hazard Data Collection Pipeline — IIT Patna",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # batch
    p_batch = sub.add_parser("batch", help="Run full historical batch collection")
    p_batch.add_argument("--days", type=int, default=7, help="Lookback window in days (default: 7)")
    p_batch.set_defaults(func=cmd_batch)

    # stream
    p_stream = sub.add_parser("stream", help="Start real-time streaming pipeline")
    p_stream.add_argument("--no-twitter", action="store_true", help="Disable Twitter stream")
    p_stream.add_argument("--no-reddit",  action="store_true", help="Disable Reddit poll")
    p_stream.add_argument("--no-weather", action="store_true", help="Disable Weather poll")
    p_stream.set_defaults(func=cmd_stream)

    # collect (single source)
    p_collect = sub.add_parser("collect", help="Run a single collector")
    p_collect.add_argument("source", choices=["twitter", "reddit", "news", "rss", "weather", "media"])
    p_collect.set_defaults(func=cmd_collect)

    # stats
    p_stats = sub.add_parser("stats", help="Show data store statistics")
    p_stats.set_defaults(func=cmd_stats)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
