import argparse
import sys
from pathlib import Path

from loguru import logger


def _cmd_ingest(args):
    from wp2_data_lake.pipeline.lake_pipeline import LakePipeline
    pipeline = LakePipeline()
    result = pipeline.run(date_str=args.date, source_type=args.source)
    pipeline.close()
    print(f"\nIngest complete:")
    for k, v in result.items():
        print(f"  {k:<25} {v}")


def _cmd_stats(args):
    from wp2_data_lake.pipeline.lake_pipeline import LakePipeline
    pipeline = LakePipeline()
    df = pipeline.stats()
    if df.empty:
        print("Lake is empty. Run: python -m wp2_data_lake.main ingest")
    else:
        print("\n── Lake Stats (Parquet/DuckDB) ───────────────────────────")
        print(df.to_string(index=False))
        print()
        print("── Hazard Summary ───────────────────────────────────────")
        print(pipeline.hazard_summary().to_string(index=False))
    pipeline.close()


def _cmd_query(args):
    from wp2_data_lake.pipeline.lake_pipeline import LakePipeline
    pipeline = LakePipeline()
    df = pipeline.query(args.sql)
    print(df.to_string(index=False))
    pipeline.close()


def _cmd_db_stats(args):
    from wp2_data_lake.pipeline.lake_pipeline import LakePipeline
    pipeline = LakePipeline()
    df = pipeline.db_stats()
    if df.empty:
        print("SQLite DB is empty. Run: python -m wp2_data_lake.main migrate")
    else:
        print("\n── SQLite Lake Stats ────────────────────────────────────")
        print(df.to_string(index=False))
        print()
        print("── Hazard Summary ───────────────────────────────────────")
        print(pipeline.sqlite.hazard_summary().to_string(index=False))
    pipeline.close()


def _cmd_db_query(args):
    from wp2_data_lake.pipeline.lake_pipeline import LakePipeline
    pipeline = LakePipeline()
    df = pipeline.db_query(args.sql)
    print(df.to_string(index=False))
    pipeline.close()


def _cmd_search(args):
    from wp2_data_lake.pipeline.lake_pipeline import LakePipeline
    pipeline = LakePipeline()
    df = pipeline.search_text(args.query, limit=args.limit)
    if df.empty:
        print("No results.")
    else:
        print(f"\n── {len(df)} result(s) for: {args.query!r} ─────────────────")
        cols = ["source_type", "hazard_type", "hazard_severity", "location_name", "text"]
        print(df[[c for c in cols if c in df.columns]].to_string(index=False))
    pipeline.close()


def _cmd_bbox(args):
    from wp2_data_lake.pipeline.lake_pipeline import LakePipeline
    pipeline = LakePipeline()
    df = pipeline.query_bbox(args.min_lat, args.max_lat, args.min_lon, args.max_lon)
    if df.empty:
        print("No records found in that bounding box.")
    else:
        print(f"\n── {len(df)} record(s) in bbox ─────────────────────────")
        cols = ["source_type", "location_name", "hazard_type", "hazard_severity", "lat", "lon"]
        print(df[[c for c in cols if c in df.columns]].to_string(index=False))
    pipeline.close()


def _cmd_export_ml(args):
    from wp2_data_lake.pipeline.lake_pipeline import LakePipeline
    pipeline = LakePipeline()
    hazard_types = [h.strip() for h in args.hazard.split(",")] if args.hazard else None
    n = pipeline.export_ml(
        output_path=Path(args.output),
        hazard_types=hazard_types,
        start_date=args.start,
        end_date=args.end,
        format=args.format,
        train_ratio=args.train_ratio,
        val_ratio=args.val_ratio,
    )
    if n:
        print(f"\nExported {n} labeled records → {args.output}")
    else:
        print("No labeled records available. Collect more data first.")
    pipeline.close()


def _cmd_migrate(args):
    from wp2_data_lake.storage.migrate import run_migration
    run_migration()


def _cmd_validate(args):
    from wp2_data_lake.pipeline.lake_pipeline import LakePipeline
    pipeline = LakePipeline()
    report = pipeline.sqlite.validate()

    t = report["totals"]
    dr = report["date_range"]

    print("\n── Data Quality Report ──────────────────────────────────────")
    print(f"  Total records       {t['total']}")
    print(f"  With text           {t['with_text']}  ({100*t['with_text']//max(t['total'],1)}%)")
    print(f"  With geo (lat/lon)  {t['with_geo']}  ({100*t['with_geo']//max(t['total'],1)}%)")
    print(f"  Labeled (hazard)    {t['labeled']}  ({100*t['labeled']//max(t['total'],1)}%)")
    print(f"  Labeled + text      {t['labeled_with_text']}  ← usable NLP training rows")

    print("\n── Class Distribution ───────────────────────────────────────")
    cd = report["class_distribution"]
    if cd.empty:
        print("  No labeled records yet.")
    else:
        print(cd.to_string(index=False))

    print("\n── Source Coverage ──────────────────────────────────────────")
    print(report["source_coverage"].to_string(index=False))

    print("\n── Date Range ───────────────────────────────────────────────")
    print(f"  First: {dr['first_date']}   Last: {dr['last_date']}")

    print("\n── Sensor Coverage ──────────────────────────────────────────")
    sc = report["sensor_coverage"]
    if sc.empty:
        print("  No weather records collected yet.")
    else:
        print(sc.to_string(index=False))

    warnings = report["low_sample_warnings"]
    if warnings:
        print("\n── Warnings ─────────────────────────────────────────────────")
        for w in warnings:
            print(f"  ⚠  {w} has < 20 labeled samples — unreliable for training")
    else:
        print("\n  All hazard classes have >= 20 samples.")

    pipeline.close()


def _cmd_export_sensors(args):
    from wp2_data_lake.pipeline.lake_pipeline import LakePipeline
    pipeline = LakePipeline()
    n = pipeline.sqlite.export_sensor_features(
        output_path=Path(args.output),
        start_date=args.start,
        end_date=args.end,
        format=args.format,
    )
    if n:
        print(f"\nExported {n} sensor feature rows → {args.output}")
        print("Columns include: rolling rainfall (3h/24h/72h), wind max, pressure drop,")
        print("                 temp change, consecutive rain readings,")
        print("                 hazard_within_24h (label), upcoming_hazard_type (label)")
    else:
        print("No weather records found. Run WP1 weather collection first.")
    pipeline.close()


def main():
    parser = argparse.ArgumentParser(
        prog="python -m wp2_data_lake.main",
        description="WP2 Data Lake — normalize, store, and export disaster data",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ── Existing commands ─────────────────────────────────────────────────────
    p_ingest = sub.add_parser("ingest", help="Ingest raw JSONL into Parquet + SQLite")
    p_ingest.add_argument("--date",   metavar="YYYY-MM-DD")
    p_ingest.add_argument("--source", metavar="SOURCE")

    sub.add_parser("stats",  help="Show Parquet/DuckDB record counts and hazard breakdown")

    p_query = sub.add_parser("query", help="Run SQL on Parquet lake (DuckDB) — table: 'lake'")
    p_query.add_argument("sql")

    # ── New SQLite commands ───────────────────────────────────────────────────
    sub.add_parser("db-stats", help="Show SQLite record counts and hazard breakdown")

    p_dbq = sub.add_parser("db-query", help="Run SQL on SQLite lake_records table")
    p_dbq.add_argument("sql")

    p_search = sub.add_parser("search", help="Full-text search on article/tweet text (FTS5)")
    p_search.add_argument("query", help="Search terms, e.g. 'flood Kerala warning'")
    p_search.add_argument("--limit", type=int, default=20, metavar="N")

    p_bbox = sub.add_parser("bbox", help="Spatial bounding-box query (lat/lon)")
    p_bbox.add_argument("--min-lat", type=float, required=True)
    p_bbox.add_argument("--max-lat", type=float, required=True)
    p_bbox.add_argument("--min-lon", type=float, required=True)
    p_bbox.add_argument("--max-lon", type=float, required=True)

    p_ml = sub.add_parser("export-ml", help="Export labeled ML training dataset (Parquet or CSV)")
    p_ml.add_argument("--output",      required=True,  metavar="PATH",
                      help="Output file path (e.g. data/ml/training.parquet)")
    p_ml.add_argument("--start",       metavar="YYYY-MM-DD", default=None)
    p_ml.add_argument("--end",         metavar="YYYY-MM-DD", default=None)
    p_ml.add_argument("--hazard",      metavar="H1,H2",      default=None,
                      help="Comma-separated hazard types to include (default: all)")
    p_ml.add_argument("--format",      choices=["parquet", "csv"], default="parquet")
    p_ml.add_argument("--train-ratio", type=float, default=0.8)
    p_ml.add_argument("--val-ratio",   type=float, default=0.1)

    sub.add_parser("migrate", help="Load existing JSONL + Parquet into SQLite (idempotent)")

    sub.add_parser("validate", help="Data quality + training-readiness report (class balance, coverage, warnings)")

    p_sensors = sub.add_parser("export-sensors", help="Export rolling sensor features for early-warning model")
    p_sensors.add_argument("--output", required=True, metavar="PATH",
                           help="Output file (e.g. data/ml/sensor_features.parquet)")
    p_sensors.add_argument("--start",  metavar="YYYY-MM-DD", default=None)
    p_sensors.add_argument("--end",    metavar="YYYY-MM-DD", default=None)
    p_sensors.add_argument("--format", choices=["parquet", "csv"], default="parquet")

    args = parser.parse_args()

    dispatch = {
        "ingest":    _cmd_ingest,
        "stats":     _cmd_stats,
        "query":     _cmd_query,
        "db-stats":  _cmd_db_stats,
        "db-query":  _cmd_db_query,
        "search":    _cmd_search,
        "bbox":      _cmd_bbox,
        "export-ml": _cmd_export_ml,
        "migrate":        _cmd_migrate,
        "validate":       _cmd_validate,
        "export-sensors": _cmd_export_sensors,
    }
    try:
        dispatch[args.command](args)
    except KeyboardInterrupt:
        logger.info("Interrupted")
        sys.exit(0)


if __name__ == "__main__":
    main()
