"""
Delta Lake compaction job.

Streaming writes produce many small Parquet files (one per micro-batch per
partition). Over time this degrades read performance significantly. This job:

  1. Runs OPTIMIZE on each Delta table — coalesces small files into ~1GB targets.
  2. Applies ZORDER on high-cardinality columns used in WHERE filters, so
     Parquet column statistics are useful for data skipping.
  3. Runs VACUUM to delete files older than the retention window (default 7 days).

Run this as a daily batch job after the streaming job has been running.
A typical schedule: once per day during low-traffic hours.

Usage:
    spark-submit compaction.py [--table TABLE] [--vacuum-hours 168]
"""

import argparse
import logging
import os
from dataclasses import dataclass
from typing import Optional

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

log = logging.getLogger("streamline.compaction")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

OUTPUT_BASE = os.getenv("OUTPUT_DIR", "./data/lake")

# Per-table ZORDER configuration.
# ZORDER reorders data within files so that rows with similar values on the
# specified columns are physically co-located. This makes column statistics
# (min/max per file) much tighter, enabling aggressive data skipping.
TABLE_CONFIGS = {
    "pushevent": {
        "path": f"{OUTPUT_BASE}/pushevent",
        "zorder_cols": ["actor.login", "repo.name"],
    },
    "pullrequestevent": {
        "path": f"{OUTPUT_BASE}/pullrequestevent",
        "zorder_cols": ["actor.login", "repo.name"],
    },
    "issuesevent": {
        "path": f"{OUTPUT_BASE}/issuesevent",
        "zorder_cols": ["actor.login", "repo.name"],
    },
    "event-counts": {
        "path": f"{OUTPUT_BASE}/event-counts",
        "zorder_cols": ["type", "window_start"],
    },
}


@dataclass
class CompactionResult:
    table: str
    files_before: int
    files_after: int
    bytes_saved: int
    vacuumed: bool


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("streamline-compaction")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Compaction is CPU-bound — allow more parallelism
        .config("spark.sql.shuffle.partitions", "auto")
        .config("spark.sql.adaptive.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def get_file_count(spark: SparkSession, path: str) -> int:
    """Return the number of active data files in a Delta table."""
    try:
        detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0]
        return detail["numFiles"]
    except Exception:
        return -1


def optimize_table(spark: SparkSession, name: str, config: dict, vacuum_hours: int) -> CompactionResult:
    path = config["path"]
    zorder_cols = config.get("zorder_cols", [])

    log.info("optimizing table=%s path=%s", name, path)

    files_before = get_file_count(spark, path)

    # OPTIMIZE coalesces small files into ~1GB targets.
    # ZORDER BY physically clusters rows by the specified columns within each file,
    # tightening min/max statistics so Spark can skip irrelevant files on reads.
    zorder_clause = f"ZORDER BY ({', '.join(zorder_cols)})" if zorder_cols else ""
    try:
        spark.sql(f"OPTIMIZE delta.`{path}` {zorder_clause}")
        log.info("optimize complete: table=%s", name)
    except Exception as e:
        log.error("optimize failed: table=%s error=%s", name, e)
        return CompactionResult(name, files_before, files_before, 0, False)

    files_after = get_file_count(spark, path)

    # VACUUM deletes files that are no longer referenced by any Delta version
    # and are older than the retention window. Default 168h = 7 days.
    # WARNING: setting retention below 7 days breaks time-travel queries.
    vacuumed = False
    try:
        spark.sql(f"VACUUM delta.`{path}` RETAIN {vacuum_hours} HOURS")
        vacuumed = True
        log.info("vacuum complete: table=%s retained=%dh", name, vacuum_hours)
    except Exception as e:
        log.warning("vacuum failed (non-fatal): table=%s error=%s", name, e)

    bytes_saved = 0
    try:
        hist = (
            spark.sql(f"DESCRIBE HISTORY delta.`{path}`")
            .filter("operation = 'OPTIMIZE'")
            .orderBy("version", ascending=False)
            .limit(1)
            .collect()
        )
        if hist:
            stats = hist[0].operationMetrics or {}
            bytes_saved = int(stats.get("numRemovedBytes", 0)) - int(stats.get("numAddedBytes", 0))
    except Exception:
        pass

    return CompactionResult(name, files_before, files_after, bytes_saved, vacuumed)


def main() -> None:
    parser = argparse.ArgumentParser(description="Delta Lake compaction + VACUUM")
    parser.add_argument("--table", help="Compact only this table (default: all)")
    parser.add_argument(
        "--vacuum-hours",
        type=int,
        default=168,
        help="VACUUM retention in hours (default: 168 = 7 days)",
    )
    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    tables = TABLE_CONFIGS
    if args.table:
        if args.table not in TABLE_CONFIGS:
            log.error("unknown table %r. choices: %s", args.table, list(TABLE_CONFIGS))
            raise SystemExit(1)
        tables = {args.table: TABLE_CONFIGS[args.table]}

    results = []
    for name, config in tables.items():
        result = optimize_table(spark, name, config, args.vacuum_hours)
        results.append(result)
        log.info(
            "result: table=%s files_before=%d files_after=%d bytes_saved=%d vacuumed=%s",
            result.table, result.files_before, result.files_after,
            result.bytes_saved, result.vacuumed,
        )

    total_bytes = sum(r.bytes_saved for r in results)
    log.info("compaction complete: tables=%d total_bytes_saved=%d", len(results), total_bytes)


if __name__ == "__main__":
    main()
