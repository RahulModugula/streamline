"""
Historical backfill job.

Reads GitHub Archive data directly from GH Archive URLs (or a local cache),
applies the same schema and transformations as the streaming job, and writes
to the Delta Lake tables using the same partition layout.

Use cases:
  - Reprocess after a bug fix in the streaming job logic
  - Backfill new event types added to ALLOWED_EVENT_TYPES
  - Recover from data loss in a time window

Key difference from streaming job: this is a bounded batch job. It reads a
date range, processes it fully, and exits. Delta's ACID guarantees mean it
can safely run alongside the streaming job on different date partitions.

Usage:
    spark-submit backfill.py --start-date 2025-10-01 --end-date 2025-10-07
"""

import argparse
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Iterator

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from schema import PAYLOAD_SCHEMAS, github_event_schema

log = logging.getLogger("streamline.backfill")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

GHARCHIVE_URL = "https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"
OUTPUT_BASE = os.getenv("OUTPUT_DIR", "./data/lake")


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("streamline-backfill")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        # Read JSON from HTTPS — needs hadoop-aws or just let Spark handle HTTP
        .config("spark.hadoop.fs.http.impl",
                "org.apache.hadoop.fs.http.HttpFileSystem")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def date_range(start: datetime, end: datetime) -> Iterator[datetime]:
    current = start
    while current <= end:
        yield current
        current += timedelta(hours=1)


def read_hour(spark: SparkSession, dt: datetime) -> DataFrame:
    """Read one hour of GH Archive data as a DataFrame."""
    url = GHARCHIVE_URL.format(
        year=dt.year, month=dt.month, day=dt.day, hour=dt.hour
    )
    log.info("reading %s", url)

    # Spark reads .json.gz directly — multiline=False because each line is one JSON object
    raw = (
        spark.read
        .option("mode", "PERMISSIVE")        # bad rows → null, not exception
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(github_event_schema)
        .json(url)
    )

    return (
        raw
        .withColumn("event_time", F.to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("event_hour", F.hour("event_time"))
        .withColumn("_ingested_at", F.lit(datetime.now(timezone.utc).isoformat()))
        .filter(F.col("id").isNotNull() & F.col("type").isNotNull())
    )


def write_batch(df: DataFrame, event_type: str) -> int:
    """Write a batch DataFrame to the Delta table. Returns rows written."""
    filtered = (
        df
        .filter(F.col("type") == event_type)
        .withColumn(
            "parsed_payload",
            F.from_json(F.col("payload"), PAYLOAD_SCHEMAS[event_type])
            if event_type in PAYLOAD_SCHEMAS else F.lit(None),
        )
    )

    count = filtered.count()
    if count == 0:
        return 0

    path = f"{OUTPUT_BASE}/{event_type.lower()}"
    (
        filtered
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("event_date", "event_hour")
        .save(path)
    )
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill GitHub Archive data")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--event-type", help="Only backfill this event type")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and count rows without writing")
    args = parser.parse_args()

    start = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(args.end_date, "%Y-%m-%d").replace(
        hour=23, tzinfo=timezone.utc
    )

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    event_types = [args.event_type] if args.event_type else list(PAYLOAD_SCHEMAS.keys())

    total_written = 0
    hours_processed = 0
    errors = 0

    for dt in date_range(start, end):
        try:
            df = read_hour(spark, dt)
            if args.dry_run:
                count = df.count()
                log.info("[dry-run] %s: %d events", dt.isoformat(), count)
                hours_processed += 1
                continue

            for event_type in event_types:
                written = write_batch(df, event_type)
                total_written += written
                if written:
                    log.info("%s %s: wrote %d rows", dt.isoformat(), event_type, written)

            hours_processed += 1
        except Exception as e:
            log.error("failed to process %s: %s", dt.isoformat(), e)
            errors += 1

    log.info(
        "backfill complete: hours_processed=%d total_written=%d errors=%d",
        hours_processed, total_written, errors,
    )
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
