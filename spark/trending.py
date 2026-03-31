"""
Trending repositories — sliding window aggregation.

Computes the top repos by activity over a 1-hour sliding window that
advances every 5 minutes. Written to a separate Delta table so a
dashboard or API can query it without touching the raw event tables.

Why sliding vs tumbling?
  Tumbling windows (no overlap) reset every hour — a repo that goes viral
  at 11:55 only shows up for 5 minutes in the 11:00 window, then resets.
  A sliding window (1h window, 5m slide) gives it credit for a full hour.

Watermark: 10 minutes — consistent with the main streaming job.

Output schema:
  window_start  TIMESTAMP
  window_end    TIMESTAMP
  repo_name     STRING
  event_count   LONG
  push_count    LONG
  pr_count      LONG
  watch_count   LONG
  computed_at   TIMESTAMP   (when this row was written)
"""

import logging
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

log = logging.getLogger("streamline.trending")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "github-events")
OUTPUT_BASE = os.getenv("OUTPUT_DIR", "./data/lake")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_DIR", "/tmp/streamline-checkpoints")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "5 minutes")

# Sliding window parameters
WINDOW_DURATION = "1 hour"
SLIDE_DURATION = "5 minutes"
TOP_N = 100  # repos per window to materialise


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("streamline-trending")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        # Sliding windows produce large state — tune state store
        .config("spark.sql.streaming.statefulOperator.stateRebalancing.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_kafka(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("kafka.isolation.level", "read_committed")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(raw_df):
    """Parse just the fields needed for trending — keep this lightweight."""
    from pyspark.sql.types import StringType, StructField, StructType

    envelope_schema = StructType([
        StructField("type", StringType(), True),
        StructField("repo", StructType([
            StructField("name", StringType(), True),
        ]), True),
        StructField("created_at", StringType(), True),
    ])

    return (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json("json_str", envelope_schema).alias("e"))
        .select(
            "e.type",
            F.col("e.repo.name").alias("repo_name"),
            F.to_timestamp("e.created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_time"),
        )
        .filter(
            F.col("repo_name").isNotNull() &
            F.col("event_time").isNotNull() &
            F.col("type").isin("PushEvent", "PullRequestEvent", "WatchEvent", "ForkEvent")
        )
        .withWatermark("event_time", "10 minutes")
    )


def compute_trending(parsed_df):
    """
    Sliding window aggregation: total events + per-type breakdown per repo.
    Uses conditional aggregation (SUM of CASE WHEN) to avoid a pivot, which
    doesn't work in streaming mode.
    """
    return (
        parsed_df
        .groupBy(
            F.window("event_time", WINDOW_DURATION, SLIDE_DURATION),
            F.col("repo_name"),
        )
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.when(F.col("type") == "PushEvent", 1).otherwise(0)).alias("push_count"),
            F.sum(F.when(F.col("type") == "PullRequestEvent", 1).otherwise(0)).alias("pr_count"),
            F.sum(F.when(F.col("type") == "WatchEvent", 1).otherwise(0)).alias("watch_count"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("computed_at", F.current_timestamp())
        .drop("window")
    )


def write_trending(trending_df) :
    return (
        trending_df
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/trending")
        .option("mergeSchema", "true")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(f"{OUTPUT_BASE}/trending")
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = read_kafka(spark)
    parsed = parse_events(raw)
    trending = compute_trending(parsed)
    query = write_trending(trending)

    log.info(
        "trending query running: window=%s slide=%s output=%s/trending",
        WINDOW_DURATION, SLIDE_DURATION, OUTPUT_BASE,
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
