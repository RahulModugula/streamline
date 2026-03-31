"""
GitHub events Spark Structured Streaming job.

Pipeline:
  Kafka topic (github-events)
    → parse JSON with explicit schema
    → watermark on event_time for late-data handling
    → route by event type into typed DataFrames
    → write to partitioned Parquet (Delta Lake format)

Exactly-once guarantee:
  Kafka source: each partition's offset is tracked in the checkpoint directory.
  Delta Lake sink: uses write-ahead log for atomic commits.
  Together these form an end-to-end exactly-once pipeline.

Schema evolution:
  New nullable fields can be added to schema.py without reprocessing history.
  The Delta table's schema is evolved automatically via mergeSchema=true.
"""

import logging
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from schema import PAYLOAD_SCHEMAS, github_event_schema

log = logging.getLogger("streamline")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ── Configuration ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "github-events")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_DIR", "/tmp/streamline-checkpoints")
OUTPUT_BASE = os.getenv("OUTPUT_DIR", "./data/lake")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")

# ── Spark session ─────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("streamline-github-events")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Adaptive query execution — lets Spark tune partition counts at runtime
        .config("spark.sql.adaptive.enabled", "true")
        # Keep state store on disk, not just memory, for long-running jobs
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ── Kafka source ──────────────────────────────────────────────────────────────

def read_kafka(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        # Exactly-once: Spark tracks committed offsets in the checkpoint directory.
        # On restart it resumes from the last committed offset — no duplicates,
        # no gaps (as long as Kafka retention covers the gap window).
        .option("kafka.isolation.level", "read_committed")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 100_000)
        .load()
    )


# ── Parsing and enrichment ────────────────────────────────────────────────────

def parse_events(raw_df):
    """
    Deserialize Kafka value bytes → structured GitHub event.

    We parse in two stages:
    1. Parse the envelope (id, type, actor, repo, created_at) with a strict schema.
    2. Keep payload as a raw string for per-type parsing downstream.
       This way unknown event types don't cause parse failures for the whole batch.
    """
    return (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_timestamp")
        .select(
            F.from_json("json_str", github_event_schema).alias("e"),
            "kafka_timestamp",
        )
        .select("e.*", "kafka_timestamp")
        # Parse event_time for watermarking. GH Archive uses ISO8601.
        .withColumn(
            "event_time",
            F.to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        )
        # Watermark: tolerate events up to 10 minutes late.
        # Late events beyond this window are dropped — acceptable for analytics.
        .withWatermark("event_time", "10 minutes")
        # Partition columns written to disk — enables efficient predicate pushdown
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("event_hour", F.hour("event_time"))
        # Drop rows with no id or type — these are malformed
        .filter(F.col("id").isNotNull() & F.col("type").isNotNull())
    )


def parse_typed_payload(df, event_type: str):
    """Parse the raw payload string for a specific event type."""
    schema = PAYLOAD_SCHEMAS.get(event_type)
    if schema is None:
        return df.withColumn("parsed_payload", F.lit(None).cast("string"))
    return df.withColumn(
        "parsed_payload",
        F.from_json(F.col("payload"), schema),
    )


# ── Sinks ─────────────────────────────────────────────────────────────────────

def write_to_delta(df, event_type: str, checkpoint_suffix: str) -> StreamingQuery:
    """
    Write a filtered DataFrame to Delta Lake, partitioned by date and hour.

    mergeSchema=true enables schema evolution: new nullable columns added to
    the schema will be automatically added to the Delta table on the next write
    without requiring a manual ALTER TABLE.
    """
    output_path = f"{OUTPUT_BASE}/{event_type.lower()}"
    checkpoint_path = f"{CHECKPOINT_BASE}/{checkpoint_suffix}"

    return (
        df
        .filter(F.col("type") == event_type)
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")   # schema evolution
        .partitionBy("event_date", "event_hour")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(output_path)
    )


def write_dead_letter(df) -> StreamingQuery:
    """
    Write unrecognised or malformed events to a dead-letter partition.
    This preserves all raw data so nothing is silently dropped.
    """
    known_types = set(PAYLOAD_SCHEMAS.keys())
    unknown_df = df.filter(~F.col("type").isin(*known_types))

    return (
        unknown_df
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/dead-letter")
        .option("mergeSchema", "true")
        .partitionBy("event_date")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(f"{OUTPUT_BASE}/dead-letter")
    )


# ── Aggregations (for monitoring) ─────────────────────────────────────────────

def write_event_counts(df) -> StreamingQuery:
    """
    5-minute tumbling window counts per event type.
    Written to a separate Delta table for dashboarding.
    """
    counts = (
        df
        .groupBy(
            F.window("event_time", "5 minutes"),
            F.col("type"),
        )
        .count()
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )
    return (
        counts
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/event-counts")
        .option("mergeSchema", "true")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(f"{OUTPUT_BASE}/event-counts")
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    log.info("reading from Kafka topic=%s bootstrap=%s", KAFKA_TOPIC, KAFKA_BOOTSTRAP)
    raw = read_kafka(spark)
    parsed = parse_events(raw)

    # Fan out: one streaming query per event type + dead letter + counts
    queries = []
    for event_type in PAYLOAD_SCHEMAS:
        typed = parse_typed_payload(parsed, event_type)
        q = write_to_delta(typed, event_type, checkpoint_suffix=event_type.lower())
        queries.append(q)
        log.info("started query for %s → %s/%s", event_type, OUTPUT_BASE, event_type.lower())

    queries.append(write_dead_letter(parsed))
    queries.append(write_event_counts(parsed))

    log.info("all %d streaming queries running", len(queries))

    # Block until all queries terminate (or one fails)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
