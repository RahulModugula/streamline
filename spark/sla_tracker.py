"""
SLA tracker for the GitHub event pipeline.

Computes end-to-end latency for each event:
  event_time (created_at in GitHub payload)
  → _ingested_at (Kafka producer timestamp)
  → processing_time (when Spark processed the micro-batch)

Three SLA thresholds:
  GOOD   < 30s    — real-time tier
  OK     < 5min   — near-real-time tier
  LATE   >= 5min  — batch tier (investigate if > 1% of events)

Usage:
    from sla_tracker import SLATracker
    tracker = SLATracker(spark)
    sla_df = tracker.compute(micro_batch_df)
    summary = tracker.summarize(sla_df, batch_id=42)
"""

import logging
from dataclasses import dataclass
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

log = logging.getLogger("sla-tracker")

# SLA thresholds in seconds
GOOD_THRESHOLD_S = 30
OK_THRESHOLD_S = 300  # 5 minutes


@dataclass
class SLASummary:
    batch_id: int
    total: int
    good: int
    ok: int
    late: int
    p50_s: float
    p95_s: float
    p99_s: float

    @property
    def good_pct(self) -> float:
        return 100 * self.good / self.total if self.total else 0.0

    @property
    def late_pct(self) -> float:
        return 100 * self.late / self.total if self.total else 0.0


class SLATracker:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def compute(self, df: DataFrame) -> Optional[DataFrame]:
        """
        Adds latency columns to the DataFrame.

        Requires 'created_at' (ISO8601 string) and '_ingested_at' columns.
        Returns None if those columns are absent.
        """
        required = {"created_at", "_ingested_at"}
        if not required.issubset(set(df.columns)):
            log.warning(
                "SLA tracking skipped: missing columns %s",
                required - set(df.columns),
            )
            return None

        # Force UTC so to_timestamp("...'Z'") parses the strings as UTC rather
        # than the session-local timezone, which varies by deployment environment.
        self.spark.conf.set("spark.sql.session.timeZone", "UTC")
        ts_fmt = "yyyy-MM-dd'T'HH:mm:ss'Z'"
        return (
            df
            .withColumn("_event_ts", F.to_timestamp("created_at", ts_fmt))
            .withColumn("_ingest_ts", F.to_timestamp("_ingested_at", ts_fmt))
            .withColumn("_proc_ts", F.current_timestamp())
            .withColumn(
                "ingest_latency_s",
                (F.col("_ingest_ts").cast("long") - F.col("_event_ts").cast("long")).cast(DoubleType()),
            )
            .withColumn(
                "e2e_latency_s",
                (F.col("_proc_ts").cast("long") - F.col("_event_ts").cast("long")).cast(DoubleType()),
            )
            .withColumn(
                "sla_tier",
                F.when(F.col("e2e_latency_s") < GOOD_THRESHOLD_S, "GOOD")
                .when(F.col("e2e_latency_s") < OK_THRESHOLD_S, "OK")
                .otherwise("LATE"),
            )
        )

    def summarize(self, sla_df: DataFrame, batch_id: int) -> SLASummary:
        """Aggregate per-batch SLA metrics into a summary object."""
        from pyspark.sql.functions import percentile_approx

        agg = sla_df.agg(
            F.count("*").alias("total"),
            F.sum(F.when(F.col("sla_tier") == "GOOD", 1).otherwise(0)).alias("good"),
            F.sum(F.when(F.col("sla_tier") == "OK", 1).otherwise(0)).alias("ok"),
            F.sum(F.when(F.col("sla_tier") == "LATE", 1).otherwise(0)).alias("late"),
            percentile_approx("e2e_latency_s", 0.50).alias("p50"),
            percentile_approx("e2e_latency_s", 0.95).alias("p95"),
            percentile_approx("e2e_latency_s", 0.99).alias("p99"),
        ).collect()[0]

        summary = SLASummary(
            batch_id=batch_id,
            total=agg.total or 0,
            good=agg.good or 0,
            ok=agg.ok or 0,
            late=agg.late or 0,
            p50_s=float(agg.p50 or 0),
            p95_s=float(agg.p95 or 0),
            p99_s=float(agg.p99 or 0),
        )

        log.info(
            "batch=%d total=%d good=%.1f%% late=%.1f%% p50=%.1fs p95=%.1fs p99=%.1fs",
            summary.batch_id,
            summary.total,
            summary.good_pct,
            summary.late_pct,
            summary.p50_s,
            summary.p95_s,
            summary.p99_s,
        )

        if summary.late_pct > 1.0:
            log.warning(
                "SLA breach: %.1f%% of events in batch %d exceeded %ds threshold",
                summary.late_pct,
                batch_id,
                OK_THRESHOLD_S,
            )

        return summary
