"""
Idempotent Delta Lake MERGE sink.

Plain append (outputMode="append") is simple but not idempotent: if Spark
restarts mid-batch after writing but before committing the checkpoint offset,
it will replay and re-append the same rows — creating duplicates.

This module uses Delta's MERGE (upsert) operation instead:
  - If a row with the same (id, event_date) already exists → skip (no-op)
  - If it doesn't exist → insert

This makes each micro-batch idempotent. The tradeoff is higher write
latency (~2–3× vs append) and more I/O, so use this sink only for tables
where duplicates are unacceptable (e.g., PullRequestEvent — we want exactly
one row per PR action, not duplicates during recovery).

The append sink in streaming_job.py remains appropriate for high-volume
event types where occasional duplicates are tolerable (e.g., WatchEvent).

Usage:
    from merge_sink import IdempotentDeltaSink
    sink = IdempotentDeltaSink(spark, output_path, merge_keys=["id", "event_date"])
    sink.write_batch(df, batch_id)
"""

import logging
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger("streamline.merge_sink")


class IdempotentDeltaSink:
    """
    Writes micro-batches to Delta using MERGE to prevent duplicate rows
    on Spark restart / Kafka offset replay.
    """

    def __init__(
        self,
        spark: SparkSession,
        path: str,
        merge_keys: list[str],
        partition_cols: Optional[list[str]] = None,
    ):
        self._spark = spark
        self._path = path
        self._merge_keys = merge_keys
        self._partition_cols = partition_cols or ["event_date", "event_hour"]

    def _ensure_table_exists(self, df: DataFrame) -> None:
        """Create the Delta table from the first batch's schema if it doesn't exist."""
        if not DeltaTable.isDeltaTable(self._spark, self._path):
            log.info("creating delta table at %s", self._path)
            (
                df.limit(0)  # empty DataFrame, just for schema
                .write
                .format("delta")
                .partitionBy(*self._partition_cols)
                .save(self._path)
            )

    def write_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """
        foreachBatch callback. Merges the micro-batch into the Delta table.

        The MERGE condition matches on all merge_keys. Matched rows are
        skipped (whenNotMatchedInsertAll handles the insert-only pattern).
        We never UPDATE existing rows — if an event exists, it was already
        written correctly.
        """
        if batch_df.isEmpty():
            log.debug("batch %d is empty, skipping", batch_id)
            return

        # Cache the batch — MERGE reads it twice (match check + insert)
        batch_df = batch_df.cache()
        count = batch_df.count()
        log.info("batch %d: %d rows to merge into %s", batch_id, count, self._path)

        self._ensure_table_exists(batch_df)

        target = DeltaTable.forPath(self._spark, self._path)

        # Build merge condition from all merge keys: "target.id = source.id AND ..."
        merge_condition = " AND ".join(
            f"target.{k} = source.{k}" for k in self._merge_keys
        )

        (
            target.alias("target")
            .merge(batch_df.alias("source"), merge_condition)
            # Insert-only: skip rows that already exist (idempotent)
            .whenNotMatchedInsertAll()
            .execute()
        )

        batch_df.unpersist()
        log.info("batch %d: merge complete", batch_id)


def write_stream_with_merge(
    df: DataFrame,
    spark: SparkSession,
    output_path: str,
    checkpoint_path: str,
    merge_keys: list[str],
    partition_cols: Optional[list[str]] = None,
    trigger_interval: str = "30 seconds",
):
    """
    Convenience wrapper: creates an IdempotentDeltaSink and wires it into
    a foreachBatch streaming query.

    foreachBatch gives us full DataFrame API access per micro-batch,
    which is required to call DeltaTable.merge().
    """
    sink = IdempotentDeltaSink(spark, output_path, merge_keys, partition_cols)

    return (
        df
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .foreachBatch(sink.write_batch)
        .start()
    )
