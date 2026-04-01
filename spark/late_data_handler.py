"""
Late-data handler for Spark Structured Streaming.

GitHub's firehose delivers events roughly in order, but network retries
and upstream buffering mean events can arrive minutes or hours late.
Watermarks reject data older than the threshold — this module captures
those rejected rows and routes them to a separate late-data partition
for auditing and optional reprocessing.

Design:
  - Primary stream uses a 10-minute watermark
  - Events outside the watermark are written to a late-data partition
  - Late-data partition is queryable for SLA analysis
  - A TTL compaction job (see compaction.py) expires rows > 7 days old

Usage in foreachBatch:
    from late_data_handler import LateDataFilter
    lf = LateDataFilter(watermark_minutes=10)
    on_time, late = lf.split(micro_batch_df, event_time_col="event_time")
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

log = logging.getLogger("late-data-handler")


class LateDataFilter:
    """
    Splits a micro-batch into on-time and late rows.

    'Late' means the event_time is more than watermark_minutes before
    the current processing time (wall clock). In a real deployment
    you'd compare against the stream's watermark value, but foreachBatch
    doesn't expose the watermark directly — wall clock is the practical
    proxy and errs on the side of keeping more data.
    """

    def __init__(self, watermark_minutes: int = 10):
        self.watermark_minutes = watermark_minutes

    def split(self, df: DataFrame, event_time_col: str = "event_time") -> Tuple[DataFrame, DataFrame]:
        """
        Returns (on_time_df, late_df).
        late_df has an extra '_late_reason' column describing how far past the watermark.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=self.watermark_minutes)
        # Compare epoch seconds (long) to avoid session-timezone ambiguity.
        # TimestampType columns cast to "long" always yield UTC epoch seconds,
        # and int(datetime.timestamp()) with a UTC-aware datetime is also UTC epoch.
        cutoff_epoch = int(cutoff.timestamp())

        late_condition = (
            F.col(event_time_col).isNotNull() &
            (F.col(event_time_col).cast("long") < cutoff_epoch)
        )

        on_time = df.filter(~late_condition)
        late = df.filter(late_condition).withColumn(
            "_late_reason",
            F.concat_ws(
                "",
                F.lit("arrived "),
                F.round(
                    (F.lit(cutoff_epoch) - F.col(event_time_col).cast("long")) / 60, 1
                ).cast("string"),
                F.lit("min after watermark"),
            ),
        )

        late_count = late.count()
        if late_count > 0:
            log.warning(
                "late data: %d rows older than %d-minute watermark",
                late_count,
                self.watermark_minutes,
            )

        return on_time, late

    def write_late_data(self, late_df: DataFrame, output_path: str, batch_id: int) -> None:
        """Append late rows to a separate late-data partition."""
        if late_df.rdd.isEmpty():
            return

        (
            late_df
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_written_at", F.current_timestamp())
            .write
            .mode("append")
            .partitionBy("type")
            .parquet(f"{output_path}/late-data")
        )

        log.info("wrote %d late rows to %s/late-data", late_df.count(), output_path)
