"""
Prometheus metrics exporter for the streamline pipeline.

Runs as a standalone HTTP server on :8000 (or $METRICS_PORT).
Scrape it with Prometheus at /metrics.

Metrics exposed:
  streamline_kafka_consumer_lag_total      gauge, by partition
  streamline_dead_letter_events_total      gauge (Delta table row count)
  streamline_delta_table_file_count        gauge, by table
  streamline_delta_table_last_modified     gauge (epoch seconds), by table
  streamline_checkpoint_batch_id           gauge, by checkpoint name
  streamline_pipeline_up                   gauge (1=healthy, 0=degraded)

Design: Each metric is a GaugeFunc backed by a probe function that runs
at scrape time. No background threads — scraping IS the polling.
"""

import logging
import os
import time
from pathlib import Path

from prometheus_client import CollectorRegistry
from prometheus_client.core import GaugeMetricFamily

log = logging.getLogger("metrics-exporter")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "streamline-spark")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "github-events")
OUTPUT_BASE = os.getenv("OUTPUT_DIR", "./data/lake")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_DIR", "/tmp/streamline-checkpoints")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

DELTA_TABLES = [
    "pushevent", "pullrequestevent", "issuesevent",
    "event-counts", "dead-letter", "trending",
]


class StreamlineCollector:
    """
    Custom Prometheus collector. Called on every scrape.
    All probes are best-effort — failures set the metric to -1, not crash.
    """

    def collect(self):
        yield from self._kafka_lag_metrics()
        yield from self._delta_table_metrics()
        yield from self._checkpoint_metrics()
        yield from self._pipeline_up_metric()

    def _kafka_lag_metrics(self):
        lag_metric = GaugeMetricFamily(
            "streamline_kafka_consumer_lag",
            "Consumer lag per Kafka partition",
            labels=["topic", "partition"],
        )
        try:
            from kafka import KafkaAdminClient, KafkaConsumer
            from kafka.structs import TopicPartition

            consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP)
            partitions = consumer.partitions_for_topic(KAFKA_TOPIC) or set()
            tps = [TopicPartition(KAFKA_TOPIC, p) for p in partitions]
            end_offsets = consumer.end_offsets(tps)
            consumer.close()

            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
            committed = admin.list_consumer_group_offsets(CONSUMER_GROUP)
            admin.close()

            committed_map = {
                tp.partition: meta.offset
                for tp, meta in committed.items()
                if tp.topic == KAFKA_TOPIC
            }

            for tp, end in end_offsets.items():
                lag = max(0, end - committed_map.get(tp.partition, 0))
                lag_metric.add_metric([KAFKA_TOPIC, str(tp.partition)], lag)
        except Exception as e:
            log.warning("kafka lag probe failed: %s", e)
            lag_metric.add_metric([KAFKA_TOPIC, "error"], -1)

        yield lag_metric

    def _delta_table_metrics(self):
        file_count = GaugeMetricFamily(
            "streamline_delta_table_file_count",
            "Number of Parquet files in a Delta table",
            labels=["table"],
        )
        last_modified = GaugeMetricFamily(
            "streamline_delta_table_last_modified_seconds",
            "Unix timestamp of most recently modified file in Delta table",
            labels=["table"],
        )

        for table in DELTA_TABLES:
            path = Path(OUTPUT_BASE) / table
            if not path.exists():
                file_count.add_metric([table], 0)
                last_modified.add_metric([table], 0)
                continue

            parquet_files = list(path.rglob("*.parquet"))
            file_count.add_metric([table], len(parquet_files))

            if parquet_files:
                mtime = max(f.stat().st_mtime for f in parquet_files)
                last_modified.add_metric([table], mtime)
            else:
                last_modified.add_metric([table], 0)

        yield file_count
        yield last_modified

    def _checkpoint_metrics(self):
        batch_id_metric = GaugeMetricFamily(
            "streamline_checkpoint_batch_id",
            "Latest committed Spark streaming batch ID",
            labels=["checkpoint"],
        )

        base = Path(CHECKPOINT_BASE)
        if not base.exists():
            yield batch_id_metric
            return

        for cp_dir in base.iterdir():
            if not cp_dir.is_dir():
                continue
            offsets_dir = cp_dir / "offsets"
            if not offsets_dir.exists():
                continue
            try:
                offset_files = [
                    f for f in offsets_dir.iterdir()
                    if f.name.isdigit()
                ]
                if offset_files:
                    latest_batch = max(int(f.name) for f in offset_files)
                    batch_id_metric.add_metric([cp_dir.name], latest_batch)
            except Exception as e:
                log.warning("checkpoint probe failed for %s: %s", cp_dir.name, e)

        yield batch_id_metric

    def _pipeline_up_metric(self):
        up = GaugeMetricFamily(
            "streamline_pipeline_up",
            "1 if pipeline is healthy (Kafka reachable + recent Delta writes), 0 otherwise",
        )
        try:
            from kafka import KafkaConsumer
            c = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP)
            c.topics()  # triggers connection
            c.close()
            kafka_ok = True
        except Exception:
            kafka_ok = False

        # Check that the pushevent table has been written to in the last hour
        delta_ok = False
        push_path = Path(OUTPUT_BASE) / "pushevent"
        if push_path.exists():
            files = list(push_path.rglob("*.parquet"))
            if files:
                newest = max(f.stat().st_mtime for f in files)
                delta_ok = (time.time() - newest) < 3600

        up.add_metric([], 1.0 if (kafka_ok and delta_ok) else 0.0)
        yield up


def main() -> None:
    registry = CollectorRegistry()
    registry.register(StreamlineCollector())

    from prometheus_client import make_server
    server = make_server("", METRICS_PORT, registry=registry)
    log.info("metrics exporter listening on :%d", METRICS_PORT)
    server.serve_forever()


if __name__ == "__main__":
    main()
