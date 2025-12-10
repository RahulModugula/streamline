"""
Kafka consumer lag monitor.

Polls consumer group offsets and topic end offsets to compute per-partition
lag. Logs a summary and exits non-zero if any partition exceeds the threshold,
making it suitable as a health check in CI or a cron-based alert.

Usage:
    python lag_monitor.py [--group GROUP] [--topic TOPIC] [--threshold N]

Exit codes:
    0 — all partitions within threshold
    1 — one or more partitions exceed threshold
    2 — Kafka connection error
"""

import argparse
import json
import logging
import sys
from dataclasses import dataclass, asdict
from typing import Dict, List

from kafka import KafkaAdminClient, KafkaConsumer
from kafka.errors import KafkaError
from kafka.structs import TopicPartition

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("lag-monitor")

KAFKA_BOOTSTRAP = "localhost:9092"
CONSUMER_GROUP = "streamline-spark"
TOPIC = "github-events"
DEFAULT_LAG_THRESHOLD = 100_000  # messages


@dataclass
class PartitionLag:
    partition: int
    latest_offset: int
    committed_offset: int
    lag: int

    @property
    def is_lagging(self) -> bool:
        return self.lag > 0

    def __str__(self) -> str:
        return (
            f"partition={self.partition} "
            f"latest={self.latest_offset} "
            f"committed={self.committed_offset} "
            f"lag={self.lag}"
        )


def get_end_offsets(bootstrap: str, topic: str) -> Dict[TopicPartition, int]:
    """Fetch the latest offset (log end offset) for every partition."""
    consumer = KafkaConsumer(bootstrap_servers=bootstrap)
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise RuntimeError(f"topic {topic!r} not found or has no partitions")
        tps = [TopicPartition(topic, p) for p in sorted(partitions)]
        return consumer.end_offsets(tps)
    finally:
        consumer.close()


def get_committed_offsets(bootstrap: str, group: str, topic: str) -> Dict[int, int]:
    """Fetch committed offsets for the consumer group."""
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        offsets = admin.list_consumer_group_offsets(group)
        return {
            tp.partition: meta.offset
            for tp, meta in offsets.items()
            if tp.topic == topic
        }
    finally:
        admin.close()


def compute_lag(bootstrap: str, group: str, topic: str) -> List[PartitionLag]:
    end_offsets = get_end_offsets(bootstrap, topic)
    committed = get_committed_offsets(bootstrap, group, topic)

    results = []
    for tp, end in sorted(end_offsets.items(), key=lambda x: x[0].partition):
        committed_offset = committed.get(tp.partition, 0)
        lag = max(0, end - committed_offset)
        results.append(PartitionLag(
            partition=tp.partition,
            latest_offset=end,
            committed_offset=committed_offset,
            lag=lag,
        ))
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka consumer lag monitor")
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    parser.add_argument("--group", default=CONSUMER_GROUP)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--threshold", type=int, default=DEFAULT_LAG_THRESHOLD,
                        help="Max acceptable lag per partition")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    try:
        lags = compute_lag(args.bootstrap, args.group, args.topic)
    except KafkaError as e:
        log.error("Kafka connection error: %s", e)
        sys.exit(2)
    except Exception as e:
        log.error("unexpected error: %s", e)
        sys.exit(2)

    total_lag = sum(p.lag for p in lags)
    max_lag = max((p.lag for p in lags), default=0)
    exceeding = [p for p in lags if p.lag > args.threshold]

    if args.json:
        print(json.dumps({
            "group": args.group,
            "topic": args.topic,
            "total_lag": total_lag,
            "max_lag": max_lag,
            "threshold": args.threshold,
            "partitions": [asdict(p) for p in lags],
            "status": "ok" if not exceeding else "lagging",
        }, indent=2))
    else:
        for p in lags:
            level = logging.WARNING if p.lag > args.threshold else logging.INFO
            log.log(level, "%s", p)
        log.info("total_lag=%d max_lag=%d threshold=%d", total_lag, max_lag, args.threshold)

    if exceeding:
        log.warning(
            "%d partition(s) exceed lag threshold of %d: %s",
            len(exceeding),
            args.threshold,
            [p.partition for p in exceeding],
        )
        sys.exit(1)

    log.info("all partitions within threshold — status=ok")


if __name__ == "__main__":
    main()
