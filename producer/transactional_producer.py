"""
Transactional Kafka producer.

Idempotent delivery (enable.idempotence=True) prevents duplicates within
a single producer session. But if we write to multiple topics — e.g.,
github-events AND github-events-avro in the same logical operation — a
crash between the two writes produces a partial state: one topic has the
message, the other doesn't.

Kafka transactions solve this: all sends within a transaction are atomic.
read_committed consumers (like our Spark job) see either all or none.

Use this producer when:
  - Writing the same event to multiple topics atomically
  - Publishing events AND updating a control/audit topic in one operation
  - Exactly-once end-to-end is a hard requirement (not just best-effort)

Overhead: ~5-15ms per transaction commit. Acceptable for batch/backfill;
use the standard idempotent producer for low-latency streaming.

Usage:
    python transactional_producer.py [--date YYYY-MM-DD] [--hour 0-23]
"""

import argparse
import gzip
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Iterator

import requests
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

log = logging.getLogger("transactional-producer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

KAFKA_BOOTSTRAP = "localhost:9092"
PRIMARY_TOPIC = "github-events"
AUDIT_TOPIC = "github-events-audit"         # receives one summary message per batch
GHARCHIVE_URL = "https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"
TRANSACTION_ID = "streamline-producer-001"  # unique per producer instance
BATCH_SIZE = 500                             # events per transaction

ALLOWED_EVENT_TYPES = {
    "PushEvent", "PullRequestEvent", "IssuesEvent",
    "WatchEvent", "ForkEvent", "CreateEvent", "DeleteEvent",
}


def ensure_topics(bootstrap: str) -> None:
    """Create topics if they don't exist (idempotent)."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    topics = [
        NewTopic(PRIMARY_TOPIC, num_partitions=4, replication_factor=1),
        NewTopic(AUDIT_TOPIC, num_partitions=1, replication_factor=1),
    ]
    futures = admin.create_topics(topics)
    for topic, future in futures.items():
        try:
            future.result()
            log.info("created topic: %s", topic)
        except KafkaException as e:
            if "TOPIC_ALREADY_EXISTS" in str(e):
                pass
            else:
                raise


def build_producer(transactional_id: str) -> Producer:
    """
    Build a transactional producer.

    transactional.id must be unique per producer process. Kafka uses it to
    fence zombie producers: if a previous instance with the same ID is still
    alive, it will be fenced (its ongoing transaction aborted) when a new
    producer with the same ID initializes.
    """
    p = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "transactional.id": transactional_id,
        # Transactions require idempotence
        "enable.idempotence": True,
        # Higher linger for batching within transactions
        "linger.ms": 5,
        "compression.type": "snappy",
    })
    p.init_transactions()
    return p


def fetch_events(year: int, month: int, day: int, hour: int) -> Iterator[dict]:
    url = GHARCHIVE_URL.format(year=year, month=month, day=day, hour=hour)
    log.info("fetching %s", url)
    resp = requests.get(url, stream=True, timeout=30)
    if resp.status_code == 404:
        return
    resp.raise_for_status()
    with gzip.GzipFile(fileobj=resp.raw) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                pass


def produce_transactionally(
    producer: Producer,
    year: int, month: int, day: int, hour: int,
) -> int:
    """
    Publish events in transactional batches of BATCH_SIZE.

    Each transaction atomically writes BATCH_SIZE events to PRIMARY_TOPIC
    and one summary record to AUDIT_TOPIC. If the process crashes mid-batch,
    the incomplete transaction is aborted and consumers never see the partial
    batch.
    """
    total_sent = 0
    batch: list[dict] = []

    def flush_batch(batch: list[dict], batch_num: int) -> None:
        nonlocal total_sent
        if not batch:
            return

        producer.begin_transaction()
        try:
            for event in batch:
                key = (event.get("repo") or {}).get("name") or event.get("id", "")
                producer.produce(
                    PRIMARY_TOPIC,
                    key=key.encode() if key else None,
                    value=json.dumps(event).encode(),
                )

            # Atomically write audit record alongside the events
            audit_record = {
                "batch_num": batch_num,
                "event_count": len(batch),
                "event_types": list({e.get("type", "") for e in batch}),
                "source_hour": f"{year}-{month:02d}-{day:02d}T{hour:02d}",
                "produced_at": datetime.now(timezone.utc).isoformat(),
                "producer_id": TRANSACTION_ID,
            }
            producer.produce(
                AUDIT_TOPIC,
                value=json.dumps(audit_record).encode(),
            )

            producer.commit_transaction()
            total_sent += len(batch)
            log.debug("committed batch %d: %d events", batch_num, len(batch))

        except KafkaException as e:
            log.error("transaction failed, aborting batch %d: %s", batch_num, e)
            producer.abort_transaction()

    batch_num = 0
    for event in fetch_events(year, month, day, hour):
        if event.get("type") not in ALLOWED_EVENT_TYPES:
            continue

        event["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        batch.append(event)

        if len(batch) >= BATCH_SIZE:
            flush_batch(batch, batch_num)
            batch = []
            batch_num += 1

    flush_batch(batch, batch_num)  # flush remainder
    log.info("done: total_sent=%d batches=%d", total_sent, batch_num + 1)
    return total_sent


def main() -> None:
    parser = argparse.ArgumentParser(description="Transactional Kafka producer")
    parser.add_argument("--date", help="YYYY-MM-DD")
    parser.add_argument("--hour", type=int)
    parser.add_argument("--transaction-id", default=TRANSACTION_ID)
    args = parser.parse_args()

    ensure_topics(KAFKA_BOOTSTRAP)
    producer = build_producer(args.transaction_id)

    if args.date:
        dt = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        dt = datetime.now(timezone.utc) - timedelta(hours=1)
    hour = args.hour if args.hour is not None else dt.hour

    produce_transactionally(producer, dt.year, dt.month, dt.day, hour)


if __name__ == "__main__":
    main()
