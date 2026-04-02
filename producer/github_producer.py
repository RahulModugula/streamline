"""
GitHub Archive producer.

Fetches hourly event dumps from https://data.gharchive.org/ and publishes
each event as an individual JSON message to Kafka. The producer is configured
for idempotent delivery — Kafka deduplicates retries within a session, giving
exactly-once at the produce side.

Usage:
    python github_producer.py [--date YYYY-MM-DD] [--hour 0-23]

If no arguments are given it processes the previous hour.
"""

import argparse
import gzip
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Iterator

import requests
from rate_limiter import RateLimiter
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("github-producer")

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "github-events"
GHARCHIVE_URL = "https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"

# Event types we care about — others are dropped at the producer to keep
# the topic lean. Filtering here is cheaper than in Spark.
ALLOWED_EVENT_TYPES = {
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "WatchEvent",
    "ForkEvent",
    "CreateEvent",
    "DeleteEvent",
}


def build_producer() -> KafkaProducer:
    """
    Build a Kafka producer with idempotent delivery enabled.

    enable.idempotence=True sets acks=all, retries=INT_MAX, and max_in_flight=5
    automatically, giving exactly-once semantics at the broker level.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        # Idempotent producer: Kafka assigns each message a sequence number.
        # The broker deduplicates retries, so network errors never produce duplicates.
        enable_idempotence=True,
        # Batch for throughput — GH Archive files are large
        batch_size=65536,
        linger_ms=20,
        compression_type="snappy",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=5,
        retry_backoff_ms=500,
    )


def fetch_events(year: int, month: int, day: int, hour: int) -> Iterator[dict]:
    """Stream events from a single GH Archive hourly dump."""
    url = GHARCHIVE_URL.format(year=year, month=month, day=day, hour=hour)
    log.info("fetching %s", url)

    resp = requests.get(url, stream=True, timeout=30)
    if resp.status_code == 404:
        log.warning("archive not found (too recent?): %s", url)
        return
    resp.raise_for_status()

    with gzip.GzipFile(fileobj=resp.raw) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                log.warning("malformed JSON line, skipping: %s", e)


def produce_hour(producer: KafkaProducer, year: int, month: int, day: int, hour: int, limiter: RateLimiter | None = None) -> int:
    """Publish one hour of events. Returns the count of messages sent."""
    sent = skipped = errors = 0

    for event in fetch_events(year, month, day, hour):
        event_type = event.get("type", "")
        if event_type not in ALLOWED_EVENT_TYPES:
            skipped += 1
            continue

        # Partition by repo so all events for a repo land on the same partition.
        # This is important for stateful aggregations downstream.
        repo = event.get("repo", {}).get("name", "")
        key = repo or event.get("id", "")

        # Attach ingest metadata for Spark watermarking
        event["_ingested_at"] = datetime.now(timezone.utc).isoformat()

        try:
            if limiter is not None:
                limiter.acquire()
            future = producer.send(TOPIC, key=key, value=event)
            future.add_errback(lambda e: log.error("delivery failed: %s", e))
            sent += 1
        except KafkaError as e:
            log.error("send error: %s", e)
            errors += 1

        # Flush every 1000 messages to bound the internal send buffer.
        # Without this, a slow broker causes the producer to accumulate
        # gigabytes of buffered records in memory before the final flush().
        # Flushing periodically also surfaces delivery errors earlier so
        # we can log them before the entire hour is produced.
        if sent % 1_000 == 0 and sent > 0:
            producer.flush()
            log.info("sent=%d skipped=%d errors=%d (checkpoint)", sent, skipped, errors)
        elif sent % 10_000 == 0 and sent > 0:
            log.info("sent=%d skipped=%d errors=%d", sent, skipped, errors)

    producer.flush()  # final flush for the remaining partial batch
    log.info("done: sent=%d skipped=%d errors=%d", sent, skipped, errors)
    return sent


def main() -> None:
    parser = argparse.ArgumentParser(description="GitHub Archive Kafka producer")
    parser.add_argument("--date", help="YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--hour", type=int, help="0-23 (default: previous hour)")
    parser.add_argument("--rate-limit", type=int, default=0, help="Max messages/sec (0=unlimited)")
    parser.add_argument(
        "--backfill-days",
        type=int,
        default=0,
        help="If set, backfill this many past days",
    )
    args = parser.parse_args()

    producer = build_producer()
    limiter = RateLimiter(args.rate_limit) if args.rate_limit > 0 else None

    try:
        if args.backfill_days > 0:
            now = datetime.now(timezone.utc)
            for day_offset in range(args.backfill_days, 0, -1):
                target = now - timedelta(days=day_offset)
                for hour in range(24):
                    produce_hour(producer, target.year, target.month, target.day, hour, limiter)
                    time.sleep(0.5)  # be a polite citizen
        else:
            if args.date:
                dt = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            else:
                dt = datetime.now(timezone.utc) - timedelta(hours=1)

            hour = args.hour if args.hour is not None else dt.hour
            produce_hour(producer, dt.year, dt.month, dt.day, hour, limiter)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
