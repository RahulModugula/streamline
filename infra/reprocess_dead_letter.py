"""
Dead letter reprocessing tool.

Events that fail schema parsing or have unknown types are written to
data/lake/dead-letter/. This tool reads them, applies triage, and either:
  - Re-routes correctable events back into the main pipeline via Kafka
  - Writes permanently unrecoverable events to a separate archive

Use cases:
  - A new event type was added to ALLOWED_EVENT_TYPES — reprocess old unknowns
  - A schema bug was fixed — reparse previously malformed events
  - Audit: inspect what went to dead letter and why

Usage:
    python reprocess_dead_letter.py --action republish
    python reprocess_dead_letter.py --action stats
    python reprocess_dead_letter.py --type PushEvent --action republish
"""

import argparse
import json
import logging
import sys
from collections import Counter
from datetime import datetime, timezone
from typing import Optional

from kafka import KafkaProducer

log = logging.getLogger("dead-letter-reprocessor")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "github-events"
DEAD_LETTER_PATH = "./data/lake/dead-letter"

KNOWN_TYPES = {
    "PushEvent", "PullRequestEvent", "IssuesEvent",
    "WatchEvent", "ForkEvent", "CreateEvent", "DeleteEvent",
}


def read_dead_letter_events(path: str, event_type: Optional[str] = None):
    """
    Read parquet files from the dead-letter Delta table using pyarrow.
    Yields raw event dicts.
    """
    try:
        import pyarrow.parquet as pq
        import pyarrow.dataset as ds
    except ImportError:
        log.error("pyarrow required: pip install pyarrow")
        sys.exit(1)

    try:
        dataset = ds.dataset(path, format="parquet")
    except Exception as e:
        log.error("failed to open dead-letter table at %s: %s", path, e)
        return

    for batch in dataset.to_batches():
        for row in batch.to_pylist():
            if event_type and row.get("type") != event_type:
                continue
            yield row


def print_stats(path: str) -> None:
    """Print a breakdown of dead-letter events by type."""
    type_counts: Counter = Counter()
    date_counts: Counter = Counter()
    total = 0

    for event in read_dead_letter_events(path):
        event_type = event.get("type", "<null>")
        event_date = str(event.get("event_date", "<unknown>"))
        type_counts[event_type] += 1
        date_counts[event_date] += 1
        total += 1

    print(f"\nDead Letter Stats ({path})")
    print(f"{'─' * 50}")
    print(f"Total events: {total:,}")
    print(f"\nBy type:")
    for t, count in type_counts.most_common(20):
        status = "✓ known" if t in KNOWN_TYPES else "✗ unknown"
        print(f"  {t:<30} {count:>8,}  {status}")
    print(f"\nBy date (top 10):")
    for d, count in date_counts.most_common(10):
        print(f"  {d:<20} {count:>8,}")


def republish_events(
    path: str,
    event_type: Optional[str],
    dry_run: bool = False,
) -> None:
    """Re-publish dead-letter events to Kafka for reprocessing."""
    producer = None if dry_run else KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        enable_idempotence=True,
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: k.encode() if k else None,
    )

    sent = skipped = 0
    for event in read_dead_letter_events(path, event_type):
        t = event.get("type", "")
        if t not in KNOWN_TYPES:
            skipped += 1
            continue

        event["_reprocessed_at"] = datetime.now(timezone.utc).isoformat()
        key = (event.get("repo") or {}).get("name") or event.get("id", "")

        if dry_run:
            log.info("[dry-run] would republish: id=%s type=%s", event.get("id"), t)
        else:
            producer.send(TOPIC, key=key, value=event)
        sent += 1

        if sent % 1000 == 0:
            log.info("republished=%d skipped=%d", sent, skipped)

    if producer:
        producer.flush()
        producer.close()

    log.info("done: republished=%d skipped=%d dry_run=%s", sent, skipped, dry_run)


def main() -> None:
    parser = argparse.ArgumentParser(description="Dead letter reprocessing")
    parser.add_argument("--path", default=DEAD_LETTER_PATH)
    parser.add_argument("--action", choices=["stats", "republish"], required=True)
    parser.add_argument("--type", dest="event_type", help="Filter by event type")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.action == "stats":
        print_stats(args.path)
    elif args.action == "republish":
        republish_events(args.path, args.event_type, args.dry_run)


if __name__ == "__main__":
    main()
