"""
Throughput benchmark for the Streamline pipeline.

Measures end-to-end latency from Kafka produce to Delta Lake availability
by producing a burst of synthetic events and polling the Delta table until
all records land.

Usage:
    # Start the stack first
    make up && make spark-job

    # Run with defaults (5000 events, 30s micro-batch interval)
    python bench/throughput_bench.py

    # Override
    python bench/throughput_bench.py --events 20000 --batch-interval 5
"""

import argparse
import json
import random
import statistics
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "github-events"

# Sample repo names to distribute events across partitions
REPOS = [
    "torvalds/linux",
    "microsoft/vscode",
    "vercel/next.js",
    "tensorflow/tensorflow",
    "golang/go",
    "rust-lang/rust",
    "facebook/react",
    "kubernetes/kubernetes",
]

ACTORS = ["alice", "bob", "carol", "dave", "eve", "frank", "grace", "heidi"]


def make_push_event(seq: int) -> dict:
    """Synthetic PushEvent matching the schema in spark/schema.py."""
    return {
        "id": str(uuid.uuid4()),
        "type": "PushEvent",
        "actor": {"id": seq % 1000, "login": random.choice(ACTORS), "display_login": ""},
        "repo": {"id": seq % 500, "name": random.choice(REPOS), "url": ""},
        "payload": json.dumps(
            {
                "push_id": seq,
                "size": random.randint(1, 20),
                "distinct_size": random.randint(1, 5),
                "commits": [
                    {
                        "sha": uuid.uuid4().hex[:40],
                        "message": f"bench commit {seq}",
                        "author": {"name": "bench", "email": "bench@example.com"},
                        "url": "",
                        "distinct": True,
                    }
                ],
            }
        ),
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def produce_events(n: int, producer: KafkaProducer) -> tuple[list[float], int]:
    """
    Produce n events to Kafka. Returns (produce_times_ms, error_count).
    Events are partitioned by repo name for locality (matches github_producer.py).
    """
    times = []
    errors = 0
    for i in range(n):
        event = make_push_event(i)
        key = event["repo"]["name"].encode()
        value = json.dumps(event).encode()
        t0 = time.monotonic()
        try:
            producer.send(KAFKA_TOPIC, key=key, value=value)
        except KafkaError:
            errors += 1
        times.append((time.monotonic() - t0) * 1000)
    producer.flush()
    return times, errors


def wait_for_delta(output_dir: str, expected_count: int, timeout_s: int) -> float | None:
    """
    Poll the Delta table until at least expected_count PushEvent rows appear.
    Returns elapsed seconds from first poll to completion, or None on timeout.

    Uses pyarrow instead of Spark to keep the benchmark lightweight.
    """
    import pathlib
    import pyarrow.dataset as ds

    parquet_path = pathlib.Path(output_dir) / "pushevent"
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            if parquet_path.exists():
                dataset = ds.dataset(str(parquet_path), format="parquet", partitioning="hive")
                count = dataset.count_rows()
                print(f"  Delta scan: {count}/{expected_count} rows available", end="\r")
                if count >= expected_count:
                    print()
                    return time.monotonic()
        except Exception:
            pass
        time.sleep(2)
    print()
    return None


def main():
    parser = argparse.ArgumentParser(description="Streamline throughput benchmark")
    parser.add_argument("--events", type=int, default=5_000, help="Events to produce")
    parser.add_argument("--batch-interval", type=int, default=30, help="Spark micro-batch interval (seconds)")
    parser.add_argument("--output-dir", default="./data/lake", help="Delta Lake output directory")
    parser.add_argument("--kafka-bootstrap", default=KAFKA_BOOTSTRAP)
    args = parser.parse_args()

    print(f"\n── Streamline Throughput Benchmark ────────────────────")
    print(f"  Events:          {args.events:,}")
    print(f"  Batch interval:  {args.batch_interval}s")
    print(f"  Output dir:      {args.output_dir}")
    print(f"───────────────────────────────────────────────────────\n")

    producer = KafkaProducer(
        bootstrap_servers=args.kafka_bootstrap,
        acks="all",
        enable_idempotence=True,
        compression_type="snappy",
    )

    # Phase 1: Measure Kafka produce throughput
    print(f"Producing {args.events:,} events to Kafka...")
    produce_start = time.monotonic()
    produce_times, errors = produce_events(args.events, producer)
    produce_elapsed = time.monotonic() - produce_start
    producer.close()

    produce_rate = args.events / produce_elapsed
    print(f"  Produce rate:  {produce_rate:,.0f} events/sec")
    print(f"  Produce p50:   {statistics.median(produce_times):.2f}ms")
    print(f"  Produce p99:   {statistics.quantiles(produce_times, n=100)[98]:.2f}ms")
    if errors:
        print(f"  Produce errors: {errors}")

    # Phase 2: Measure Delta Lake write latency
    # The first micro-batch after events land defines end-to-end latency.
    print(f"\nWaiting for events to appear in Delta Lake...")
    poll_start = time.monotonic()
    landed_at = wait_for_delta(args.output_dir, args.events, timeout_s=300)

    if landed_at is None:
        print("  TIMEOUT: events did not land in Delta within 5 minutes.")
        print("  Check that the Spark streaming job is running: make spark-job")
        return

    e2e_latency = landed_at - produce_start
    delta_lag = landed_at - poll_start

    print(f"\n── Results ─────────────────────────────────────────────")
    print(f"  Events produced:        {args.events - errors:,}")
    print(f"  Kafka throughput:       {produce_rate:,.0f} events/sec")
    print(f"  Produce latency p99:    {statistics.quantiles(produce_times, n=100)[98]:.1f}ms")
    print(f"  Delta write latency:    {delta_lag:.1f}s (time from produce-complete to Delta)")
    print(f"  End-to-end latency:     {e2e_latency:.1f}s (first event to Delta available)")
    print(f"  Micro-batch interval:   {args.batch_interval}s (expected upper bound on Delta lag)")
    print(f"────────────────────────────────────────────────────────\n")


if __name__ == "__main__":
    main()
