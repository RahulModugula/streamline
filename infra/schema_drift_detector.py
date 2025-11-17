"""
Schema drift detector.

Samples N recent messages from the Kafka topic, infers field presence and
types from the raw JSON, and diffs against the declared PySpark schemas in
spark/schema.py.

Why this matters: GitHub occasionally adds new fields to their event payloads
without announcement. Without detection, those fields silently land in the
dead-letter queue or get dropped. This tool runs as a daily cron job to catch
drift before it causes data loss.

Output:
  - NEW fields: candidates to add to schema.py (add as nullable)
  - REMOVED fields: possibly deprecated in GH API (safe to leave nullable)
  - TYPE MISMATCH: field exists in both but type disagrees (investigate)

Usage:
    python schema_drift_detector.py [--sample-size 500] [--event-type PushEvent]
    python schema_drift_detector.py --all-types
"""

import argparse
import json
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from kafka import KafkaConsumer
from kafka.structs import TopicPartition

log = logging.getLogger("schema-drift")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "github-events"
DEFAULT_SAMPLE = 500


# ── Declared schema fingerprints (flattened dot-notation) ─────────────────────
# These mirror spark/schema.py. Update when schema.py changes.

DECLARED_FIELDS = {
    "PushEvent": {
        "push_id": "long",
        "size": "int",
        "distinct_size": "int",
        "ref": "string",
        "head": "string",
        "before": "string",
        "commits": "array",
        "commits[].sha": "string",
        "commits[].message": "string",
        "commits[].distinct": "boolean",
        "commits[].author.name": "string",
        "commits[].author.email": "string",
    },
    "PullRequestEvent": {
        "action": "string",
        "number": "int",
        "pull_request.id": "long",
        "pull_request.number": "int",
        "pull_request.state": "string",
        "pull_request.title": "string",
        "pull_request.draft": "boolean",
        "pull_request.merged": "boolean",
        "pull_request.additions": "int",
        "pull_request.deletions": "int",
        "pull_request.changed_files": "int",
        "pull_request.review_comments": "int",
    },
    "IssuesEvent": {
        "action": "string",
        "issue.id": "long",
        "issue.number": "int",
        "issue.title": "string",
        "issue.state": "string",
        "issue.comments": "int",
    },
}


@dataclass
class DriftReport:
    event_type: str
    sample_count: int
    new_fields: list[str] = field(default_factory=list)
    removed_fields: list[str] = field(default_factory=list)
    type_mismatches: list[str] = field(default_factory=list)

    @property
    def has_drift(self) -> bool:
        return bool(self.new_fields or self.removed_fields or self.type_mismatches)

    def print(self) -> None:
        status = "⚠️  DRIFT DETECTED" if self.has_drift else "✓  no drift"
        print(f"\n[{self.event_type}] {status}  (sample={self.sample_count})")
        for f in self.new_fields:
            print(f"  + NEW    {f}  ← add to schema.py as nullable")
        for f in self.removed_fields:
            print(f"  - GONE   {f}  ← may be deprecated")
        for f in self.type_mismatches:
            print(f"  ≠ TYPE   {f}")


def _infer_type(v: Any) -> str:
    if isinstance(v, bool):
        return "boolean"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "double"
    if isinstance(v, list):
        return "array"
    if isinstance(v, dict):
        return "struct"
    return "string"


def _flatten(obj: Any, prefix: str = "") -> dict[str, str]:
    """Recursively flatten a dict into dot-notation field → inferred_type."""
    result = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            full_key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                result.update(_flatten(v, full_key))
            elif isinstance(v, list) and v and isinstance(v[0], dict):
                result[full_key] = "array"
                result.update(_flatten(v[0], f"{full_key}[]"))
            else:
                result[full_key] = _infer_type(v)
    return result


def sample_events(bootstrap: str, topic: str, n: int) -> list[dict]:
    """Read up to n messages from the tail of each partition."""
    consumer = KafkaConsumer(bootstrap_servers=bootstrap)
    partitions = consumer.partitions_for_topic(topic) or set()
    tps = [TopicPartition(topic, p) for p in partitions]

    end_offsets = consumer.end_offsets(tps)
    assignments = {}
    for tp in tps:
        end = end_offsets[tp]
        start = max(0, end - (n // len(tps)))
        assignments[tp] = start

    consumer.assign(tps)
    for tp, offset in assignments.items():
        consumer.seek(tp, offset)

    events = []
    consumer.poll(timeout_ms=1000)  # wake up
    for tp in tps:
        target = end_offsets[tp]
        while consumer.position(tp) < target and len(events) < n:
            records = consumer.poll(timeout_ms=500, max_records=100)
            for msgs in records.values():
                for msg in msgs:
                    try:
                        events.append(json.loads(msg.value.decode()))
                    except Exception:
                        pass

    consumer.close()
    return events


def detect_drift(events: list[dict], event_type: str) -> DriftReport:
    declared = DECLARED_FIELDS.get(event_type, {})
    filtered = [e for e in events if e.get("type") == event_type]

    if not filtered:
        log.warning("no %s events in sample", event_type)
        return DriftReport(event_type, 0)

    # Aggregate all observed fields across the sample
    observed: dict[str, set[str]] = defaultdict(set)
    for event in filtered:
        payload_str = event.get("payload", "{}")
        try:
            payload = json.loads(payload_str) if isinstance(payload_str, str) else payload_str
        except Exception:
            continue
        for field_path, field_type in _flatten(payload).items():
            observed[field_path].add(field_type)

    observed_fields = {k: list(v)[0] for k, v in observed.items()}

    report = DriftReport(event_type=event_type, sample_count=len(filtered))

    for f in observed_fields:
        if f not in declared:
            report.new_fields.append(f)

    for f in declared:
        if f not in observed_fields:
            report.removed_fields.append(f)

    for f in declared:
        if f in observed_fields and declared[f] != observed_fields[f]:
            report.type_mismatches.append(
                f"{f}: declared={declared[f]} observed={observed_fields[f]}"
            )

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka schema drift detector")
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE)
    parser.add_argument("--event-type", help="Check one event type")
    parser.add_argument("--all-types", action="store_true", help="Check all declared types")
    args = parser.parse_args()

    log.info("sampling %d messages from %s", args.sample_size, args.topic)
    try:
        events = sample_events(args.bootstrap, args.topic, args.sample_size)
    except Exception as e:
        log.error("failed to sample Kafka: %s", e)
        sys.exit(2)

    log.info("sampled %d events", len(events))

    types_to_check = list(DECLARED_FIELDS.keys()) if args.all_types else \
        [args.event_type] if args.event_type else list(DECLARED_FIELDS.keys())

    reports = [detect_drift(events, t) for t in types_to_check]
    for r in reports:
        r.print()

    if any(r.has_drift for r in reports):
        print("\nAction required: update spark/schema.py to match live data.")
        sys.exit(1)
    else:
        print("\nAll schemas match live data.")


if __name__ == "__main__":
    main()
