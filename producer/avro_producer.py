"""
Avro-serialized GitHub Archive producer with Schema Registry.

Compared to the JSON producer:
  - ~40% smaller messages (Avro binary vs JSON text)
  - Schema enforced at write time — malformed events are rejected before Kafka
  - Schema evolution tracked centrally in Schema Registry
  - Consumers can always decode any message by fetching the schema ID embedded
    in each message's 5-byte wire format prefix

Schema compatibility mode: FORWARD — new schema versions may add optional
(default-value) fields, but cannot remove or rename existing fields.
This ensures old consumers can read new messages.

Usage:
    python avro_producer.py [--date YYYY-MM-DD] [--hour 0-23]
"""

import argparse
import gzip
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Iterator

import requests
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

log = logging.getLogger("avro-producer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "github-events-avro"
GHARCHIVE_URL = "https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"

ALLOWED_EVENT_TYPES = {
    "PushEvent", "PullRequestEvent", "IssuesEvent",
    "WatchEvent", "ForkEvent", "CreateEvent", "DeleteEvent",
}

# Avro schema for the GitHub event envelope.
# Using Avro unions (["null", "string"]) for nullable fields.
# Schema evolution rule: only add new fields with defaults — never remove.
GITHUB_EVENT_AVRO_SCHEMA = """
{
  "type": "record",
  "name": "GitHubEvent",
  "namespace": "io.streamline.github",
  "doc": "GitHub Archive event envelope. Schema version 2.",
  "fields": [
    {"name": "id",           "type": "string"},
    {"name": "type",         "type": "string"},
    {"name": "actor_login",  "type": ["null", "string"], "default": null},
    {"name": "actor_id",     "type": ["null", "long"],   "default": null},
    {"name": "repo_name",    "type": ["null", "string"], "default": null},
    {"name": "repo_id",      "type": ["null", "long"],   "default": null},
    {"name": "payload",      "type": ["null", "string"], "default": null},
    {"name": "public",       "type": ["null", "boolean"],"default": null},
    {"name": "created_at",   "type": "string"},
    {"name": "_ingested_at", "type": "string"},
    {"name": "_schema_version", "type": "int", "default": 2,
     "doc": "Bumped when schema evolves — allows consumers to detect version drift"}
  ]
}
"""


def flatten_event(event: dict) -> dict:
    """Flatten nested GitHub event into the Avro schema's flat structure."""
    actor = event.get("actor") or {}
    repo = event.get("repo") or {}
    return {
        "id": str(event.get("id", "")),
        "type": event.get("type", ""),
        "actor_login": actor.get("login"),
        "actor_id": actor.get("id"),
        "repo_name": repo.get("name"),
        "repo_id": repo.get("id"),
        "payload": json.dumps(event.get("payload")) if event.get("payload") else None,
        "public": event.get("public"),
        "created_at": event.get("created_at", ""),
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
        "_schema_version": 2,
    }


def build_producer():
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    avro_serializer = AvroSerializer(
        sr_client,
        GITHUB_EVENT_AVRO_SCHEMA,
        conf={"auto.register.schemas": True},
    )
    string_serializer = StringSerializer("utf_8")

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "enable.idempotence": True})
    return producer, avro_serializer, string_serializer, sr_client


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


def produce_hour(producer, avro_serializer, string_serializer, year, month, day, hour):
    sent = skipped = 0
    ctx = SerializationContext(TOPIC, MessageField.VALUE)

    for event in fetch_events(year, month, day, hour):
        if event.get("type") not in ALLOWED_EVENT_TYPES:
            skipped += 1
            continue

        flat = flatten_event(event)
        key = flat["repo_name"] or flat["id"]

        producer.produce(
            topic=TOPIC,
            key=string_serializer(key),
            value=avro_serializer(flat, ctx),
            on_delivery=lambda err, msg: log.error("delivery failed: %s", err) if err else None,
        )
        sent += 1
        if sent % 10_000 == 0:
            producer.poll(0)
            log.info("sent=%d skipped=%d", sent, skipped)

    producer.flush()
    log.info("done: sent=%d skipped=%d", sent, skipped)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="YYYY-MM-DD")
    parser.add_argument("--hour", type=int)
    args = parser.parse_args()

    producer, avro_serializer, string_serializer, _ = build_producer()

    if args.date:
        dt = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        dt = datetime.now(timezone.utc) - timedelta(hours=1)
    hour = args.hour if args.hour is not None else dt.hour

    produce_hour(producer, avro_serializer, string_serializer,
                 dt.year, dt.month, dt.day, hour)


if __name__ == "__main__":
    main()
