# streamline

A real-time event pipeline built on Kafka and Spark Structured Streaming. Ingests GitHub Archive data (public event stream), processes it with PySpark, and writes to a partitioned Delta Lake — demonstrating exactly-once semantics, schema evolution, and late-data handling at scale.

No AI. Real data engineering.

## Architecture

```
GitHub Archive (hourly .json.gz)
        │
        ▼
   Kafka Producer           ← idempotent delivery, snappy compression
        │
        ▼
  Kafka (github-events)     ← 4 partitions, partitioned by repo name
        │
        ▼
Spark Structured Streaming  ← watermark, exactly-once, typed schemas
        │
   ┌────┴──────────────────────────────────────┐
   ▼              ▼               ▼             ▼
PushEvent   PullRequestEvent  IssuesEvent   Dead Letter
   │              │               │             │
   └──────────────┴───────────────┴─────────────┘
                          │
                     Delta Lake
              (Parquet, partitioned by date/hour)
```

## Exactly-Once Semantics

This pipeline achieves end-to-end exactly-once through two complementary mechanisms:

1. **Producer side** — `enable.idempotence=True` in the Kafka producer assigns each message a sequence number. The broker deduplicates retries within a producer session, so network errors never cause duplicate messages.

2. **Consumer side** — Spark's checkpoint directory records committed Kafka offsets atomically with each micro-batch write. On restart, Spark resumes from the last committed offset. Delta Lake's write-ahead log makes each micro-batch write atomic — a partial write is never visible to readers.

## Schema Evolution

GitHub's event schema changes over time. We handle this with:

- **Explicit schemas** — defined in `spark/schema.py` rather than inferred at runtime. Inference is slow and breaks on schema drift.
- **Nullable new fields** — fields added in newer schema versions are `nullable=True`. Old events that don't have the field parse as `null` without error.
- **`mergeSchema=true`** on Delta writes — new fields are automatically added to the Delta table schema on first encounter, no `ALTER TABLE` required.
- **Dead-letter queue** — events that fail to parse or have unknown types are written to `data/lake/dead-letter/` for investigation rather than silently dropped.

## Late Data Handling

Events are watermarked at `created_at` with a 10-minute tolerance. Events arriving up to 10 minutes late are included in their correct time window. Events beyond the watermark are dropped — acceptable for analytics, and this bound keeps Spark's state store from growing unboundedly.

## Quick Start

### Prerequisites
- Docker + Docker Compose
- Python 3.11+
- Java 11+ (for Spark)

### Run locally

```bash
# Start Kafka + MinIO
make up

# In a separate terminal — produce one hour of events
make producer

# In a separate terminal — start the Spark streaming job
make spark-job
```

### Run tests (no cluster needed — uses Spark local mode)

```bash
cd tests && pip install -r requirements.txt && pytest -v
```

## Data Lake Layout

```
data/lake/
├── pushevent/
│   └── event_date=2025-10-14/
│       └── event_hour=12/
│           └── part-00000-*.parquet
├── pullrequestevent/
├── issuesevent/
├── dead-letter/
└── event-counts/      ← 5-minute tumbling window aggregates
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `github-events` | Topic to consume from |
| `CHECKPOINT_DIR` | `/tmp/streamline-checkpoints` | Spark checkpoint location |
| `OUTPUT_DIR` | `./data/lake` | Delta Lake output path |
| `TRIGGER_INTERVAL` | `30 seconds` | Micro-batch interval |

## Project Structure

```
streamline/
├── producer/
│   └── github_producer.py    idempotent Kafka producer, GH Archive fetcher
├── spark/
│   ├── schema.py             explicit PySpark schemas + schema versioning
│   └── streaming_job.py      Structured Streaming job, Delta Lake writes
├── tests/
│   └── test_schema.py        schema parsing + evolution tests (local mode)
├── docker-compose.yml        Kafka + Schema Registry + MinIO
└── Makefile
```
