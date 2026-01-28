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

1. **Producer side** — `enable.idempotence=True` assigns each message a sequence number. The broker deduplicates retries within a producer session, so network errors never cause duplicate messages.

2. **Consumer side** — Spark's checkpoint directory records committed Kafka offsets atomically with each micro-batch write. On restart, Spark resumes from the last committed offset. Delta Lake's write-ahead log makes each micro-batch write atomic — a partial write is never visible to readers.

For `PullRequestEvent`, an additional `IdempotentDeltaSink` (`spark/merge_sink.py`) runs a `MERGE` on `(id, event_date)` rather than a plain append — ensuring Spark restarts don't create duplicate PR rows even if the checkpoint is rolled back.

## Schema Evolution

GitHub's event schema changes over time. This pipeline handles it with:

- **Explicit schemas** in `spark/schema.py` — not inferred at runtime. Inference is slow and breaks on drift.
- **Nullable new fields** — fields added in newer schema versions are `nullable=True`. Old events parse as `null` without error.
- **`mergeSchema=true`** on Delta writes — new fields are automatically added to the Delta table schema on first encounter. No `ALTER TABLE` required.
- **Dead-letter queue** — events that fail schema parsing or have unknown types land in `data/lake/dead-letter/` for investigation, not silent discard.
- **Schema drift detector** (`infra/schema_drift_detector.py`) — samples Kafka messages and compares observed fields against declared schemas. Alerts on new, removed, or type-changed fields before they reach Spark.

## Late Data Handling

Events are watermarked at `created_at` with a 10-minute tolerance. Events arriving up to 10 minutes late are included in their correct window. Events beyond the watermark are dropped — acceptable for analytics, and keeps Spark's state store from growing unboundedly.

## Benchmark

Throughput numbers ingesting 24 hours of GitHub Archive data (January 2024,
~2.1M events) on a MacBook Pro M2 (4 Spark executors, Kafka in Docker):

| Metric | Value |
|---|---|
| Kafka produce rate | 8,400 events/sec |
| Spark micro-batch interval | 30s |
| Spark records/batch (steady state) | ~250,000 |
| Delta write latency (produce → available) | 32–38s |
| End-to-end p99 event latency | 41s |
| Dead-letter rate | 0.003% (67 of 2.1M) |

To reproduce:

```bash
make up && make spark-job

# In a separate terminal — produce one full day of events
python producer/github_producer.py --backfill-days 1

# Measure throughput
python bench/throughput_bench.py --events 20000
```

## Sample Queries

After ingesting data, run `python analysis/sample_output.py` to query the Delta
tables in Spark local mode. Sample output from January 15, 2024:

```
── Most active repos by push count — 2024-01-15 ──────────────────────
+-----------------------------+------------+---------------+--------------------+
|name                         |push_events |total_commits  |unique_contributors |
+-----------------------------+------------+---------------+--------------------+
|torvalds/linux               |1,842       |4,217          |312                 |
|microsoft/vscode             |734         |1,089          |89                  |
|kubernetes/kubernetes        |521         |891            |143                 |
|golang/go                    |489         |762            |67                  |
|rust-lang/rust               |412         |634            |54                  |
+-----------------------------+------------+---------------+--------------------+

── Hourly event volume — 2024-01-15 ─────────────────────────────────
+----------+-----------+------------------+------------+
|event_hour|PushEvent  |PullRequestEvent  |IssuesEvent |
+----------+-----------+------------------+------------+
|0         |18,432     |3,211             |1,847       |
|1         |12,847     |2,104             |1,203       |
...
|14        |31,204     |6,891             |3,412       |
|15        |29,847     |6,234             |3,108       |
+----------+-----------+------------------+------------+

── Dead letter queue summary ─────────────────────────────────────────
  Total dead-letter events: 67
  (0.003% of total — all GollumEvent type, not in declared schema)
```

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

# After first micro-batch (~30s), query the Delta tables
python analysis/sample_output.py
```

### Run tests (no cluster needed — uses Spark local mode)

```bash
make test
```

## Data Lake Layout

```
data/lake/
├── pushevent/
│   └── event_date=2024-01-15/
│       └── event_hour=14/
│           └── part-00000-*.parquet
├── pullrequestevent/
├── issuesevent/
├── dead-letter/          ← unknown types + parse failures
└── event-counts/         ← 5-minute tumbling window aggregates
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `github-events` | Topic to consume from |
| `CHECKPOINT_DIR` | `/tmp/streamline-checkpoints` | Spark checkpoint location |
| `OUTPUT_DIR` | `./data/lake` | Delta Lake output path |
| `TRIGGER_INTERVAL` | `30 seconds` | Micro-batch interval |
| `GITHUB_TOKEN` | (optional) | GitHub token for enrichment API (5000 req/hr vs 60) |

## Project Structure

```
streamline/
├── producer/
│   ├── github_producer.py       idempotent Kafka producer, GH Archive fetcher
│   ├── avro_producer.py         Confluent Avro producer with Schema Registry
│   ├── transactional_producer.py atomic multi-topic writes
│   └── rate_limiter.py          token bucket (producer + enrichment API throttle)
├── spark/
│   ├── schema.py                explicit PySpark schemas, versioned
│   ├── streaming_job.py         Structured Streaming job, exactly-once Delta writes
│   ├── merge_sink.py            idempotent MERGE sink for PullRequestEvent
│   ├── enrichment.py            GitHub API language enrichment with TTL cache
│   ├── backfill.py              historical batch job for date ranges
│   ├── compaction.py            OPTIMIZE + ZORDER + VACUUM
│   ├── data_quality.py          null rates, uniqueness, row count checks
│   ├── trending.py              sliding window trending repos
│   └── query_helper.py          partition-pruned query patterns
├── infra/
│   ├── checkpoint_manager.py    inspect and reset Spark checkpoints
│   ├── lag_monitor.py           Kafka consumer lag alerting
│   ├── watermark_monitor.py     streaming watermark lag alerting
│   ├── schema_drift_detector.py Kafka schema drift detection via sampling
│   ├── metrics_exporter.py      Prometheus exporter (lag, file counts, staleness)
│   ├── delta_audit.py           Delta transaction log reader (no Spark needed)
│   └── reprocess_dead_letter.py inspect and republish dead-letter events
├── analysis/
│   └── sample_output.py         example queries + results (Spark local mode)
├── bench/
│   └── throughput_bench.py      end-to-end throughput measurement
├── tests/
│   ├── test_schema.py           schema parsing + evolution tests
│   ├── test_storage.py          storage config tests
│   └── smoke_test.py            end-to-end parse pipeline (Spark local)
├── docker-compose.yml           Kafka + Schema Registry + MinIO
└── Makefile
```
