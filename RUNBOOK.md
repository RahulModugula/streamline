# Streamline Runbook

Operational procedures for the streamline GitHub events pipeline.

## Pipeline Components

| Component | Command | Port |
|---|---|---|
| Kafka | `docker-compose up -d kafka` | 9092 |
| Schema Registry | `docker-compose up -d schema-registry` | 8081 |
| MinIO | `docker-compose up -d minio` | 9000 / 9001 |
| Spark streaming job | `make spark-job` | — |
| Prometheus exporter | `python infra/metrics_exporter.py` | 8000 |

---

## Incident Playbook

### P1: Streaming job not writing (watermark stalled)

**Detect:**
```bash
python infra/watermark_monitor.py --crit-minutes 30
```

**Investigate:**
1. Check Kafka consumer lag: `python infra/lag_monitor.py --json`
2. Check if Spark job process is alive: `ps aux | grep spark`
3. Check checkpoint staleness: `python infra/checkpoint_manager.py list`

**Remediate:**
- If lag is high but job is alive: add more Spark executor cores or increase `SPARK_EXECUTOR_MEMORY`
- If job has crashed: restart with `make spark-job` — Spark resumes from checkpoint
- If checkpoint is corrupt: `python infra/checkpoint_manager.py reset --name <query> --confirm`

---

### P2: Dead letter queue growing

**Detect:**
```bash
python infra/reprocess_dead_letter.py --action stats
```

**Investigate:**
- New unknown event types → add to `ALLOWED_EVENT_TYPES` and `PAYLOAD_SCHEMAS`
- Schema drift → run `python infra/schema_drift_detector.py --all-types`
- Producer bug → check producer logs for serialization errors

**Remediate:**
1. Fix schema in `spark/schema.py`
2. Reprocess dead letter: `python infra/reprocess_dead_letter.py --action republish --dry-run`
3. If dry-run looks correct: `python infra/reprocess_dead_letter.py --action republish`

---

### P3: Duplicate rows detected in DQ check

**Detect:**
```bash
spark-submit spark/data_quality.py --all-tables --date YYYY-MM-DD
```

**Root cause:** Usually means the MERGE sink was bypassed (e.g., someone ran a backfill with plain `append` mode).

**Remediate:**
1. Identify duplicate event IDs
2. Run a targeted Delta DELETE to remove duplicates
3. Verify the MERGE sink is active in `streaming_job.py`

---

### P4: Schema drift detected

**Detect:**
```bash
python infra/schema_drift_detector.py --all-types
```

**Remediate:**
1. Review new fields — add nullable fields to `spark/schema.py`
2. Bump `SCHEMA_VERSION`
3. `mergeSchema=true` on Delta writes handles new columns automatically
4. Restart streaming job to pick up new schema

---

## Compaction Schedule

Run daily at 03:00 UTC after low-traffic window:

```bash
spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  spark/compaction.py --vacuum-hours 168
```

---

## Backfill Procedure

To reprocess a date range (e.g., after a bug fix):

```bash
spark-submit spark/backfill.py \
  --start-date 2025-11-01 \
  --end-date 2025-11-07 \
  --dry-run   # review first

# If dry-run looks correct:
spark-submit spark/backfill.py \
  --start-date 2025-11-01 \
  --end-date 2025-11-07
```

Safe to run alongside the streaming job — Delta ACID prevents conflicts on different partitions.

---

## Useful Queries

```python
from spark.query_helper import top_active_repos, pr_merge_rate, hourly_event_volume
```

Top repos by push count this week:
```python
top_active_repos(spark, "2025-11-10", "2025-11-16").show(20)
```

Find gaps in hourly volume (streaming job downtime):
```python
hourly_event_volume(spark, "2025-11-15").show(24)
```

What did a backfill add (Delta time-travel):
```python
repos_changed_between_versions(spark, "pushevent", old_version=42, new_version=55).show()
```
