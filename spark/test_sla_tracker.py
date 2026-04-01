"""Unit tests for SLA tracker."""
from datetime import datetime, timedelta, timezone
from functools import reduce

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test-sla-tracker")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _make_event(spark, created_offset_s: int, ingest_offset_s: int):
    """Create a single-row DataFrame with controlled timestamps."""
    now = datetime.now(timezone.utc)
    created = (now - timedelta(seconds=created_offset_s)).strftime("%Y-%m-%dT%H:%M:%SZ")
    ingested = (now - timedelta(seconds=ingest_offset_s)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return spark.createDataFrame([{
        "id": "e1", "type": "PushEvent",
        "created_at": created,
        "_ingested_at": ingested,
    }])


def test_good_event_classified_correctly(spark):
    from sla_tracker import GOOD_THRESHOLD_S, SLATracker

    tracker = SLATracker(spark)
    df = _make_event(spark, created_offset_s=10, ingest_offset_s=5)
    result = tracker.compute(df)
    row = result.collect()[0]
    assert row.sla_tier == "GOOD"
    assert 0 < row.e2e_latency_s < GOOD_THRESHOLD_S + 5  # allow small wall-clock drift


def test_late_event_classified_correctly(spark):
    from sla_tracker import OK_THRESHOLD_S, SLATracker

    tracker = SLATracker(spark)
    df = _make_event(spark, created_offset_s=600, ingest_offset_s=30)
    result = tracker.compute(df)
    row = result.collect()[0]
    assert row.sla_tier == "LATE"
    assert row.e2e_latency_s >= OK_THRESHOLD_S


def test_missing_columns_returns_none(spark):
    from sla_tracker import SLATracker

    tracker = SLATracker(spark)
    df = spark.createDataFrame([{"id": "e1", "type": "PushEvent"}])
    assert tracker.compute(df) is None


def test_summarize_counts_tiers(spark):
    from sla_tracker import SLATracker

    tracker = SLATracker(spark)
    events = [
        _make_event(spark, 10, 5),    # GOOD
        _make_event(spark, 10, 5),    # GOOD
        _make_event(spark, 600, 30),  # LATE
    ]
    combined = reduce(lambda a, b: a.union(b), [tracker.compute(e) for e in events])
    summary = tracker.summarize(combined, batch_id=1)
    assert summary.total == 3
    assert summary.good == 2
    assert summary.late == 1
