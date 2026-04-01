"""Unit tests for the late-data handler."""

import pytest
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test-late-data")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture(scope="module")
def schema():
    return StructType([
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("event_time", TimestampType(), True),
    ])


def test_on_time_events_pass_through(spark, schema):
    from late_data_handler import LateDataFilter

    now = datetime.now(timezone.utc)
    events = [
        ("e1", "PushEvent", now - timedelta(minutes=1)),
        ("e2", "PushEvent", now - timedelta(minutes=3)),
    ]
    df = spark.createDataFrame(events, schema=schema)

    lf = LateDataFilter(watermark_minutes=10)
    on_time, late = lf.split(df, event_time_col="event_time")

    assert on_time.count() == 2
    assert late.count() == 0


def test_late_events_are_separated(spark, schema):
    from late_data_handler import LateDataFilter

    now = datetime.now(timezone.utc)
    events = [
        ("e1", "PushEvent", now - timedelta(minutes=1)),   # on-time
        ("e2", "PushEvent", now - timedelta(minutes=30)),  # late
        ("e3", "PushEvent", now - timedelta(hours=2)),     # very late
    ]
    df = spark.createDataFrame(events, schema=schema)

    lf = LateDataFilter(watermark_minutes=10)
    on_time, late = lf.split(df, event_time_col="event_time")

    assert on_time.count() == 1
    assert late.count() == 2


def test_late_rows_have_reason_column(spark, schema):
    from late_data_handler import LateDataFilter

    now = datetime.now(timezone.utc)
    events = [("e1", "PushEvent", now - timedelta(minutes=30))]
    df = spark.createDataFrame(events, schema=schema)

    lf = LateDataFilter(watermark_minutes=10)
    _, late = lf.split(df, event_time_col="event_time")

    row = late.collect()[0]
    assert "_late_reason" in late.columns
    assert "min after watermark" in row._late_reason


def test_null_event_time_treated_as_on_time(spark, schema):
    """Null event_time rows pass through as on-time — null means unknown, not late."""
    from late_data_handler import LateDataFilter

    events = [("e1", "PushEvent", None)]
    df = spark.createDataFrame(events, schema=schema)

    lf = LateDataFilter(watermark_minutes=10)
    on_time, late = lf.split(df, event_time_col="event_time")

    assert on_time.count() == 1
    assert late.count() == 0
