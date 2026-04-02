"""
Unit tests for trending repos aggregation logic.
Runs in Spark local mode — tests the batch compute_trending function
with static DataFrames (no Kafka or streaming required).
"""

from datetime import datetime

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from trending import compute_trending


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-trending")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


_schema = StructType([
    StructField("type", StringType(), True),
    StructField("repo_name", StringType(), True),
    StructField("event_time", TimestampType(), True),
])


def test_compute_trending_returns_expected_columns(spark):
    """Output should include window_start, window_end, repo_name, event_count, and type counts."""
    rows = [
        ("PushEvent", "octocat/Hello-World", datetime(2025, 11, 15, 10, 0, 0)),
        ("PushEvent", "octocat/Hello-World", datetime(2025, 11, 15, 10, 1, 0)),
        ("WatchEvent", "octocat/Hello-World", datetime(2025, 11, 15, 10, 2, 0)),
    ]
    df = spark.createDataFrame(rows, schema=_schema)
    result = compute_trending(df)

    expected_cols = {"window_start", "window_end", "repo_name",
                     "event_count", "push_count", "pr_count", "watch_count",
                     "computed_at"}
    assert expected_cols.issubset(set(result.columns))


def test_empty_input_returns_empty_result(spark):
    """An empty DataFrame should produce an empty trending result."""
    df = spark.createDataFrame([], schema=_schema)
    result = compute_trending(df)
    assert result.count() == 0


def test_repos_with_more_events_rank_higher(spark):
    """A repo with more events should have a higher event_count than one with fewer."""
    rows = (
        [("PushEvent", "popular/repo", datetime(2025, 11, 15, 10, m, 0)) for m in range(10)] +
        [("PushEvent", "quiet/repo", datetime(2025, 11, 15, 10, 0, 0))]
    )
    df = spark.createDataFrame(rows, schema=_schema)
    result = compute_trending(df)

    # Collapse across windows to get total event counts per repo
    totals = (
        result
        .groupBy("repo_name")
        .agg(F.sum("event_count").alias("total"))
        .collect()
    )
    by_repo = {r.repo_name: r.total for r in totals}
    assert by_repo["popular/repo"] > by_repo["quiet/repo"]
