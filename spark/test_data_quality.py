"""
Unit tests for data quality validation checks.
Runs in Spark local mode with temporary Delta tables.
"""

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from data_quality import (
    check_not_null,
    check_row_count,
    check_uniqueness,
    validate_table,
)


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-data-quality")
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
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("event_date", StringType(), False),
    StructField("event_hour", IntegerType(), True),
])


def test_null_rate_fails_when_above_threshold(spark):
    """Columns with >1% null rate should return a FAIL result."""
    rows = [("e1", "PushEvent", "2025-11-15", 10)] * 90 + \
           [(None, "PushEvent", "2025-11-15", 10)] * 10
    df = spark.createDataFrame(rows, schema=_schema)
    result = check_not_null(df, "id", total=100)
    assert result.status == "FAIL"
    assert "exceeds 1% threshold" in result.detail


def test_null_rate_passes_when_no_nulls(spark):
    """All non-null values should produce a PASS result."""
    rows = [("e1", "PushEvent", "2025-11-15", 10),
            ("e2", "PushEvent", "2025-11-15", 11)]
    df = spark.createDataFrame(rows, schema=_schema)
    result = check_not_null(df, "id", total=2)
    assert result.status == "PASS"


def test_uniqueness_detects_duplicates(spark):
    """Duplicate IDs should produce a FAIL result."""
    rows = [("e1", "PushEvent", "2025-11-15", 10),
            ("e1", "PushEvent", "2025-11-15", 11),
            ("e2", "PushEvent", "2025-11-15", 12)]
    df = spark.createDataFrame(rows, schema=_schema)
    result = check_uniqueness(df, "id", total=3)
    assert result.status == "FAIL"
    assert "1 duplicate" in result.detail


def test_row_count_warns_on_low_hours(spark):
    """Hours with fewer rows than the minimum should trigger a WARN."""
    # Hour 10 has 1 row (below threshold of 5), hour 11 has 5 rows
    rows = [("e1", "PushEvent", "2025-11-15", 10)] + \
           [(f"e{i}", "PushEvent", "2025-11-15", 11) for i in range(2, 7)]
    df = spark.createDataFrame(rows, schema=_schema)
    result = check_row_count(df, total=6, min_per_hour=5)
    assert result.status == "WARN"
    assert "hour=10" in result.detail


def test_validate_table_skips_when_table_missing(spark, tmp_path, monkeypatch):
    """When the Delta table path doesn't exist, validate_table returns SKIP."""
    # Point OUTPUT_BASE at a directory that has no tables
    monkeypatch.setattr("data_quality.OUTPUT_BASE", str(tmp_path / "nonexistent"))
    report = validate_table(spark, "pushevent", "2025-11-15")
    assert report.row_count == 0
    assert any(c.status == "SKIP" for c in report.checks)
    assert any("not found" in c.detail for c in report.checks)
