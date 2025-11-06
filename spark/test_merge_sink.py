"""
Unit tests for the idempotent MERGE sink.
Runs in Spark local mode with a temporary Delta table.
"""

import os
import tempfile

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from merge_sink import IdempotentDeltaSink


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-merge-sink")
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
    StructField("id", StringType(), False),
    StructField("event_date", StringType(), False),
    StructField("event_hour", IntegerType(), True),
    StructField("type", StringType(), True),
])


def test_first_batch_inserts_all_rows(spark, tmp_path):
    sink = IdempotentDeltaSink(
        spark, str(tmp_path), merge_keys=["id", "event_date"],
        partition_cols=["event_date"],
    )
    df = spark.createDataFrame(
        [("e1", "2025-10-14", 10, "PushEvent"),
         ("e2", "2025-10-14", 10, "PushEvent")],
        schema=_schema,
    )
    sink.write_batch(df, batch_id=0)

    result = spark.read.format("delta").load(str(tmp_path))
    assert result.count() == 2


def test_replayed_batch_does_not_duplicate(spark, tmp_path):
    sink = IdempotentDeltaSink(
        spark, str(tmp_path), merge_keys=["id", "event_date"],
        partition_cols=["event_date"],
    )
    df = spark.createDataFrame(
        [("e1", "2025-10-14", 10, "PushEvent")],
        schema=_schema,
    )
    sink.write_batch(df, batch_id=0)
    # Replay same batch (simulates Spark restart + Kafka offset rewind)
    sink.write_batch(df, batch_id=0)

    result = spark.read.format("delta").load(str(tmp_path))
    assert result.count() == 1, "replayed batch must not insert duplicates"


def test_new_rows_added_on_second_batch(spark, tmp_path):
    sink = IdempotentDeltaSink(
        spark, str(tmp_path), merge_keys=["id", "event_date"],
        partition_cols=["event_date"],
    )
    batch1 = spark.createDataFrame([("e1", "2025-10-14", 10, "PushEvent")], schema=_schema)
    batch2 = spark.createDataFrame([("e2", "2025-10-14", 11, "PREvent")], schema=_schema)
    sink.write_batch(batch1, 0)
    sink.write_batch(batch2, 1)

    result = spark.read.format("delta").load(str(tmp_path))
    assert result.count() == 2
