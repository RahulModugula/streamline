"""
Query helpers for the streamline Delta Lake tables.

These functions demonstrate efficient query patterns against the partitioned
Delta tables. Key principles:

  1. Always filter on partition columns first (event_date, event_hour).
     Spark will prune partitions before scanning any Parquet files.

  2. Use Delta time-travel (VERSION AS OF / TIMESTAMP AS OF) to query
     historical snapshots — useful for debugging or reproducing analyses.

  3. Avoid SELECT * on wide tables — only read columns you need.
     Parquet's column storage means unused columns cost nothing to skip.

  4. Cache repeated reads with .cache() — a streaming Delta table is
     modified by the writer, so each .read creates a new snapshot.
     Cache if you'll query the same snapshot multiple times.
"""

import logging
import os
from datetime import date, timedelta
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger("streamline.query")

OUTPUT_BASE = os.getenv("OUTPUT_DIR", "./data/lake")


# ── Partition-pruned reads ────────────────────────────────────────────────────

def read_table(
    spark: SparkSession,
    table: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    hours: Optional[list[int]] = None,
    version: Optional[int] = None,
) -> DataFrame:
    """
    Read a Delta table with partition pruning and optional time-travel.

    Args:
        table:      Table name under OUTPUT_BASE (e.g. "pushevent")
        start_date: Inclusive lower bound on event_date (YYYY-MM-DD)
        end_date:   Inclusive upper bound on event_date (YYYY-MM-DD)
        hours:      List of hours to include (e.g. [9,10,11] for morning)
        version:    Delta version for time-travel (None = latest)
    """
    path = f"{OUTPUT_BASE}/{table}"
    reader = spark.read.format("delta")

    if version is not None:
        reader = reader.option("versionAsOf", version)

    df = reader.load(path)

    if start_date:
        df = df.filter(F.col("event_date") >= start_date)
    if end_date:
        df = df.filter(F.col("event_date") <= end_date)
    if hours:
        df = df.filter(F.col("event_hour").isin(hours))

    return df


# ── Common analytical queries ─────────────────────────────────────────────────

def top_active_repos(
    spark: SparkSession,
    start_date: str,
    end_date: str,
    n: int = 20,
) -> DataFrame:
    """
    Top N repos by push count in a date range.
    Partition-pruned: only scans the requested date range.
    """
    return (
        read_table(spark, "pushevent", start_date, end_date)
        .groupBy("repo.name")
        .agg(
            F.count("*").alias("push_count"),
            F.countDistinct("actor.login").alias("unique_contributors"),
            F.sum("parsed_payload.size").alias("total_commits"),
        )
        .orderBy(F.desc("push_count"))
        .limit(n)
    )


def pr_merge_rate(
    spark: SparkSession,
    start_date: str,
    end_date: str,
) -> DataFrame:
    """
    PR merge rate per repo: merged / opened, grouped by date.
    Useful for understanding repo velocity.
    """
    return (
        read_table(spark, "pullrequestevent", start_date, end_date)
        .groupBy("event_date", "repo.name")
        .agg(
            F.sum(F.when(F.col("parsed_payload.action") == "opened", 1).otherwise(0)).alias("opened"),
            F.sum(F.when(F.col("parsed_payload.action") == "closed", 1).otherwise(0)).alias("closed"),
            F.sum(F.when(
                (F.col("parsed_payload.action") == "closed") &
                (F.col("parsed_payload.pull_request.merged") == True), 1
            ).otherwise(0)).alias("merged"),
        )
        .withColumn(
            "merge_rate",
            F.when(F.col("closed") > 0, F.col("merged") / F.col("closed")).otherwise(None)
        )
        .orderBy("event_date", F.desc("opened"))
    )


def hourly_event_volume(
    spark: SparkSession,
    check_date: str,
) -> DataFrame:
    """
    Events per hour per type for a single day.
    Good for spotting gaps (streaming job downtime) or spikes.
    """
    tables = ["pushevent", "pullrequestevent", "issuesevent"]
    dfs = []
    for table in tables:
        df = (
            read_table(spark, table, check_date, check_date)
            .groupBy("event_hour")
            .agg(F.count("*").alias("event_count"))
            .withColumn("event_type", F.lit(table))
        )
        dfs.append(df)

    from functools import reduce
    return (
        reduce(DataFrame.unionByName, dfs)
        .orderBy("event_type", "event_hour")
    )


def repos_changed_between_versions(
    spark: SparkSession,
    table: str,
    old_version: int,
    new_version: int,
) -> DataFrame:
    """
    Delta time-travel: find repos that appear in new_version but not old_version.
    Useful for auditing what a backfill or reprocessing job added.
    """
    old_repos = (
        read_table(spark, table, version=old_version)
        .select(F.col("repo.name").alias("repo_name"))
        .distinct()
    )
    new_repos = (
        read_table(spark, table, version=new_version)
        .select(F.col("repo.name").alias("repo_name"))
        .distinct()
    )
    return new_repos.subtract(old_repos)


def contributor_activity(
    spark: SparkSession,
    login: str,
    start_date: str,
    end_date: str,
) -> DataFrame:
    """
    All activity for a specific GitHub user across push and PR events.
    Demonstrates cross-table queries with partition pruning.
    """
    pushes = (
        read_table(spark, "pushevent", start_date, end_date)
        .filter(F.col("actor.login") == login)
        .select(
            F.col("event_date"),
            F.lit("push").alias("action"),
            F.col("repo.name").alias("repo"),
            F.col("parsed_payload.size").alias("detail"),
        )
    )
    prs = (
        read_table(spark, "pullrequestevent", start_date, end_date)
        .filter(F.col("actor.login") == login)
        .select(
            F.col("event_date"),
            F.col("parsed_payload.action").alias("action"),
            F.col("repo.name").alias("repo"),
            F.col("parsed_payload.number").cast("string").alias("detail"),
        )
    )
    return pushes.unionByName(prs).orderBy("event_date", "action")
