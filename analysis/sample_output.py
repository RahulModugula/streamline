"""
Sample queries against the Delta Lake produced by Streamline.

Run after ingesting at least one hour of GitHub Archive data:

    make up
    make producer      # ingests one hour (~50k events)
    make spark-job     # wait for first micro-batch (~30s)
    python analysis/sample_output.py

All queries use partition pruning (event_date, event_hour filters) and
run in Spark local mode — no cluster required.
"""

import os
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "./data/lake")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("streamline-analysis")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.adaptive.enabled", "true")
        .master("local[*]")
        .getOrCreate()
    )


def section(title: str):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def most_active_repos(spark: SparkSession, event_date: str, top_n: int = 10):
    """
    Top repositories by push count for a given day.
    Shows total commits pushed and unique contributors.
    """
    section(f"Most active repos by push count — {event_date}")

    df = (
        spark.read.format("delta")
        .load(f"{OUTPUT_DIR}/pushevent")
        .filter(F.col("event_date") == event_date)
        .groupBy("repo.name")
        .agg(
            F.count("*").alias("push_events"),
            F.sum(F.size("payload.commits")).alias("total_commits"),
            F.countDistinct("actor.login").alias("unique_contributors"),
        )
        .orderBy(F.desc("push_events"))
        .limit(top_n)
    )

    df.show(truncate=False)
    return df


def pr_merge_rate(spark: SparkSession, event_date: str):
    """
    Pull request open vs merge rate per repo for a given day.
    Useful for measuring repo health and release velocity.
    """
    section(f"PR open / merge rate — {event_date}")

    df = (
        spark.read.format("delta")
        .load(f"{OUTPUT_DIR}/pullrequestevent")
        .filter(F.col("event_date") == event_date)
        .groupBy("repo.name", "payload.action")
        .count()
        .groupBy("name")
        .pivot("action", ["opened", "closed"])
        .agg(F.first("count"))
        .withColumnRenamed("opened", "prs_opened")
        .withColumnRenamed("closed", "prs_closed")
        .withColumn(
            "merge_rate",
            F.round(F.col("prs_closed") / F.col("prs_opened"), 2),
        )
        .orderBy(F.desc("prs_opened"))
        .limit(15)
    )

    df.show(truncate=False)
    return df


def hourly_event_volume(spark: SparkSession, event_date: str):
    """
    Event volume by type and hour. Use this to spot ingestion gaps or
    producer outages — a missing hour means the producer was down.
    """
    section(f"Hourly event volume — {event_date}")

    tables = {
        "PushEvent": "pushevent",
        "PullRequestEvent": "pullrequestevent",
        "IssuesEvent": "issuesevent",
    }

    dfs = []
    for event_type, table in tables.items():
        try:
            dfs.append(
                spark.read.format("delta")
                .load(f"{OUTPUT_DIR}/{table}")
                .filter(F.col("event_date") == event_date)
                .groupBy("event_hour")
                .agg(F.count("*").alias(event_type))
            )
        except Exception:
            pass  # table may not exist if no events of this type were ingested

    if not dfs:
        print("  No data found. Run make producer first.")
        return

    result = dfs[0]
    for df in dfs[1:]:
        result = result.join(df, "event_hour", "full_outer")

    result.orderBy("event_hour").show()


def dead_letter_summary(spark: SparkSession):
    """
    Events in the dead-letter queue — unknown types or parse failures.
    A healthy pipeline should have zero or near-zero dead letters.
    """
    section("Dead letter queue summary")

    try:
        df = spark.read.format("delta").load(f"{OUTPUT_DIR}/dead-letter")
        count = df.count()
        print(f"  Total dead-letter events: {count:,}")
        if count > 0:
            print("\n  Breakdown by event type:")
            df.groupBy("type").count().orderBy(F.desc("count")).show()
    except Exception:
        print("  Dead letter table not found (no failed events — good!)")


def schema_version_distribution(spark: SparkSession, event_date: str):
    """
    Checks if events with different schema versions are coexisting in the
    table. During a schema migration, you may see v2 and v3 events in the
    same partition — nullable fields handle this transparently.
    """
    section(f"Schema version distribution — {event_date}")

    try:
        df = (
            spark.read.format("delta")
            .load(f"{OUTPUT_DIR}/pullrequestevent")
            .filter(F.col("event_date") == event_date)
        )
        cols = df.columns

        # Detect schema version by checking for version-gated fields.
        # review_comments was added in v3; draft was added in v2.
        has_review_comments = "review_comments" in str(df.schema)
        has_draft = "draft" in str(df.schema)

        print(f"  Schema fields detected: {len(cols)}")
        print(f"  Contains draft field (v2+):           {has_draft}")
        print(f"  Contains review_comments field (v3+):  {has_review_comments}")

        # Show null rates for schema-evolved fields to confirm backward compat.
        if has_review_comments:
            null_rate = df.filter(F.col("payload.review_comments").isNull()).count() / max(df.count(), 1)
            print(f"  review_comments null rate (old events): {null_rate:.1%}")

    except Exception as e:
        print(f"  Could not read pullrequestevent: {e}")


def event_counts_aggregates(spark: SparkSession):
    """
    5-minute tumbling window aggregates written by the streaming job.
    Shows total event volume per window — useful for capacity planning.
    """
    section("5-minute event count aggregates (last 10 windows)")

    try:
        df = (
            spark.read.format("delta")
            .load(f"{OUTPUT_DIR}/event-counts")
            .orderBy(F.desc("window_start"))
            .limit(10)
            .select("window_start", "window_end", "event_type", "event_count")
        )
        df.show(truncate=False)
    except Exception:
        print("  event-counts table not found. Make sure the streaming job is running.")


def main():
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    event_date = os.environ.get("ANALYSIS_DATE", yesterday)

    print(f"\nStreamline — Sample Delta Lake Queries")
    print(f"Output dir:   {OUTPUT_DIR}")
    print(f"Analysis date: {event_date}")

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    most_active_repos(spark, event_date)
    pr_merge_rate(spark, event_date)
    hourly_event_volume(spark, event_date)
    dead_letter_summary(spark)
    schema_version_distribution(spark, event_date)
    event_counts_aggregates(spark)

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
