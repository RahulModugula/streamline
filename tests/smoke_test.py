"""
End-to-end smoke test for the streamline pipeline.

Tests the full data path:
  synthetic events → JSON serialization → Spark parsing → schema validation → output shape

Does NOT require a running Kafka cluster or Spark cluster.
Runs in Spark local mode with synthetic data injected directly as DataFrames.

Run with: pytest tests/smoke_test.py -v
"""

import json
import sys
from datetime import datetime, timezone
from typing import List

import pytest

sys.path.insert(0, "/Users/rahul/Projects/streamline/spark")


@pytest.fixture(scope="module")
def spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("streamline-smoke-test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def make_push_event(repo: str = "octocat/Hello-World", n_commits: int = 3) -> dict:
    return {
        "id": f"push-{repo}-{n_commits}",
        "type": "PushEvent",
        "actor": {
            "id": 1, "login": "octocat", "display_login": "octocat",
            "gravatar_id": "", "url": "", "avatar_url": "",
        },
        "repo": {"id": 1001, "name": repo, "url": ""},
        "payload": json.dumps({
            "push_id": 12345, "size": n_commits, "distinct_size": n_commits,
            "ref": "refs/heads/main", "head": "abc123", "before": "def456",
            "commits": [
                {"sha": f"c{i}", "message": f"commit {i}", "distinct": True,
                 "url": "", "author": {"name": "Dev", "email": "dev@example.com"}}
                for i in range(n_commits)
            ],
        }),
        "public": True,
        "created_at": "2025-11-01T10:00:00Z",
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def make_pr_event(repo: str = "octocat/Hello-World", action: str = "opened") -> dict:
    return {
        "id": f"pr-{repo}-{action}",
        "type": "PullRequestEvent",
        "actor": {"id": 2, "login": "dev", "display_login": "dev",
                  "gravatar_id": "", "url": "", "avatar_url": ""},
        "repo": {"id": 1001, "name": repo, "url": ""},
        "payload": json.dumps({
            "action": action,
            "number": 42,
            "pull_request": {
                "id": 999, "number": 42, "state": "open", "locked": False,
                "title": "Add feature", "body": "Description",
                "user": {"login": "dev", "id": 2, "type": "User", "site_admin": False},
                "created_at": "2025-11-01T10:00:00Z",
                "updated_at": "2025-11-01T10:00:00Z",
                "closed_at": None, "merged_at": None,
                "draft": False, "merged": False,
                "additions": 50, "deletions": 10, "changed_files": 5,
                "review_comments": 2,
            },
        }),
        "public": True,
        "created_at": "2025-11-01T10:05:00Z",
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def make_unknown_event() -> dict:
    return {
        "id": "unknown-123",
        "type": "GollumEvent",  # wiki edit — not in our schema
        "actor": {"id": 3, "login": "wikieditor", "display_login": "wikieditor",
                  "gravatar_id": "", "url": "", "avatar_url": ""},
        "repo": {"id": 1001, "name": "octocat/wiki", "url": ""},
        "payload": json.dumps({"pages": []}),
        "public": True,
        "created_at": "2025-11-01T10:10:00Z",
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
    }


class TestEventParsing:
    """Validate that the streaming job's parse logic handles all event shapes."""

    def test_push_event_parsed_and_enriched(self, spark):
        from pyspark.sql import functions as F
        from schema import github_event_schema, push_payload_schema

        event = make_push_event(n_commits=5)
        df = spark.createDataFrame([{"json_str": json.dumps(event)}])

        parsed = df.select(
            F.from_json("json_str", github_event_schema).alias("e")
        ).select("e.*")

        row = parsed.collect()[0]
        assert row.type == "PushEvent"
        assert row.repo.name == "octocat/Hello-World"
        assert row.actor.login == "octocat"

        payload_df = parsed.select(
            F.from_json("payload", push_payload_schema).alias("p")
        )
        payload_row = payload_df.collect()[0].p
        assert payload_row.size == 5
        assert len(payload_row.commits) == 5

    def test_pr_event_all_fields_present(self, spark):
        from pyspark.sql import functions as F
        from schema import pr_payload_schema

        event = make_pr_event(action="merged")
        df = spark.createDataFrame([{"payload": event["payload"]}])
        parsed = df.select(F.from_json("payload", pr_payload_schema).alias("p"))
        row = parsed.collect()[0].p

        assert row.action == "merged"
        assert row.pull_request.additions == 50
        assert row.pull_request.review_comments == 2

    def test_event_time_watermark_column(self, spark):
        from pyspark.sql import functions as F
        from schema import github_event_schema

        event = make_push_event()
        df = spark.createDataFrame([{"json_str": json.dumps(event)}])
        parsed = df.select(F.from_json("json_str", github_event_schema).alias("e")).select("e.*")
        enriched = parsed.withColumn(
            "event_time",
            F.to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        )
        row = enriched.collect()[0]
        assert row.event_time is not None
        assert row.event_time.year == 2025
        assert row.event_time.month == 11

    def test_unknown_event_type_identified(self, spark):
        from pyspark.sql import functions as F
        from schema import github_event_schema, PAYLOAD_SCHEMAS

        event = make_unknown_event()
        df = spark.createDataFrame([{"json_str": json.dumps(event)}])
        parsed = df.select(F.from_json("json_str", github_event_schema).alias("e")).select("e.*")
        row = parsed.collect()[0]

        assert row.type == "GollumEvent"
        assert row.type not in PAYLOAD_SCHEMAS, "unknown type should not be in PAYLOAD_SCHEMAS"

    def test_malformed_json_yields_null_not_exception(self, spark):
        from pyspark.sql import functions as F
        from schema import github_event_schema

        df = spark.createDataFrame([{"json_str": "}{broken json"}])
        parsed = df.select(F.from_json("json_str", github_event_schema).alias("e"))
        row = parsed.collect()[0].e
        assert row is None

    def test_batch_of_mixed_events(self, spark):
        from pyspark.sql import functions as F
        from schema import github_event_schema

        events = [
            make_push_event("repo/a"),
            make_pr_event("repo/b"),
            make_unknown_event(),
        ]
        df = spark.createDataFrame([{"json_str": json.dumps(e)} for e in events])
        parsed = (
            df.select(F.from_json("json_str", github_event_schema).alias("e"))
            .select("e.*")
            .filter(F.col("id").isNotNull())
        )
        assert parsed.count() == 3

        known_types = {"PushEvent", "PullRequestEvent", "IssuesEvent",
                       "WatchEvent", "ForkEvent", "CreateEvent", "DeleteEvent"}
        dead_letter = parsed.filter(~F.col("type").isin(*known_types))
        assert dead_letter.count() == 1  # only GollumEvent


class TestRateLimiter:
    """Validate rate limiter behavior without I/O."""

    def test_bucket_capacity_enforced(self):
        sys.path.insert(0, "/Users/rahul/Projects/streamline/producer")
        from rate_limiter import RateLimiter
        limiter = RateLimiter(rate=1000, capacity=10)
        assert limiter.available_tokens == 10
        limiter.acquire(10)
        assert limiter.available_tokens == pytest.approx(0, abs=1)
        assert limiter.try_acquire() is False

    def test_nonblocking_try_acquire(self):
        sys.path.insert(0, "/Users/rahul/Projects/streamline/producer")
        from rate_limiter import RateLimiter
        limiter = RateLimiter(rate=1, capacity=1)
        assert limiter.try_acquire() is True
        assert limiter.try_acquire() is False
