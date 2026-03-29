"""
Unit tests for GitHub event schemas.
These run without a Spark cluster — PySpark in local mode starts quickly.
"""

import json
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    """Local Spark session for tests — no cluster needed."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("streamline-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def sample_push_event():
    return {
        "id": "12345678901",
        "type": "PushEvent",
        "actor": {"id": 1, "login": "octocat", "display_login": "octocat",
                  "gravatar_id": "", "url": "https://api.github.com/users/octocat",
                  "avatar_url": "https://avatars.githubusercontent.com/u/1"},
        "repo": {"id": 1296269, "name": "octocat/Hello-World",
                 "url": "https://api.github.com/repos/octocat/Hello-World"},
        "payload": json.dumps({
            "push_id": 987654321,
            "size": 1,
            "distinct_size": 1,
            "ref": "refs/heads/main",
            "head": "abc123",
            "before": "def456",
            "commits": [{"sha": "abc123", "message": "fix: resolve bug",
                         "distinct": True, "url": "https://...",
                         "author": {"name": "Ock", "email": "ock@example.com"}}],
        }),
        "public": True,
        "created_at": "2025-10-14T12:00:00Z",
        "_ingested_at": "2025-10-14T12:00:01Z",
    }


@pytest.fixture(scope="session")
def sample_pr_event():
    return {
        "id": "22222222222",
        "type": "PullRequestEvent",
        "actor": {"id": 2, "login": "contributor", "display_login": "contributor",
                  "gravatar_id": "", "url": "", "avatar_url": ""},
        "repo": {"id": 1296269, "name": "octocat/Hello-World", "url": ""},
        "payload": json.dumps({
            "action": "opened",
            "number": 42,
            "pull_request": {
                "url": "https://...", "id": 999, "number": 42,
                "state": "open", "locked": False, "title": "Add feature",
                "user": {"login": "contributor", "id": 2, "type": "User", "site_admin": False},
                "body": "This PR adds a feature",
                "created_at": "2025-10-14T12:00:00Z", "updated_at": "2025-10-14T12:00:00Z",
                "closed_at": None, "merged_at": None,
                "draft": False, "merged": False,
                "additions": 10, "deletions": 2, "changed_files": 3,
                "review_comments": 0,
            },
        }),
        "public": True,
        "created_at": "2025-10-14T12:05:00Z",
        "_ingested_at": "2025-10-14T12:05:01Z",
    }


class TestSchemaValidation:
    def test_push_event_parses_correctly(self, spark, sample_push_event):
        from schema import github_event_schema, push_payload_schema

        df = spark.createDataFrame([sample_push_event])
        parsed = df.select(
            F.from_json(F.col("payload"), push_payload_schema).alias("p")
        )
        row = parsed.collect()[0].p
        assert row.size == 1
        assert row.ref == "refs/heads/main"
        assert len(row.commits) == 1
        assert row.commits[0].message == "fix: resolve bug"

    def test_pr_event_parses_correctly(self, spark, sample_pr_event):
        from schema import pr_payload_schema

        df = spark.createDataFrame([sample_pr_event])
        parsed = df.select(
            F.from_json(F.col("payload"), pr_payload_schema).alias("p")
        )
        row = parsed.collect()[0].p
        assert row.action == "opened"
        assert row.pull_request.title == "Add feature"
        assert row.pull_request.additions == 10

    def test_schema_evolution_new_nullable_field(self, spark):
        """
        Simulates schema evolution: an event with a NEW field that didn't
        exist in an older schema version. With nullable=True on new fields,
        old data parses without error (missing field → null, not exception).
        """
        from schema import pr_payload_schema
        from pyspark.sql.types import IntegerType, StructField

        # Simulate a future PR event with a new field not in current schema
        future_payload = json.dumps({
            "action": "closed",
            "number": 99,
            "pull_request": {
                "id": 888, "number": 99, "state": "closed",
                "locked": False, "title": "Old PR",
                "merged": True, "draft": False, "additions": 5, "deletions": 1,
                "changed_files": 1, "review_comments": 2,
                "NEW_FUTURE_FIELD": "this_field_did_not_exist",  # unknown field
            },
        })

        df = spark.createDataFrame([{"payload": future_payload}])
        # Should parse without error — unknown fields are ignored
        parsed = df.select(F.from_json("payload", pr_payload_schema).alias("p"))
        row = parsed.collect()[0].p
        assert row.action == "closed"
        # review_comments was added in schema v3 and should be present
        assert row.pull_request.review_comments == 2

    def test_malformed_event_returns_null_not_exception(self, spark):
        """from_json returns null struct on malformed input, never raises."""
        from schema import github_event_schema

        df = spark.createDataFrame([{"json_str": "not valid json at all"}])
        parsed = df.select(
            F.from_json("json_str", github_event_schema).alias("e")
        )
        row = parsed.collect()[0].e
        assert row is None  # null struct, not an exception

    def test_event_time_parsing(self, spark, sample_push_event):
        """created_at ISO8601 string should parse to a Spark timestamp."""
        df = spark.createDataFrame([sample_push_event])
        result = df.select(
            F.to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_time")
        )
        row = result.collect()[0]
        assert row.event_time is not None
        assert row.event_time.year == 2025
        assert row.event_time.month == 10
