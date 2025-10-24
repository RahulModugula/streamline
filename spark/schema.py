"""
GitHub event schemas for Spark Structured Streaming.

Schemas are defined explicitly rather than inferred at runtime for two reasons:
1. Performance: Spark skips the expensive schema-inference pass.
2. Schema evolution: new optional fields can be added as nullable without
   breaking existing data. Old fields can be deprecated but not removed
   (forward/backward compatibility).

Schema versioning convention: bump SCHEMA_VERSION when fields are added.
Never remove or rename fields in a deployed schema — add new ones instead.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SCHEMA_VERSION = 3  # bump when adding new fields

# ── Shared building blocks ────────────────────────────────────────────────────

actor_schema = StructType([
    StructField("id", LongType(), nullable=True),
    StructField("login", StringType(), nullable=True),
    StructField("display_login", StringType(), nullable=True),
    StructField("gravatar_id", StringType(), nullable=True),
    StructField("url", StringType(), nullable=True),
    StructField("avatar_url", StringType(), nullable=True),
])

repo_schema = StructType([
    StructField("id", LongType(), nullable=True),
    StructField("name", StringType(), nullable=True),  # "owner/repo"
    StructField("url", StringType(), nullable=True),
])

# ── PushEvent ─────────────────────────────────────────────────────────────────

commit_schema = StructType([
    StructField("sha", StringType(), nullable=True),
    StructField("author", StructType([
        StructField("email", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
    ]), nullable=True),
    StructField("message", StringType(), nullable=True),
    StructField("distinct", BooleanType(), nullable=True),
    StructField("url", StringType(), nullable=True),
])

push_payload_schema = StructType([
    StructField("push_id", LongType(), nullable=True),
    StructField("size", IntegerType(), nullable=True),
    StructField("distinct_size", IntegerType(), nullable=True),
    StructField("ref", StringType(), nullable=True),
    StructField("head", StringType(), nullable=True),
    StructField("before", StringType(), nullable=True),
    StructField("commits", ArrayType(commit_schema), nullable=True),
])

# ── PullRequestEvent ──────────────────────────────────────────────────────────

pr_user_schema = StructType([
    StructField("login", StringType(), nullable=True),
    StructField("id", LongType(), nullable=True),
    StructField("type", StringType(), nullable=True),
    StructField("site_admin", BooleanType(), nullable=True),
])

pull_request_schema = StructType([
    StructField("url", StringType(), nullable=True),
    StructField("id", LongType(), nullable=True),
    StructField("number", IntegerType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("locked", BooleanType(), nullable=True),
    StructField("title", StringType(), nullable=True),
    StructField("user", pr_user_schema, nullable=True),
    StructField("body", StringType(), nullable=True),
    StructField("created_at", StringType(), nullable=True),
    StructField("updated_at", StringType(), nullable=True),
    StructField("closed_at", StringType(), nullable=True),
    StructField("merged_at", StringType(), nullable=True),
    StructField("draft", BooleanType(), nullable=True),         # added in v2
    StructField("merged", BooleanType(), nullable=True),
    StructField("additions", IntegerType(), nullable=True),
    StructField("deletions", IntegerType(), nullable=True),
    StructField("changed_files", IntegerType(), nullable=True),
    StructField("review_comments", IntegerType(), nullable=True),  # added in v3
])

pr_payload_schema = StructType([
    StructField("action", StringType(), nullable=True),
    StructField("number", IntegerType(), nullable=True),
    StructField("pull_request", pull_request_schema, nullable=True),
])

# ── IssuesEvent ───────────────────────────────────────────────────────────────

issue_schema = StructType([
    StructField("url", StringType(), nullable=True),
    StructField("id", LongType(), nullable=True),
    StructField("number", IntegerType(), nullable=True),
    StructField("title", StringType(), nullable=True),
    StructField("user", pr_user_schema, nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("locked", BooleanType(), nullable=True),
    StructField("body", StringType(), nullable=True),
    StructField("created_at", StringType(), nullable=True),
    StructField("updated_at", StringType(), nullable=True),
    StructField("closed_at", StringType(), nullable=True),
    StructField("comments", IntegerType(), nullable=True),
])

issues_payload_schema = StructType([
    StructField("action", StringType(), nullable=True),
    StructField("issue", issue_schema, nullable=True),
])

# ── Generic envelope ──────────────────────────────────────────────────────────
# Top-level schema for all GitHub events. The payload field is kept as a raw
# string here and parsed downstream per event_type using from_json().
# This lets us handle unknown event types without dropping the entire batch.

github_event_schema = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("type", StringType(), nullable=True),
    StructField("actor", actor_schema, nullable=True),
    StructField("repo", repo_schema, nullable=True),
    StructField("payload", StringType(), nullable=True),  # raw JSON string
    StructField("public", BooleanType(), nullable=True),
    StructField("created_at", StringType(), nullable=True),
    StructField("_ingested_at", StringType(), nullable=True),  # producer metadata
])

# Map event type → payload schema (used by the streaming job for typed parsing)
PAYLOAD_SCHEMAS = {
    "PushEvent": push_payload_schema,
    "PullRequestEvent": pr_payload_schema,
    "IssuesEvent": issues_payload_schema,
}
