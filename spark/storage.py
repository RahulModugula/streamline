"""
Storage configuration for the streamline pipeline.

Abstracts over local filesystem and S3-compatible storage (MinIO in dev,
AWS S3 in prod). The streaming job and backfill job import from here
instead of hardcoding paths.

Set OUTPUT_STORAGE=s3 and the S3_* env vars to write to MinIO/S3.
Default is local filesystem.
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class StorageConfig:
    mode: str               # "local" or "s3"
    base_path: str          # s3a://bucket/prefix or ./data/lake
    checkpoint_base: str    # where Spark writes streaming state

    # S3-specific (also used for MinIO)
    endpoint: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    bucket: Optional[str] = None

    def table_path(self, table: str) -> str:
        return f"{self.base_path}/{table}"

    def checkpoint_path(self, suffix: str) -> str:
        return f"{self.checkpoint_base}/{suffix}"


def load_storage_config() -> StorageConfig:
    """Load storage config from environment variables."""
    mode = os.getenv("OUTPUT_STORAGE", "local")

    if mode == "s3":
        bucket = os.getenv("S3_BUCKET", "streamline")
        prefix = os.getenv("S3_PREFIX", "lake").rstrip("/")
        endpoint = os.getenv("S3_ENDPOINT", "http://localhost:9000")  # MinIO default
        return StorageConfig(
            mode="s3",
            base_path=f"s3a://{bucket}/{prefix}",
            checkpoint_base=f"s3a://{bucket}/checkpoints",
            endpoint=endpoint,
            access_key=os.getenv("S3_ACCESS_KEY", "streamline"),
            secret_key=os.getenv("S3_SECRET_KEY", "streamline"),
            bucket=bucket,
        )

    # Local filesystem
    return StorageConfig(
        mode="local",
        base_path=os.getenv("OUTPUT_DIR", "./data/lake"),
        checkpoint_base=os.getenv("CHECKPOINT_DIR", "/tmp/streamline-checkpoints"),
    )


def configure_spark_for_storage(spark_builder, cfg: StorageConfig):
    """
    Apply storage-specific Spark configs to a SparkSession builder.
    For S3/MinIO, configures the s3a:// filesystem with the correct endpoint
    and credentials. For local, no-op.
    """
    if cfg.mode != "s3":
        return spark_builder

    return (
        spark_builder
        .config("spark.hadoop.fs.s3a.endpoint", cfg.endpoint)
        .config("spark.hadoop.fs.s3a.access.key", cfg.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", cfg.secret_key)
        # Path-style access required for MinIO (virtual-hosted style is AWS default)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Connection pool tuning for high-throughput streaming writes
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
    )
