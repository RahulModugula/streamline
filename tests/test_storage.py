"""Unit tests for storage configuration."""
import os


def test_local_storage_defaults():
    # Clear any env overrides
    for k in ("OUTPUT_STORAGE", "OUTPUT_DIR", "CHECKPOINT_DIR"):
        os.environ.pop(k, None)

    import sys
    sys.path.insert(0, "/Users/rahul/Projects/streamline/spark")
    # Force re-import so env changes take effect
    if "storage" in sys.modules:
        del sys.modules["storage"]
    from storage import load_storage_config

    cfg = load_storage_config()
    assert cfg.mode == "local"
    assert cfg.base_path == "./data/lake"
    assert cfg.table_path("pushevent") == "./data/lake/pushevent"
    assert cfg.endpoint is None


def test_s3_storage_from_env():
    os.environ["OUTPUT_STORAGE"] = "s3"
    os.environ["S3_BUCKET"] = "my-bucket"
    os.environ["S3_ENDPOINT"] = "http://minio:9000"
    os.environ["S3_ACCESS_KEY"] = "testkey"
    os.environ["S3_SECRET_KEY"] = "testsecret"

    import sys
    sys.path.insert(0, "/Users/rahul/Projects/streamline/spark")
    if "storage" in sys.modules:
        del sys.modules["storage"]
    from storage import load_storage_config

    cfg = load_storage_config()
    assert cfg.mode == "s3"
    assert cfg.base_path == "s3a://my-bucket/lake"
    assert cfg.checkpoint_base == "s3a://my-bucket/checkpoints"
    assert cfg.endpoint == "http://minio:9000"
    assert cfg.table_path("pushevent") == "s3a://my-bucket/lake/pushevent"

    # cleanup
    for k in ("OUTPUT_STORAGE", "S3_BUCKET", "S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY"):
        os.environ.pop(k, None)
    if "storage" in sys.modules:
        del sys.modules["storage"]


def test_s3_path_includes_prefix():
    os.environ["OUTPUT_STORAGE"] = "s3"
    os.environ["S3_BUCKET"] = "data-lake"
    os.environ["S3_PREFIX"] = "github/events"

    import sys
    sys.path.insert(0, "/Users/rahul/Projects/streamline/spark")
    if "storage" in sys.modules:
        del sys.modules["storage"]
    from storage import load_storage_config

    cfg = load_storage_config()
    assert cfg.base_path == "s3a://data-lake/github/events"

    for k in ("OUTPUT_STORAGE", "S3_BUCKET", "S3_PREFIX"):
        os.environ.pop(k, None)
    if "storage" in sys.modules:
        del sys.modules["storage"]
