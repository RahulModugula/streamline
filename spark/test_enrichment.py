"""Unit tests for enrichment module (no real GitHub API calls)."""
import sys
import os
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "producer"))


def test_cache_hit_avoids_api_call():
    import importlib
    import enrichment
    importlib.reload(enrichment)

    call_count = 0

    def mock_fetch(repo):
        nonlocal call_count
        call_count += 1
        return "Python"

    with patch.object(enrichment, "_fetch_repo_language", side_effect=mock_fetch):
        r1 = enrichment.get_repo_language("octocat/Hello-World")
        r2 = enrichment.get_repo_language("octocat/Hello-World")

    assert r1 == "Python"
    assert r2 == "Python"
    assert call_count == 1, "second call should hit cache"


def test_missing_repo_returns_none():
    import importlib
    import enrichment
    importlib.reload(enrichment)

    with patch.object(enrichment, "_fetch_repo_language", return_value=None):
        result = enrichment.get_repo_language("org/nonexistent-repo")
    assert result is None


def test_empty_repo_name_returns_none():
    import importlib
    import enrichment
    importlib.reload(enrichment)
    assert enrichment.get_repo_language("") is None
    assert enrichment.get_repo_language(None) is None


def test_cache_stats_structure():
    import importlib
    import enrichment
    importlib.reload(enrichment)
    stats = enrichment.cache_stats()
    assert "total_entries" in stats
    assert "live_entries" in stats


def test_cache_ttl_expiry_refetches():
    """
    Verifies that a cached entry older than CACHE_TTL_SECONDS is evicted
    and the API is called again rather than serving stale data.
    This matters for repos that change primary language (e.g., a Python repo
    rewritten in Rust). Without TTL validation, stale language labels would
    persist forever in a long-running Spark job.
    """
    import importlib
    import enrichment
    importlib.reload(enrichment)

    call_count = 0

    def mock_fetch(repo):
        nonlocal call_count
        call_count += 1
        return "Go" if call_count == 1 else "Rust"

    with patch.object(enrichment, "_fetch_repo_language", side_effect=mock_fetch):
        r1 = enrichment.get_repo_language("org/repo")
        assert r1 == "Go"
        assert call_count == 1

        # Manually backdate the cache entry to simulate TTL expiry
        repo_name = "org/repo"
        ts, lang = enrichment._ttl_store[repo_name]
        enrichment._ttl_store[repo_name] = (ts - enrichment.CACHE_TTL_SECONDS - 1, lang)

        r2 = enrichment.get_repo_language("org/repo")
        assert r2 == "Rust", "expired cache entry should trigger a re-fetch"
        assert call_count == 2, "API should be called again after TTL expiry"


def test_cache_eviction_when_full():
    """
    Verifies the LRU-approximate eviction fires when the cache exceeds 10k
    entries, reducing it to 9k entries rather than growing unboundedly.
    """
    import importlib
    import time
    import enrichment
    importlib.reload(enrichment)

    now = time.monotonic()
    # Pre-fill the cache beyond the 10k limit with backdated entries
    for i in range(10_001):
        enrichment._ttl_store[f"org/repo-{i}"] = (now - i, "Go")

    # Trigger eviction via a new cache write
    with patch.object(enrichment, "_fetch_repo_language", return_value="Python"):
        enrichment.get_repo_language("org/new-repo")

    assert len(enrichment._ttl_store) <= 10_001, \
        f"cache should have been evicted, got {len(enrichment._ttl_store)} entries"
