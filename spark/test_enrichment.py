"""Unit tests for enrichment module (no real GitHub API calls)."""
import sys
import os
from unittest.mock import patch, MagicMock

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
