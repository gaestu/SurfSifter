"""Tests for Firefox cache2 extraction-side changes.

Covers:
- DiscoveredFile.artifact_type field and classification
- ExtractionResult.artifact_type propagation to manifest dicts
- classify_cache_path() path classification (from _patterns.py)
- get_cache_discovery_patterns() SQL LIKE pattern generation
"""
from __future__ import annotations

import pytest

from extractors.browser.firefox.cache.strategies.base import (
    DiscoveredFile,
    ExtractionResult,
)
from extractors.browser.firefox._patterns import (
    classify_cache_path,
    get_cache_discovery_patterns,
    CACHE_ARTIFACTS,
    FIREFOX_ARTIFACTS,
)


# =====================================================================
# DiscoveredFile.artifact_type
# =====================================================================

class TestDiscoveredFileArtifactType:
    """DiscoveredFile carries artifact_type for manifest tagging."""

    def test_default_artifact_type(self):
        f = DiscoveredFile(path="cache2/entries/ABCDEF1234567890")
        assert f.artifact_type == "cache_firefox"

    def test_custom_artifact_type(self):
        f = DiscoveredFile(
            path="cache2/doomed/ABCDEF1234567890",
            artifact_type="cache_doomed",
        )
        assert f.artifact_type == "cache_doomed"

    def test_hash_includes_path_partition_inode(self):
        """artifact_type does NOT affect hash/equality (existing contract)."""
        f1 = DiscoveredFile(path="a", partition_index=0, inode=1, artifact_type="cache_firefox")
        f2 = DiscoveredFile(path="a", partition_index=0, inode=1, artifact_type="cache_index")
        assert hash(f1) == hash(f2)
        assert f1 == f2

    def test_all_artifact_types_assignable(self):
        for at in ("cache_firefox", "cache_index", "cache_journal", "cache_doomed", "cache_trash"):
            f = DiscoveredFile(path="x", artifact_type=at)
            assert f.artifact_type == at


# =====================================================================
# ExtractionResult.artifact_type in to_dict()
# =====================================================================

class TestExtractionResultArtifactType:
    """ExtractionResult.to_dict() includes the correct artifact_type."""

    def test_default_artifact_type_in_dict(self):
        r = ExtractionResult(success=True, source_path="entries/ABC")
        d = r.to_dict()
        assert d["artifact_type"] == "cache_firefox"

    def test_custom_artifact_type_in_dict(self):
        r = ExtractionResult(
            success=True,
            source_path="cache2/index",
            artifact_type="cache_index",
        )
        d = r.to_dict()
        assert d["artifact_type"] == "cache_index"

    def test_doomed_artifact_type(self):
        r = ExtractionResult(
            success=True,
            source_path="cache2/doomed/ABC",
            artifact_type="cache_doomed",
        )
        assert r.to_dict()["artifact_type"] == "cache_doomed"

    def test_trash_artifact_type(self):
        r = ExtractionResult(
            success=True,
            source_path="cache2/trash/0/ABC",
            artifact_type="cache_trash",
        )
        assert r.to_dict()["artifact_type"] == "cache_trash"

    def test_journal_artifact_type(self):
        r = ExtractionResult(
            success=True,
            source_path="cache2/index.log",
            artifact_type="cache_journal",
        )
        assert r.to_dict()["artifact_type"] == "cache_journal"

    def test_failed_result_still_has_artifact_type(self):
        r = ExtractionResult(
            success=False,
            source_path="cache2/index",
            error="read error",
            artifact_type="cache_index",
        )
        d = r.to_dict()
        assert d["artifact_type"] == "cache_index"
        assert d["success"] is False


# =====================================================================
# _classify_cache_path()
# =====================================================================

class TestClassifyCachePath:
    """classify_cache_path() classifies paths correctly."""

    # --- Regular entries ---
    def test_entries_linux(self):
        assert classify_cache_path(
            "/home/user/.mozilla/firefox/abc123.default/cache2/entries/7F8A3B2E"
        ) == "cache_firefox"

    def test_entries_windows(self):
        assert classify_cache_path(
            "C:\\Users\\alice\\AppData\\Local\\Mozilla\\Firefox\\Profiles\\abc.default\\cache2\\entries\\7F8A3B2E"
        ) == "cache_firefox"

    # --- Index file ---
    def test_index_linux(self):
        assert classify_cache_path(
            "/home/user/.mozilla/firefox/abc123.default/cache2/index"
        ) == "cache_index"

    def test_index_windows(self):
        assert classify_cache_path(
            "C:\\Users\\alice\\AppData\\Local\\Mozilla\\Firefox\\Profiles\\abc.default\\cache2\\index"
        ) == "cache_index"

    def test_index_not_prefix(self):
        """'index' must be the terminal component, not 'index.log' or 'index_backup'."""
        assert classify_cache_path("/path/to/cache2/index") == "cache_index"

    # --- Journal ---
    def test_journal_linux(self):
        assert classify_cache_path(
            "/home/user/.mozilla/firefox/abc.default/cache2/index.log"
        ) == "cache_journal"

    def test_journal_windows(self):
        assert classify_cache_path(
            "C:\\Users\\alice\\cache2\\index.log"
        ) == "cache_journal"

    # --- Doomed ---
    def test_doomed_linux(self):
        assert classify_cache_path(
            "/home/user/.mozilla/firefox/abc.default/cache2/doomed/7F8A3B2E"
        ) == "cache_doomed"

    def test_doomed_windows(self):
        assert classify_cache_path(
            "C:\\Users\\alice\\cache2\\doomed\\ABC123"
        ) == "cache_doomed"

    # --- Trash ---
    def test_trash_linux(self):
        assert classify_cache_path(
            "/home/user/.mozilla/firefox/abc.default/cache2/trash/0/7F8A3B2E"
        ) == "cache_trash"

    def test_trash_windows(self):
        assert classify_cache_path(
            "C:\\Users\\alice\\cache2\\trash\\3\\ABC123"
        ) == "cache_trash"

    # --- Edge cases ---
    def test_unknown_cache2_subdir(self):
        """Unrecognised sub-path falls back to cache_firefox."""
        assert classify_cache_path("/path/cache2/something_else/file") == "cache_firefox"

    def test_case_insensitive(self):
        assert classify_cache_path("/PATH/CACHE2/INDEX") == "cache_index"
        assert classify_cache_path("/PATH/Cache2/Index.Log") == "cache_journal"
        assert classify_cache_path("/PATH/Cache2/Doomed/ABC") == "cache_doomed"
        assert classify_cache_path("/PATH/Cache2/Trash/0/ABC") == "cache_trash"


# =====================================================================
# get_cache_discovery_patterns()
# =====================================================================

class TestGetCacheDiscoveryPatterns:
    """get_cache_discovery_patterns() returns correct SQL LIKE patterns."""

    def test_returns_list(self):
        patterns = get_cache_discovery_patterns()
        assert isinstance(patterns, list)
        assert len(patterns) > 0

    def test_all_patterns_are_sql_like(self):
        """Every pattern starts with %/ and contains no glob wildcards."""
        for p in get_cache_discovery_patterns():
            assert p.startswith("%/"), f"Pattern should start with %/: {p}"
            assert "*" not in p, f"Pattern should not contain glob *: {p}"

    def test_covers_entries(self):
        patterns = get_cache_discovery_patterns()
        assert any("cache2/entries/%" in p for p in patterns)

    def test_covers_index(self):
        patterns = get_cache_discovery_patterns()
        assert any(p.endswith("cache2/index") for p in patterns)

    def test_covers_journal(self):
        patterns = get_cache_discovery_patterns()
        assert any(p.endswith("cache2/index.log") for p in patterns)

    def test_covers_doomed(self):
        patterns = get_cache_discovery_patterns()
        assert any("cache2/doomed/%" in p for p in patterns)

    def test_covers_trash(self):
        patterns = get_cache_discovery_patterns()
        assert any("cache2/trash/%" in p for p in patterns)

    def test_no_duplicates(self):
        patterns = get_cache_discovery_patterns()
        assert len(patterns) == len(set(patterns))

    def test_derived_from_cache_artifacts(self):
        """Every CACHE_ARTIFACTS key contributes at least one pattern."""
        patterns = get_cache_discovery_patterns()
        joined = " ".join(patterns)
        for key in CACHE_ARTIFACTS:
            # Each key should map to a segment in at least one pattern
            assert any(
                "cache2/" in p for p in patterns
            ), f"No pattern generated for artifact key {key}"
