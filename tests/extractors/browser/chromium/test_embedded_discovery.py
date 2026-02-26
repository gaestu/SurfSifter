"""Tests for embedded Chromium root discovery."""

import sqlite3

import pytest

from extractors.browser.chromium._embedded_discovery import (
    _detect_signal,
    _extract_profile_root,
    discover_artifacts_with_embedded_roots,
    discover_embedded_roots,
    get_embedded_root_paths,
)


@pytest.fixture
def evidence_db(tmp_path):
    db_path = tmp_path / "embedded.sqlite"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE file_list (
            id INTEGER PRIMARY KEY,
            evidence_id INTEGER NOT NULL,
            file_path TEXT,
            file_name TEXT,
            extension TEXT,
            size_bytes INTEGER,
            inode INTEGER,
            deleted INTEGER DEFAULT 0,
            partition_index INTEGER
        )
        """
    )
    conn.commit()
    yield conn
    conn.close()


def _insert_rows(conn: sqlite3.Connection, rows):
    conn.executemany(
        """
        INSERT INTO file_list
        (evidence_id, file_path, file_name, extension, size_bytes, inode, deleted, partition_index)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()


def test_discover_embedded_roots_positive_and_partitioned(evidence_db):
    evidence_id = 1
    rows = [
        # Embedded root (partition 2): at least two distinct signals.
        (evidence_id, "ProgramData/SomeApp/User Data/Default/Network/Cookies", "Cookies", "", 1, 101, 0, 2),
        (evidence_id, "ProgramData/SomeApp/User Data/Default/History", "History", "", 1, 102, 0, 2),
        (evidence_id, "ProgramData/SomeApp/User Data/Default/Local Storage/leveldb/000003.log", "000003.log", ".log", 1, 103, 0, 2),
        # Different embedded root on another partition.
        (evidence_id, "Users/Bob/AppData/Roaming/FooBrowser/User Data/Default/History", "History", "", 1, 201, 0, 3),
        (evidence_id, "Users/Bob/AppData/Roaming/FooBrowser/User Data/Default/Cookies", "Cookies", "", 1, 202, 0, 3),
    ]
    _insert_rows(evidence_db, rows)

    roots = discover_embedded_roots(evidence_db, evidence_id)

    assert len(roots) == 2
    assert any(root.root_path == "ProgramData/SomeApp/User Data" and root.partition_index == 2 for root in roots)
    assert any(root.root_path == "Users/Bob/AppData/Roaming/FooBrowser/User Data" and root.partition_index == 3 for root in roots)


def test_rejects_single_signal_and_known_browser_roots(evidence_db):
    evidence_id = 1
    rows = [
        # Single-signal app candidate (should be rejected).
        (evidence_id, "ProgramData/SingleSignal/User Data/Default/History", "History", "", 1, 301, 0, 2),
        # Known Chrome root (should be excluded).
        (evidence_id, "Users/Alice/AppData/Local/Google/Chrome/User Data/Default/History", "History", "", 1, 401, 0, 2),
        (evidence_id, "Users/Alice/AppData/Local/Google/Chrome/User Data/Default/Cookies", "Cookies", "", 1, 402, 0, 2),
    ]
    _insert_rows(evidence_db, rows)

    roots = discover_embedded_roots(evidence_db, evidence_id)
    assert roots == []


def test_discovery_scopes_embedded_patterns_per_partition(evidence_db):
    evidence_id = 1
    rows = [
        # Partition 2 qualifies as embedded root.
        (evidence_id, "ProgramData/ScopedApp/User Data/Default/History", "History", "", 1, 501, 0, 2),
        (evidence_id, "ProgramData/ScopedApp/User Data/Default/Cookies", "Cookies", "", 1, 502, 0, 2),
        # Same-looking path on partition 0 but only one signal.
        (evidence_id, "ProgramData/ScopedApp/User Data/Default/Cookies", "Cookies", "", 1, 601, 0, 0),
    ]
    _insert_rows(evidence_db, rows)

    result, roots = discover_artifacts_with_embedded_roots(
        evidence_db,
        evidence_id,
        artifact="cookies",
        filename_patterns=["Cookies"],
        path_patterns=["%Google%Chrome%"],  # intentionally excludes embedded path
    )

    assert roots
    assert get_embedded_root_paths(roots, partition_index=2) == ["ProgramData/ScopedApp/User Data"]
    assert result.matches_by_partition.get(2)
    assert result.matches_by_partition.get(0) is None


# ---------------------------------------------------------------------------
# Signal detection unit tests
# ---------------------------------------------------------------------------

class TestDetectSignal:
    """Unit tests for _detect_signal."""

    def test_visited_links_by_path(self):
        assert _detect_signal("/Application/cache/Visited Links", "Visited Links") == "visited_links"

    def test_visited_links_by_filename(self):
        assert _detect_signal("/some/path/Visited Links", "Visited Links") == "visited_links"

    def test_old_localstorage_by_path(self):
        assert _detect_signal(
            "/Application/cache/Local Storage/https_example.net_0.localstorage",
            "https_example.net_0.localstorage",
        ) == "local_storage"

    def test_modern_localstorage_still_works(self):
        assert _detect_signal(
            "/App/User Data/Default/Local Storage/leveldb/000003.log",
            "000003.log",
        ) == "local_storage"

    def test_cache_index(self):
        assert _detect_signal("/App/Default/Cache/index", "index") == "cache"

    def test_cookies(self):
        assert _detect_signal("/App/Default/Cookies", "Cookies") == "cookies"

    def test_network_cookies(self):
        assert _detect_signal("/App/Default/Network/Cookies", "Cookies") == "cookies"

    def test_unrelated_file_returns_none(self):
        assert _detect_signal("/some/random/file.txt", "file.txt") is None


# ---------------------------------------------------------------------------
# Profile root extraction unit tests
# ---------------------------------------------------------------------------

class TestExtractProfileRoot:
    """Unit tests for _extract_profile_root."""

    def test_cache_strips_only_index(self):
        """Cache signal should strip /index, not /cache/index — supports flat CefSharp layouts."""
        root = _extract_profile_root("/Application/cache/index", "cache")
        assert root == "/Application/cache"

    def test_cache_standard_layout(self):
        """Standard Cache/index still yields parent of the Cache/ directory."""
        root = _extract_profile_root("/App/User Data/Default/Cache/index", "cache")
        assert root == "/App/User Data/Default/Cache"

    def test_cache_cache_data_index(self):
        """Cache_Data/index (modern simple cache) strip to parent of Cache_Data."""
        root = _extract_profile_root("/App/Default/Cache/Cache_Data/index", "cache")
        assert root == "/App/Default/Cache/Cache_Data"

    def test_visited_links(self):
        root = _extract_profile_root("/Application/cache/Visited Links", "visited_links")
        assert root == "/Application/cache"

    def test_old_localstorage(self):
        root = _extract_profile_root(
            "/Application/cache/Local Storage/https_example.net_0.localstorage",
            "local_storage",
        )
        assert root == "/Application/cache"

    def test_modern_localstorage_leveldb(self):
        root = _extract_profile_root(
            "/App/User Data/Default/Local Storage/leveldb/000003.log",
            "local_storage",
        )
        assert root == "/App/User Data/Default"


# ---------------------------------------------------------------------------
# CefSharp flat layout integration test (Application-style)
# ---------------------------------------------------------------------------

def test_cefsharp_flat_layout_discovered(evidence_db):
    """
    CefSharp/CEF flat layout: blockfile cache data, Cookies, Visited Links,
    and old-format Local Storage all live directly under the same directory.
    The directory may be named 'cache' which previously confused root extraction.
    """
    evidence_id = 1
    rows = [
        # Application CefSharp layout: everything under /Application/cache/
        (evidence_id, "/Application/cache/Cookies", "Cookies", "", 1024, 10, 0, 0),
        (evidence_id, "/Application/cache/Visited Links", "Visited Links", "", 512, 11, 0, 0),
        (evidence_id, "/Application/cache/Local Storage/https_example.net_0.localstorage", "https_example.net_0.localstorage", ".localstorage", 256, 12, 0, 0),
        (evidence_id, "/Application/cache/index", "index", "", 256, 13, 0, 0),
        (evidence_id, "/Application/cache/data_0", "data_0", "", 4096, 14, 0, 0),
        (evidence_id, "/Application/cache/data_1", "data_1", "", 4096, 15, 0, 0),
        (evidence_id, "/Application/cache/data_2", "data_2", "", 4096, 16, 0, 0),
        (evidence_id, "/Application/cache/data_3", "data_3", "", 4096, 17, 0, 0),
    ]
    _insert_rows(evidence_db, rows)

    roots = discover_embedded_roots(evidence_db, evidence_id)

    assert len(roots) == 1
    root = roots[0]
    assert root.root_path == "/Application/cache"
    assert root.partition_index == 0
    # Must have at least cookies + visited_links (possibly also cache, local_storage)
    assert "cookies" in root.signals
    assert "visited_links" in root.signals
    assert root.signal_count >= 2


def test_cefsharp_flat_layout_cookies_plus_visited_links_only(evidence_db):
    """Minimal CefSharp root: just Cookies + Visited Links suffices for acceptance."""
    evidence_id = 1
    rows = [
        (evidence_id, "/SomeApp/data/Cookies", "Cookies", "", 1024, 20, 0, 1),
        (evidence_id, "/SomeApp/data/Visited Links", "Visited Links", "", 512, 21, 0, 1),
    ]
    _insert_rows(evidence_db, rows)

    roots = discover_embedded_roots(evidence_db, evidence_id)

    assert len(roots) == 1
    assert roots[0].root_path == "/SomeApp/data"
    assert set(roots[0].signals) == {"cookies", "visited_links"}


def test_standard_embedded_unchanged_after_cache_fix(evidence_db):
    """
    Standard embedded layout (EBWebView-style with Default/ profile subfolder)
    must still be discovered correctly after cache root extraction changes.
    """
    evidence_id = 1
    rows = [
        (evidence_id, "/AppData/Local/SomeApp/EBWebView/Default/History", "History", "", 1, 30, 0, 0),
        (evidence_id, "/AppData/Local/SomeApp/EBWebView/Default/Cookies", "Cookies", "", 1, 31, 0, 0),
        (evidence_id, "/AppData/Local/SomeApp/EBWebView/Default/Cache/index", "index", "", 1, 32, 0, 0),
    ]
    _insert_rows(evidence_db, rows)

    roots = discover_embedded_roots(evidence_db, evidence_id)

    # History + Cookies cluster at /AppData/Local/SomeApp/EBWebView
    # Cache now sits at .../Default/Cache (1 signal) — harmless extra root, rejected
    ebwebview_roots = [r for r in roots if "EBWebView" in r.root_path and r.signal_count >= 2]
    assert len(ebwebview_roots) == 1
    assert ebwebview_roots[0].root_path == "/AppData/Local/SomeApp/EBWebView"
    assert "cookies" in ebwebview_roots[0].signals
    assert "history" in ebwebview_roots[0].signals
