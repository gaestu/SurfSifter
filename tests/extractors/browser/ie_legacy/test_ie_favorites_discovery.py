"""
Tests for IE Favorites discovery via file_list.

Verifies that .url files under Favorites paths are correctly discovered
using glob-based filename patterns.
"""
import sqlite3

import pytest

from extractors._shared.file_list_discovery import discover_from_file_list


@pytest.fixture
def evidence_db(tmp_path):
    """Create an evidence database with file_list table."""
    db_path = tmp_path / "test_evidence.sqlite"
    conn = sqlite3.connect(str(db_path))

    conn.execute("""
        CREATE TABLE file_list (
            id INTEGER PRIMARY KEY,
            evidence_id INTEGER NOT NULL,
            file_path TEXT,
            file_name TEXT,
            extension TEXT,
            size_bytes INTEGER,
            created_ts TEXT,
            modified_ts TEXT,
            accessed_ts TEXT,
            inode INTEGER,
            deleted INTEGER DEFAULT 0,
            partition_index INTEGER
        )
    """)

    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def populated_db(evidence_db):
    """Populate file_list with IE Favorites test data."""
    evidence_id = 1

    test_files = [
        # .url files under Favorites
        (evidence_id, "/Users/testuser/Favorites/Google.url",
         "Google.url", ".url", 120, None, None, None, 1001, 0, 3),
        (evidence_id, "/Users/testuser/Favorites/Links/Bing.url",
         "Bing.url", ".url", 115, None, None, None, 1002, 0, 3),
        (evidence_id, "/Users/testuser/Favorites/News/CNN.url",
         "CNN.url", ".url", 130, None, None, None, 1003, 0, 3),

        # Non-.url file under Favorites (should NOT match)
        (evidence_id, "/Users/testuser/Favorites/desktop.ini",
         "desktop.ini", ".ini", 50, None, None, None, 1004, 0, 3),

        # .url file NOT under Favorites (should NOT match path pattern)
        (evidence_id, "/Users/testuser/Desktop/shortcut.url",
         "shortcut.url", ".url", 100, None, None, None, 1005, 0, 3),

        # Unrelated file
        (evidence_id, "/Windows/System32/config/SYSTEM",
         "SYSTEM", None, 10000000, None, None, None, 9999, 0, 3),
    ]

    evidence_db.executemany(
        """INSERT INTO file_list
           (evidence_id, file_path, file_name, extension, size_bytes,
            created_ts, modified_ts, accessed_ts, inode, deleted, partition_index)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        test_files,
    )
    evidence_db.commit()
    return evidence_db


class TestIeFavoritesDiscovery:
    """Tests for IE Favorites .url file discovery."""

    def test_discovers_url_files_in_favorites(self, populated_db):
        """discover_from_file_list finds .url files under Favorites paths."""
        result = discover_from_file_list(
            populated_db,
            evidence_id=1,
            filename_patterns=["*.url"],
            path_patterns=["%Favorites%"],
        )

        assert not result.is_empty
        all_matches = result.get_all_matches()
        matched_names = {m.file_name for m in all_matches}

        assert "Google.url" in matched_names
        assert "Bing.url" in matched_names
        assert "CNN.url" in matched_names
        assert len(all_matches) == 3

    def test_does_not_match_non_url_files(self, populated_db):
        """Non-.url files under Favorites are excluded."""
        result = discover_from_file_list(
            populated_db,
            evidence_id=1,
            filename_patterns=["*.url"],
            path_patterns=["%Favorites%"],
        )

        matched_names = {m.file_name for m in result.get_all_matches()}
        assert "desktop.ini" not in matched_names

    def test_does_not_match_url_files_outside_favorites(self, populated_db):
        """.url files outside Favorites paths are excluded."""
        result = discover_from_file_list(
            populated_db,
            evidence_id=1,
            filename_patterns=["*.url"],
            path_patterns=["%Favorites%"],
        )

        matched_names = {m.file_name for m in result.get_all_matches()}
        assert "shortcut.url" not in matched_names

    def test_empty_file_list_returns_empty(self, evidence_db):
        """Empty file_list yields empty result."""
        result = discover_from_file_list(
            evidence_db,
            evidence_id=1,
            filename_patterns=["*.url"],
            path_patterns=["%Favorites%"],
        )

        assert result.is_empty
