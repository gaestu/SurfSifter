"""
Tests for IE INetCookies discovery via file_list.

Verifies that .cookie files under INetCookies paths are correctly discovered
even when file_list paths have a leading slash that the glob patterns lack.
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
    """Populate file_list with INetCookies test data."""
    evidence_id = 1

    test_files = [
        # INetCookies .cookie files (leading slash in path)
        (evidence_id,
         "/Users/testuser/AppData/Local/Microsoft/Windows/INetCookies/test.cookie",
         "test.cookie", ".cookie", 256, None, None, None, 2001, 0, 3),
        (evidence_id,
         "/Users/testuser/AppData/Local/Microsoft/Windows/INetCookies/Low/tracking.cookie",
         "tracking.cookie", ".cookie", 512, None, None, None, 2002, 0, 3),

        # Legacy text-based cookies
        (evidence_id,
         "/Users/testuser/AppData/Roaming/Microsoft/Windows/Cookies/user@example[1].txt",
         "user@example[1].txt", ".txt", 180, None, None, None, 2003, 0, 3),

        # System profile cookie
        (evidence_id,
         "/Windows/System32/config/systemprofile/AppData/Local/Microsoft/Windows/INetCookies/sys.cookie",
         "sys.cookie", ".cookie", 100, None, None, None, 2004, 0, 3),

        # Unrelated file (should NOT match)
        (evidence_id,
         "/Users/testuser/Documents/report.docx",
         "report.docx", ".docx", 50000, None, None, None, 9999, 0, 3),
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


class TestIeInetcookiesDiscovery:
    """Tests for IE INetCookies file discovery with leading-slash normalization."""

    def test_glob_pattern_matches_leading_slash_path(self, populated_db):
        """Glob pattern without leading slash matches file_list paths with leading slash."""
        result = discover_from_file_list(
            populated_db,
            evidence_id=1,
            filename_patterns=["*.cookie"],
            path_patterns=[
                "Users/*/AppData/Local/Microsoft/Windows/INetCookies/*.cookie",
            ],
        )

        assert not result.is_empty
        matched_names = {m.file_name for m in result.get_all_matches()}
        assert "test.cookie" in matched_names

    def test_glob_pattern_matches_low_integrity_path(self, populated_db):
        """Glob pattern matches Low integrity INetCookies subfolder."""
        result = discover_from_file_list(
            populated_db,
            evidence_id=1,
            filename_patterns=["*.cookie"],
            path_patterns=[
                "Users/*/AppData/Local/Microsoft/Windows/INetCookies/Low/*.cookie",
            ],
        )

        assert not result.is_empty
        matched_names = {m.file_name for m in result.get_all_matches()}
        assert "tracking.cookie" in matched_names

    def test_does_not_match_unrelated_files(self, populated_db):
        """Unrelated files are not matched by INetCookies patterns."""
        result = discover_from_file_list(
            populated_db,
            evidence_id=1,
            filename_patterns=["*.cookie"],
            path_patterns=[
                "Users/*/AppData/Local/Microsoft/Windows/INetCookies/*.cookie",
            ],
        )

        matched_names = {m.file_name for m in result.get_all_matches()}
        assert "report.docx" not in matched_names

    def test_pattern_starting_with_percent_not_double_prefixed(self, populated_db):
        """Patterns already starting with % are NOT double-prefixed."""
        result = discover_from_file_list(
            populated_db,
            evidence_id=1,
            filename_patterns=["*.cookie"],
            path_patterns=["%INetCookies%"],
        )

        assert not result.is_empty
        matched_names = {m.file_name for m in result.get_all_matches()}
        assert "test.cookie" in matched_names
        assert "tracking.cookie" in matched_names
        assert "sys.cookie" in matched_names

    def test_pattern_starting_with_slash_not_prefixed(self, populated_db):
        """Patterns starting with / are NOT prefixed with %."""
        result = discover_from_file_list(
            populated_db,
            evidence_id=1,
            filename_patterns=["*.cookie"],
            path_patterns=[
                "/Users/*/AppData/Local/Microsoft/Windows/INetCookies/*.cookie",
            ],
        )

        assert not result.is_empty
        matched_names = {m.file_name for m in result.get_all_matches()}
        assert "test.cookie" in matched_names

    def test_empty_file_list_returns_empty(self, evidence_db):
        """Empty file_list yields empty result."""
        result = discover_from_file_list(
            evidence_db,
            evidence_id=1,
            filename_patterns=["*.cookie"],
            path_patterns=[
                "Users/*/AppData/Local/Microsoft/Windows/INetCookies/*.cookie",
            ],
        )

        assert result.is_empty
