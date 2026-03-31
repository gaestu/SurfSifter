"""
Tests for deleted-artifact discovery controls (Workstream E).

Verifies that:
- discover_from_file_list with exclude_deleted=True (default) excludes deleted rows
- discover_from_file_list with exclude_deleted=False includes deleted rows
- FileListMatch.deleted tracks provenance
- Config include_deleted=True is properly passed through by extractors
"""
import sqlite3
import pytest
from unittest.mock import MagicMock, patch

from extractors._shared.file_list_discovery import (
    FileListMatch,
    discover_from_file_list,
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def evidence_db_with_deleted(tmp_path):
    """Create evidence DB with both normal and deleted file_list rows."""
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
            inode INTEGER,
            deleted INTEGER DEFAULT 0,
            partition_index INTEGER
        )
    """)

    evidence_id = 1
    rows = [
        # Normal (not deleted) recovery file
        (evidence_id,
         "Users/John/AppData/Local/Microsoft/Internet Explorer/Recovery/Active/abc.dat",
         "abc.dat", ".dat", 4096, 100, 0, 3),
        # Deleted recovery file
        (evidence_id,
         "Users/John/AppData/Local/Microsoft/Internet Explorer/Recovery/Active/deleted.dat",
         "deleted.dat", ".dat", 2048, 101, 1, 3),
        # Normal DOMStore file
        (evidence_id,
         "Users/John/AppData/Local/Packages/Microsoft.MicrosoftEdge_8wekyb3d8bbwe/AC/MicrosoftEdge/User/Default/DOMStore/foo.xml",
         "foo.xml", ".xml", 512, 200, 0, 3),
        # Deleted DOMStore file
        (evidence_id,
         "Users/John/AppData/Local/Packages/Microsoft.MicrosoftEdge_8wekyb3d8bbwe/AC/MicrosoftEdge/User/Default/DOMStore/bar.xml",
         "bar.xml", ".xml", 256, 201, 1, 3),
        # Normal INetCookies file
        (evidence_id,
         "Users/John/AppData/Local/Microsoft/Windows/INetCookies/cookie1.cookie",
         "cookie1.cookie", ".cookie", 128, 300, 0, 3),
        # Deleted INetCookies file
        (evidence_id,
         "Users/John/AppData/Local/Microsoft/Windows/INetCookies/cookie2.cookie",
         "cookie2.cookie", ".cookie", 64, 301, 1, 3),
    ]

    conn.executemany("""
        INSERT INTO file_list
        (evidence_id, file_path, file_name, extension, size_bytes, inode, deleted, partition_index)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    yield conn
    conn.close()


# ============================================================================
# Tests: discover_from_file_list deleted filtering
# ============================================================================

class TestDeletedFiltering:
    """Tests that exclude_deleted parameter works correctly."""

    def test_default_excludes_deleted(self, evidence_db_with_deleted):
        """Default (exclude_deleted=True) hides deleted rows."""
        result = discover_from_file_list(
            evidence_db_with_deleted, 1,
            path_patterns=["%Recovery%Active%"],
        )
        assert result.total_matches == 1
        match = result.get_all_matches()[0]
        assert "deleted.dat" not in match.file_name
        assert match.deleted is False

    def test_exclude_deleted_false_includes_all(self, evidence_db_with_deleted):
        """exclude_deleted=False returns both normal and deleted rows."""
        result = discover_from_file_list(
            evidence_db_with_deleted, 1,
            path_patterns=["%Recovery%Active%"],
            exclude_deleted=False,
        )
        assert result.total_matches == 2
        names = {m.file_name for m in result.get_all_matches()}
        assert "abc.dat" in names
        assert "deleted.dat" in names

    def test_deleted_flag_set_on_match(self, evidence_db_with_deleted):
        """FileListMatch.deleted is True for deleted rows."""
        result = discover_from_file_list(
            evidence_db_with_deleted, 1,
            path_patterns=["%Recovery%Active%"],
            exclude_deleted=False,
        )
        matches = result.get_all_matches()
        by_name = {m.file_name: m for m in matches}
        assert by_name["abc.dat"].deleted is False
        assert by_name["deleted.dat"].deleted is True

    def test_deleted_in_to_dict(self, evidence_db_with_deleted):
        """FileListMatch.to_dict() includes deleted key only when True."""
        result = discover_from_file_list(
            evidence_db_with_deleted, 1,
            path_patterns=["%Recovery%Active%"],
            exclude_deleted=False,
        )
        by_name = {m.file_name: m for m in result.get_all_matches()}
        assert "deleted" not in by_name["abc.dat"].to_dict()
        assert by_name["deleted.dat"].to_dict()["deleted"] is True

    def test_inetcookies_default_excludes_deleted(self, evidence_db_with_deleted):
        """INetCookies path discovery excludes deleted by default."""
        result = discover_from_file_list(
            evidence_db_with_deleted, 1,
            path_patterns=["%INetCookies%"],
        )
        assert result.total_matches == 1
        assert result.get_all_matches()[0].file_name == "cookie1.cookie"

    def test_inetcookies_include_deleted(self, evidence_db_with_deleted):
        """INetCookies path discovery with exclude_deleted=False includes deleted."""
        result = discover_from_file_list(
            evidence_db_with_deleted, 1,
            path_patterns=["%INetCookies%"],
            exclude_deleted=False,
        )
        assert result.total_matches == 2

    def test_domstore_default_excludes_deleted(self, evidence_db_with_deleted):
        """DOMStore path discovery excludes deleted by default."""
        result = discover_from_file_list(
            evidence_db_with_deleted, 1,
            path_patterns=["%DOMStore%"],
        )
        assert result.total_matches == 1
        assert result.get_all_matches()[0].file_name == "foo.xml"

    def test_domstore_include_deleted(self, evidence_db_with_deleted):
        """DOMStore path discovery with exclude_deleted=False includes deleted."""
        result = discover_from_file_list(
            evidence_db_with_deleted, 1,
            path_patterns=["%DOMStore%"],
            exclude_deleted=False,
        )
        assert result.total_matches == 2


# ============================================================================
# Tests: Extractor config pass-through
# ============================================================================

class TestExtractorConfigPassthrough:
    """Tests that extractor config include_deleted is passed through."""

    def test_tab_recovery_passes_include_deleted(self):
        """IETabRecoveryExtractor passes include_deleted to discovery."""
        from extractors.browser.ie_legacy.tab_recovery.extractor import (
            IETabRecoveryExtractor,
        )
        extractor = IETabRecoveryExtractor()

        mock_conn = MagicMock()
        mock_callbacks = MagicMock()
        mock_callbacks.is_cancelled.return_value = False

        with patch(
            "extractors.browser.ie_legacy.tab_recovery.extractor.discover_from_file_list"
        ) as mock_discover:
            mock_discover.return_value = MagicMock(
                is_empty=True, matches_by_partition={}, get_all_matches=lambda: []
            )
            extractor._discover_files_multi_partition(
                mock_conn, 1, mock_callbacks, include_deleted=True
            )
            mock_discover.assert_called_once()
            _, kwargs = mock_discover.call_args
            assert kwargs["exclude_deleted"] is False

    def test_tab_recovery_default_excludes_deleted(self):
        """IETabRecoveryExtractor excludes deleted by default."""
        from extractors.browser.ie_legacy.tab_recovery.extractor import (
            IETabRecoveryExtractor,
        )
        extractor = IETabRecoveryExtractor()

        mock_conn = MagicMock()
        mock_callbacks = MagicMock()
        mock_callbacks.is_cancelled.return_value = False

        with patch(
            "extractors.browser.ie_legacy.tab_recovery.extractor.discover_from_file_list"
        ) as mock_discover:
            mock_discover.return_value = MagicMock(
                is_empty=True, matches_by_partition={}, get_all_matches=lambda: []
            )
            extractor._discover_files_multi_partition(
                mock_conn, 1, mock_callbacks
            )
            mock_discover.assert_called_once()
            _, kwargs = mock_discover.call_args
            assert kwargs["exclude_deleted"] is True
