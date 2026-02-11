"""
Tests for file_list_discovery module.

Tests multi-partition discovery functionality using file_list table.
"""
import sqlite3
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from extractors._shared.file_list_discovery import (
    FileListMatch,
    FileListDiscoveryResult,
    discover_from_file_list,
    glob_to_sql_like,
    check_file_list_available,
    get_partition_stats,
    get_ewf_paths_from_evidence_fs,
)


# ============================================================================
# Test Fixtures
# ============================================================================

@pytest.fixture
def evidence_db(tmp_path):
    """Create an in-memory evidence database with file_list table."""
    db_path = tmp_path / "test_evidence.sqlite"
    conn = sqlite3.connect(str(db_path))

    # Create file_list table matching real schema
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
    """Populate file_list with multi-partition test data."""
    evidence_id = 1

    # Partition 3: Main Windows partition with browser data
    test_files = [
        # Chrome on partition 3
        (evidence_id, "Users/John/AppData/Local/Google/Chrome/User Data/Default/History",
         "History", None, 1024000, 12345, 0, 3),
        (evidence_id, "Users/John/AppData/Local/Google/Chrome/User Data/Default/Cookies",
         "Cookies", None, 512000, 12346, 0, 3),
        (evidence_id, "Users/John/AppData/Local/Google/Chrome/User Data/Profile 1/History",
         "History", None, 800000, 12347, 0, 3),

        # Firefox on partition 3
        (evidence_id, "Users/John/AppData/Roaming/Mozilla/Firefox/Profiles/abc123.default/places.sqlite",
         "places.sqlite", ".sqlite", 2048000, 12350, 0, 3),
        (evidence_id, "Users/John/AppData/Roaming/Mozilla/Firefox/Profiles/abc123.default/cookies.sqlite",
         "cookies.sqlite", ".sqlite", 256000, 12351, 0, 3),

        # Edge on partition 3
        (evidence_id, "Users/John/AppData/Local/Microsoft/Edge/User Data/Default/History",
         "History", None, 900000, 12360, 0, 3),

        # Partition 4: Secondary partition with old Chrome installation
        (evidence_id, "Users/OldUser/AppData/Local/Google/Chrome/User Data/Default/History",
         "History", None, 600000, 22345, 0, 4),
        (evidence_id, "Users/OldUser/AppData/Local/Google/Chrome/User Data/Default/Cookies",
         "Cookies", None, 300000, 22346, 0, 4),

        # Partition 5: Portable Firefox
        (evidence_id, "PortableApps/FirefoxPortable/Data/profile/places.sqlite",
         "places.sqlite", ".sqlite", 1500000, 32350, 0, 5),

        # Deleted file on partition 3 (should be excluded by default)
        (evidence_id, "Users/John/AppData/Local/Google/Chrome/User Data/Default/History-journal",
         "History-journal", None, 50000, 12348, 1, 3),

        # Unrelated files
        (evidence_id, "Windows/System32/config/SYSTEM",
         "SYSTEM", None, 10000000, 99999, 0, 3),
        (evidence_id, "Users/John/Documents/report.docx",
         "report.docx", ".docx", 50000, 88888, 0, 3),
    ]

    evidence_db.executemany("""
        INSERT INTO file_list
        (evidence_id, file_path, file_name, extension, size_bytes, inode, deleted, partition_index)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, test_files)
    evidence_db.commit()

    return evidence_db


@pytest.fixture
def ntfs_inode_db(evidence_db):
    """
    Populate file_list with NTFS-style inodes (MFT-ATTR-ID format).

    NTFS inodes from SleuthKit bodyfile format look like "3869-128-4"
    where: MFT_RECORD-ATTRIBUTE_TYPE-ATTRIBUTE_ID
    """
    evidence_id = 1

    # Insert with TEXT inode values (NTFS MFT format)
    evidence_db.execute("""
        INSERT INTO file_list
        (evidence_id, file_path, file_name, extension, size_bytes, inode, deleted, partition_index)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (evidence_id, "/Users/Acer/AppData/Local/Microsoft/Windows/WebCache/WebCacheV01.dat",
          "WebCacheV01.dat", ".dat", 15728640, "3869-128-4", 0, 3))

    evidence_db.execute("""
        INSERT INTO file_list
        (evidence_id, file_path, file_name, extension, size_bytes, inode, deleted, partition_index)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (evidence_id, "/Windows/System32/config/systemprofile/AppData/Local/Microsoft/Windows/WebCache/WebCacheV01.dat",
          "WebCacheV01.dat", ".dat", 26738688, "12563-128-5", 0, 3))

    evidence_db.execute("""
        INSERT INTO file_list
        (evidence_id, file_path, file_name, extension, size_bytes, inode, deleted, partition_index)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (evidence_id, "/Windows.old/Users/LIDIA-1/AppData/Local/Microsoft/Windows/WebCache/WebCacheV01.dat",
          "WebCacheV01.dat", ".dat", 75497472, "89697-128-4", 0, 3))

    evidence_db.commit()
    return evidence_db


# ============================================================================
# Tests: glob_to_sql_like
# ============================================================================

class TestGlobToSqlLike:
    """Tests for glob pattern to SQL LIKE conversion."""

    def test_simple_wildcard(self):
        """Single * converts to %."""
        assert glob_to_sql_like("Users/*/AppData") == "Users/%/AppData"

    def test_double_wildcard(self):
        """** converts to % (flattened)."""
        assert glob_to_sql_like("**/*.sqlite") == "%/%.sqlite"

    def test_question_mark(self):
        """? converts to _."""
        assert glob_to_sql_like("file?.db") == "file_.db"

    def test_no_wildcards(self):
        """Literal patterns pass through unchanged."""
        assert glob_to_sql_like("History") == "History"
        assert glob_to_sql_like("places.sqlite") == "places.sqlite"

    def test_escape_sql_chars(self):
        """SQL special characters are escaped."""
        assert glob_to_sql_like("file%name") == "file\\%name"
        assert glob_to_sql_like("file_name") == "file\\_name"

    def test_complex_pattern(self):
        """Complex patterns convert correctly."""
        result = glob_to_sql_like("Users/*/AppData/Local/Google/Chrome/*/History")
        assert result == "Users/%/AppData/Local/Google/Chrome/%/History"


# ============================================================================
# Tests: FileListMatch and FileListDiscoveryResult
# ============================================================================

class TestFileListMatch:
    """Tests for FileListMatch dataclass."""

    def test_to_dict(self):
        """to_dict returns expected keys."""
        match = FileListMatch(
            file_path="Users/John/History",
            file_name="History",
            partition_index=3,
            inode=12345,
            size_bytes=1024,
            extension=None,
        )

        d = match.to_dict()
        assert d["file_path"] == "Users/John/History"
        assert d["file_name"] == "History"
        assert d["partition_index"] == 3
        assert d["inode"] == 12345
        assert d["size_bytes"] == 1024
        assert d["logical_path"] == "Users/John/History"  # Alias
        assert d["path"] == "Users/John/History"  # Alias


class TestFileListDiscoveryResult:
    """Tests for FileListDiscoveryResult dataclass."""

    def test_is_empty(self):
        """is_empty returns True when no matches."""
        result = FileListDiscoveryResult()
        assert result.is_empty is True
        assert result.is_multi_partition is False

    def test_is_multi_partition(self):
        """is_multi_partition detects multiple partitions."""
        result = FileListDiscoveryResult(
            matches_by_partition={3: [], 4: []},
            partitions_with_matches=[3, 4],
            total_matches=5,
        )
        assert result.is_multi_partition is True

    def test_single_partition(self):
        """Single partition not flagged as multi."""
        result = FileListDiscoveryResult(
            matches_by_partition={3: []},
            partitions_with_matches=[3],
            total_matches=5,
        )
        assert result.is_multi_partition is False

    def test_get_all_matches(self):
        """get_all_matches flattens partition groups."""
        match1 = FileListMatch("path1", "file1", 3)
        match2 = FileListMatch("path2", "file2", 3)
        match3 = FileListMatch("path3", "file3", 4)

        result = FileListDiscoveryResult(
            matches_by_partition={3: [match1, match2], 4: [match3]},
            total_matches=3,
            partitions_with_matches=[3, 4],
        )

        all_matches = result.get_all_matches()
        assert len(all_matches) == 3
        assert match1 in all_matches
        assert match3 in all_matches

    def test_get_partition_summary(self):
        """get_partition_summary returns readable string."""
        match1 = FileListMatch("path1", "file1", 3)
        match2 = FileListMatch("path2", "file2", 4)

        result = FileListDiscoveryResult(
            matches_by_partition={3: [match1], 4: [match2]},
            total_matches=2,
            partitions_with_matches=[3, 4],
        )

        summary = result.get_partition_summary()
        assert "2 matches" in summary
        assert "partition 3" in summary
        assert "partition 4" in summary


# ============================================================================
# Tests: discover_from_file_list
# ============================================================================

class TestDiscoverFromFileList:
    """Tests for discover_from_file_list function."""

    def test_no_patterns_returns_empty(self, evidence_db):
        """Returns empty result if no patterns specified."""
        result = discover_from_file_list(evidence_db, 1)
        assert result.is_empty

    def test_discover_by_filename(self, populated_db):
        """Discover files by exact filename."""
        result = discover_from_file_list(
            populated_db, 1,
            filename_patterns=["History"],
        )

        # Should find History files on partitions 3 and 4
        assert not result.is_empty
        assert result.is_multi_partition
        assert 3 in result.partitions_with_matches
        assert 4 in result.partitions_with_matches

        # Check counts
        assert len(result.matches_by_partition[3]) == 3  # Chrome Default, Profile 1, Edge
        assert len(result.matches_by_partition[4]) == 1  # Old Chrome

    def test_discover_by_filename_and_path(self, populated_db):
        """Discover files by filename AND path pattern."""
        result = discover_from_file_list(
            populated_db, 1,
            filename_patterns=["History"],
            path_patterns=["%Chrome%"],
        )

        # Should find only Chrome History files
        assert not result.is_empty
        assert result.is_multi_partition

        # Partition 3: Chrome Default + Profile 1
        assert len(result.matches_by_partition[3]) == 2
        # Partition 4: Old Chrome
        assert len(result.matches_by_partition[4]) == 1
        # Edge should be excluded
        assert result.total_matches == 3

    def test_discover_by_extension(self, populated_db):
        """Discover files by extension."""
        result = discover_from_file_list(
            populated_db, 1,
            extension_filter=[".sqlite"],
        )

        # Should find Firefox places.sqlite and cookies.sqlite
        assert not result.is_empty
        assert result.total_matches == 3  # 2 on partition 3, 1 on partition 5
        assert 3 in result.partitions_with_matches
        assert 5 in result.partitions_with_matches

    def test_discover_excludes_deleted(self, populated_db):
        """Deleted files excluded by default."""
        result = discover_from_file_list(
            populated_db, 1,
            filename_patterns=["History-journal"],
        )

        # Deleted file should be excluded
        assert result.is_empty

    def test_discover_includes_deleted_when_requested(self, populated_db):
        """Can include deleted files."""
        result = discover_from_file_list(
            populated_db, 1,
            filename_patterns=["History-journal"],
            exclude_deleted=False,
        )

        # Now should find the deleted file
        assert not result.is_empty
        assert result.total_matches == 1

    def test_discover_with_partition_filter(self, populated_db):
        """Can filter to specific partitions."""
        result = discover_from_file_list(
            populated_db, 1,
            filename_patterns=["History"],
            partition_filter={3},
        )

        # Should only find files on partition 3
        assert not result.is_empty
        assert not result.is_multi_partition
        assert result.partitions_with_matches == [3]
        assert 4 not in result.matches_by_partition

    def test_discover_firefox_sqlite(self, populated_db):
        """Discover Firefox SQLite databases."""
        result = discover_from_file_list(
            populated_db, 1,
            filename_patterns=["places.sqlite"],
            path_patterns=["%Firefox%"],
        )

        # Should find Firefox on partition 3 and portable on partition 5
        assert result.is_multi_partition
        assert 3 in result.partitions_with_matches
        assert 5 in result.partitions_with_matches

    def test_glob_pattern_in_filename(self, populated_db):
        """Glob patterns work in filename."""
        result = discover_from_file_list(
            populated_db, 1,
            filename_patterns=["*.sqlite"],
        )

        # Should find all .sqlite files
        assert result.total_matches == 3

    def test_nonexistent_evidence_id(self, populated_db):
        """Returns empty for wrong evidence_id."""
        result = discover_from_file_list(
            populated_db, 999,
            filename_patterns=["History"],
        )

        assert result.is_empty

    def test_ntfs_mft_inode_format(self, ntfs_inode_db):
        """
        Parse NTFS MFT inode format (e.g., '3869-128-4').

        SleuthKit bodyfile format encodes NTFS inodes as MFT_RECORD-ATTR_TYPE-ATTR_ID.
        The discovery should extract the MFT record number from this format.
        """
        result = discover_from_file_list(
            ntfs_inode_db, 1,
            filename_patterns=["WebCacheV01.dat"],
            path_patterns=["%WebCache%"],
        )

        # Should find all 3 WebCache files
        assert result.total_matches == 3
        assert result.partitions_with_matches == [3]

        matches = result.matches_by_partition[3]

        # Verify inodes are extracted correctly (MFT record number only)
        inodes = {m.inode for m in matches}
        assert 3869 in inodes   # From "3869-128-4"
        assert 12563 in inodes  # From "12563-128-5"
        assert 89697 in inodes  # From "89697-128-4"

        # Verify all paths are found including Windows.old
        paths = {m.file_path for m in matches}
        assert "/Users/Acer/AppData/Local/Microsoft/Windows/WebCache/WebCacheV01.dat" in paths
        assert "/Windows/System32/config/systemprofile/AppData/Local/Microsoft/Windows/WebCache/WebCacheV01.dat" in paths
        assert "/Windows.old/Users/LIDIA-1/AppData/Local/Microsoft/Windows/WebCache/WebCacheV01.dat" in paths


# ============================================================================
# Tests: Utility Functions
# ============================================================================

class TestCheckFileListAvailable:
    """Tests for check_file_list_available function."""

    def test_empty_database(self, evidence_db):
        """Returns False for empty file_list."""
        available, count = check_file_list_available(evidence_db, 1)
        assert available is False
        assert count == 0

    def test_populated_database(self, populated_db):
        """Returns True for populated file_list."""
        available, count = check_file_list_available(populated_db, 1)
        assert available is True
        assert count > 0


class TestGetPartitionStats:
    """Tests for get_partition_stats function."""

    def test_partition_stats(self, populated_db):
        """Returns correct per-partition counts."""
        stats = get_partition_stats(populated_db, 1)

        assert 3 in stats
        assert 4 in stats
        assert 5 in stats

        # Partition 3 has most files
        assert stats[3] > stats[4]
        assert stats[3] > stats[5]


class TestGetEwfPathsFromEvidenceFs:
    """Tests for get_ewf_paths_from_evidence_fs function."""

    def test_with_ewf_paths_attribute(self):
        """Extracts from ewf_paths attribute."""
        mock_fs = MagicMock()
        mock_fs.ewf_paths = [Path("/evidence/image.E01")]

        result = get_ewf_paths_from_evidence_fs(mock_fs)
        assert result == [Path("/evidence/image.E01")]

    def test_with_source_path_attribute(self):
        """Falls back to source_path attribute."""
        mock_fs = MagicMock()
        mock_fs.ewf_paths = None
        mock_fs.source_path = Path("/evidence/disk.E01")

        result = get_ewf_paths_from_evidence_fs(mock_fs)
        assert result == [Path("/evidence/disk.E01")]

    def test_with_non_ewf_source(self):
        """Returns None for non-EWF sources."""
        mock_fs = MagicMock()
        mock_fs.ewf_paths = None
        mock_fs.source_path = Path("/mnt/evidence")

        result = get_ewf_paths_from_evidence_fs(mock_fs)
        assert result is None

    def test_with_no_attributes(self):
        """Returns None when no relevant attributes."""
        mock_fs = MagicMock(spec=[])

        result = get_ewf_paths_from_evidence_fs(mock_fs)
        assert result is None


# ============================================================================
# Tests: Integration with Real Evidence Database
# ============================================================================

class TestIntegrationWithRealDatabase:
    """Integration tests using actual evidence database structure."""

    @pytest.fixture
    def real_schema_db(self, tmp_path):
        """Create DB with real evidence schema."""
        db_path = tmp_path / "evidence.sqlite"
        conn = sqlite3.connect(str(db_path))

        # Use real schema with all columns
        conn.execute("""
            CREATE TABLE file_list (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                partition_index INTEGER,
                import_timestamp TEXT,
                run_id TEXT
            )
        """)

        # Create indexes like real schema
        conn.execute("CREATE INDEX idx_file_list_evidence_extension ON file_list(evidence_id, extension)")
        conn.execute("CREATE INDEX idx_file_list_name ON file_list(file_name)")

        conn.commit()
        yield conn
        conn.close()

    def test_query_performance_with_index(self, real_schema_db):
        """Queries use indexes efficiently."""
        # Insert many files to test index usage
        evidence_id = 1
        files = []
        for i in range(10000):
            partition = 3 if i % 10 != 0 else 4
            files.append((
                evidence_id,
                f"Users/User{i % 100}/AppData/Local/App{i}/data.db",
                "data.db",
                ".db",
                1000 + i,
                i,
                0,
                partition,
            ))

        # Add some History files we're looking for
        for i in range(5):
            files.append((
                evidence_id,
                f"Users/User{i}/AppData/Local/Google/Chrome/User Data/Default/History",
                "History",
                None,
                500000,
                90000 + i,
                0,
                3,
            ))

        real_schema_db.executemany("""
            INSERT INTO file_list
            (evidence_id, file_path, file_name, extension, size_bytes, inode, deleted, partition_index)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, files)
        real_schema_db.commit()

        # Query should be fast with index
        result = discover_from_file_list(
            real_schema_db, 1,
            filename_patterns=["History"],
            path_patterns=["%Chrome%"],
        )

        assert result.total_matches == 5
        assert 3 in result.partitions_with_matches
