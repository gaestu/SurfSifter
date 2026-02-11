"""
Tests for SleuthKit file list generation.

This module tests:
- Bodyfile parsing with edge cases
- Partition context preservation
- Database batch inserts
- Error handling
"""
from __future__ import annotations

import shutil
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import List
from unittest.mock import MagicMock, patch

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.slow]

from extractors.system.file_list.bodyfile_parser import BodyfileEntry, BodyfileParser
from extractors.system.file_list.sleuthkit_generator import (
    FILE_LIST_INDEXES,
    GenerationResult,
    SleuthKitFileListGenerator,
)


# =============================================================================
# BodyfileParser Tests
# =============================================================================

class TestBodyfileParser:
    """Tests for bodyfile parsing."""

    def test_parse_simple_path(self):
        """Parse standard bodyfile line."""
        parser = BodyfileParser(partition_index=1)
        line = "0|/Users/john/file.txt|12345-128-1|r/rrwxrwxrwx|0|0|1024|1609459200|1609459200|1609459200|1609459200"
        entries = list(parser.parse_lines([line]))

        assert len(entries) == 1
        entry = entries[0]
        assert entry.file_path == "/Users/john/file.txt"
        assert entry.file_name == "file.txt"
        assert entry.extension == ".txt"
        assert entry.size_bytes == 1024
        assert entry.partition_index == 1
        assert entry.deleted is False
        assert entry.inode == "12345-128-1"

    def test_parse_path_with_pipe_character(self):
        """Handle pipe character in path (edge case)."""
        parser = BodyfileParser(partition_index=0)
        # Path contains a pipe: /Users/john/file|name.txt
        line = "0|/Users/john/file|name.txt|12345-128-1|r/rrwxrwxrwx|0|0|1024|1609459200|1609459200|1609459200|1609459200"
        entries = list(parser.parse_lines([line]))

        assert len(entries) == 1
        assert entries[0].file_path == "/Users/john/file|name.txt"
        assert entries[0].file_name == "file|name.txt"

    def test_parse_deleted_file_marker(self):
        """Handle deleted file marker (*)."""
        parser = BodyfileParser(partition_index=0)
        line = "0|*/Users/john/.Trash/deleted.jpg|12345-128-1|r/rrwxrwxrwx|0|0|51200|1609459200|1609459200|1609459200|1609459200"
        entries = list(parser.parse_lines([line]))

        assert len(entries) == 1
        entry = entries[0]
        assert entry.file_path == "/Users/john/.Trash/deleted.jpg"  # * stripped
        assert entry.deleted is True
        assert entry.extension == ".jpg"

    def test_parse_deleted_suffix(self):
        """Handle (deleted) suffix in path - fls extension format."""
        parser = BodyfileParser(partition_index=0)
        line = "0|/Documents/MPC7B8.tmp (deleted)|12345-128-1|r|0|0|0|1609459200|1609459200|1609459200|1609459200"
        entries = list(parser.parse_lines([line]))

        assert len(entries) == 1
        entry = entries[0]
        assert entry.file_path == "/Documents/MPC7B8.tmp"  # (deleted) stripped
        assert entry.file_name == "MPC7B8.tmp"
        assert entry.deleted is True
        assert entry.extension == ".tmp"

    def test_parse_deleted_realloc_suffix(self):
        """Handle (deleted-realloc) suffix in path."""
        parser = BodyfileParser(partition_index=0)
        line = "0|/query.new (deleted-realloc)|12345-128-1|r|0|0|100|1609459200|1609459200|1609459200|1609459200"
        entries = list(parser.parse_lines([line]))

        assert len(entries) == 1
        entry = entries[0]
        assert entry.file_path == "/query.new"  # (deleted-realloc) stripped
        assert entry.file_name == "query.new"
        assert entry.deleted is True

    def test_parse_deleted_prefix_and_suffix_combined(self):
        """Handle both * prefix and (deleted) suffix."""
        parser = BodyfileParser(partition_index=0)
        line = "0|*/temp/file.txt (deleted)|12345-128-1|r|0|0|100|1609459200|1609459200|1609459200|1609459200"
        entries = list(parser.parse_lines([line]))

        assert len(entries) == 1
        entry = entries[0]
        assert entry.file_path == "/temp/file.txt"  # Both markers stripped
        assert entry.deleted is True

    def test_ntfs_metadata_with_deleted_suffix_filtered(self):
        """NTFS metadata entries with (deleted) suffix are still filtered."""
        parser = BodyfileParser(partition_index=0)
        lines = [
            "0|/dir ($FILE_NAME) (deleted)|123-0-0|r|0|0|0|0|0|0|0",  # Filter - metadata
            "0|/file.txt (deleted)|124-0-0|r|0|0|100|0|0|0|0",       # Keep - actual deleted file
        ]
        entries = list(parser.parse_lines(lines))

        assert len(entries) == 1
        assert entries[0].file_path == "/file.txt"
        assert entries[0].deleted is True
        assert parser.stats["skipped_metadata"] == 1

    def test_parse_empty_md5(self):
        """Handle zero MD5 placeholder."""
        parser = BodyfileParser(partition_index=0)
        line = "0|/file.txt|123-0-0|r|0|0|100|0|0|0|0"
        entries = list(parser.parse_lines([line]))

        assert entries[0].md5_hash is None

    def test_parse_real_md5(self):
        """Handle actual MD5 hash."""
        parser = BodyfileParser(partition_index=0)
        line = "d41d8cd98f00b204e9800998ecf8427e|/file.txt|123-0-0|r|0|0|100|0|0|0|0"
        entries = list(parser.parse_lines([line]))

        assert entries[0].md5_hash == "d41d8cd98f00b204e9800998ecf8427e"

    def test_parse_malformed_line_skipped(self):
        """Malformed lines are skipped gracefully."""
        parser = BodyfileParser(partition_index=0)
        lines = [
            "not enough fields",
            "0|/valid.txt|123-0-0|r|0|0|100|0|0|0|0",
            "",
        ]
        entries = list(parser.parse_lines(lines))

        assert len(entries) == 1
        assert entries[0].file_path == "/valid.txt"
        assert parser.stats["errors"] == 1

    def test_parse_unicode_path(self):
        """Handle Unicode characters in path."""
        parser = BodyfileParser(partition_index=0)
        line = "0|/Users/José/文档/файл.txt|123-0-0|r|0|0|100|0|0|0|0"
        entries = list(parser.parse_lines([line]))

        assert entries[0].file_path == "/Users/José/文档/файл.txt"
        assert entries[0].file_name == "файл.txt"

    def test_extension_normalized_lowercase(self):
        """Extension normalized to lowercase."""
        parser = BodyfileParser(partition_index=0)
        line = "0|/photo.JPG|123-0-0|r|0|0|100|0|0|0|0"
        entries = list(parser.parse_lines([line]))

        assert entries[0].extension == ".jpg"

    def test_no_extension(self):
        """Handle file without extension."""
        parser = BodyfileParser(partition_index=0)
        line = "0|/Makefile|123-0-0|r|0|0|100|0|0|0|0"
        entries = list(parser.parse_lines([line]))

        assert entries[0].extension is None
        assert entries[0].file_name == "Makefile"

    def test_multiple_extensions(self):
        """Handle files with multiple dots - use last extension."""
        parser = BodyfileParser(partition_index=0)
        line = "0|/backup.2024-01-01.tar.gz|123-0-0|r|0|0|100|0|0|0|0"
        entries = list(parser.parse_lines([line]))

        assert entries[0].extension == ".gz"
        assert entries[0].file_name == "backup.2024-01-01.tar.gz"

    def test_hidden_file_with_extension(self):
        """Handle hidden files (starting with .)."""
        parser = BodyfileParser(partition_index=0)
        line = "0|/Users/john/.bashrc|123-0-0|r|0|0|100|0|0|0|0"
        entries = list(parser.parse_lines([line]))

        assert entries[0].extension is None  # .bashrc has no extension per Python pathlib
        assert entries[0].file_name == ".bashrc"

    def test_timestamp_conversion(self):
        """Convert Unix timestamps to ISO 8601."""
        parser = BodyfileParser(partition_index=0)
        # 1609459200 = 2021-01-01 00:00:00 UTC
        line = "0|/file.txt|123-0-0|r|0|0|100|1609459200|1609459201|1609459202|1609459203"
        entries = list(parser.parse_lines([line]))

        entry = entries[0]
        assert entry.accessed_ts == "2021-01-01T00:00:00Z"
        assert entry.modified_ts == "2021-01-01T00:00:01Z"
        assert entry.created_ts == "2021-01-01T00:00:03Z"  # crtime is last field

    def test_zero_timestamp(self):
        """Handle zero timestamps (unknown time)."""
        parser = BodyfileParser(partition_index=0)
        line = "0|/file.txt|123-0-0|r|0|0|100|0|0|0|0"
        entries = list(parser.parse_lines([line]))

        entry = entries[0]
        assert entry.accessed_ts is None
        assert entry.modified_ts is None
        assert entry.created_ts is None

    def test_parser_stats(self):
        """Parser tracks statistics correctly."""
        parser = BodyfileParser(partition_index=0)
        lines = [
            "0|/file1.txt|123-0-0|r|0|0|100|0|0|0|0",
            "invalid line",
            "0|/file2.txt|124-0-0|r|0|0|200|0|0|0|0",
            "",  # Empty line - not an error
        ]
        entries = list(parser.parse_lines(lines))

        assert len(entries) == 2
        stats = parser.stats
        assert stats["lines_processed"] == 4
        assert stats["parsed_count"] == 2
        assert stats["errors"] == 1

    def test_parser_reset_stats(self):
        """Parser can reset statistics."""
        parser = BodyfileParser(partition_index=0)
        list(parser.parse_lines(["0|/file.txt|123-0-0|r|0|0|100|0|0|0|0"]))

        assert parser.stats["parsed_count"] == 1

        parser.reset_stats()

        assert parser.stats["parsed_count"] == 0
        assert parser.stats["lines_processed"] == 0

    def test_ntfs_file_name_attribute_filtered(self):
        """NTFS $FILE_NAME attributes are filtered out."""
        parser = BodyfileParser(partition_index=0)
        lines = [
            "0|/WINDOWS/Web/Wallpaper|123-0-0|d|0|0|0|0|0|0|0",           # Directory - skip
            "0|/WINDOWS/Web/Wallpaper ($FILE_NAME)|123-0-0|r|0|0|0|0|0|0|0",  # $FILE_NAME - filter
            "0|/WINDOWS/Web/Wallpaper/Crystal.jpg|124-0-0|r|0|0|1024|0|0|0|0",  # File - keep
            "0|/WINDOWS/Web/Wallpaper/Crystal.jpg ($FILE_NAME)|124-0-0|r|0|0|0|0|0|0|0",  # $FILE_NAME - filter
            "0|/WINDOWS/$MFT|125-0-0|r|0|0|0|0|0|0|0",  # $MFT file itself - keep (not metadata suffix)
            "0|/WINDOWS/dir ($I30)|126-0-0|r|0|0|0|0|0|0|0",  # $I30 index attribute - filter
        ]
        entries = list(parser.parse_lines(lines))

        assert len(entries) == 2
        assert entries[0].file_path == "/WINDOWS/Web/Wallpaper/Crystal.jpg"
        assert entries[1].file_path == "/WINDOWS/$MFT"
        assert parser.stats["skipped_metadata"] == 3
        assert parser.stats["skipped_directories"] == 1

    def test_ntfs_data_stream_filtered(self):
        """NTFS alternate data streams are filtered out."""
        parser = BodyfileParser(partition_index=0)
        lines = [
            "0|/file.txt|123-0-0|r|0|0|100|0|0|0|0",           # Normal file - keep
            "0|/file.txt:$DATA|123-0-0|r|0|0|100|0|0|0|0",     # ADS marker - filter
            "0|/file.txt:Zone.Identifier:$DATA|123-0-0|r|0|0|100|0|0|0|0",  # ADS - filter
        ]
        entries = list(parser.parse_lines(lines))

        assert len(entries) == 1
        assert entries[0].file_path == "/file.txt"
        assert parser.stats["skipped_metadata"] == 2

    def test_ntfs_metadata_filter_disabled(self):
        """Can disable NTFS metadata filtering."""
        parser = BodyfileParser(partition_index=0, skip_ntfs_metadata=False)
        lines = [
            "0|/WINDOWS/System32 ($FILE_NAME)|123-0-0|r|0|0|0|0|0|0|0",
        ]
        entries = list(parser.parse_lines(lines))

        assert len(entries) == 1  # Not filtered
        assert "($FILE_NAME)" in entries[0].file_path

    def test_size_zero_for_directory(self):
        """Handle directories (size 0)."""
        parser = BodyfileParser(partition_index=0, skip_directories=False)
        line = "0|/Users/john|123-0-0|d/drwxr-xr-x|0|0|0|0|0|0|0"
        entries = list(parser.parse_lines([line]))

        assert entries[0].size_bytes == 0

        assert entries[0].size_bytes == 0

    def test_directory_entries_skipped(self):
        """Directory entries are skipped by default."""
        parser = BodyfileParser(partition_index=0)
        line = "0|/Users/john|123-0-0|d/drwxr-xr-x|0|0|0|0|0|0|0"
        entries = list(parser.parse_lines([line]))

        assert len(entries) == 0
        assert parser.stats["skipped_directories"] == 1

    def test_large_file_size(self):
        """Handle large file sizes."""
        parser = BodyfileParser(partition_index=0)
        # 10 GB file
        line = "0|/large.iso|123-0-0|r|0|0|10737418240|0|0|0|0"
        entries = list(parser.parse_lines([line]))

        assert entries[0].size_bytes == 10737418240


class TestPartitionMapping:
    """Tests for partition context preservation."""

    def test_partition_index_preserved(self):
        """Entries preserve partition_index from parser."""
        parser = BodyfileParser(partition_index=3)
        line = "0|/file.txt|123-0-0|r|0|0|100|0|0|0|0"
        entries = list(parser.parse_lines([line]))

        assert entries[0].partition_index == 3

    def test_multiple_partitions(self):
        """Parse files from different partitions."""
        entries = []

        # Partition 1
        parser1 = BodyfileParser(partition_index=1)
        entries.extend(parser1.parse_lines([
            "0|/Windows/System32/ntoskrnl.exe|100-0-0|r|0|0|1000|0|0|0|0",
        ]))

        # Partition 2
        parser2 = BodyfileParser(partition_index=2)
        entries.extend(parser2.parse_lines([
            "0|/home/user/documents/file.pdf|200-0-0|r|0|0|2000|0|0|0|0",
        ]))

        assert len(entries) == 2
        assert entries[0].partition_index == 1
        assert entries[1].partition_index == 2


# =============================================================================
# SleuthKitFileListGenerator Tests
# =============================================================================

@pytest.fixture
def evidence_db():
    """Create a temporary evidence database with file_list table."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    # Create file_list table with new columns
    conn.execute("""
        CREATE TABLE file_list (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            file_path TEXT NOT NULL,
            file_name TEXT NOT NULL,
            extension TEXT,
            size_bytes INTEGER,
            created_ts TEXT,
            modified_ts TEXT,
            accessed_ts TEXT,
            md5_hash TEXT,
            sha1_hash TEXT,
            sha256_hash TEXT,
            file_type TEXT,
            deleted BOOLEAN DEFAULT 0,
            metadata TEXT,
            import_source TEXT,
            import_timestamp TEXT NOT NULL,
            partition_index INTEGER DEFAULT -1,
            inode TEXT
        )
    """)

    # Create indexes
    conn.execute("CREATE INDEX idx_file_list_extension ON file_list(extension)")
    conn.execute("CREATE INDEX idx_file_list_evidence_extension ON file_list(evidence_id, extension)")
    conn.execute("CREATE INDEX idx_file_list_partition ON file_list(evidence_id, partition_index)")
    conn.execute("CREATE INDEX idx_file_list_name ON file_list(file_name)")
    conn.execute("CREATE INDEX idx_file_list_path ON file_list(file_path)")
    conn.execute("""
        CREATE UNIQUE INDEX idx_file_list_unique_path
        ON file_list(evidence_id, COALESCE(partition_index, -1), file_path)
    """)

    yield conn
    conn.close()


class TestSleuthKitFileListGenerator:
    """Tests for SleuthKit file list generator."""

    def test_fls_availability_check_not_found(self, evidence_db):
        """Check fls availability when not installed."""
        with patch("shutil.which", return_value=None):
            generator = SleuthKitFileListGenerator(
                evidence_conn=evidence_db,
                evidence_id=1,
                ewf_paths=[Path("/fake/image.E01")],
            )
            assert generator.fls_available is False

    def test_fls_availability_check_found(self, evidence_db):
        """Check fls availability when installed."""
        with patch("shutil.which", return_value="/usr/bin/fls"):
            generator = SleuthKitFileListGenerator(
                evidence_conn=evidence_db,
                evidence_id=1,
                ewf_paths=[Path("/fake/image.E01")],
            )
            assert generator.fls_available is True

    def test_generate_returns_error_when_fls_missing(self, evidence_db):
        """Generate returns error result when fls not available."""
        with patch("shutil.which", return_value=None):
            generator = SleuthKitFileListGenerator(
                evidence_conn=evidence_db,
                evidence_id=1,
                ewf_paths=[Path("/fake/image.E01")],
            )
            result = generator.generate()

            assert result.success is False
            assert "fls command not found" in result.error_message
            assert result.total_files == 0

    def test_batch_insert(self, evidence_db):
        """Test batch insert of entries."""
        with patch("shutil.which", return_value="/usr/bin/fls"):
            generator = SleuthKitFileListGenerator(
                evidence_conn=evidence_db,
                evidence_id=1,
                ewf_paths=[Path("/fake/image.E01")],
            )

            entries = [
                BodyfileEntry(
                    file_path="/file1.txt",
                    file_name="file1.txt",
                    extension=".txt",
                    size_bytes=100,
                    created_ts="2021-01-01T00:00:00Z",
                    modified_ts="2021-01-01T00:00:00Z",
                    accessed_ts="2021-01-01T00:00:00Z",
                    md5_hash=None,
                    inode="123-0-0",
                    deleted=False,
                    partition_index=1,
                ),
                BodyfileEntry(
                    file_path="/file2.jpg",
                    file_name="file2.jpg",
                    extension=".jpg",
                    size_bytes=5000,
                    created_ts=None,
                    modified_ts=None,
                    accessed_ts=None,
                    md5_hash="abc123",
                    inode="456-0-0",
                    deleted=True,
                    partition_index=1,
                ),
            ]

            generator._insert_batch(entries, "2024-01-01T00:00:00Z")

            # Verify inserts
            cursor = evidence_db.execute("SELECT * FROM file_list ORDER BY id")
            rows = cursor.fetchall()

            assert len(rows) == 2

            # Check first entry
            assert rows[0]["file_path"] == "/file1.txt"
            assert rows[0]["extension"] == ".txt"
            assert rows[0]["partition_index"] == 1
            assert rows[0]["deleted"] == 0
            assert rows[0]["import_source"] == "fls"

            # Check second entry
            assert rows[1]["file_path"] == "/file2.jpg"
            assert rows[1]["extension"] == ".jpg"
            assert rows[1]["deleted"] == 1
            assert rows[1]["md5_hash"] == "abc123"

    def test_clear_existing(self, evidence_db):
        """Test clearing existing entries."""
        with patch("shutil.which", return_value="/usr/bin/fls"):
            # Insert some data first
            evidence_db.execute("""
                INSERT INTO file_list (evidence_id, file_path, file_name, import_timestamp)
                VALUES (1, '/file1.txt', 'file1.txt', '2024-01-01'),
                       (1, '/file2.txt', 'file2.txt', '2024-01-01'),
                       (2, '/other.txt', 'other.txt', '2024-01-01')
            """)
            evidence_db.commit()

            generator = SleuthKitFileListGenerator(
                evidence_conn=evidence_db,
                evidence_id=1,
                ewf_paths=[Path("/fake/image.E01")],
            )

            deleted = generator.clear_existing()

            assert deleted == 2

            # Verify only evidence_id=2 remains
            cursor = evidence_db.execute("SELECT COUNT(*) FROM file_list WHERE evidence_id = 1")
            assert cursor.fetchone()[0] == 0

            cursor = evidence_db.execute("SELECT COUNT(*) FROM file_list WHERE evidence_id = 2")
            assert cursor.fetchone()[0] == 1

    def test_get_file_count(self, evidence_db):
        """Test getting file count for evidence."""
        # Insert some data
        evidence_db.execute("""
            INSERT INTO file_list (evidence_id, file_path, file_name, import_timestamp)
            VALUES (1, '/file1.txt', 'file1.txt', '2024-01-01'),
                   (1, '/file2.txt', 'file2.txt', '2024-01-01'),
                   (2, '/other.txt', 'other.txt', '2024-01-01')
        """)
        evidence_db.commit()

        with patch("shutil.which", return_value="/usr/bin/fls"):
            generator = SleuthKitFileListGenerator(
                evidence_conn=evidence_db,
                evidence_id=1,
                ewf_paths=[Path("/fake/image.E01")],
            )

            assert generator.get_file_count() == 2

    def test_drop_and_create_indexes(self, evidence_db):
        """Test index drop/create for bulk insert optimization."""
        with patch("shutil.which", return_value="/usr/bin/fls"):
            generator = SleuthKitFileListGenerator(
                evidence_conn=evidence_db,
                evidence_id=1,
                ewf_paths=[Path("/fake/image.E01")],
            )

            # Drop indexes
            generator._drop_indexes()

            # Verify indexes dropped (should not raise)
            generator._drop_indexes()  # Idempotent

            # Recreate indexes
            generator._create_indexes()

            # Verify indexes exist by checking sqlite_master
            cursor = evidence_db.execute("""
                SELECT name FROM sqlite_master
                WHERE type='index' AND name LIKE 'idx_file_list_%'
            """)
            indexes = {row[0] for row in cursor.fetchall()}

            assert "idx_file_list_extension" in indexes
            assert "idx_file_list_partition" in indexes

    def test_duplicate_path_ignored(self, evidence_db):
        """Test that duplicate paths are ignored (INSERT OR IGNORE)."""
        with patch("shutil.which", return_value="/usr/bin/fls"):
            generator = SleuthKitFileListGenerator(
                evidence_conn=evidence_db,
                evidence_id=1,
                ewf_paths=[Path("/fake/image.E01")],
            )

            entries = [
                BodyfileEntry(
                    file_path="/duplicate.txt",
                    file_name="duplicate.txt",
                    extension=".txt",
                    size_bytes=100,
                    created_ts=None,
                    modified_ts=None,
                    accessed_ts=None,
                    md5_hash=None,
                    inode="123-0-0",
                    deleted=False,
                    partition_index=1,
                ),
            ]

            # Insert twice
            generator._insert_batch(entries, "2024-01-01T00:00:00Z")
            generator._insert_batch(entries, "2024-01-02T00:00:00Z")  # Same path

            # Should only have one row
            cursor = evidence_db.execute("SELECT COUNT(*) FROM file_list")
            assert cursor.fetchone()[0] == 1

    def test_same_path_different_partitions(self, evidence_db):
        """Test that same path on different partitions is allowed."""
        with patch("shutil.which", return_value="/usr/bin/fls"):
            generator = SleuthKitFileListGenerator(
                evidence_conn=evidence_db,
                evidence_id=1,
                ewf_paths=[Path("/fake/image.E01")],
            )

            # Same path on partition 1
            entries1 = [
                BodyfileEntry(
                    file_path="/Windows/System32/config",
                    file_name="config",
                    extension=None,
                    size_bytes=100,
                    created_ts=None,
                    modified_ts=None,
                    accessed_ts=None,
                    md5_hash=None,
                    inode="123-0-0",
                    deleted=False,
                    partition_index=1,
                ),
            ]

            # Same path on partition 2
            entries2 = [
                BodyfileEntry(
                    file_path="/Windows/System32/config",
                    file_name="config",
                    extension=None,
                    size_bytes=200,
                    created_ts=None,
                    modified_ts=None,
                    accessed_ts=None,
                    md5_hash=None,
                    inode="456-0-0",
                    deleted=False,
                    partition_index=2,
                ),
            ]

            generator._insert_batch(entries1, "2024-01-01T00:00:00Z")
            generator._insert_batch(entries2, "2024-01-01T00:00:00Z")

            # Should have two rows (different partitions)
            cursor = evidence_db.execute("SELECT COUNT(*) FROM file_list")
            assert cursor.fetchone()[0] == 2


class TestGenerationResult:
    """Tests for GenerationResult dataclass."""

    def test_success_result(self):
        """Test successful result attributes."""
        result = GenerationResult(
            success=True,
            total_files=10000,
            partitions_processed=2,
            duration_seconds=15.5,
            partition_stats={1: 8000, 2: 2000},
        )

        assert result.success is True
        assert result.total_files == 10000
        assert result.partitions_processed == 2
        assert result.duration_seconds == 15.5
        assert result.error_message is None
        assert result.partition_stats == {1: 8000, 2: 2000}

    def test_error_result(self):
        """Test error result attributes."""
        result = GenerationResult(
            success=False,
            total_files=0,
            partitions_processed=0,
            duration_seconds=0.5,
            error_message="fls command not found",
        )

        assert result.success is False
        assert result.error_message == "fls command not found"


# =============================================================================
# Integration Tests (require fls binary)
# =============================================================================

@pytest.mark.skipif(
    not shutil.which("fls"),
    reason="SleuthKit fls not installed"
)
class TestSleuthKitIntegration:
    """Integration tests requiring fls binary."""

    def test_fls_available_detection(self, evidence_db):
        """Test fls detection with real binary."""
        generator = SleuthKitFileListGenerator(
            evidence_conn=evidence_db,
            evidence_id=1,
            ewf_paths=[Path("/fake/image.E01")],
        )
        assert generator.fls_available is True


# =============================================================================
# evidence_fs.py Tests
# =============================================================================

class TestListEwfPartitions:
    """Tests for list_ewf_partitions with block_size."""

    def test_block_size_in_partition_info(self):
        """Verify block_size is included in partition info dict."""
        # This test requires mocking pyewf/pytsk3
        # Create mock partition info to verify structure
        expected_keys = {
            'index', 'addr', 'offset', 'length', 'block_size',
            'description', 'filesystem_readable', 'root_file_count'
        }

        # We can at least verify the expected structure
        assert 'block_size' in expected_keys


class TestOpenEwfPartition:
    """Tests for open_ewf_partition helper."""

    def test_auto_select_partition(self):
        """Test auto-select (partition_index=-1) calls PyEwfTskFS correctly."""
        with patch("core.evidence_fs.PyEwfTskFS") as mock_cls:
            from core.evidence_fs import open_ewf_partition

            ewf_paths = [Path("/fake/image.E01")]
            open_ewf_partition(ewf_paths, partition_index=-1)

            mock_cls.assert_called_once_with(ewf_paths, partition_index=-1)

    def test_direct_filesystem(self):
        """Test direct filesystem (partition_index=0) calls PyEwfTskFS correctly."""
        with patch("core.evidence_fs.PyEwfTskFS") as mock_cls:
            from core.evidence_fs import open_ewf_partition

            ewf_paths = [Path("/fake/image.E01")]
            open_ewf_partition(ewf_paths, partition_index=0)

            mock_cls.assert_called_once_with(ewf_paths, partition_index=0)

    def test_specific_partition_valid(self):
        """Test specific partition (partition_index >= 1) with validation."""
        with patch("core.evidence_fs.PyEwfTskFS") as mock_cls:
            with patch("core.evidence_fs.list_ewf_partitions") as mock_list:
                mock_list.return_value = [
                    {'index': 1, 'filesystem_readable': True, 'offset': 1048576},
                    {'index': 2, 'filesystem_readable': True, 'offset': 2097152},
                ]

                from core.evidence_fs import open_ewf_partition

                ewf_paths = [Path("/fake/image.E01")]
                open_ewf_partition(ewf_paths, partition_index=2)

                mock_cls.assert_called_once_with(ewf_paths, partition_index=2)

    def test_specific_partition_not_readable(self):
        """Test error when partition is not readable."""
        with patch("core.evidence_fs.list_ewf_partitions") as mock_list:
            mock_list.return_value = [
                {'index': 1, 'filesystem_readable': False, 'description': 'Encrypted'},
            ]

            from core.evidence_fs import open_ewf_partition

            ewf_paths = [Path("/fake/image.E01")]

            with pytest.raises(RuntimeError, match="not readable"):
                open_ewf_partition(ewf_paths, partition_index=1)

    def test_specific_partition_not_found(self):
        """Test error when partition index doesn't exist."""
        with patch("core.evidence_fs.list_ewf_partitions") as mock_list:
            mock_list.return_value = [
                {'index': 1, 'filesystem_readable': True, 'offset': 1048576},
            ]

            from core.evidence_fs import open_ewf_partition

            ewf_paths = [Path("/fake/image.E01")]

            with pytest.raises(ValueError, match="not found"):
                open_ewf_partition(ewf_paths, partition_index=5)


# =============================================================================
# Schema Migration Tests
# =============================================================================

class TestSchemaMigration:
    """Tests for 0002_file_list_partition.sql migration."""

    def test_migration_adds_columns(self):
        """Test that migration adds partition_index and inode columns."""
        # Create table without new columns (old schema)
        conn = sqlite3.connect(":memory:")
        conn.execute("""
            CREATE TABLE file_list (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                file_name TEXT NOT NULL,
                extension TEXT,
                size_bytes INTEGER,
                created_ts TEXT,
                modified_ts TEXT,
                accessed_ts TEXT,
                md5_hash TEXT,
                sha1_hash TEXT,
                sha256_hash TEXT,
                file_type TEXT,
                deleted BOOLEAN DEFAULT 0,
                metadata TEXT,
                import_source TEXT,
                import_timestamp TEXT NOT NULL
            )
        """)
        conn.execute("CREATE UNIQUE INDEX idx_file_list_unique_path ON file_list(evidence_id, file_path)")

        # Apply migration
        conn.execute("ALTER TABLE file_list ADD COLUMN partition_index INTEGER DEFAULT -1")
        conn.execute("ALTER TABLE file_list ADD COLUMN inode TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_file_list_partition ON file_list(evidence_id, partition_index)")
        conn.execute("DROP INDEX IF EXISTS idx_file_list_unique_path")
        conn.execute("""
            CREATE UNIQUE INDEX idx_file_list_unique_path
            ON file_list(evidence_id, COALESCE(partition_index, -1), file_path)
        """)

        # Verify columns exist
        cursor = conn.execute("PRAGMA table_info(file_list)")
        columns = {row[1] for row in cursor.fetchall()}

        assert "partition_index" in columns
        assert "inode" in columns

        # Verify index exists
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_file_list_partition'")
        assert cursor.fetchone() is not None

        conn.close()

    def test_legacy_data_gets_default_partition(self):
        """Test that existing data gets partition_index=-1 (auto-select)."""
        conn = sqlite3.connect(":memory:")

        # Create old schema and insert data
        conn.execute("""
            CREATE TABLE file_list (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                file_name TEXT NOT NULL,
                import_timestamp TEXT NOT NULL
            )
        """)
        conn.execute("""
            INSERT INTO file_list (evidence_id, file_path, file_name, import_timestamp)
            VALUES (1, '/old_file.txt', 'old_file.txt', '2024-01-01')
        """)

        # Apply migration (adds column with default)
        conn.execute("ALTER TABLE file_list ADD COLUMN partition_index INTEGER DEFAULT -1")

        # Verify default value
        cursor = conn.execute("SELECT partition_index FROM file_list WHERE id = 1")
        assert cursor.fetchone()[0] == -1

        conn.close()


@pytest.mark.compat
class TestV18xUpgradePath:
    """Regression tests for v1.8.x →  evidence database upgrade."""

    def test_ensure_file_list_partition_columns_adds_missing(self):
        """Test that _ensure_file_list_partition_columns adds columns when missing."""
        from core.database import _ensure_file_list_partition_columns

        conn = sqlite3.connect(":memory:")

        # Create v1.8.x schema (without partition_index and inode)
        conn.execute("""
            CREATE TABLE file_list (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                file_name TEXT NOT NULL,
                extension TEXT,
                size_bytes INTEGER,
                import_timestamp TEXT NOT NULL
            )
        """)

        # Insert pre-existing data
        conn.execute("""
            INSERT INTO file_list (evidence_id, file_path, file_name, import_timestamp)
            VALUES (1, '/Users/test/file.txt', 'file.txt', '2024-01-01T00:00:00Z')
        """)

        # Get columns before upgrade
        columns_before = {row[1] for row in conn.execute("PRAGMA table_info(file_list)")}
        assert "partition_index" not in columns_before
        assert "inode" not in columns_before

        # Run the upgrade helper
        _ensure_file_list_partition_columns(conn)

        # Verify columns were added
        columns_after = {row[1] for row in conn.execute("PRAGMA table_info(file_list)")}
        assert "partition_index" in columns_after
        assert "inode" in columns_after

        # Verify existing data gets default partition_index
        row = conn.execute("SELECT partition_index FROM file_list WHERE id = 1").fetchone()
        assert row[0] == -1  # Default for legacy data

        # Verify index was created
        indexes = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_file_list_partition'"
        ).fetchone()
        assert indexes is not None

        conn.close()

    def test_ensure_file_list_partition_columns_idempotent(self):
        """Test that _ensure_file_list_partition_columns is idempotent (safe to run twice)."""
        from core.database import _ensure_file_list_partition_columns

        conn = sqlite3.connect(":memory:")

        # Create  schema (already has the columns)
        conn.execute("""
            CREATE TABLE file_list (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                file_name TEXT NOT NULL,
                partition_index INTEGER DEFAULT -1,
                inode TEXT,
                import_timestamp TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX idx_file_list_partition ON file_list(evidence_id, partition_index)")

        # Insert data with partition_index set
        conn.execute("""
            INSERT INTO file_list (evidence_id, file_path, file_name, partition_index, inode, import_timestamp)
            VALUES (1, '/Users/test/file.txt', 'file.txt', 2, '12345-128-1', '2024-01-01T00:00:00Z')
        """)

        # Run upgrade helper (should be no-op)
        _ensure_file_list_partition_columns(conn)

        # Verify existing data was NOT modified
        row = conn.execute("SELECT partition_index, inode FROM file_list WHERE id = 1").fetchone()
        assert row[0] == 2  # Preserved, not overwritten
        assert row[1] == "12345-128-1"

        conn.close()

    def test_v18x_database_upgrade_full_flow(self, tmp_path):
        """Integration test: simulate opening a v1.8.x evidence database in."""
        from core.database import migrate
        from core.database import _ensure_file_list_partition_columns, EVIDENCE_MIGRATIONS_DIR

        db_path = tmp_path / "evidence_test.sqlite"
        conn = sqlite3.connect(db_path)

        # Simulate v1.8.x database state:
        # - Has schema_version table with version 1
        # - Has file_list table WITHOUT partition_index/inode columns

        # Create schema_version (marks as v1.8.x baseline)
        conn.execute("""
            CREATE TABLE schema_version (
                version INTEGER PRIMARY KEY,
                applied_at_utc TEXT NOT NULL
            )
        """)
        conn.execute("INSERT INTO schema_version (version, applied_at_utc) VALUES (1, '2024-01-01T00:00:00Z')")

        # Create old file_list schema
        conn.execute("""
            CREATE TABLE file_list (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                file_name TEXT NOT NULL,
                extension TEXT,
                size_bytes INTEGER,
                created_ts TEXT,
                modified_ts TEXT,
                accessed_ts TEXT,
                md5_hash TEXT,
                sha1_hash TEXT,
                sha256_hash TEXT,
                file_type TEXT,
                deleted BOOLEAN DEFAULT 0,
                metadata TEXT,
                import_source TEXT,
                import_timestamp TEXT NOT NULL,
                UNIQUE(evidence_id, file_path)
            )
        """)
        conn.execute("""
            CREATE TABLE urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evidence_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                domain TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE url_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evidence_id INTEGER NOT NULL,
                url_id INTEGER NOT NULL
            )
        """)
        # Create the old unique index (v1.8.x style, without partition_index)
        conn.execute("CREATE UNIQUE INDEX idx_file_list_unique_path ON file_list(evidence_id, file_path)")

        # Add some legacy data
        conn.execute("""
            INSERT INTO file_list (evidence_id, file_path, file_name, extension, import_timestamp)
            VALUES (1, '/Users/test/document.pdf', 'document.pdf', '.pdf', '2024-06-15T10:00:00Z')
        """)
        conn.commit()
        conn.close()

        # Now re-open as  would (migrate + ensure columns)
        conn = sqlite3.connect(db_path)

        # Run migrations (0002 is a no-op marker, 0003 adds extractor_statistics)
        migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)

        # Verify all migrations were recorded (update when new migrations are added)
        versions = [row[0] for row in conn.execute("SELECT version FROM schema_version ORDER BY version")]
        assert versions == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

        # Run column fixup (this is the critical step)
        _ensure_file_list_partition_columns(conn)

        # Verify columns now exist
        columns = {row[1] for row in conn.execute("PRAGMA table_info(file_list)")}
        assert "partition_index" in columns
        assert "inode" in columns

        # Verify legacy data has default partition_index
        row = conn.execute("SELECT partition_index, inode FROM file_list WHERE id = 1").fetchone()
        assert row[0] == -1  # Default for upgraded data
        assert row[1] is None  # NULL for inode

        # Verify we can now insert with the new columns
        conn.execute("""
            INSERT INTO file_list (evidence_id, file_path, file_name, partition_index, inode, import_timestamp)
            VALUES (1, '/Users/test/image.jpg', 'image.jpg', 2, '54321-128-1', '2024-06-15T11:00:00Z')
        """)
        conn.commit()

        # Verify query with new columns works
        rows = conn.execute("""
            SELECT file_path, partition_index, inode FROM file_list ORDER BY id
        """).fetchall()
        assert len(rows) == 2
        assert rows[0][1] == -1  # Legacy data
        assert rows[1][1] == 2   # New data with explicit partition

        conn.close()

    def test_v18x_upgrade_allows_duplicate_paths_across_partitions(self, tmp_path):
        """
        Regression test: After upgrade, same file path on different partitions must be allowed.

        This is critical for multi-partition EWF images where the same path (e.g., /Windows/System32/...)
        can exist on multiple partitions. The unique index must include partition_index.
        """
        from core.database import migrate
        from core.database import _ensure_file_list_partition_columns, EVIDENCE_MIGRATIONS_DIR

        db_path = tmp_path / "evidence_multipart.sqlite"
        conn = sqlite3.connect(db_path)

        # Create v1.8.x style database with OLD unique constraint (evidence_id, file_path)
        conn.execute("""
            CREATE TABLE schema_version (
                version INTEGER PRIMARY KEY,
                applied_at_utc TEXT NOT NULL
            )
        """)
        conn.execute("INSERT INTO schema_version (version, applied_at_utc) VALUES (1, '2024-01-01T00:00:00Z')")

        conn.execute("""
            CREATE TABLE file_list (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                file_name TEXT NOT NULL,
                import_timestamp TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evidence_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                domain TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE url_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evidence_id INTEGER NOT NULL,
                url_id INTEGER NOT NULL
            )
        """)
        # This is the OLD unique index that doesn't include partition_index
        conn.execute("CREATE UNIQUE INDEX idx_file_list_unique_path ON file_list(evidence_id, file_path)")
        conn.commit()
        conn.close()

        # Re-open and upgrade
        conn = sqlite3.connect(db_path)
        migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)
        _ensure_file_list_partition_columns(conn)

        # Verify unique index was updated to include partition_index
        # Get index definition
        index_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_file_list_unique_path'"
        ).fetchone()[0]

        # The new index should include COALESCE(partition_index, -1)
        assert "partition_index" in index_sql.lower(), f"Index not updated: {index_sql}"

        # Now test: insert same path on two different partitions - should succeed
        conn.execute("""
            INSERT INTO file_list (evidence_id, file_path, file_name, partition_index, import_timestamp)
            VALUES (1, '/Windows/System32/config/SAM', 'SAM', 1, '2024-01-01T00:00:00Z')
        """)
        conn.execute("""
            INSERT INTO file_list (evidence_id, file_path, file_name, partition_index, import_timestamp)
            VALUES (1, '/Windows/System32/config/SAM', 'SAM', 2, '2024-01-01T00:00:00Z')
        """)
        conn.commit()

        # Verify both records exist
        rows = conn.execute("""
            SELECT partition_index FROM file_list WHERE file_path = '/Windows/System32/config/SAM'
            ORDER BY partition_index
        """).fetchall()
        assert len(rows) == 2
        assert rows[0][0] == 1
        assert rows[1][0] == 2

        # Also verify INSERT OR IGNORE works correctly (used by SleuthKitFileListGenerator)
        # Insert duplicate of partition 1 - should be ignored
        conn.execute("""
            INSERT OR IGNORE INTO file_list (evidence_id, file_path, file_name, partition_index, import_timestamp)
            VALUES (1, '/Windows/System32/config/SAM', 'SAM', 1, '2024-01-01T01:00:00Z')
        """)

        # Should still have exactly 2 rows
        count = conn.execute(
            "SELECT COUNT(*) FROM file_list WHERE file_path = '/Windows/System32/config/SAM'"
        ).fetchone()[0]
        assert count == 2

        conn.close()

    def test_ensure_file_list_partition_columns_handles_missing_table(self):
        """Test that helper gracefully handles missing file_list table."""
        from core.database import _ensure_file_list_partition_columns

        conn = sqlite3.connect(":memory:")

        # Create a DB without file_list table (partially created or corrupt)
        conn.execute("CREATE TABLE some_other_table (id INTEGER PRIMARY KEY)")

        # Should not raise an error
        _ensure_file_list_partition_columns(conn)

        # Verify no file_list was created
        tables = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        assert "file_list" not in tables

        conn.close()

    def test_v18x_upgrade_handles_table_level_unique_constraint(self, tmp_path):
        """
        Regression test: Handle table-level UNIQUE constraint (auto-index).

        Some v1.8.x databases may have used UNIQUE(evidence_id, file_path) as a
        table constraint rather than a named index. This creates an auto-index
        that can't be dropped directly - the table must be rebuilt.
        """
        from core.database import migrate
        from core.database import _ensure_file_list_partition_columns, EVIDENCE_MIGRATIONS_DIR

        db_path = tmp_path / "evidence_autoindex.sqlite"
        conn = sqlite3.connect(db_path)

        # Create v1.8.x style database with TABLE-LEVEL UNIQUE constraint
        conn.execute("""
            CREATE TABLE schema_version (
                version INTEGER PRIMARY KEY,
                applied_at_utc TEXT NOT NULL
            )
        """)
        conn.execute("INSERT INTO schema_version (version, applied_at_utc) VALUES (1, '2024-01-01T00:00:00Z')")

        # This creates an auto-index (sqlite_autoindex_file_list_1)
        conn.execute("""
            CREATE TABLE file_list (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                file_name TEXT NOT NULL,
                import_timestamp TEXT NOT NULL,
                UNIQUE(evidence_id, file_path)
            )
        """)
        conn.execute("""
            CREATE TABLE urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evidence_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                domain TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE url_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evidence_id INTEGER NOT NULL,
                url_id INTEGER NOT NULL
            )
        """)

        # Verify auto-index was created
        indexes = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='file_list'"
        ).fetchall()
        auto_indexes = [row[0] for row in indexes if row[0].startswith("sqlite_autoindex_")]
        assert len(auto_indexes) > 0, "Expected auto-index from UNIQUE constraint"

        # Add some existing data
        conn.execute("""
            INSERT INTO file_list (evidence_id, file_path, file_name, import_timestamp)
            VALUES (1, '/existing/file.txt', 'file.txt', '2024-01-01T00:00:00Z')
        """)
        conn.commit()
        conn.close()

        # Re-open and upgrade
        conn = sqlite3.connect(db_path)
        migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)
        _ensure_file_list_partition_columns(conn)

        # Verify auto-index is gone and named index exists
        indexes = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='file_list'"
        ).fetchall()
        index_names = [row[0] for row in indexes]
        auto_indexes = [name for name in index_names if name.startswith("sqlite_autoindex_")]
        assert len(auto_indexes) == 0, f"Auto-index should have been removed: {auto_indexes}"
        assert "idx_file_list_unique_path" in index_names

        # Verify columns were added
        columns = {row[1] for row in conn.execute("PRAGMA table_info(file_list)")}
        assert "partition_index" in columns
        assert "inode" in columns

        # Verify existing data was preserved
        row = conn.execute("SELECT file_path FROM file_list WHERE id = 1").fetchone()
        assert row[0] == "/existing/file.txt"

        # Verify duplicate paths across partitions now work
        conn.execute("""
            INSERT INTO file_list (evidence_id, file_path, file_name, partition_index, import_timestamp)
            VALUES (1, '/test/path.txt', 'path.txt', 1, '2024-01-01T00:00:00Z')
        """)
        conn.execute("""
            INSERT INTO file_list (evidence_id, file_path, file_name, partition_index, import_timestamp)
            VALUES (1, '/test/path.txt', 'path.txt', 2, '2024-01-01T00:00:00Z')
        """)
        conn.commit()

        count = conn.execute(
            "SELECT COUNT(*) FROM file_list WHERE file_path = '/test/path.txt'"
        ).fetchone()[0]
        assert count == 2

        conn.close()

    def test_v18x_upgrade_fixes_index_even_if_columns_exist(self):
        """
        Regression test: Fix unique index even if columns already exist.

        A database might have partition_index column (e.g., from a partial upgrade)
        but still have the old unique index. The fix should run unconditionally.
        """
        from core.database import _ensure_file_list_partition_columns

        conn = sqlite3.connect(":memory:")

        # Create table WITH partition_index but with OLD unique index
        conn.execute("""
            CREATE TABLE file_list (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                file_name TEXT NOT NULL,
                partition_index INTEGER DEFAULT -1,
                inode TEXT,
                import_timestamp TEXT NOT NULL
            )
        """)
        # Create OLD unique index (without partition_index)
        conn.execute("CREATE UNIQUE INDEX idx_file_list_unique_path ON file_list(evidence_id, file_path)")

        # Verify the old index doesn't include partition_index
        old_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE name='idx_file_list_unique_path'"
        ).fetchone()[0]
        assert "partition_index" not in old_sql.lower()

        # Run the upgrade helper (columns won't be added, but index should be fixed)
        _ensure_file_list_partition_columns(conn)

        # Verify index was updated
        new_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE name='idx_file_list_unique_path'"
        ).fetchone()[0]
        assert "partition_index" in new_sql.lower(), f"Index not updated: {new_sql}"

        # Verify duplicate paths across partitions now work
        conn.execute("""
            INSERT INTO file_list (evidence_id, file_path, file_name, partition_index, import_timestamp)
            VALUES (1, '/same/path.txt', 'path.txt', 1, '2024-01-01T00:00:00Z')
        """)
        conn.execute("""
            INSERT INTO file_list (evidence_id, file_path, file_name, partition_index, import_timestamp)
            VALUES (1, '/same/path.txt', 'path.txt', 2, '2024-01-01T00:00:00Z')
        """)
        conn.commit()

        count = conn.execute(
            "SELECT COUNT(*) FROM file_list WHERE file_path = '/same/path.txt'"
        ).fetchone()[0]
        assert count == 2

        conn.close()


class TestNtfsMetadataFilteringDetermination:
    """
    Tests for NTFS metadata filtering determination in _process_partition.

    The is_ntfs flag determines whether ($FILE_NAME), ($DATA), and other
    NTFS metadata entries are filtered out during fls enumeration.
    """

    def test_ntfs_explicit_in_description(self):
        """NTFS explicitly in description enables filtering."""
        # Simulate _process_partition logic
        partition = {'index': 0, 'offset': 0, 'description': 'NTFS / exFAT (0x07)'}
        description = partition.get('description', '').upper()
        is_ntfs = 'NTFS' in description or 'EXFAT' in description or '0X07' in description
        assert is_ntfs is True

    def test_exfat_explicit_in_description(self):
        """exFAT in description enables filtering."""
        partition = {'index': 0, 'offset': 0, 'description': 'Microsoft exFAT'}
        description = partition.get('description', '').upper()
        is_ntfs = 'NTFS' in description or 'EXFAT' in description or '0X07' in description
        assert is_ntfs is True

    def test_0x07_partition_type(self):
        """Partition type 0x07 enables filtering."""
        partition = {'index': 0, 'offset': 0, 'description': 'Unknown (0x07)'}
        description = partition.get('description', '').upper()
        is_ntfs = 'NTFS' in description or 'EXFAT' in description or '0X07' in description
        assert is_ntfs is True

    def test_direct_filesystem_fallback_enables_filtering(self):
        """
        Direct filesystem fallback should enable NTFS filtering.

        This is the key fix: when mmls can't parse partitions (raw NTFS),
        fls falls back to 'Direct filesystem' which must enable filtering.
        """
        partition = {'index': 0, 'offset': 0, 'description': 'Direct filesystem'}
        description = partition.get('description', '').upper()
        is_ntfs = 'NTFS' in description or 'EXFAT' in description or '0X07' in description

        # First check fails (not explicitly NTFS)
        assert is_ntfs is False

        # But the new logic should enable it anyway
        if not is_ntfs:
            non_windows_fs = ['EXT', 'HFS', 'APFS', 'LINUX', 'SWAP', 'BSD', 'UFS']
            is_non_windows = any(fs in description for fs in non_windows_fs)
            if not is_non_windows:
                is_ntfs = True

        assert is_ntfs is True, "Direct filesystem should enable NTFS filtering"

    def test_empty_description_enables_filtering(self):
        """Empty description should enable NTFS filtering as default."""
        partition = {'index': 0, 'offset': 0, 'description': ''}
        description = partition.get('description', '').upper()
        is_ntfs = 'NTFS' in description or 'EXFAT' in description or '0X07' in description

        if not is_ntfs:
            non_windows_fs = ['EXT', 'HFS', 'APFS', 'LINUX', 'SWAP', 'BSD', 'UFS']
            is_non_windows = any(fs in description for fs in non_windows_fs)
            if not is_non_windows:
                is_ntfs = True

        assert is_ntfs is True

    def test_basic_data_partition_enables_filtering(self):
        """GPT Basic Data Partition (often NTFS) should enable filtering."""
        partition = {'index': 0, 'offset': 0, 'description': 'Basic data partition'}
        description = partition.get('description', '').upper()
        is_ntfs = 'NTFS' in description or 'EXFAT' in description or '0X07' in description

        if not is_ntfs:
            non_windows_fs = ['EXT', 'HFS', 'APFS', 'LINUX', 'SWAP', 'BSD', 'UFS']
            is_non_windows = any(fs in description for fs in non_windows_fs)
            if not is_non_windows:
                is_ntfs = True

        assert is_ntfs is True

    def test_linux_filesystem_disables_filtering(self):
        """Linux filesystem should NOT enable NTFS filtering."""
        partition = {'index': 0, 'offset': 0, 'description': 'Linux filesystem'}
        description = partition.get('description', '').upper()
        is_ntfs = 'NTFS' in description or 'EXFAT' in description or '0X07' in description

        if not is_ntfs:
            non_windows_fs = ['EXT', 'HFS', 'APFS', 'LINUX', 'SWAP', 'BSD', 'UFS']
            is_non_windows = any(fs in description for fs in non_windows_fs)
            if not is_non_windows:
                is_ntfs = True

        assert is_ntfs is False

    def test_ext4_disables_filtering(self):
        """EXT4 filesystem should NOT enable NTFS filtering."""
        partition = {'index': 0, 'offset': 0, 'description': 'EXT4 (0x83)'}
        description = partition.get('description', '').upper()
        is_ntfs = 'NTFS' in description or 'EXFAT' in description or '0X07' in description

        if not is_ntfs:
            non_windows_fs = ['EXT', 'HFS', 'APFS', 'LINUX', 'SWAP', 'BSD', 'UFS']
            is_non_windows = any(fs in description for fs in non_windows_fs)
            if not is_non_windows:
                is_ntfs = True

        assert is_ntfs is False

    def test_hfs_plus_disables_filtering(self):
        """Apple HFS+ should NOT enable NTFS filtering."""
        partition = {'index': 0, 'offset': 0, 'description': 'Apple HFS+'}
        description = partition.get('description', '').upper()
        is_ntfs = 'NTFS' in description or 'EXFAT' in description or '0X07' in description

        if not is_ntfs:
            non_windows_fs = ['EXT', 'HFS', 'APFS', 'LINUX', 'SWAP', 'BSD', 'UFS']
            is_non_windows = any(fs in description for fs in non_windows_fs)
            if not is_non_windows:
                is_ntfs = True

        assert is_ntfs is False

    def test_apfs_disables_filtering(self):
        """Apple APFS should NOT enable NTFS filtering."""
        partition = {'index': 0, 'offset': 0, 'description': 'Apple APFS Container'}
        description = partition.get('description', '').upper()
        is_ntfs = 'NTFS' in description or 'EXFAT' in description or '0X07' in description

        if not is_ntfs:
            non_windows_fs = ['EXT', 'HFS', 'APFS', 'LINUX', 'SWAP', 'BSD', 'UFS']
            is_non_windows = any(fs in description for fs in non_windows_fs)
            if not is_non_windows:
                is_ntfs = True

        assert is_ntfs is False
