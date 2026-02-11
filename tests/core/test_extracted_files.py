"""
Test suite for Extracted Files Audit Table

Tests database helpers, batch operations, and query functionality
for the universal extracted_files audit table.

Initial implementation.
"""

from datetime import datetime, timezone
import json
import sqlite3
import pytest

from core.database import (
    EVIDENCE_MIGRATIONS_DIR,
    migrate,
)
from core.database.helpers import (
    insert_extracted_file,
    insert_extracted_files,
    insert_extracted_files_batch,
    get_extracted_files,
    get_extracted_file_by_id,
    get_extracted_file_by_sha256,
    get_extraction_stats,
    get_distinct_extractors,
    get_distinct_run_ids,
    delete_extracted_files_by_run,
    delete_extracted_files_by_extractor,
    get_evidence_table_counts,
    purge_evidence_data,
)
from core.enums import ExtractionStatus


@pytest.fixture
def evidence_db(tmp_path):
    """Create a temporary evidence database with schema."""
    db_path = tmp_path / "test_evidence.sqlite"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")

    # Apply all evidence migrations
    migrate(conn, EVIDENCE_MIGRATIONS_DIR)

    yield conn
    conn.close()


class TestInsertExtractedFile:
    """Tests for single record insertion."""

    def test_insert_basic_record(self, evidence_db):
        """Insert minimal required fields."""
        row_id = insert_extracted_file(
            evidence_db,
            evidence_id=1,
            extractor_name="filesystem_images",
            run_id="fs_20260124_120000_abc12345",
            dest_rel_path="extracted/img1.jpg",
            dest_filename="img1.jpg",
        )

        assert row_id > 0

        # Verify record
        row = evidence_db.execute(
            "SELECT * FROM extracted_files WHERE id = ?", (row_id,)
        ).fetchone()

        assert row["evidence_id"] == 1
        assert row["extractor_name"] == "filesystem_images"
        assert row["run_id"] == "fs_20260124_120000_abc12345"
        assert row["dest_rel_path"] == "extracted/img1.jpg"
        assert row["dest_filename"] == "img1.jpg"
        assert row["status"] == "ok"
        assert row["extracted_at_utc"] is not None

    def test_insert_with_all_fields(self, evidence_db):
        """Insert record with all optional fields."""
        metadata = {"http_status": 200, "content_encoding": "gzip"}

        row_id = insert_extracted_file(
            evidence_db,
            evidence_id=1,
            extractor_name="cache_firefox",
            run_id="ff_20260124_130000_def45678",
            dest_rel_path="extracted/cache/image.png",
            dest_filename="image.png",
            source_path="Users/John/AppData/Local/Mozilla/Firefox/Profiles/cache2/entries/abc123",
            source_inode="292163-128-4",
            partition_index=2,
            source_offset_bytes=1024000,
            source_block_size=512,
            size_bytes=45678,
            file_type="PNG",
            mime_type="image/png",
            md5="d41d8cd98f00b204e9800998ecf8427e",
            sha256="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            status=ExtractionStatus.OK,
            error_message=None,
            extractor_version="1.2.0",
            metadata_json=json.dumps(metadata),
        )

        row = evidence_db.execute(
            "SELECT * FROM extracted_files WHERE id = ?", (row_id,)
        ).fetchone()

        assert row["source_path"] == "Users/John/AppData/Local/Mozilla/Firefox/Profiles/cache2/entries/abc123"
        assert row["source_inode"] == "292163-128-4"
        assert row["partition_index"] == 2
        assert row["source_offset_bytes"] == 1024000
        assert row["size_bytes"] == 45678
        assert row["file_type"] == "PNG"
        assert row["sha256"] == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        assert row["extractor_version"] == "1.2.0"
        assert json.loads(row["metadata_json"]) == metadata

    def test_insert_with_error_status(self, evidence_db):
        """Insert record with error status."""
        row_id = insert_extracted_file(
            evidence_db,
            evidence_id=1,
            extractor_name="foremost",
            run_id="fm_20260124_140000_ghi78901",
            dest_rel_path="carved/00012345.jpg",
            dest_filename="00012345.jpg",
            status=ExtractionStatus.ERROR,
            error_message="Truncated file - unexpected EOF",
        )

        row = evidence_db.execute(
            "SELECT status, error_message FROM extracted_files WHERE id = ?", (row_id,)
        ).fetchone()

        assert row["status"] == "error"
        assert row["error_message"] == "Truncated file - unexpected EOF"


class TestInsertExtractedFilesBatch:
    """Tests for batch insertion."""

    def test_batch_insert_basic(self, evidence_db):
        """Batch insert with minimal fields."""
        files = [
            {"dest_rel_path": "extracted/img1.jpg", "dest_filename": "img1.jpg"},
            {"dest_rel_path": "extracted/img2.png", "dest_filename": "img2.png"},
            {"dest_rel_path": "extracted/img3.gif", "dest_filename": "img3.gif"},
        ]

        count = insert_extracted_files_batch(
            evidence_db,
            evidence_id=1,
            extractor_name="filesystem_images",
            run_id="fs_20260124_150000_jkl23456",
            files=files,
            extractor_version="1.8.3",
        )

        assert count == 3

        # Verify all records have correct common fields
        rows = evidence_db.execute(
            "SELECT * FROM extracted_files WHERE run_id = ?",
            ("fs_20260124_150000_jkl23456",)
        ).fetchall()

        assert len(rows) == 3
        for row in rows:
            assert row["extractor_name"] == "filesystem_images"
            assert row["extractor_version"] == "1.8.3"
            assert row["status"] == "ok"

    def test_batch_insert_with_all_fields(self, evidence_db):
        """Batch insert with full file metadata."""
        files = [
            {
                "dest_rel_path": "extracted/p0/img1.jpg",
                "dest_filename": "img1.jpg",
                "source_path": "Users/John/Pictures/img1.jpg",
                "inode": 12345,  # Test 'inode' key mapping to source_inode
                "partition_index": 0,
                "size_bytes": 12345,
                "detected_type": "jpeg",  # Test 'detected_type' key mapping to file_type
                "md5": "abc123",
                "sha256": "def456",
            },
            {
                "dest_rel_path": "extracted/p0/img2.png",
                "dest_filename": "img2.png",
                "source_path": "Users/John/Pictures/img2.png",
                "source_inode": "67890-128-1",  # Test 'source_inode' key directly
                "partition_index": 0,
                "size_bytes": 67890,
                "file_type": "png",  # Test 'file_type' key directly
                "md5": "ghi789",
                "sha256": "jkl012",
            },
        ]

        count = insert_extracted_files_batch(
            evidence_db,
            evidence_id=1,
            extractor_name="filesystem_images",
            run_id="fs_20260124_160000_mno34567",
            files=files,
        )

        assert count == 2

        rows = evidence_db.execute(
            "SELECT * FROM extracted_files WHERE run_id = ? ORDER BY id",
            ("fs_20260124_160000_mno34567",)
        ).fetchall()

        # First record - uses 'inode' and 'detected_type' key mappings
        assert rows[0]["source_inode"] == "12345"  # Converted to string
        assert rows[0]["file_type"] == "jpeg"

        # Second record - uses direct keys
        assert rows[1]["source_inode"] == "67890-128-1"
        assert rows[1]["file_type"] == "png"

    def test_batch_insert_empty_list(self, evidence_db):
        """Batch insert with empty list returns 0."""
        count = insert_extracted_files_batch(
            evidence_db,
            evidence_id=1,
            extractor_name="test",
            run_id="test_run",
            files=[],
        )

        assert count == 0


class TestGetExtractedFiles:
    """Tests for query functions."""

    @pytest.fixture
    def populated_db(self, evidence_db):
        """Populate DB with test data."""
        # Insert data from two different extractors and runs
        files_fs = [
            {"dest_rel_path": "extracted/img1.jpg", "dest_filename": "img1.jpg",
             "file_type": "jpeg", "size_bytes": 1000, "sha256": "sha_fs_1"},
            {"dest_rel_path": "extracted/img2.png", "dest_filename": "img2.png",
             "file_type": "png", "size_bytes": 2000, "sha256": "sha_fs_2"},
        ]
        insert_extracted_files_batch(
            evidence_db, 1, "filesystem_images", "fs_run_001", files_fs, "1.8.3"
        )

        files_cache = [
            {"dest_rel_path": "cache/entry1.jpg", "dest_filename": "entry1.jpg",
             "file_type": "jpeg", "size_bytes": 500, "sha256": "sha_cache_1",
             "status": ExtractionStatus.OK},
            {"dest_rel_path": "cache/entry2.gif", "dest_filename": "entry2.gif",
             "file_type": "gif", "size_bytes": 300, "sha256": "sha_cache_2",
             "status": ExtractionStatus.ERROR, "error_message": "Corrupt data"},
        ]
        insert_extracted_files_batch(
            evidence_db, 1, "cache_firefox", "ff_run_001", files_cache, "2.0.0"
        )

        # Second run from filesystem_images
        files_fs_2 = [
            {"dest_rel_path": "extracted/img3.webp", "dest_filename": "img3.webp",
             "file_type": "webp", "size_bytes": 1500, "sha256": "sha_fs_3",
             "partition_index": 1},
        ]
        insert_extracted_files_batch(
            evidence_db, 1, "filesystem_images", "fs_run_002", files_fs_2, "1.8.3"
        )

        return evidence_db

    def test_get_all_files(self, populated_db):
        """Get all extracted files without filters."""
        files = get_extracted_files(populated_db, evidence_id=1)
        assert len(files) == 5

    def test_filter_by_extractor(self, populated_db):
        """Filter by extractor name."""
        files = get_extracted_files(populated_db, evidence_id=1, extractor_name="filesystem_images")
        assert len(files) == 3
        assert all(f["extractor_name"] == "filesystem_images" for f in files)

    def test_filter_by_run_id(self, populated_db):
        """Filter by specific run."""
        files = get_extracted_files(populated_db, evidence_id=1, run_id="ff_run_001")
        assert len(files) == 2
        assert all(f["run_id"] == "ff_run_001" for f in files)

    def test_filter_by_status(self, populated_db):
        """Filter by extraction status."""
        files = get_extracted_files(populated_db, evidence_id=1, status=ExtractionStatus.ERROR)
        assert len(files) == 1
        assert files[0]["dest_filename"] == "entry2.gif"

    def test_filter_by_file_type(self, populated_db):
        """Filter by detected file type."""
        files = get_extracted_files(populated_db, evidence_id=1, file_type="jpeg")
        assert len(files) == 2

    def test_filter_by_partition(self, populated_db):
        """Filter by partition index."""
        files = get_extracted_files(populated_db, evidence_id=1, partition_index=1)
        assert len(files) == 1
        assert files[0]["dest_filename"] == "img3.webp"

    def test_pagination(self, populated_db):
        """Test limit and offset for pagination."""
        # Get first 2
        page1 = get_extracted_files(populated_db, evidence_id=1, limit=2, offset=0)
        assert len(page1) == 2

        # Get next 2
        page2 = get_extracted_files(populated_db, evidence_id=1, limit=2, offset=2)
        assert len(page2) == 2

        # Ensure no overlap
        page1_ids = {f["id"] for f in page1}
        page2_ids = {f["id"] for f in page2}
        assert page1_ids.isdisjoint(page2_ids)


class TestGetExtractedFileById:
    """Tests for single record lookup."""

    def test_get_existing_record(self, evidence_db):
        """Fetch record by ID."""
        row_id = insert_extracted_file(
            evidence_db, 1, "test", "run1", "path/file.txt", "file.txt"
        )

        record = get_extracted_file_by_id(evidence_db, 1, row_id)
        assert record is not None
        assert record["id"] == row_id
        assert record["dest_filename"] == "file.txt"

    def test_get_nonexistent_record(self, evidence_db):
        """Return None for missing ID."""
        record = get_extracted_file_by_id(evidence_db, 1, 99999)
        assert record is None


class TestGetExtractedFileBySha256:
    """Tests for SHA256 lookup."""

    def test_find_by_hash(self, evidence_db):
        """Find record by SHA256 hash."""
        insert_extracted_file(
            evidence_db, 1, "test", "run1", "file1.jpg", "file1.jpg",
            sha256="unique_hash_abc123"
        )

        record = get_extracted_file_by_sha256(evidence_db, 1, "unique_hash_abc123")
        assert record is not None
        assert record["sha256"] == "unique_hash_abc123"

    def test_find_nonexistent_hash(self, evidence_db):
        """Return None for missing hash."""
        record = get_extracted_file_by_sha256(evidence_db, 1, "nonexistent_hash")
        assert record is None

    def test_returns_most_recent(self, evidence_db):
        """When multiple records have same hash, return most recent."""
        # Insert two records with same SHA256 (different runs)
        insert_extracted_file(
            evidence_db, 1, "test", "run1", "old/file.jpg", "file.jpg",
            sha256="shared_hash_xyz"
        )
        insert_extracted_file(
            evidence_db, 1, "test", "run2", "new/file.jpg", "file.jpg",
            sha256="shared_hash_xyz"
        )

        record = get_extracted_file_by_sha256(evidence_db, 1, "shared_hash_xyz")
        assert record["dest_rel_path"] == "new/file.jpg"  # Most recent (higher ID)


class TestGetExtractionStats:
    """Tests for statistics aggregation."""

    @pytest.fixture
    def stats_db(self, evidence_db):
        """Populate DB with diverse test data for stats."""
        files_ok = [
            {"dest_rel_path": f"ok{i}.jpg", "dest_filename": f"ok{i}.jpg",
             "file_type": "jpeg", "size_bytes": 1000 * (i + 1), "status": "ok"}
            for i in range(5)
        ]
        files_err = [
            {"dest_rel_path": f"err{i}.png", "dest_filename": f"err{i}.png",
             "file_type": "png", "size_bytes": 500, "status": "error"}
            for i in range(2)
        ]

        insert_extracted_files_batch(evidence_db, 1, "extractor_a", "run_a", files_ok)
        insert_extracted_files_batch(evidence_db, 1, "extractor_b", "run_b", files_err)

        return evidence_db

    def test_total_counts(self, stats_db):
        """Verify total count and size."""
        stats = get_extraction_stats(stats_db, 1)

        assert stats["total_count"] == 7
        assert stats["total_size_bytes"] == sum(1000 * (i + 1) for i in range(5)) + 500 * 2

    def test_by_extractor(self, stats_db):
        """Verify breakdown by extractor."""
        stats = get_extraction_stats(stats_db, 1)

        assert stats["by_extractor"]["extractor_a"] == 5
        assert stats["by_extractor"]["extractor_b"] == 2

    def test_by_status(self, stats_db):
        """Verify breakdown by status."""
        stats = get_extraction_stats(stats_db, 1)

        assert stats["by_status"]["ok"] == 5
        assert stats["by_status"]["error"] == 2

    def test_by_file_type(self, stats_db):
        """Verify breakdown by file type."""
        stats = get_extraction_stats(stats_db, 1)

        assert stats["by_file_type"]["jpeg"] == 5
        assert stats["by_file_type"]["png"] == 2

    def test_error_count(self, stats_db):
        """Verify error count."""
        stats = get_extraction_stats(stats_db, 1)
        assert stats["error_count"] == 2

    def test_filter_by_run(self, stats_db):
        """Verify stats filtered by run_id."""
        stats = get_extraction_stats(stats_db, 1, run_id="run_a")

        assert stats["total_count"] == 5
        assert stats["error_count"] == 0


class TestGetDistinctValues:
    """Tests for distinct value lookups."""

    @pytest.fixture
    def distinct_db(self, evidence_db):
        """Populate with multiple extractors and runs."""
        for ext in ["ext_a", "ext_b", "ext_c"]:
            for run_idx in range(2):
                insert_extracted_file(
                    evidence_db, 1, ext, f"{ext}_run_{run_idx}",
                    f"{ext}/file{run_idx}.txt", f"file{run_idx}.txt"
                )
        return evidence_db

    def test_get_distinct_extractors(self, distinct_db):
        """Get list of unique extractor names."""
        extractors = get_distinct_extractors(distinct_db, 1)

        assert len(extractors) == 3
        assert set(extractors) == {"ext_a", "ext_b", "ext_c"}

    def test_get_all_run_ids(self, distinct_db):
        """Get all unique run IDs."""
        run_ids = get_distinct_run_ids(distinct_db, 1)

        assert len(run_ids) == 6  # 3 extractors × 2 runs each

    def test_get_run_ids_by_extractor(self, distinct_db):
        """Get run IDs filtered by extractor."""
        run_ids = get_distinct_run_ids(distinct_db, 1, extractor_name="ext_a")

        assert len(run_ids) == 2
        assert all("ext_a" in rid for rid in run_ids)


class TestDeleteExtractedFiles:
    """Tests for deletion functions."""

    def test_delete_by_run(self, evidence_db):
        """Delete records for a specific run."""
        # Insert two runs
        insert_extracted_files_batch(
            evidence_db, 1, "test", "run_to_delete",
            [{"dest_rel_path": f"f{i}.txt", "dest_filename": f"f{i}.txt"} for i in range(3)]
        )
        insert_extracted_files_batch(
            evidence_db, 1, "test", "run_to_keep",
            [{"dest_rel_path": f"k{i}.txt", "dest_filename": f"k{i}.txt"} for i in range(2)]
        )

        # Delete one run
        deleted = delete_extracted_files_by_run(evidence_db, 1, "run_to_delete")

        assert deleted == 3

        # Verify only kept run remains
        remaining = get_extracted_files(evidence_db, 1)
        assert len(remaining) == 2
        assert all(f["run_id"] == "run_to_keep" for f in remaining)

    def test_delete_by_extractor(self, evidence_db):
        """Delete all records for an extractor."""
        # Insert from two extractors
        insert_extracted_files_batch(
            evidence_db, 1, "ext_to_delete", "run1",
            [{"dest_rel_path": f"d{i}.txt", "dest_filename": f"d{i}.txt"} for i in range(4)]
        )
        insert_extracted_files_batch(
            evidence_db, 1, "ext_to_delete", "run2",
            [{"dest_rel_path": f"d{i}b.txt", "dest_filename": f"d{i}b.txt"} for i in range(2)]
        )
        insert_extracted_files_batch(
            evidence_db, 1, "ext_to_keep", "run3",
            [{"dest_rel_path": f"k{i}.txt", "dest_filename": f"k{i}.txt"} for i in range(3)]
        )

        # Delete all from one extractor
        deleted = delete_extracted_files_by_extractor(evidence_db, 1, "ext_to_delete")

        assert deleted == 6  # 4 + 2 from both runs

        # Verify other extractor's data remains
        remaining = get_extracted_files(evidence_db, 1)
        assert len(remaining) == 3
        assert all(f["extractor_name"] == "ext_to_keep" for f in remaining)


class TestBatchOperationsIntegration:
    """Tests for integration with batch operations."""

    def test_extracted_files_in_table_counts(self, evidence_db):
        """Verify extracted_files included in get_evidence_table_counts."""
        insert_extracted_files_batch(
            evidence_db, 1, "test", "run1",
            [{"dest_rel_path": f"f{i}.txt", "dest_filename": f"f{i}.txt"} for i in range(5)]
        )

        counts = get_evidence_table_counts(evidence_db, 1)

        assert "extracted_files" in counts
        assert counts["extracted_files"] == 5

    def test_extracted_files_in_purge(self, evidence_db):
        """Verify extracted_files purged by purge_evidence_data."""
        insert_extracted_files_batch(
            evidence_db, 1, "test", "run1",
            [{"dest_rel_path": f"f{i}.txt", "dest_filename": f"f{i}.txt"} for i in range(3)]
        )

        # Verify data exists
        assert get_evidence_table_counts(evidence_db, 1)["extracted_files"] == 3

        # Purge
        purge_evidence_data(evidence_db, 1)

        # Verify purged
        assert get_evidence_table_counts(evidence_db, 1)["extracted_files"] == 0


class TestAuditTableBehavior:
    """Tests verifying audit table behavior (no unique constraints)."""

    def test_duplicate_records_allowed(self, evidence_db):
        """Multiple records with same content allowed (audit semantics)."""
        # Insert same file data twice (simulating two extraction runs)
        for run_id in ["run_1", "run_2"]:
            insert_extracted_file(
                evidence_db, 1, "test", run_id,
                "same/path/file.txt", "file.txt",
                sha256="same_hash_123"
            )

        # Both should exist
        files = get_extracted_files(evidence_db, 1)
        assert len(files) == 2

        # Both have same SHA256
        assert all(f["sha256"] == "same_hash_123" for f in files)

    def test_different_evidence_ids_isolated(self, evidence_db):
        """Records for different evidence_ids are isolated."""
        for ev_id in [1, 2, 3]:
            insert_extracted_file(
                evidence_db, ev_id, "test", f"run_{ev_id}",
                f"ev{ev_id}/file.txt", "file.txt"
            )

        # Query for evidence 1 only
        files = get_extracted_files(evidence_db, evidence_id=1)
        assert len(files) == 1
        assert files[0]["run_id"] == "run_1"


class TestSchemaValidity:
    """Tests verifying schema definition validity."""

    def test_schema_registered(self):
        """Verify EXTRACTED_FILES_SCHEMA in TABLE_SCHEMAS registry."""
        from core.database.schema import TABLE_SCHEMAS, EXTRACTED_FILES_SCHEMA

        assert "extracted_files" in TABLE_SCHEMAS
        assert TABLE_SCHEMAS["extracted_files"] is EXTRACTED_FILES_SCHEMA

    def test_schema_supports_run_delete(self):
        """Verify schema marked as supporting run deletion."""
        from core.database.schema import EXTRACTED_FILES_SCHEMA

        assert EXTRACTED_FILES_SCHEMA.supports_run_delete is True

    def test_schema_conflict_action(self):
        """Verify schema uses FAIL conflict action (audit table)."""
        from core.database.schema import EXTRACTED_FILES_SCHEMA, ConflictAction

        assert EXTRACTED_FILES_SCHEMA.conflict_action == ConflictAction.FAIL


class TestExtractionStatusEnum:
    """Tests verifying ExtractionStatus enum usage."""

    def test_status_values(self, evidence_db):
        """Verify all ExtractionStatus values can be used."""
        statuses = [
            ExtractionStatus.OK,
            ExtractionStatus.PARTIAL,
            ExtractionStatus.ERROR,
            ExtractionStatus.SKIPPED,
        ]

        for i, status in enumerate(statuses):
            insert_extracted_file(
                evidence_db, 1, "test", f"run_{i}",
                f"file_{status}.txt", f"file_{status}.txt",
                status=status
            )

        # Verify all inserted
        files = get_extracted_files(evidence_db, 1)
        assert len(files) == 4

        # Verify status values
        inserted_statuses = {f["status"] for f in files}
        assert inserted_statuses == {"ok", "partial", "error", "skipped"}
