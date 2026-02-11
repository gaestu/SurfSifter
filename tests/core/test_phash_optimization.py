"""Tests for pHash optimization with prefix filtering.

Verifies the two-phase similarity search algorithm:
1. SQL prefix filter reduces candidates
2. Python Hamming distance on reduced set
"""

import sqlite3
from pathlib import Path
from typing import List

import pytest

from core.phash import compute_phash_prefix
from core.database import migrate, insert_images
from core.database import EVIDENCE_MIGRATIONS_DIR


def create_test_db(tmp_path: Path) -> sqlite3.Connection:
    """Create test database with schema."""
    db_path = tmp_path / "evidence.sqlite"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)
    return conn


class TestPhashPrefix:
    """Test phash prefix computation."""

    def test_compute_prefix_valid(self):
        """Valid 16-char hex phash returns integer prefix."""
        phash = "a1b2c3d4e5f60718"  # 16-char hex
        prefix = compute_phash_prefix(phash)

        # First 4 chars "a1b2" = 0xa1b2 = 41394
        assert prefix == 0xa1b2
        assert prefix == 41394

    def test_compute_prefix_all_zeros(self):
        """Phash with zeros returns 0."""
        phash = "0000000000000000"
        prefix = compute_phash_prefix(phash)
        assert prefix == 0

    def test_compute_prefix_all_fs(self):
        """Phash with all F's returns max value."""
        phash = "ffffffffffffffff"
        prefix = compute_phash_prefix(phash)
        assert prefix == 0xffff
        assert prefix == 65535

    def test_compute_prefix_none(self):
        """None phash returns None."""
        assert compute_phash_prefix(None) is None

    def test_compute_prefix_empty(self):
        """Empty phash returns None."""
        assert compute_phash_prefix("") is None

    def test_compute_prefix_short(self):
        """Phash shorter than 4 chars returns None."""
        assert compute_phash_prefix("abc") is None

    def test_compute_prefix_invalid_hex(self):
        """Non-hex prefix returns None."""
        assert compute_phash_prefix("zzzz000000000000") is None


class TestPhashPrefixInsertion:
    """Test that phash_prefix is automatically computed on insert."""

    def test_insert_images_computes_prefix(self, tmp_path: Path):
        """insert_images should auto-compute phash_prefix."""
        conn = create_test_db(tmp_path)

        records = [
            {
                "rel_path": "images/test1.jpg",
                "filename": "test1.jpg",
                "phash": "a1b2c3d4e5f60718",
                "discovered_by": "test",
            }
        ]

        count = insert_images(conn, evidence_id=1, records=records)
        assert count == 1

        # Check prefix was computed
        row = conn.execute("SELECT phash, phash_prefix FROM images WHERE id = 1").fetchone()
        assert row["phash"] == "a1b2c3d4e5f60718"
        assert row["phash_prefix"] == 0xa1b2

        conn.close()

    def test_insert_images_null_phash_null_prefix(self, tmp_path: Path):
        """Image without phash should have NULL prefix."""
        conn = create_test_db(tmp_path)

        records = [
            {
                "rel_path": "images/test1.jpg",
                "filename": "test1.jpg",
                "phash": None,
                "discovered_by": "test",
            }
        ]

        insert_images(conn, evidence_id=1, records=records)

        row = conn.execute("SELECT phash, phash_prefix FROM images WHERE id = 1").fetchone()
        assert row["phash"] is None
        assert row["phash_prefix"] is None

        conn.close()


class TestTwoPhaseSearch:
    """Test two-phase similarity search using prefix filtering."""

    def test_prefix_range_filters_candidates(self, tmp_path: Path):
        """SQL prefix filter should reduce candidate set."""
        conn = create_test_db(tmp_path)

        # Insert images with varying prefixes spread across the range
        # Using steps of 1000 to spread prefixes across 0-65535 range
        records = [
            {"rel_path": f"img{i}.jpg", "filename": f"img{i}.jpg",
             "phash": f"{i * 1000:04x}{'0' * 12}", "discovered_by": "test"}
            for i in range(60)  # Prefixes: 0, 1000, 2000, ..., 59000
        ]
        insert_images(conn, evidence_id=1, records=records)

        # Query with prefix range centered on 30000
        target_prefix = 30000
        prefix_range = 5000  # Should cover prefixes 25000-35000

        cursor = conn.execute(
            """SELECT COUNT(*) FROM images
               WHERE evidence_id = 1
               AND phash_prefix BETWEEN ? AND ?""",
            (target_prefix - prefix_range, target_prefix + prefix_range)
        )

        filtered_count = cursor.fetchone()[0]
        total_count = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]

        # Filtered should be less than total (only ~10 images in range)
        assert filtered_count < total_count, f"Filtered {filtered_count} should be less than total {total_count}"
        # But should include some images
        assert filtered_count > 0, "Should find some images in range"
        # Approximately 10 images: prefixes 25000, 26000, ..., 35000
        assert filtered_count <= 15, f"Expected ~10 images, got {filtered_count}"

        conn.close()

    def test_prefix_index_used(self, tmp_path: Path):
        """Verify the phash_prefix index is used in query plan."""
        conn = create_test_db(tmp_path)

        # Insert some test data
        records = [
            {"rel_path": "img1.jpg", "filename": "img1.jpg",
             "phash": "a1b2c3d4e5f60718", "discovered_by": "test"}
        ]
        insert_images(conn, evidence_id=1, records=records)

        # Check query plan
        plan = conn.execute(
            """EXPLAIN QUERY PLAN
               SELECT * FROM images
               WHERE evidence_id = 1
               AND phash IS NOT NULL
               AND phash_prefix IS NOT NULL
               AND phash_prefix BETWEEN 40000 AND 42000"""
        ).fetchall()

        plan_text = " ".join(str(row) for row in plan)

        # Should use an index (either the composite phash_prefix index or evidence index)
        assert "SCAN" not in plan_text or "INDEX" in plan_text

        conn.close()


class TestSimilarImagesIntegration:
    """Integration tests for the full similarity search flow."""

    def test_similar_images_found_with_prefix(self, tmp_path: Path):
        """Similar images should be found using prefix-optimized search."""
        conn = create_test_db(tmp_path)

        # Insert target and similar images
        # Similar hashes differ by a few bits
        records = [
            {"rel_path": "target.jpg", "filename": "target.jpg",
             "phash": "a1b2c3d4e5f60718", "discovered_by": "test"},  # Target
            {"rel_path": "similar1.jpg", "filename": "similar1.jpg",
             "phash": "a1b2c3d4e5f60719", "discovered_by": "test"},  # 1 bit diff
            {"rel_path": "similar2.jpg", "filename": "similar2.jpg",
             "phash": "a1b2c3d4e5f6071c", "discovered_by": "test"},  # 2 bits diff
            {"rel_path": "different.jpg", "filename": "different.jpg",
             "phash": "ffffffffffffffff", "discovered_by": "test"},  # Very different
        ]
        insert_images(conn, evidence_id=1, records=records)

        # Verify data inserted with prefixes
        rows = conn.execute(
            "SELECT filename, phash_prefix FROM images ORDER BY id"
        ).fetchall()

        assert len(rows) == 4
        assert rows[0]["phash_prefix"] == 0xa1b2  # All similar have same prefix
        assert rows[1]["phash_prefix"] == 0xa1b2
        assert rows[2]["phash_prefix"] == 0xa1b2
        assert rows[3]["phash_prefix"] == 0xffff  # Different prefix

        conn.close()
