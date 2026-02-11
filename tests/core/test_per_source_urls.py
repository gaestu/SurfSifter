"""
Test per-source URL storage

Tests URL storage behavior after removing deduplication constraints.
Each URL observation is now stored as a distinct forensic event with
its own timestamp for timeline reconstruction.

Key behaviors tested:
- Same URL from different extractors → both allowed
- Same URL from different files → both allowed
- Same URL from different runs → both allowed
- Same URL with same provenance → ALSO allowed (no deduplication)
- Aggregation done at query/report time, not insert time
"""
import pytest
import sqlite3
from pathlib import Path

from core.database import migrate, EVIDENCE_MIGRATIONS_DIR

# Path to migrations
SCHEMA_PATH = EVIDENCE_MIGRATIONS_DIR / "0001_evidence_schema.sql"


@pytest.fixture
def db_with_migration(tmp_path: Path):
    """Create database with schema and migration applied."""
    db_path = tmp_path / "test_evidence.sqlite"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Apply consolidated baseline schema via migrate()
    if not SCHEMA_PATH.exists():
        pytest.skip(f"Schema file not found: {SCHEMA_PATH}")
    migrations_dir = SCHEMA_PATH.parent
    migrate(conn, migrations_dir=migrations_dir)

    yield conn
    conn.close()


def _insert_url(conn, evidence_id: int, url: str, discovered_by: str,
                source_path: str = None, run_id: str = None):
    """Helper to insert URL record."""
    conn.execute("""
        INSERT INTO urls (
            evidence_id, url, discovered_by, source_path, run_id
        ) VALUES (?, ?, ?, ?, ?)
    """, (evidence_id, url, discovered_by, source_path, run_id))
    conn.commit()


def _count_urls(conn) -> int:
    """Helper to count URLs in table."""
    return conn.execute("SELECT COUNT(*) FROM urls").fetchone()[0]


class TestMultiSourceUrls:
    """Test that same URL from different sources is allowed."""

    def test_same_url_different_extractors(self, db_with_migration):
        """Same URL from bulk_extractor and browser_history should both insert."""
        conn = db_with_migration

        _insert_url(conn, 1, "https://evil.com", "bulk_extractor:url", "url.txt:100", None)
        _insert_url(conn, 1, "https://evil.com", "browser_history", "History.db", "run_abc")

        assert _count_urls(conn) == 2, "Should allow same URL from different extractors"

    def test_same_url_different_files(self, db_with_migration):
        """Same URL from Profile 1 and Profile 2 should both insert."""
        conn = db_with_migration

        _insert_url(conn, 1, "https://evil.com", "browser_history", "Profile 1/History", "run_abc")
        _insert_url(conn, 1, "https://evil.com", "browser_history", "Profile 2/History", "run_abc")

        assert _count_urls(conn) == 2, "Should allow same URL from different source files"

    def test_same_url_different_runs(self, db_with_migration):
        """Same URL from different extraction runs should both insert."""
        conn = db_with_migration

        _insert_url(conn, 1, "https://evil.com", "cache_simple:v0.67", "Cache/f_001", "run_abc")
        _insert_url(conn, 1, "https://evil.com", "cache_simple:v0.67", "Cache/f_001", "run_def")

        assert _count_urls(conn) == 2, "Should allow same URL from different runs"

    def test_same_url_different_evidence(self, db_with_migration):
        """Same URL from different evidence IDs should both insert."""
        conn = db_with_migration

        _insert_url(conn, 1, "https://evil.com", "browser_history", "History.db", "run_abc")
        _insert_url(conn, 2, "https://evil.com", "browser_history", "History.db", "run_abc")

        assert _count_urls(conn) == 2, "Should allow same URL from different evidence"


class TestDuplicatesAllowed:
    """Test that duplicate URLs are now allowed (- no deduplication).

    Each URL observation is a distinct forensic event. Timeline and reports
    can aggregate at query time when needed.
    """

    def test_duplicate_with_run_id_allowed(self, db_with_migration):
        """Duplicate insert with same run_id should be allowed (each is a visit event)."""
        conn = db_with_migration

        _insert_url(conn, 1, "https://evil.com", "cache_simple:v0.67", "Cache/f_001", "run_abc")
        _insert_url(conn, 1, "https://evil.com", "cache_simple:v0.67", "Cache/f_001", "run_abc")

        assert _count_urls(conn) == 2, "Should allow duplicate URL events"

    def test_duplicate_with_null_run_id_allowed(self, db_with_migration):
        """Duplicate insert with NULL run_id should be allowed."""
        conn = db_with_migration

        _insert_url(conn, 1, "https://evil.com", "bulk_extractor:url", "url.txt:100", None)
        _insert_url(conn, 1, "https://evil.com", "bulk_extractor:url", "url.txt:100", None)

        assert _count_urls(conn) == 2, "Should allow duplicate with NULL run_id"

    def test_null_run_id_different_offset_allowed(self, db_with_migration):
        """NULL run_id with different source_path should be allowed."""
        conn = db_with_migration

        _insert_url(conn, 1, "https://evil.com", "bulk_extractor:url", "url.txt:100", None)
        _insert_url(conn, 1, "https://evil.com", "bulk_extractor:url", "url.txt:200", None)

        assert _count_urls(conn) == 2, "Should allow NULL run_id with different source_path"

    def test_null_source_path_allowed(self, db_with_migration):
        """NULL source_path duplicates should be allowed."""
        conn = db_with_migration

        _insert_url(conn, 1, "https://evil.com", "test_extractor", None, "run_abc")
        _insert_url(conn, 1, "https://evil.com", "test_extractor", None, "run_abc")

        assert _count_urls(conn) == 2, "Should allow duplicate with NULL source_path"

    def test_both_nulls_allowed(self, db_with_migration):
        """Both source_path and run_id NULL duplicates should be allowed."""
        conn = db_with_migration

        _insert_url(conn, 1, "https://evil.com", "test_extractor", None, None)
        _insert_url(conn, 1, "https://evil.com", "test_extractor", None, None)

        assert _count_urls(conn) == 2, "Should allow duplicate with both NULLs"


class TestMigration:
    """Test migration mechanics."""

    def test_migration_idempotent(self, tmp_path: Path):
        """Running migration twice should not error."""
        if not SCHEMA_PATH.exists():
            pytest.skip("Migration files not found")

        db_path = tmp_path / "test.sqlite"
        conn = sqlite3.connect(db_path)

        migrations_dir = SCHEMA_PATH.parent
        migrate(conn, migrations_dir=migrations_dir)
        migrate(conn, migrations_dir=migrations_dir)  # Second run should not error

        # Verify provenance unique index is REMOVED
        indexes = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_urls_provenance_unique'"
        ).fetchall()
        assert len(indexes) == 0, "Provenance unique index should be removed"

        conn.close()

    def test_old_index_removed(self, db_with_migration):
        """Old unique index should be dropped."""
        conn = db_with_migration

        indexes = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_urls_evidence_url_unique'"
        ).fetchall()
        assert len(indexes) == 0, "Old unique index should be removed"

    def test_provenance_index_removed(self, db_with_migration):
        """Provenance unique index should be removed."""
        conn = db_with_migration

        indexes = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_urls_provenance_unique'"
        ).fetchall()
        assert len(indexes) == 0, "Provenance unique index should be removed"

    def test_timestamp_index_exists(self, db_with_migration):
        """Timestamp index for timeline queries should exist."""
        conn = db_with_migration

        indexes = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_urls_evidence_first_seen'"
        ).fetchall()
        assert len(indexes) == 1, "Timestamp index should exist for timeline queries"


class TestBackwardCompatibility:
    """Test that existing code continues to work."""

    def test_insert_urls_function(self, db_with_migration):
        """insert_urls() from core.db should work with new constraint."""
        from core.database import insert_urls

        conn = db_with_migration

        insert_urls(conn, 1, [{
            "url": "https://test.com",
            "domain": "test.com",
            "scheme": "https",
            "discovered_by": "test",
            "source_path": "test.txt",
            "run_id": "test_run",
        }])

        assert _count_urls(conn) == 1, "insert_urls() should work with new constraint"

    def test_insert_urls_with_none_values(self, db_with_migration):
        """insert_urls() should handle None values correctly."""
        from core.database import insert_urls

        conn = db_with_migration

        # Insert with None values (like bulk_extractor does)
        insert_urls(conn, 1, [{
            "url": "https://test.com",
            "discovered_by": "bulk_extractor:url",
            "source_path": "url.txt:100",
            "run_id": None,  # bulk_extractor doesn't set run_id
        }])

        # Insert same URL from different source
        insert_urls(conn, 1, [{
            "url": "https://test.com",
            "discovered_by": "browser_history",
            "source_path": "History.db",
            "run_id": "run_abc",
        }])

        assert _count_urls(conn) == 2, "Should allow same URL from different sources via insert_urls()"
