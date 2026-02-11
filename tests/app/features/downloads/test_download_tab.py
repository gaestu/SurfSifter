"""
Tests for Download Tab UI components.

Tests pagination, filters, and core functionality without requiring full GUI.
"""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest
from core.database import EVIDENCE_MIGRATIONS_DIR, init_db, migrate, slugify_label


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def temp_case_folder():
    """Create a temporary case folder with databases."""
    with tempfile.TemporaryDirectory() as tmpdir:
        case_folder = Path(tmpdir) / "test_case"
        case_folder.mkdir()

        # Create case database (migrated)
        case_db = case_folder / "test_case_surfsifter.sqlite"
        case_conn = init_db(case_folder, db_path=case_db)
        with case_conn:
            case_conn.execute(
                "INSERT INTO cases (case_id, title, created_at_utc) VALUES ('TEST001', 'Test Case', '2025-01-01T00:00:00Z')"
            )
            case_conn.execute(
                "INSERT INTO evidences (id, case_id, label, source_path, added_at_utc) VALUES (1, 1, 'EV001', '/test/image.E01', '2025-01-01T00:00:00Z')"
            )
        case_conn.close()

        # Create evidence database at the correct path (matches slugify_label)
        slug = slugify_label("EV001", 1)
        evidence_db_folder = case_folder / "evidences" / slug
        evidence_db_folder.mkdir(parents=True)
        evidence_db = evidence_db_folder / f"evidence_{slug}.sqlite"

        with sqlite3.connect(evidence_db) as conn:
            # Apply baseline evidence schema
            migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)

            # Insert test URLs with various extensions
            test_urls = [
                (1, 1, "https://example.com/image1.jpg", "example.com", "https", "bulk_extractor"),
                (2, 1, "https://example.com/image2.png", "example.com", "https", "bulk_extractor"),
                (3, 1, "https://test.org/doc.pdf", "test.org", "https", "regex"),
                (4, 1, "https://example.com/video.mp4", "example.com", "https", "bulk_extractor"),
                (5, 1, "https://other.net/archive.zip", "other.net", "https", "regex"),
            ]
            conn.executemany(
                "INSERT INTO urls (id, evidence_id, url, domain, scheme, discovered_by) VALUES (?, ?, ?, ?, ?, ?)",
                test_urls
            )

            # Insert a tag
            conn.execute(
                "INSERT INTO tags (id, evidence_id, name, name_normalized, created_by) VALUES (1, 1, 'suspicious', 'suspicious', 'manual')"
            )

            # Tag URL 1
            conn.execute(
                "INSERT INTO tag_associations (tag_id, evidence_id, artifact_type, artifact_id, tagged_by) VALUES (1, 1, 'url', 1, 'manual')"
            )

            # Add URL match
            conn.execute(
                "INSERT INTO url_matches (evidence_id, url_id, list_name, match_type) VALUES (1, 2, 'test_list', 'exact')"
            )

            conn.commit()

        yield case_folder, case_db


# -----------------------------------------------------------------------------
# Unit Tests for Data Layer
# -----------------------------------------------------------------------------

class TestDownloadableUrlFilters:
    """Tests for list_downloadable_urls and count_downloadable_urls with filters."""

    def test_list_downloadable_urls_basic(self, temp_case_folder):
        """Test basic URL listing."""
        from app.data.case_data import CaseDataAccess

        case_folder, case_db = temp_case_folder
        case_data = CaseDataAccess(case_folder, case_db)

        urls = case_data.list_downloadable_urls(1)
        assert len(urls) >= 1  # Should have at least our test URLs

    def test_count_downloadable_urls(self, temp_case_folder):
        """Test URL counting."""
        from app.data.case_data import CaseDataAccess

        case_folder, case_db = temp_case_folder
        case_data = CaseDataAccess(case_folder, case_db)

        count = case_data.count_downloadable_urls(1)
        assert count >= 1

    def test_filter_by_file_type_image(self, temp_case_folder):
        """Test filtering by image file type."""
        from app.data.case_data import CaseDataAccess

        case_folder, case_db = temp_case_folder
        case_data = CaseDataAccess(case_folder, case_db)

        urls = case_data.list_downloadable_urls(1, file_type="image")
        # Should include .jpg and .png URLs
        for url in urls:
            assert url["url"].endswith((".jpg", ".png", ".gif", ".jpeg", ".webp", ".bmp"))

    def test_filter_by_domain(self, temp_case_folder):
        """Test filtering by domain."""
        from app.data.case_data import CaseDataAccess

        case_folder, case_db = temp_case_folder
        case_data = CaseDataAccess(case_folder, case_db)

        urls = case_data.list_downloadable_urls(1, domain_filter="example.com")
        for url in urls:
            assert "example.com" in url.get("domain", "")

    def test_filter_by_search_text(self, temp_case_folder):
        """Test filtering by search text."""
        from app.data.case_data import CaseDataAccess

        case_folder, case_db = temp_case_folder
        case_data = CaseDataAccess(case_folder, case_db)

        urls = case_data.list_downloadable_urls(1, search_text="image1")
        assert any("image1" in url["url"] for url in urls)

    def test_filter_by_tag(self, temp_case_folder):
        """Test filtering by tag."""
        from app.data.case_data import CaseDataAccess

        case_folder, case_db = temp_case_folder
        case_data = CaseDataAccess(case_folder, case_db)

        urls = case_data.list_downloadable_urls(1, tag_filter="suspicious")
        # URL 1 is tagged with 'suspicious'
        url_ids = [u["id"] for u in urls]
        assert 1 in url_ids

    def test_filter_by_match_matched(self, temp_case_folder):
        """Test filtering for matched URLs."""
        from app.data.case_data import CaseDataAccess

        case_folder, case_db = temp_case_folder
        case_data = CaseDataAccess(case_folder, case_db)

        urls = case_data.list_downloadable_urls(1, match_filter="matched")
        # URL 2 has a match
        url_ids = [u["id"] for u in urls]
        assert 2 in url_ids

    def test_filter_by_match_unmatched(self, temp_case_folder):
        """Test filtering for unmatched URLs."""
        from app.data.case_data import CaseDataAccess

        case_folder, case_db = temp_case_folder
        case_data = CaseDataAccess(case_folder, case_db)

        urls = case_data.list_downloadable_urls(1, match_filter="unmatched")
        # URL 2 has a match, so should NOT be in unmatched list
        url_ids = [u["id"] for u in urls]
        assert 2 not in url_ids

    def test_filter_by_specific_list(self, temp_case_folder):
        """Test filtering by specific match list name."""
        from app.data.case_data import CaseDataAccess

        case_folder, case_db = temp_case_folder
        case_data = CaseDataAccess(case_folder, case_db)

        urls = case_data.list_downloadable_urls(1, match_filter="test_list")
        # URL 2 is matched by test_list
        url_ids = [u["id"] for u in urls]
        assert 2 in url_ids

    def test_pagination(self, temp_case_folder):
        """Test pagination with limit and offset."""
        from app.data.case_data import CaseDataAccess

        case_folder, case_db = temp_case_folder
        case_data = CaseDataAccess(case_folder, case_db)

        # Get first 2
        urls_page1 = case_data.list_downloadable_urls(1, limit=2, offset=0)

        # Get next 2
        urls_page2 = case_data.list_downloadable_urls(1, limit=2, offset=2)

        # Ensure no overlap
        ids_page1 = {u["id"] for u in urls_page1}
        ids_page2 = {u["id"] for u in urls_page2}
        assert ids_page1.isdisjoint(ids_page2), "Pages should not overlap"

    def test_count_matches_filters(self, temp_case_folder):
        """Test that count respects filters."""
        from app.data.case_data import CaseDataAccess

        case_folder, case_db = temp_case_folder
        case_data = CaseDataAccess(case_folder, case_db)

        count_all = case_data.count_downloadable_urls(1)
        count_matched = case_data.count_downloadable_urls(1, match_filter="matched")
        count_unmatched = case_data.count_downloadable_urls(1, match_filter="unmatched")

        # matched + unmatched should equal total
        assert count_matched + count_unmatched == count_all


class TestUrlMatchLists:
    """Tests for list_url_match_lists method."""

    def test_list_url_match_lists(self, temp_case_folder):
        """Test listing available match list names."""
        from app.data.case_data import CaseDataAccess

        case_folder, case_db = temp_case_folder
        case_data = CaseDataAccess(case_folder, case_db)

        lists = case_data.list_url_match_lists(1)
        assert "test_list" in lists


# -----------------------------------------------------------------------------
# Tests for Download Tab UI (non-GUI)
# -----------------------------------------------------------------------------

class TestAvailableUrlsWorker:
    """Tests for AvailableUrlsWorker parameters."""

    def test_worker_accepts_tag_filter(self, temp_case_folder):
        """Test that worker accepts tag_filter parameter."""
        from app.features.downloads.tab import AvailableUrlsWorker

        case_folder, case_db = temp_case_folder

        worker = AvailableUrlsWorker(
            case_folder=case_folder,
            case_db_path=case_db,
            evidence_id=1,
            file_type="image",
            domain_filter=None,
            search_text=None,
            tag_filter="suspicious",
            match_filter=None,
            limit=100,
            offset=0,
        )

        assert worker.tag_filter == "suspicious"
        assert worker.match_filter is None

    def test_worker_accepts_match_filter(self, temp_case_folder):
        """Test that worker accepts match_filter parameter."""
        from app.features.downloads.tab import AvailableUrlsWorker

        case_folder, case_db = temp_case_folder

        worker = AvailableUrlsWorker(
            case_folder=case_folder,
            case_db_path=case_db,
            evidence_id=1,
            file_type=None,
            domain_filter=None,
            search_text=None,
            tag_filter=None,
            match_filter="matched",
            limit=500,
            offset=0,
        )

        assert worker.match_filter == "matched"
        assert worker.limit == 500


class TestPaginationHelper:
    """Tests for pagination logic."""

    def test_total_pages_calculation(self):
        """Test _total_pages property calculation."""
        # Simulate the calculation
        page_size = 500

        # 0 items = 1 page (minimum)
        total_count = 0
        total_pages = max(1, (total_count + page_size - 1) // page_size)
        assert total_pages == 1

        # 500 items = 1 page
        total_count = 500
        total_pages = max(1, (total_count + page_size - 1) // page_size)
        assert total_pages == 1

        # 501 items = 2 pages
        total_count = 501
        total_pages = max(1, (total_count + page_size - 1) // page_size)
        assert total_pages == 2

        # 1000 items = 2 pages
        total_count = 1000
        total_pages = max(1, (total_count + page_size - 1) // page_size)
        assert total_pages == 2

        # 100000 items = 200 pages
        total_count = 100000
        total_pages = max(1, (total_count + page_size - 1) // page_size)
        assert total_pages == 200


class TestUrlExtensionBackfill:
    """Tests for file_extension backfill optimization."""

    def test_backfill_url_extensions(self, temp_case_folder):
        """Test that backfill populates file_extension and file_type columns."""
        from app.data.case_data import CaseDataAccess

        case_folder, case_db = temp_case_folder
        with CaseDataAccess(case_folder, case_db) as case_data:
            # Run backfill
            updated, elapsed = case_data.backfill_url_extensions(1)

            # Should have updated all 5 URLs
            assert updated == 5
            assert elapsed >= 0

    def test_backfill_is_idempotent(self, temp_case_folder):
        """Test that running backfill twice doesn't double-update."""
        from app.data.case_data import CaseDataAccess

        case_folder, case_db = temp_case_folder
        with CaseDataAccess(case_folder, case_db) as case_data:
            # First run
            updated1, _ = case_data.backfill_url_extensions(1)
            assert updated1 == 5

            # Second run should update nothing
            updated2, _ = case_data.backfill_url_extensions(1)
            assert updated2 == 0

    def test_backfill_status(self, temp_case_folder):
        """Test get_extension_backfill_status reports correct counts."""
        from app.data.case_data import CaseDataAccess

        case_folder, case_db = temp_case_folder
        with CaseDataAccess(case_folder, case_db) as case_data:
            # Before backfill
            status = case_data.get_extension_backfill_status(1)
            assert status['total'] == 5
            assert status['pending'] == 5
            assert status['backfilled'] == 0

            # After backfill
            case_data.backfill_url_extensions(1)
            status = case_data.get_extension_backfill_status(1)
            assert status['total'] == 5
            assert status['pending'] == 0
            assert status['backfilled'] == 5

    def test_optimized_query_uses_file_type(self, temp_case_folder):
        """Test that after backfill, queries use indexed file_type column."""
        from app.data.case_data import CaseDataAccess

        case_folder, case_db = temp_case_folder
        with CaseDataAccess(case_folder, case_db) as case_data:
            # Backfill first
            case_data.backfill_url_extensions(1)

            # Query for images - should use optimized path
            urls = case_data.list_downloadable_urls(1, file_type="image")

            # Should find the 2 image URLs (.jpg, .png)
            assert len(urls) == 2
            extensions = {u['url'].split('.')[-1] for u in urls}
            assert extensions == {'jpg', 'png'}

    def test_optimized_query_all_types(self, temp_case_folder):
        """Test optimized query returns all downloadable types."""
        from app.data.case_data import CaseDataAccess

        case_folder, case_db = temp_case_folder
        with CaseDataAccess(case_folder, case_db) as case_data:
            # Backfill first
            case_data.backfill_url_extensions(1)

            # Query for all types
            urls = case_data.list_downloadable_urls(1)

            # All 5 URLs have downloadable extensions
            assert len(urls) == 5

    def test_fallback_query_without_backfill(self, temp_case_folder):
        """Test that queries work even without backfill (fallback path)."""
        from app.data.case_data import CaseDataAccess
        import sqlite3
        from core.database import slugify_label

        case_folder, case_db = temp_case_folder

        # Clear file_type column to simulate pre-backfill state
        slug = slugify_label("EV001", 1)
        evidence_db = case_folder / "evidences" / slug / f"evidence_{slug}.sqlite"
        with sqlite3.connect(evidence_db) as conn:
            conn.execute("UPDATE urls SET file_type = NULL, file_extension = NULL")
            conn.commit()

        with CaseDataAccess(case_folder, case_db) as case_data:
            # Should still work via fallback LIKE queries
            urls = case_data.list_downloadable_urls(1, file_type="image")

            # Should find the 2 image URLs
            assert len(urls) == 2


class TestUrlGrouping:
    """Tests for URL grouping/deduplication."""

    def test_duplicate_urls_grouped(self, temp_case_folder):
        """Test that same URL from multiple sources appears only once."""
        from app.data.case_data import CaseDataAccess
        import sqlite3
        from core.database import slugify_label

        case_folder, case_db = temp_case_folder

        # Add duplicate URL from different source
        slug = slugify_label("EV001", 1)
        evidence_db = case_folder / "evidences" / slug / f"evidence_{slug}.sqlite"
        with sqlite3.connect(evidence_db) as conn:
            conn.execute(
                "INSERT INTO urls (evidence_id, url, domain, scheme, discovered_by) "
                "VALUES (1, 'https://example.com/image1.jpg', 'example.com', 'https', 'cache_simple')"
            )
            conn.commit()

        with CaseDataAccess(case_folder, case_db) as case_data:
            urls = case_data.list_downloadable_urls(1)

            # Count how many times image1.jpg appears
            image1_urls = [u for u in urls if "image1.jpg" in u["url"]]

            # Should appear only once (grouped)
            assert len(image1_urls) == 1

            # But source_count should be 2
            assert image1_urls[0]["source_count"] == 2

            # And discovered_by should contain both sources
            sources = image1_urls[0]["discovered_by"]
            assert "bulk_extractor" in sources
            assert "cache_simple" in sources

    def test_count_returns_distinct_urls(self, temp_case_folder):
        """Test that count returns number of distinct URLs."""
        from app.data.case_data import CaseDataAccess
        import sqlite3
        from core.database import slugify_label

        case_folder, case_db = temp_case_folder

        # Add duplicate URL
        slug = slugify_label("EV001", 1)
        evidence_db = case_folder / "evidences" / slug / f"evidence_{slug}.sqlite"
        with sqlite3.connect(evidence_db) as conn:
            conn.execute(
                "INSERT INTO urls (evidence_id, url, domain, scheme, discovered_by) "
                "VALUES (1, 'https://example.com/image1.jpg', 'example.com', 'https', 'cache_simple')"
            )
            conn.commit()

        with CaseDataAccess(case_folder, case_db) as case_data:
            # Count should still be 5 (not 6) because of deduplication
            count = case_data.count_downloadable_urls(1)
            assert count == 5

    def test_format_sources_single(self):
        """Test _format_sources with single source."""
        from app.features.downloads.subtab_available import AvailableDownloadsPanel

        # Create minimal panel to test method
        panel = AvailableDownloadsPanel.__new__(AvailableDownloadsPanel)

        result = panel._format_sources("bulk_extractor", 1)
        assert result == "bulk_extractor"

    def test_format_sources_multiple_distinct(self):
        """Test _format_sources with multiple distinct sources."""
        from app.features.downloads.subtab_available import AvailableDownloadsPanel

        panel = AvailableDownloadsPanel.__new__(AvailableDownloadsPanel)

        result = panel._format_sources("bulk_extractor,cache_simple,browser_history", 3)
        assert result == "bulk_extractor (+2)"

    def test_format_sources_same_source_multiple_times(self):
        """Test _format_sources when same source found URL multiple times."""
        from app.features.downloads.subtab_available import AvailableDownloadsPanel

        panel = AvailableDownloadsPanel.__new__(AvailableDownloadsPanel)

        # Same source found the URL 5 times
        result = panel._format_sources("bulk_extractor", 5)
        assert result == "bulk_extractor (×5)"

    def test_format_sources_empty(self):
        """Test _format_sources with empty input."""
        from app.features.downloads.subtab_available import AvailableDownloadsPanel

        panel = AvailableDownloadsPanel.__new__(AvailableDownloadsPanel)

        result = panel._format_sources("", 0)
        assert result == "—"

        result = panel._format_sources(None, 0)
        assert result == "—"
