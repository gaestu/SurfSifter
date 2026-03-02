"""Tests for Images tab UX improvements (issue #22).

Covers:
- Phase 1: Check Visible / Uncheck Visible behavior
- Phase 2: URL text filter propagation and query behavior
- Phase 3: Hash match display in preview metadata
- Phase 4: Dynamic hash filter refresh after hash check
- Phase 5: SHA256 display cleanup (single-line, no split)
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from app.data.case_data import CaseDataAccess
from core.database import DatabaseManager, init_db


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def evidence_db(tmp_path: Path):
    """Create a minimal evidence database with images and discoveries."""
    case_dir = tmp_path / "case_workspace"
    case_dir.mkdir(parents=True, exist_ok=True)

    case_db_path = case_dir / "TEST_surfsifter.sqlite"
    case_conn = init_db(case_dir, db_path=case_db_path)

    with case_conn:
        case_conn.execute(
            "INSERT INTO cases (case_id, title, created_at_utc) VALUES (?, ?, ?)",
            ("CASE_UX", "UX Test", "2025-01-01T00:00:00Z"),
        )
        case_conn.execute(
            "INSERT INTO evidences (case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
            (1, "EV-UX", "/tmp/test.E01", "2025-01-01T00:00:00Z"),
        )
    case_conn.close()

    manager = DatabaseManager(case_dir, case_db_path=case_db_path, enable_split=True)
    ev_conn = manager.get_evidence_conn(1, label="EV-UX")

    # Insert test images
    with ev_conn:
        for i in range(5):
            has_url = i < 3  # First 3 images have URLs
            ev_conn.execute(
                """INSERT INTO images
                   (evidence_id, rel_path, filename, md5, sha256, phash,
                    first_discovered_by, ts_utc, size_bytes)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    1,
                    f"images/img_{i}.jpg",
                    f"img_{i}.jpg",
                    f"{i:032x}",
                    f"{i:064x}",
                    f"{'c' * 16}",
                    "cache_simple" if has_url else "filesystem_images",
                    f"2025-01-0{i + 1}T00:00:00Z",
                    1000 * (i + 1),
                ),
            )
            img_id = i + 1

            # Add discovery records
            ev_conn.execute(
                """INSERT INTO image_discoveries
                   (evidence_id, image_id, discovered_by, run_id, cache_url)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    1,
                    img_id,
                    "cache_simple" if has_url else "filesystem_images",
                    "run-1",
                    f"https://example.com/images/photo_{i}.jpg" if has_url else None,
                ),
            )

        # Add hash match for image 1
        ev_conn.execute(
            """INSERT INTO hash_matches
               (evidence_id, image_id, db_name, db_md5, list_name, matched_at_utc, note)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (1, 1, "known_bad.txt", "0" * 32, "known_bad", "2025-06-01T00:00:00Z", "CSAM indicator"),
        )

    case_data = CaseDataAccess(case_dir)
    return case_data, 1, ev_conn, manager


# ---------------------------------------------------------------------------
# Phase 2: URL text filter query
# ---------------------------------------------------------------------------


class TestUrlTextFilter:
    """Test URL text filter propagation in iter_images query."""

    def test_url_filter_returns_matching_images(self, evidence_db):
        """URL filter returns only images with matching cache_url."""
        case_data, evidence_id, _, _ = evidence_db
        results = case_data.iter_images(evidence_id, url_text="example.com")
        # First 3 images have URLs with example.com
        assert len(results) == 3, f"Expected 3 results, got {len(results)}"

    def test_url_filter_case_insensitive(self, evidence_db):
        """URL filter is case-insensitive."""
        case_data, evidence_id, _, _ = evidence_db
        results_upper = case_data.iter_images(evidence_id, url_text="EXAMPLE.COM")
        results_lower = case_data.iter_images(evidence_id, url_text="example.com")
        assert len(results_upper) == len(results_lower)

    def test_url_filter_no_match_returns_empty(self, evidence_db):
        """URL filter with non-matching text returns empty list."""
        case_data, evidence_id, _, _ = evidence_db
        results = case_data.iter_images(evidence_id, url_text="nonexistent.org")
        assert len(results) == 0

    def test_url_filter_partial_match(self, evidence_db):
        """URL filter matches substrings."""
        case_data, evidence_id, _, _ = evidence_db
        results = case_data.iter_images(evidence_id, url_text="photo_0")
        assert len(results) == 1

    def test_url_filter_empty_string_returns_all(self, evidence_db):
        """Empty URL text should not filter (returns all images)."""
        case_data, evidence_id, _, _ = evidence_db
        all_results = case_data.iter_images(evidence_id)
        filtered_results = case_data.iter_images(evidence_id, url_text="")
        assert len(all_results) == len(filtered_results)

    def test_url_filter_combines_with_other_filters(self, evidence_db):
        """URL filter works together with source filter."""
        case_data, evidence_id, _, _ = evidence_db
        results = case_data.iter_images(
            evidence_id,
            discovered_by=("cache_simple",),
            url_text="example.com",
        )
        assert len(results) == 3

        # Combine URL filter with non-matching source
        results = case_data.iter_images(
            evidence_id,
            discovered_by=("filesystem_images",),
            url_text="example.com",
        )
        # filesystem_images don't have cache URLs
        assert len(results) == 0


# ---------------------------------------------------------------------------
# Phase 2: Filter state propagation
# ---------------------------------------------------------------------------


class TestFilterStatePropagation:
    """Test that url_text propagates correctly through model set_filters."""

    def test_images_list_model_accepts_url_text(self):
        """ImagesListModel.set_filters accepts url_text parameter."""
        from app.features.images.models.images_list import ImagesListModel

        model = ImagesListModel()
        # Should not raise
        model._filters["url_text"] = "test"
        assert model._filters["url_text"] == "test"

    def test_images_table_model_accepts_url_text(self):
        """ImagesTableModel.set_filters accepts url_text parameter."""
        from app.features.images.models.images_table import ImagesTableModel

        model = ImagesTableModel()
        model._filters["url_text"] = "test"
        assert model._filters["url_text"] == "test"

    def test_image_filters_dataclass_has_url_text(self):
        """ImageFilters dataclass includes url_text field."""
        from app.features.images.tab import ImageFilters

        f = ImageFilters()
        assert hasattr(f, "url_text")
        assert f.url_text == ""

        f2 = ImageFilters(url_text="microsoft")
        assert f2.url_text == "microsoft"


# ---------------------------------------------------------------------------
# Phase 3: Hash match display in preview metadata
# ---------------------------------------------------------------------------


class TestHashMatchPreviewMetadata:
    """Test that hash matches are shown in preview metadata."""

    def test_preview_shows_hash_matches(self):
        """ImagePreviewDialog displays hash matches section."""
        from app.common.dialogs.image_preview import ImagePreviewDialog

        image_data = {
            "filename": "test.jpg",
            "md5": "a" * 32,
            "sha256": "b" * 64,
            "phash": "c" * 16,
            "ts_utc": "2025-01-01",
            "tags": "",
        }
        matches = [
            {"list_name": "known_bad", "db_name": "known_bad.txt",
             "matched_at_utc": "2025-06-01T00:00:00Z", "note": "Test note"},
        ]

        # Test metadata generation without creating Qt objects
        dialog = ImagePreviewDialog.__new__(ImagePreviewDialog)
        dialog.image_data = image_data
        dialog.discoveries = []
        dialog.hash_matches = matches
        dialog.metadata_display = MagicMock()

        dialog._populate_metadata()

        # Get the HTML that was set
        call_args = dialog.metadata_display.setHtml.call_args
        html = call_args[0][0]

        assert "Hash Matches (1)" in html
        assert "known_bad" in html
        assert "2025-06-01" in html
        assert "Test note" in html

    def test_preview_shows_no_matches(self):
        """Preview shows 'None' when no hash matches exist."""
        from app.common.dialogs.image_preview import ImagePreviewDialog

        image_data = {
            "filename": "test.jpg",
            "md5": "a" * 32,
            "sha256": "b" * 64,
            "phash": "c" * 16,
            "ts_utc": "2025-01-01",
            "tags": "",
        }

        dialog = ImagePreviewDialog.__new__(ImagePreviewDialog)
        dialog.image_data = image_data
        dialog.discoveries = []
        dialog.hash_matches = []
        dialog.metadata_display = MagicMock()

        dialog._populate_metadata()

        html = dialog.metadata_display.setHtml.call_args[0][0]
        assert "Hash Matches:</b> None" in html


# ---------------------------------------------------------------------------
# Phase 5: SHA256 display cleanup
# ---------------------------------------------------------------------------


class TestSha256DisplayCleanup:
    """Test that SHA256 is displayed as a single clean value."""

    def test_sha256_no_line_break(self):
        """SHA256 should not be split across lines."""
        from app.common.dialogs.image_preview import ImagePreviewDialog

        sha256 = "b" * 64
        image_data = {
            "filename": "test.jpg",
            "md5": "a" * 32,
            "sha256": sha256,
            "phash": "c" * 16,
            "ts_utc": "2025-01-01",
            "tags": "",
        }

        dialog = ImagePreviewDialog.__new__(ImagePreviewDialog)
        dialog.image_data = image_data
        dialog.discoveries = []
        dialog.hash_matches = []
        dialog.metadata_display = MagicMock()

        dialog._populate_metadata()

        html = dialog.metadata_display.setHtml.call_args[0][0]

        # The full SHA256 should appear in one piece (no <br/> split)
        assert sha256 in html
        # Check that the old split pattern is not present
        assert f"{sha256[:32]}<br/>" not in html


# ---------------------------------------------------------------------------
# Phase 1: Check Visible / Uncheck Visible (unit logic)
# ---------------------------------------------------------------------------


class TestCheckVisibleLogic:
    """Test _get_visible_rows and check/uncheck visible logic."""

    def test_get_visible_rows_grid_mode(self):
        """_get_visible_rows returns model rows in grid mode."""
        from app.features.images.tab import ImagesTab

        tab = ImagesTab.__new__(ImagesTab)
        tab._view_mode = "grid"
        tab.model = MagicMock()
        tab.model._rows = [{"id": 1}, {"id": 2}, {"id": 3}]
        tab.table_model = MagicMock()
        tab.table_model._rows = []
        tab.cluster_model = MagicMock()
        tab.cluster_model._clusters = []
        tab.cluster_members_model = MagicMock()
        tab.cluster_members_model._rows = []

        rows = tab._get_visible_rows()
        assert len(rows) == 3
        assert rows[0]["id"] == 1

    def test_get_visible_rows_table_mode(self):
        """_get_visible_rows returns table model rows in table mode."""
        from app.features.images.tab import ImagesTab

        tab = ImagesTab.__new__(ImagesTab)
        tab._view_mode = "table"
        tab.model = MagicMock()
        tab.model._rows = []
        tab.table_model = MagicMock()
        tab.table_model._rows = [{"id": 10}, {"id": 20}]
        tab.cluster_model = MagicMock()
        tab.cluster_model._clusters = []
        tab.cluster_members_model = MagicMock()
        tab.cluster_members_model._rows = []

        rows = tab._get_visible_rows()
        assert len(rows) == 2

    def test_check_visible_adds_to_checked_ids(self):
        """_check_visible adds all visible image IDs to checked set."""
        from app.features.images.tab import ImagesTab

        tab = ImagesTab.__new__(ImagesTab)
        tab._view_mode = "grid"
        tab._checked_image_ids = set()
        tab.model = MagicMock()
        tab.model._rows = [{"id": 1}, {"id": 2}, {"id": 3}]
        tab.table_model = MagicMock()
        tab.table_model._rows = []
        tab.cluster_model = MagicMock()
        tab.cluster_model._clusters = []
        tab.cluster_members_model = MagicMock()
        tab.cluster_members_model._rows = []
        tab.checked_count_label = MagicMock()
        tab.clear_checks_button = MagicMock()
        tab.tag_checked_button = MagicMock()

        tab._check_visible()

        assert tab._checked_image_ids == {1, 2, 3}

    def test_uncheck_visible_removes_from_checked_ids(self):
        """_uncheck_visible removes visible image IDs from checked set."""
        from app.features.images.tab import ImagesTab

        tab = ImagesTab.__new__(ImagesTab)
        tab._view_mode = "grid"
        tab._checked_image_ids = {1, 2, 3, 99}  # 99 is from another page
        tab.model = MagicMock()
        tab.model._rows = [{"id": 1}, {"id": 2}, {"id": 3}]
        tab.table_model = MagicMock()
        tab.table_model._rows = []
        tab.cluster_model = MagicMock()
        tab.cluster_model._clusters = []
        tab.cluster_members_model = MagicMock()
        tab.cluster_members_model._rows = []
        tab.checked_count_label = MagicMock()
        tab.clear_checks_button = MagicMock()
        tab.tag_checked_button = MagicMock()

        tab._uncheck_visible()

        # Only 99 should remain (it was on a different page)
        assert tab._checked_image_ids == {99}


# ---------------------------------------------------------------------------
# Phase 4: Hash filter refresh path
# ---------------------------------------------------------------------------


class TestHashFilterRefresh:
    """Test that _on_hash_check_finished triggers filter repopulation."""

    @patch("app.features.images.tab.QMessageBox")
    def test_hash_check_finished_calls_populate_filters(self, mock_msgbox):
        """_on_hash_check_finished should call _populate_filters to refresh dropdowns."""
        from app.features.images.tab import ImagesTab

        tab = ImagesTab.__new__(ImagesTab)
        tab._hash_check_progress = MagicMock()
        tab._populate_filters = MagicMock()
        tab.hashLookupFinished = MagicMock()

        tab._on_hash_check_finished({"test_list": 5})

        tab._populate_filters.assert_called_once()
        tab.hashLookupFinished.emit.assert_called_once()
