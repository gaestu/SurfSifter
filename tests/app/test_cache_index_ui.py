"""Tests for CacheIndexTableModel and CacheContainer (non-GUI)."""
from __future__ import annotations

import pytest

from core.database.helpers import (
    insert_firefox_cache_index_entries,
)


def _make_entry(hash_char: str, **overrides) -> dict:
    """Build a minimal valid firefox_cache_index entry dict."""
    entry = {
        "run_id": "run-test",
        "source_path": "/cache2/index",
        "entry_hash": hash_char * 40,
        "browser": "firefox",
    }
    entry.update(overrides)
    return entry


class TestCacheIndexTableModel:
    """Test the CacheIndexTableModel column/header definitions."""

    def test_column_count(self):
        from app.features.browser_inventory.cache.index_model import (
            CacheIndexTableModel,
        )
        assert len(CacheIndexTableModel.COLUMNS) == 10

    def test_header_count_matches_columns(self):
        from app.features.browser_inventory.cache.index_model import (
            CacheIndexTableModel,
        )
        assert len(CacheIndexTableModel.HEADERS) == len(CacheIndexTableModel.COLUMNS)

    def test_headers_are_strings(self):
        from app.features.browser_inventory.cache.index_model import (
            CacheIndexTableModel,
        )
        for header in CacheIndexTableModel.HEADERS:
            assert isinstance(header, str)
            assert len(header) > 0

    def test_column_names_are_valid(self):
        from app.features.browser_inventory.cache.index_model import (
            CacheIndexTableModel,
        )
        expected_columns = [
            "entry_hash", "url", "content_type_name", "frecency",
            "file_size_kb", "entry_source", "has_entry_file",
            "is_removed", "is_anonymous", "is_pinned",
        ]
        assert CacheIndexTableModel.COLUMNS == expected_columns

    def test_default_page_size(self):
        from app.features.browser_inventory.cache.index_model import (
            CacheIndexTableModel,
        )
        assert CacheIndexTableModel.DEFAULT_PAGE_SIZE == 500


class TestCacheContainerImports:
    """Test that cache module exports are correct."""

    def test_cache_container_importable(self):
        from app.features.browser_inventory.cache import CacheContainer
        assert CacheContainer is not None

    def test_cache_subtab_still_importable(self):
        from app.features.browser_inventory.cache import CacheSubtab
        assert CacheSubtab is not None

    def test_container_module_exports(self):
        from app.features.browser_inventory.cache import __all__
        assert "CacheContainer" in __all__
        assert "CacheSubtab" in __all__


class TestCacheIndexDialogImport:
    """Test that the details dialog is importable."""

    def test_dialog_importable(self):
        from app.features.browser_inventory.cache.index_dialog import (
            CacheIndexDetailsDialog,
        )
        assert CacheIndexDetailsDialog is not None


class TestCacheIndexWidgetImport:
    """Test that the index widget is importable."""

    def test_widget_importable(self):
        from app.features.browser_inventory.cache.index_widget import (
            CacheIndexSubtab,
        )
        assert CacheIndexSubtab is not None

    def test_content_type_map(self):
        from app.features.browser_inventory.cache.index_widget import (
            CONTENT_TYPE_MAP,
        )
        assert 0 in CONTENT_TYPE_MAP  # Unknown
        assert 3 in CONTENT_TYPE_MAP  # Image
        assert 6 in CONTENT_TYPE_MAP  # WASM
        assert len(CONTENT_TYPE_MAP) == 7
