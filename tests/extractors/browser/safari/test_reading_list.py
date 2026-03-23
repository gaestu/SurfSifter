"""Tests for Safari Reading List parser and extractor."""

from __future__ import annotations

import fnmatch
import json
import plistlib
from datetime import datetime, timezone
from pathlib import Path

from extractors.browser.safari._parsers import (
    SafariReadingListItem,
    _datetime_to_utc_iso,
    get_reading_list_stats,
    parse_reading_list,
)
from extractors.browser.safari.reading_list import SafariReadingListExtractor
from extractors.extractor_registry import ExtractorRegistry


# =============================================================================
# Test Helpers
# =============================================================================


class _Callbacks:
    def on_step(self, step_name: str) -> None:
        return None

    def on_log(self, message: str, level: str = "info") -> None:
        return None

    def on_error(self, error: str, details: str = "") -> None:
        return None

    def on_progress(self, current: int, total: int, message: str = "") -> None:
        return None

    def is_cancelled(self) -> bool:
        return False


class _FakeEvidenceFS:
    def __init__(self, file_map: dict[str, bytes]):
        self.file_map = file_map
        self.fs_type = "APFS"
        self.source_path = "/tmp/evidence.E01"
        self.partition_index = 0

    def iter_paths(self, pattern: str):
        for path in self.file_map:
            if fnmatch.fnmatch(path, pattern) or fnmatch.fnmatch(f"/{path}", pattern):
                yield path

    def read_file(self, path: str) -> bytes:
        if path in self.file_map:
            return self.file_map[path]
        alt = path.lstrip("/")
        if alt in self.file_map:
            return self.file_map[alt]
        raise FileNotFoundError(path)


def _plist_bytes(payload: object) -> bytes:
    return plistlib.dumps(payload, fmt=plistlib.FMT_BINARY)


def _make_reading_list_plist(items: list[dict] | None = None) -> dict:
    """Build a realistic Bookmarks.plist with a Reading List folder."""
    if items is None:
        # plistlib uses naive datetimes (Apple convention)
        items = [
            {
                "WebBookmarkType": "WebBookmarkTypeLeaf",
                "URLString": "https://example.com/article",
                "URIDictionary": {"title": "Example Article"},
                "ReadingList": {
                    "DateAdded": datetime(2024, 6, 15, 10, 30, 0),
                    "DateLastFetched": datetime(2024, 6, 15, 10, 31, 0),
                    "PreviewText": "This is an example article about...",
                    "FetchResult": 1,
                },
            },
            {
                "WebBookmarkType": "WebBookmarkTypeLeaf",
                "URLString": "https://news.ycombinator.com/",
                "URIDictionary": {"title": "Hacker News"},
                "ReadingList": {
                    "DateAdded": datetime(2024, 7, 1, 14, 0, 0),
                },
            },
        ]

    return {
        "WebBookmarkType": "WebBookmarkTypeList",
        "Title": "",
        "Children": [
            {
                "WebBookmarkType": "WebBookmarkTypeList",
                "Title": "BookmarksBar",
                "Children": [
                    {
                        "WebBookmarkType": "WebBookmarkTypeLeaf",
                        "URLString": "https://regular-bookmark.com/",
                        "URIDictionary": {"title": "Regular Bookmark"},
                    },
                ],
            },
            {
                "WebBookmarkType": "WebBookmarkTypeList",
                "Title": "com.apple.ReadingList",
                "Children": items,
            },
        ],
    }


# =============================================================================
# Parser Tests
# =============================================================================


class TestParseReadingList:
    """Test parse_reading_list function."""

    def test_parse_multiple_items(self, tmp_path: Path) -> None:
        """Parse a plist with multiple Reading List items."""
        plist_data = _make_reading_list_plist()
        path = tmp_path / "Bookmarks.plist"
        path.write_bytes(_plist_bytes(plist_data))

        items = parse_reading_list(path)

        assert len(items) == 2

        # First item — fully populated
        assert items[0].url == "https://example.com/article"
        assert items[0].title == "Example Article"
        assert items[0].date_added == datetime(2024, 6, 15, 10, 30, 0)
        assert items[0].date_added_utc is not None
        assert "2024-06-15" in items[0].date_added_utc
        assert items[0].date_last_fetched is not None
        assert items[0].preview_text == "This is an example article about..."
        assert items[0].fetch_result == 1

        # Second item — minimal metadata
        assert items[1].url == "https://news.ycombinator.com/"
        assert items[1].title == "Hacker News"
        assert items[1].date_added is not None
        assert items[1].date_last_fetched is None
        assert items[1].date_last_viewed is None
        assert items[1].preview_text is None
        assert items[1].fetch_result is None

    def test_parse_with_date_last_viewed(self, tmp_path: Path) -> None:
        """Parse an item with DateLastViewed (newer Safari versions)."""
        items_data = [
            {
                "WebBookmarkType": "WebBookmarkTypeLeaf",
                "URLString": "https://example.com/viewed",
                "URIDictionary": {"title": "Viewed Article"},
                "ReadingList": {
                    "DateAdded": datetime(2024, 8, 1, 12, 0, 0),
                    "DateLastViewed": datetime(2024, 8, 5, 9, 0, 0),
                    "FetchResult": 1,
                },
            },
        ]
        plist_data = _make_reading_list_plist(items_data)
        path = tmp_path / "Bookmarks.plist"
        path.write_bytes(_plist_bytes(plist_data))

        items = parse_reading_list(path)
        assert len(items) == 1
        assert items[0].date_last_viewed is not None
        assert items[0].date_last_viewed_utc is not None
        assert "2024-08-05" in items[0].date_last_viewed_utc

    def test_parse_empty_reading_list(self, tmp_path: Path) -> None:
        """Parse a plist with an empty Reading List folder."""
        plist_data = _make_reading_list_plist(items=[])
        path = tmp_path / "Bookmarks.plist"
        path.write_bytes(_plist_bytes(plist_data))

        items = parse_reading_list(path)
        assert items == []

    def test_parse_no_reading_list_folder(self, tmp_path: Path) -> None:
        """Parse a plist with no Reading List folder at all."""
        plist_data = {
            "WebBookmarkType": "WebBookmarkTypeList",
            "Title": "",
            "Children": [
                {
                    "WebBookmarkType": "WebBookmarkTypeList",
                    "Title": "BookmarksBar",
                    "Children": [],
                },
            ],
        }
        path = tmp_path / "Bookmarks.plist"
        path.write_bytes(_plist_bytes(plist_data))

        items = parse_reading_list(path)
        assert items == []

    def test_parse_missing_optional_fields(self, tmp_path: Path) -> None:
        """Parse an item with no ReadingList metadata dict."""
        items_data = [
            {
                "WebBookmarkType": "WebBookmarkTypeLeaf",
                "URLString": "https://example.com/minimal",
                "URIDictionary": {"title": "Minimal"},
            },
        ]
        plist_data = _make_reading_list_plist(items_data)
        path = tmp_path / "Bookmarks.plist"
        path.write_bytes(_plist_bytes(plist_data))

        items = parse_reading_list(path)
        assert len(items) == 1
        assert items[0].url == "https://example.com/minimal"
        assert items[0].date_added is None
        assert items[0].preview_text is None
        assert items[0].fetch_result is None

    def test_parse_skips_blank_urls(self, tmp_path: Path) -> None:
        """Items with empty URLString are skipped."""
        items_data = [
            {
                "WebBookmarkType": "WebBookmarkTypeLeaf",
                "URLString": "",
                "URIDictionary": {"title": "No URL"},
                "ReadingList": {"DateAdded": datetime(2024, 1, 1)},
            },
            {
                "WebBookmarkType": "WebBookmarkTypeLeaf",
                "URLString": "https://valid.com/",
                "URIDictionary": {"title": "Valid"},
                "ReadingList": {"DateAdded": datetime(2024, 1, 2)},
            },
        ]
        plist_data = _make_reading_list_plist(items_data)
        path = tmp_path / "Bookmarks.plist"
        path.write_bytes(_plist_bytes(plist_data))

        items = parse_reading_list(path)
        assert len(items) == 1
        assert items[0].url == "https://valid.com/"

    def test_parse_skips_folder_children(self, tmp_path: Path) -> None:
        """Only WebBookmarkTypeLeaf entries are parsed."""
        items_data = [
            {
                "WebBookmarkType": "WebBookmarkTypeList",
                "Title": "SubFolder",
                "Children": [],
            },
            {
                "WebBookmarkType": "WebBookmarkTypeLeaf",
                "URLString": "https://real.com/",
                "URIDictionary": {"title": "Real"},
                "ReadingList": {"DateAdded": datetime(2024, 1, 1)},
            },
        ]
        plist_data = _make_reading_list_plist(items_data)
        path = tmp_path / "Bookmarks.plist"
        path.write_bytes(_plist_bytes(plist_data))

        items = parse_reading_list(path)
        assert len(items) == 1
        assert items[0].url == "https://real.com/"

    def test_parse_corrupt_file(self, tmp_path: Path) -> None:
        """Corrupt files return empty list."""
        path = tmp_path / "Bookmarks.plist"
        path.write_bytes(b"not-a-plist")

        items = parse_reading_list(path)
        assert items == []

    def test_parse_binary_plist_format(self, tmp_path: Path) -> None:
        """Binary plist format is handled correctly."""
        plist_data = _make_reading_list_plist()
        path = tmp_path / "Bookmarks.plist"
        # Explicitly write as binary plist
        with open(path, "wb") as f:
            plistlib.dump(plist_data, f, fmt=plistlib.FMT_BINARY)

        items = parse_reading_list(path)
        assert len(items) == 2

    def test_parse_xml_plist_format(self, tmp_path: Path) -> None:
        """XML plist format is handled correctly."""
        plist_data = _make_reading_list_plist()
        path = tmp_path / "Bookmarks.plist"
        with open(path, "wb") as f:
            plistlib.dump(plist_data, f, fmt=plistlib.FMT_XML)

        items = parse_reading_list(path)
        assert len(items) == 2


class TestDatetimeToUtcIso:
    """Test _datetime_to_utc_iso edge cases."""

    def test_none_returns_none(self) -> None:
        assert _datetime_to_utc_iso(None) is None

    def test_naive_datetime(self) -> None:
        result = _datetime_to_utc_iso(datetime(2024, 6, 15, 10, 30, 0))
        assert result is not None
        assert "2024-06-15" in result
        assert result.endswith("+00:00")

    def test_aware_datetime(self) -> None:
        from datetime import timedelta
        tz_plus2 = timezone(timedelta(hours=2))
        dt = datetime(2024, 6, 15, 12, 30, 0, tzinfo=tz_plus2)
        result = _datetime_to_utc_iso(dt)
        assert result is not None
        # 12:30 +02:00 = 10:30 UTC
        assert "10:30:00" in result


# =============================================================================
# Stats Tests
# =============================================================================


class TestReadingListStats:
    """Test get_reading_list_stats function."""

    def test_stats_with_items(self) -> None:
        items = [
            SafariReadingListItem(
                url="https://example.com/a",
                title="A",
                date_added=None,
                date_added_utc=None,
                date_last_fetched=None,
                date_last_fetched_utc=None,
                date_last_viewed=None,
                date_last_viewed_utc=None,
                preview_text=None,
                fetch_result=1,
            ),
            SafariReadingListItem(
                url="https://example.com/b",
                title="B",
                date_added=None,
                date_added_utc=None,
                date_last_fetched=None,
                date_last_fetched_utc=None,
                date_last_viewed=None,
                date_last_viewed_utc=None,
                preview_text=None,
                fetch_result=None,
            ),
            SafariReadingListItem(
                url="https://example.com/a",
                title="A duplicate",
                date_added=None,
                date_added_utc=None,
                date_last_fetched=None,
                date_last_fetched_utc=None,
                date_last_viewed=None,
                date_last_viewed_utc=None,
                preview_text=None,
                fetch_result=1,
            ),
        ]
        stats = get_reading_list_stats(items)
        assert stats["total_items"] == 3
        assert stats["unique_urls"] == 2
        assert stats["fetched_count"] == 2
        assert stats["unfetched_count"] == 1
        assert stats["failed_count"] == 0

    def test_stats_empty(self) -> None:
        stats = get_reading_list_stats([])
        assert stats["total_items"] == 0
        assert stats["unique_urls"] == 0
        assert stats["fetched_count"] == 0
        assert stats["unfetched_count"] == 0
        assert stats["failed_count"] == 0


# =============================================================================
# Extractor Tests
# =============================================================================


class TestSafariReadingListExtractor:
    """Test SafariReadingListExtractor class."""

    def test_metadata(self) -> None:
        extractor = SafariReadingListExtractor()
        meta = extractor.metadata
        assert meta.name == "safari_reading_list"
        assert "Reading List" in meta.display_name
        assert meta.can_extract is True
        assert meta.can_ingest is True

    def test_registry_discovery(self) -> None:
        registry = ExtractorRegistry()
        assert "safari_reading_list" in registry.list_names()
        assert isinstance(registry.get("safari_reading_list"), SafariReadingListExtractor)

    def test_can_run_extraction_with_fs(self) -> None:
        extractor = SafariReadingListExtractor()
        from unittest.mock import MagicMock
        can_run, _ = extractor.can_run_extraction(MagicMock())
        assert can_run is True

    def test_can_run_extraction_without_fs(self) -> None:
        extractor = SafariReadingListExtractor()
        can_run, msg = extractor.can_run_extraction(None)
        assert can_run is False
        assert "No evidence" in msg

    def test_can_run_ingestion_no_manifest(self, tmp_path: Path) -> None:
        extractor = SafariReadingListExtractor()
        can_run, msg = extractor.can_run_ingestion(tmp_path)
        assert can_run is False

    def test_get_output_dir(self, tmp_path: Path) -> None:
        extractor = SafariReadingListExtractor()
        output = extractor.get_output_dir(tmp_path, "evidence1")
        assert "safari_reading_list" in str(output)

    def test_extraction_copies_bookmarks_files(self, tmp_path: Path) -> None:
        """run_extraction discovers and copies Bookmarks.plist files."""
        plist_data = _plist_bytes(_make_reading_list_plist())
        fs = _FakeEvidenceFS(
            {
                "Users/alice/Library/Safari/Bookmarks.plist": plist_data,
            }
        )

        extractor = SafariReadingListExtractor()
        output_dir = tmp_path / "out"
        ok = extractor.run_extraction(fs, output_dir, {"evidence_id": 1}, _Callbacks())
        assert ok is True

        manifests = sorted(output_dir.glob("*/manifest.json"))
        assert manifests, "No extraction manifest generated"
        manifest = json.loads(manifests[-1].read_text())

        files = manifest["files"]
        assert len(files) == 1
        assert files[0]["artifact_type"] == "reading_list_plist"
        assert files[0]["profile"] == "alice"

    def test_extraction_no_files_found(self, tmp_path: Path) -> None:
        """run_extraction with no matching files produces skipped status."""
        fs = _FakeEvidenceFS({})
        extractor = SafariReadingListExtractor()
        output_dir = tmp_path / "out"
        ok = extractor.run_extraction(fs, output_dir, {"evidence_id": 1}, _Callbacks())
        assert ok is True  # skipped is not an error

        manifests = sorted(output_dir.glob("*/manifest.json"))
        assert manifests
        manifest = json.loads(manifests[-1].read_text())
        assert manifest["status"] == "skipped"
