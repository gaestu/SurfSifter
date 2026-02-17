"""
Tests for Safari Sessions Extractor.

Tests cover:
- Session and recently-closed tab parsing (_parsers.py)
- SafariSessionsExtractor metadata and methods
- URL dual-write wiring
"""

import inspect
import plistlib
from datetime import datetime, timezone
from unittest.mock import MagicMock

from extractors.browser.safari._parsers import (
    parse_session_plist,
    parse_recently_closed_tabs,
    get_session_stats,
    SafariSessionTab,
    SafariSessionWindow,
)
from extractors.browser.safari.sessions import SafariSessionsExtractor


# =============================================================================
# Parser Tests
# =============================================================================


class TestSafariSessionParsers:
    """Test Safari session parsers."""

    def test_parse_session_plist_basic(self, tmp_path):
        """Parse one window with one valid tab and back/forward history."""
        plist_path = tmp_path / "LastSession.plist"

        plist_data = {
            "SessionVersion": 1,
            "SessionWindows": [
                {
                    "SelectedTabIndex": 0,
                    "IsPrivateWindow": True,
                    "TabStates": [
                        {
                            "TabURL": "https://example.com/",
                            "TabTitle": "Example",
                            "TabUUID": "tab-1",
                            "LastVisitTime": 100.0,
                            "IsAppTab": True,
                            "BackForwardList": {
                                "CurrentIndex": 1,
                                "Entries": [
                                    {"URL": "https://example.com/a", "Title": "A"},
                                    {"URL": "about:blank", "Title": "Blank"},
                                ],
                            },
                        },
                        {
                            "TabURL": "about:blank",
                            "TabTitle": "Blank",
                        },
                    ],
                }
            ],
        }

        with open(plist_path, "wb") as f:
            plistlib.dump(plist_data, f)

        parsed = parse_session_plist(plist_path)

        assert len(parsed["windows"]) == 1
        assert len(parsed["tabs"]) == 1
        assert len(parsed["history"]) == 1

        window = parsed["windows"][0]
        assert window.is_private is True
        assert window.selected_tab_index == 0

        tab = parsed["tabs"][0]
        assert tab.tab_url == "https://example.com/"
        assert tab.tab_title == "Example"
        assert tab.is_pinned is True
        assert tab.tab_uuid == "tab-1"

        history = parsed["history"][0]
        assert history["url"] == "https://example.com/a"
        assert history["title"] == "A"

    def test_parse_session_plist_empty(self, tmp_path):
        """Empty session windows returns empty parse result."""
        plist_path = tmp_path / "LastSession.plist"

        with open(plist_path, "wb") as f:
            plistlib.dump({"SessionWindows": []}, f)

        parsed = parse_session_plist(plist_path)
        assert parsed["windows"] == []
        assert parsed["tabs"] == []
        assert parsed["history"] == []

    def test_parse_session_plist_missing_optional_fields(self, tmp_path):
        """Missing optional tab keys should not fail parsing."""
        plist_path = tmp_path / "LastSession.plist"

        plist_data = {
            "SessionWindows": [
                {
                    "TabStates": [
                        {
                            "TabURL": "https://no-optional.example/",
                        }
                    ]
                }
            ]
        }

        with open(plist_path, "wb") as f:
            plistlib.dump(plist_data, f)

        parsed = parse_session_plist(plist_path)

        assert len(parsed["tabs"]) == 1
        tab = parsed["tabs"][0]
        assert tab.tab_title == ""
        assert tab.last_visit_time is None
        assert tab.is_pinned is False

    def test_parse_session_state_archive_best_effort(self, tmp_path):
        """SessionState archive fallback should extract URL/title when present."""
        plist_path = tmp_path / "LastSession.plist"

        archive_blob = plistlib.dumps(
            {
                "entries": [
                    {
                        "URL": "https://state.example/",
                        "Title": "State Title",
                    }
                ]
            },
            fmt=plistlib.FMT_BINARY,
        )

        plist_data = {
            "SessionWindows": [
                {
                    "TabStates": [
                        {
                            "TabURL": "https://tab.example/",
                            "SessionState": archive_blob,
                        }
                    ]
                }
            ]
        }

        with open(plist_path, "wb") as f:
            plistlib.dump(plist_data, f)

        parsed = parse_session_plist(plist_path)
        assert len(parsed["tabs"]) == 1
        assert any(h["url"] == "https://state.example/" for h in parsed["history"])

    def test_parse_recently_closed_tabs(self, tmp_path):
        """Parse recently closed tabs with timestamp conversion."""
        plist_path = tmp_path / "RecentlyClosedTabs.plist"

        plist_data = [
            {
                "TabURL": "https://closed.example/",
                "TabTitle": "Closed",
                "DateClosed": 60.0,
            },
            {
                "TabURL": "about:blank",
                "TabTitle": "Skip",
            },
        ]

        with open(plist_path, "wb") as f:
            plistlib.dump(plist_data, f)

        closed_tabs = parse_recently_closed_tabs(plist_path)

        assert len(closed_tabs) == 1
        assert closed_tabs[0].tab_url == "https://closed.example/"
        assert closed_tabs[0].tab_title == "Closed"
        assert closed_tabs[0].date_closed is not None

    def test_get_session_stats(self):
        """Session stats should include counts and date range."""
        ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
        tabs = [
            SafariSessionTab(
                tab_url="https://a.example/",
                tab_title="A",
                last_visit_time=ts,
                tab_index=0,
                window_index=0,
                is_pinned=True,
                tab_uuid=None,
                back_forward_entries=[],
            )
        ]
        windows = SafariSessionWindow(
            window_index=0,
            selected_tab_index=0,
            is_private=False,
            tab_count=1,
        )

        stats = get_session_stats([windows], tabs)

        assert stats["total_windows"] == 1
        assert stats["total_tabs"] == 1
        assert stats["pinned_tabs"] == 1
        assert stats["date_range"]["earliest"] is not None


# =============================================================================
# Extractor Tests
# =============================================================================


class TestSafariSessionsExtractor:
    """Test SafariSessionsExtractor class."""

    def test_extractor_metadata(self):
        """Extractor has expected metadata."""
        extractor = SafariSessionsExtractor()
        meta = extractor.metadata

        assert meta.name == "safari_sessions"
        assert "Safari" in meta.display_name
        assert "Sessions" in meta.display_name
        assert meta.can_extract is True
        assert meta.can_ingest is True

    def test_can_run_extraction_with_fs(self):
        """can_run_extraction returns True with evidence filesystem."""
        extractor = SafariSessionsExtractor()
        mock_fs = MagicMock()

        can_run, _ = extractor.can_run_extraction(mock_fs)
        assert can_run is True

    def test_can_run_extraction_without_fs(self):
        """can_run_extraction returns False without evidence filesystem."""
        extractor = SafariSessionsExtractor()

        can_run, msg = extractor.can_run_extraction(None)
        assert can_run is False
        assert "No evidence" in msg

    def test_get_output_dir(self, tmp_path):
        """get_output_dir returns expected directory."""
        extractor = SafariSessionsExtractor()
        output = extractor.get_output_dir(tmp_path, "evidence1")
        assert "safari_sessions" in str(output)

    def test_run_extraction_creates_manifest(self, tmp_path):
        """run_extraction creates manifest even when no files are found."""
        extractor = SafariSessionsExtractor()
        output_dir = tmp_path / "output"

        mock_fs = MagicMock()
        mock_fs.iter_paths = MagicMock(return_value=[])

        callbacks = MagicMock()
        config = {"evidence_id": 1, "evidence_label": "test"}

        result = extractor.run_extraction(mock_fs, output_dir, config, callbacks)

        assert result is True
        assert (output_dir / "manifest.json").exists()


# =============================================================================
# Dual-Write Tests
# =============================================================================


class TestSafariSessionsDualWrite:
    """Tests for dual-write to urls table."""

    def test_extractor_imports_insert_urls(self):
        """Extractor module imports insert_urls."""
        from extractors.browser.safari.sessions import extractor as module

        source = inspect.getsource(module)
        assert "insert_urls" in source
        assert "from core.database import" in source

    def test_ingestion_collects_url_records(self):
        """run_ingestion builds URL records and cross-posts to urls table."""
        source = inspect.getsource(SafariSessionsExtractor.run_ingestion)
        helper_source = inspect.getsource(SafariSessionsExtractor._make_url_record)

        assert "url_records" in source
        assert "domain" in helper_source
        assert "scheme" in helper_source
        assert "context" in helper_source
        assert "insert_urls" in source

    def test_url_context_provenance(self):
        """URL context should include session and closed-tab provenance."""
        source = inspect.getsource(SafariSessionsExtractor.run_ingestion)

        assert "session:safari" in source
        assert "closed_tab:safari" in source
