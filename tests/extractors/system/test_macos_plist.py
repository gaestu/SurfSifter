"""
Tests for macOS Plist Extractor — parsers and extractor metadata.
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from extractors.system.macos_plist.parsers import (
    parse_system_version,
    parse_system_profiler,
    parse_global_preferences,
    parse_network_config,
    parse_app_info_plist,
    parse_install_receipt,
    parse_launch_services,
    parse_recent_items,
    parse_finder_preferences,
    parse_spotlight_preferences,
    parse_quarantine_events,
    parse_default_browser,
)


# ---------------------------------------------------------------------------
# SystemVersion parser
# ---------------------------------------------------------------------------

class TestSystemVersionParser:
    def test_valid_system_version(self):
        plist = {
            "ProductName": "macOS",
            "ProductVersion": "14.2.1",
            "ProductBuildVersion": "23C71",
        }
        results = parse_system_version(plist, "/System/Library/CoreServices/SystemVersion.plist", "test_run")
        assert len(results) == 1
        assert results[0]["type"] == "system:os_version_macos"
        assert "14.2.1" in results[0]["value"]
        assert "23C71" in results[0]["value"]
        assert results[0]["run_id"] == "test_run"
        assert results[0]["provenance"] == "macos_plist"

    def test_missing_version(self):
        results = parse_system_version({"ProductName": "macOS"}, "/path", "run1")
        assert results == []

    def test_empty_plist(self):
        results = parse_system_version({}, "/path", "run1")
        assert results == []

    def test_version_without_build(self):
        plist = {"ProductVersion": "13.0"}
        results = parse_system_version(plist, "/path", "run1")
        assert len(results) == 1
        assert "13.0" in results[0]["value"]
        assert "(" not in results[0]["value"]  # No build version → no parens


# ---------------------------------------------------------------------------
# SystemProfiler parser
# ---------------------------------------------------------------------------

class TestSystemProfilerParser:
    def test_cpu_names_dict(self):
        plist = {"CPU Names": {"arm64e": "Apple M1 Pro"}}
        results = parse_system_profiler(plist, "/path", "run1")
        # Should produce exactly one per-arch entry (no duplicate stringified-dict)
        assert len(results) == 1
        assert results[0]["type"] == "system:hardware_macos"
        assert "arm64e" in results[0]["name"]
        assert "Apple M1 Pro" in results[0]["value"]

    def test_empty_plist(self):
        assert parse_system_profiler({}, "/p", "r") == []


# ---------------------------------------------------------------------------
# GlobalPreferences parser
# ---------------------------------------------------------------------------

class TestGlobalPreferencesParser:
    def test_timezone_and_locale(self):
        plist = {
            "AppleTimezone": "Europe/Berlin",
            "AppleLocale": "de_DE",
            "AppleLanguages": ["de-DE", "en-US"],
        }
        results = parse_global_preferences(plist, "/path", "run1")
        assert len(results) == 3
        types = {r["name"] for r in results}
        assert "Timezone" in types
        assert "Locale" in types
        assert "Preferred Languages" in types

    def test_empty_plist(self):
        assert parse_global_preferences({}, "/p", "r") == []


# ---------------------------------------------------------------------------
# Network config parser
# ---------------------------------------------------------------------------

class TestNetworkConfigParser:
    def test_network_services(self):
        plist = {
            "NetworkServices": {
                "AAAA-BBBB": {
                    "UserDefinedName": "Wi-Fi",
                    "IPv4": {"ConfigMethod": "DHCP"},
                }
            }
        }
        results = parse_network_config(plist, "/path", "run1")
        assert len(results) >= 1
        assert any("Wi-Fi" in r["name"] for r in results)

    def test_computer_name(self):
        plist = {"System": {"System": {"ComputerName": "MacBook-Pro"}}}
        results = parse_network_config(plist, "/path", "run1")
        assert any("Computer Name" in r["name"] for r in results)
        assert any("MacBook-Pro" in r["value"] for r in results)

    def test_empty_plist(self):
        assert parse_network_config({}, "/p", "r") == []


# ---------------------------------------------------------------------------
# App Info.plist parser
# ---------------------------------------------------------------------------

class TestAppInfoPlistParser:
    def test_valid_app(self):
        plist = {
            "CFBundleName": "Safari",
            "CFBundleShortVersionString": "17.2",
            "CFBundleIdentifier": "com.apple.Safari",
        }
        results = parse_app_info_plist(plist, "/Applications/Safari.app/Contents/Info.plist", "run1")
        assert len(results) == 1
        assert results[0]["type"] == "system:installed_app_macos"
        assert "Safari" in results[0]["value"]
        assert "17.2" in results[0]["value"]

    def test_missing_bundle_name_uses_identifier(self):
        plist = {"CFBundleIdentifier": "com.apple.Safari"}
        results = parse_app_info_plist(plist, "/path", "run1")
        assert len(results) == 1
        assert "com.apple.Safari" in results[0]["value"]

    def test_no_name_or_id(self):
        results = parse_app_info_plist({"CFBundleVersion": "1.0"}, "/path", "run1")
        assert results == []

    def test_display_name_fallback(self):
        plist = {"CFBundleDisplayName": "My App", "CFBundleVersion": "2.0"}
        results = parse_app_info_plist(plist, "/path", "run1")
        assert len(results) == 1
        assert "My App" in results[0]["name"]


# ---------------------------------------------------------------------------
# Install receipt parser
# ---------------------------------------------------------------------------

class TestInstallReceiptParser:
    def test_valid_receipt(self):
        plist = {
            "PackageIdentifier": "com.apple.pkg.CLTools_Executables",
            "PackageVersion": "15.0.0.0.1.1694021235",
            "InstallDate": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            "InstallProcessName": "installer",
        }
        results = parse_install_receipt(plist, "/var/db/receipts/test.plist", "run1")
        assert len(results) == 1
        assert results[0]["type"] == "system:install_receipt_macos"
        assert "com.apple.pkg.CLTools_Executables" in results[0]["value"]
        # detected_at_utc should be set from InstallDate
        assert results[0]["detected_at_utc"] is not None

    def test_missing_pkg_id(self):
        results = parse_install_receipt({"PackageVersion": "1.0"}, "/path", "run1")
        assert results == []


# ---------------------------------------------------------------------------
# Launch Services parser
# ---------------------------------------------------------------------------

class TestLaunchServicesParser:
    def test_url_scheme_handler(self):
        plist = {
            "LSHandlers": [
                {"LSHandlerURLScheme": "http", "LSHandlerRoleAll": "com.google.chrome"},
                {"LSHandlerURLScheme": "mailto", "LSHandlerRoleAll": "com.apple.mail"},
            ]
        }
        results = parse_launch_services(plist, "/path", "run1")
        assert len(results) == 2
        assert all(r["type"] == "execution:launch_services" for r in results)

    def test_content_type_handler(self):
        plist = {
            "LSHandlers": [
                {"LSHandlerContentType": "public.html", "LSHandlerRoleAll": "com.apple.safari"},
            ]
        }
        results = parse_launch_services(plist, "/path", "run1")
        assert len(results) == 1
        assert "Content Type Handler" in results[0]["name"]

    def test_empty_handlers(self):
        assert parse_launch_services({"LSHandlers": []}, "/p", "r") == []

    def test_no_handlers_key(self):
        assert parse_launch_services({}, "/p", "r") == []

    def test_non_list_handlers(self):
        assert parse_launch_services({"LSHandlers": "invalid"}, "/p", "r") == []


# ---------------------------------------------------------------------------
# Recent items parser
# ---------------------------------------------------------------------------

class TestRecentItemsParser:
    def test_recent_documents(self):
        plist = {
            "RecentDocuments": {
                "CustomListItems": [
                    {"Name": "report.pdf"},
                    {"Name": "budget.xlsx"},
                ]
            }
        }
        results = parse_recent_items(plist, "/path", "run1")
        assert len(results) == 2
        assert all(r["type"] == "user_activity:recent_items" for r in results)
        names = [r["name"] for r in results]
        assert any("report.pdf" in n for n in names)

    def test_recent_applications(self):
        plist = {
            "RecentApplications": {
                "CustomListItems": [
                    {"Name": "Safari"},
                ]
            }
        }
        results = parse_recent_items(plist, "/path", "run1")
        assert len(results) == 1

    def test_empty_plist(self):
        assert parse_recent_items({}, "/p", "r") == []

    def test_empty_items_list(self):
        plist = {"RecentDocuments": {"CustomListItems": []}}
        assert parse_recent_items(plist, "/p", "r") == []


# ---------------------------------------------------------------------------
# Finder preferences parser
# ---------------------------------------------------------------------------

class TestFinderPreferencesParser:
    def test_recent_folders(self):
        plist = {
            "FXRecentFolders": [
                {"name": "Documents"},
                {"name": "Downloads"},
            ]
        }
        results = parse_finder_preferences(plist, "/path", "run1")
        assert len(results) == 2
        assert all(r["type"] == "user_activity:finder_prefs" for r in results)

    def test_show_all_files(self):
        plist = {"AppleShowAllFiles": True}
        results = parse_finder_preferences(plist, "/path", "run1")
        assert len(results) == 1
        assert "Show All Files" in results[0]["name"]

    def test_empty_plist(self):
        assert parse_finder_preferences({}, "/p", "r") == []


# ---------------------------------------------------------------------------
# Spotlight preferences parser
# ---------------------------------------------------------------------------

class TestSpotlightPreferencesParser:
    def test_shortcuts(self):
        plist = {
            "UserShortcuts": {
                "password": {"DISPLAY_NAME": "password"},
                "budget": {"DISPLAY_NAME": "budget report"},
            }
        }
        results = parse_spotlight_preferences(plist, "/path", "run1")
        assert len(results) == 2
        assert all(r["type"] == "user_activity:spotlight_searches" for r in results)

    def test_disabled_categories(self):
        plist = {
            "orderedItems": [
                {"name": "APPLICATIONS", "enabled": True},
                {"name": "BOOKMARKS", "enabled": False},
                {"name": "MESSAGES", "enabled": False},
            ]
        }
        results = parse_spotlight_preferences(plist, "/path", "run1")
        assert len(results) == 1
        assert "BOOKMARKS" in results[0]["value"]
        assert "MESSAGES" in results[0]["value"]

    def test_empty_plist(self):
        assert parse_spotlight_preferences({}, "/p", "r") == []


# ---------------------------------------------------------------------------
# Quarantine events parser (SQLite)
# ---------------------------------------------------------------------------

class TestQuarantineParser:
    """Quarantine events are forensically important."""

    def test_valid_quarantine_db(self, tmp_path):
        """Create a mock QuarantineEventsV2 SQLite database and parse it."""
        db_path = tmp_path / "QuarantineEventsV2"
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE LSQuarantineEvent (
                LSQuarantineEventIdentifier TEXT,
                LSQuarantineTimeStamp REAL,
                LSQuarantineAgentBundleIdentifier TEXT,
                LSQuarantineAgentName TEXT,
                LSQuarantineDataURLString TEXT,
                LSQuarantineOriginURLString TEXT,
                LSQuarantineSenderName TEXT,
                LSQuarantineSenderAddress TEXT,
                LSQuarantineTypeNumber INTEGER
            )
        """)
        # Cocoa timestamp for 2024-01-15 12:00:00 UTC
        # Cocoa epoch = 2001-01-01 00:00:00 UTC
        # Seconds between 2001-01-01 and 2024-01-15 12:00:00 UTC
        cocoa_ts = (datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc) -
                    datetime(2001, 1, 1, tzinfo=timezone.utc)).total_seconds()
        conn.execute(
            "INSERT INTO LSQuarantineEvent VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("event-uuid-1", cocoa_ts, "com.apple.Safari",
             "Safari", "https://example.com/file.zip",
             "https://example.com/download", None, None, 0),
        )
        conn.commit()
        conn.close()

        results = parse_quarantine_events(str(db_path), "/original/path", "run1")
        assert len(results) == 1
        assert results[0]["type"] == "user_activity:quarantine_events"
        assert "Safari" in results[0]["name"]
        assert results[0]["detected_at_utc"] is not None
        extra = json.loads(results[0]["extra_json"])
        assert extra["agent_name"] == "Safari"
        assert extra["origin_url"] == "https://example.com/download"

    def test_invalid_file(self, tmp_path):
        """Non-SQLite file should return empty list (graceful handling)."""
        bad_file = tmp_path / "QuarantineEventsV2"
        bad_file.write_text("not a database")
        results = parse_quarantine_events(str(bad_file), "/path", "run1")
        assert results == []

    def test_missing_file(self, tmp_path):
        """Missing file should return empty list."""
        results = parse_quarantine_events(str(tmp_path / "nonexistent"), "/path", "run1")
        assert results == []


# ---------------------------------------------------------------------------
# Default browser parser
# ---------------------------------------------------------------------------

class TestDefaultBrowserParser:
    def test_http_handler(self):
        plist = {
            "LSHandlers": [
                {"LSHandlerURLScheme": "http", "LSHandlerRoleAll": "com.google.chrome"},
                {"LSHandlerURLScheme": "https", "LSHandlerRoleAll": "com.google.chrome"},
                {"LSHandlerURLScheme": "ftp", "LSHandlerRoleAll": "com.apple.finder"},
            ]
        }
        results = parse_default_browser(plist, "/path", "run1")
        assert len(results) == 2  # Only http and https
        assert all(r["type"] == "browser_trace:default_browser_macos" for r in results)
        assert all("com.google.chrome" in r["value"] for r in results)

    def test_no_http_handler(self):
        plist = {
            "LSHandlers": [
                {"LSHandlerURLScheme": "ftp", "LSHandlerRoleAll": "com.apple.finder"},
            ]
        }
        results = parse_default_browser(plist, "/path", "run1")
        assert results == []

    def test_empty_handlers(self):
        assert parse_default_browser({"LSHandlers": []}, "/p", "r") == []


# ---------------------------------------------------------------------------
# Extractor class tests
# ---------------------------------------------------------------------------

class TestSystemMacosPlistExtractor:
    def test_metadata(self):
        from extractors.system.macos_plist import SystemMacosPlistExtractor
        ext = SystemMacosPlistExtractor()
        assert ext.metadata.name == "system_macos_plist"
        assert ext.metadata.display_name == "macOS Plist Reader"
        assert ext.metadata.category == "system"
        assert ext.metadata.can_extract is True
        assert ext.metadata.can_ingest is True
        assert ext.metadata.requires_tools == []

    def test_can_run_extraction_always_true(self):
        from extractors.system.macos_plist import SystemMacosPlistExtractor
        ext = SystemMacosPlistExtractor()
        can_run, reason = ext.can_run_extraction(None)
        assert can_run is True
        assert reason == ""

    def test_can_run_ingestion_no_manifest(self, tmp_path):
        from extractors.system.macos_plist import SystemMacosPlistExtractor
        ext = SystemMacosPlistExtractor()
        can_run, reason = ext.can_run_ingestion(tmp_path)
        assert can_run is False
        assert "manifest" in reason.lower()

    def test_can_run_ingestion_with_manifest(self, tmp_path):
        from extractors.system.macos_plist import SystemMacosPlistExtractor
        ext = SystemMacosPlistExtractor()
        (tmp_path / "manifest.json").write_text("{}")
        can_run, reason = ext.can_run_ingestion(tmp_path)
        assert can_run is True

    def test_has_existing_output(self, tmp_path):
        from extractors.system.macos_plist import SystemMacosPlistExtractor
        ext = SystemMacosPlistExtractor()
        assert ext.has_existing_output(tmp_path) is False
        (tmp_path / "manifest.json").write_text("{}")
        assert ext.has_existing_output(tmp_path) is True

    def test_get_output_dir(self, tmp_path):
        from extractors.system.macos_plist import SystemMacosPlistExtractor
        ext = SystemMacosPlistExtractor()
        result = ext.get_output_dir(tmp_path, "test-evidence")
        assert result == tmp_path / "evidences" / "test-evidence" / "macos_plist"

    def test_generate_run_id(self):
        from extractors.system.macos_plist import SystemMacosPlistExtractor
        ext = SystemMacosPlistExtractor()
        run_id = ext._generate_run_id()
        assert "_" in run_id
        assert len(run_id) > 10


# ---------------------------------------------------------------------------
# Cross-parser consistency checks
# ---------------------------------------------------------------------------

class TestParserConsistency:
    """Verify all parsers return properly structured indicator dicts."""

    @pytest.fixture()
    def _all_parser_results(self):
        """Collect non-empty results from all plist parsers."""
        results = []
        results.extend(parse_system_version(
            {"ProductName": "macOS", "ProductVersion": "14.0", "ProductBuildVersion": "23A"},
            "/path", "run1",
        ))
        results.extend(parse_global_preferences(
            {"AppleTimezone": "UTC"}, "/path", "run1",
        ))
        results.extend(parse_app_info_plist(
            {"CFBundleName": "Test", "CFBundleIdentifier": "com.test"}, "/path", "run1",
        ))
        results.extend(parse_install_receipt(
            {"PackageIdentifier": "com.test.pkg"}, "/path", "run1",
        ))
        results.extend(parse_launch_services(
            {"LSHandlers": [{"LSHandlerURLScheme": "http", "LSHandlerRoleAll": "com.test"}]},
            "/path", "run1",
        ))
        results.extend(parse_recent_items(
            {"RecentDocuments": {"CustomListItems": [{"Name": "test.txt"}]}},
            "/path", "run1",
        ))
        results.extend(parse_default_browser(
            {"LSHandlers": [{"LSHandlerURLScheme": "http", "LSHandlerRoleAll": "com.test"}]},
            "/path", "run1",
        ))
        return results

    def test_all_have_required_fields(self, _all_parser_results):
        required_fields = {"type", "name", "value", "path", "hive", "confidence", "provenance", "run_id"}
        for result in _all_parser_results:
            missing = required_fields - set(result.keys())
            assert not missing, f"Missing fields {missing} in {result['type']} indicator"

    def test_all_have_macos_plist_provenance(self, _all_parser_results):
        for result in _all_parser_results:
            assert result["provenance"] == "macos_plist", f"Bad provenance in {result['type']}"

    def test_all_have_correct_run_id(self, _all_parser_results):
        for result in _all_parser_results:
            assert result["run_id"] == "run1"

    def test_types_follow_convention(self, _all_parser_results):
        """All type values should be colon-separated category:artifact."""
        for result in _all_parser_results:
            assert ":" in result["type"], f"Type {result['type']} missing colon separator"
