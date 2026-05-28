"""
Regression tests for browser artifact quality workstreams A–H.

Covers:
  A – Extension dedup (manifest + DB)
  B – Path normalization via normalize_evidence_path()
  C – get_latest_browser_inventory() collapses rerun duplicates
  D – browser_indicators table + aggregate_tor_indicators() scanner
  E – Tor state discovery pattern fix (pt_state/*, keys/*)
  F – Unsupported artifact discovery constants
  G – Chrome Preferences config parser → browser_config
  H – Coverage report generator (core.coverage_report)

No E01 images, no external tools, no Qt — in-memory SQLite only.
"""
from __future__ import annotations

import sqlite3
import json

import pytest


# ---------------------------------------------------------------------------
# Workstream A: Extension Dedup
# ---------------------------------------------------------------------------

class TestExtensionDedup:
    """Workstream A: Idempotent extraction and ingestion."""

    def test_manifest_dedup_removes_duplicates(self):
        """Discovery-level dedup should collapse duplicate extensions
        by (source_path, extension_id, version)."""
        extensions = [
            {"source_path": "/Users/test/Chrome/Extensions/abc/1.0/manifest.json",
             "extension_id": "abc", "version": "1.0", "name": "Test"},
            {"source_path": "/Users/test/Chrome/Extensions/abc/1.0/manifest.json",
             "extension_id": "abc", "version": "1.0", "name": "Test"},
            {"source_path": "/Users/test/Chrome/Extensions/def/2.0/manifest.json",
             "extension_id": "def", "version": "2.0", "name": "Other"},
        ]

        seen: set = set()
        unique = []
        for ext in extensions:
            key = (ext.get("source_path", ""),
                   ext.get("extension_id", ""),
                   ext.get("version", ""))
            if key not in seen:
                seen.add(key)
                unique.append(ext)

        assert len(unique) == 2

    def test_different_versions_preserved(self):
        """Different versions of the same extension should survive dedup."""
        extensions = [
            {"source_path": "/p1", "extension_id": "abc", "version": "1.0", "name": "v1"},
            {"source_path": "/p2", "extension_id": "abc", "version": "2.0", "name": "v2"},
        ]

        seen: set = set()
        unique = []
        for ext in extensions:
            key = (ext.get("source_path", ""),
                   ext.get("extension_id", ""),
                   ext.get("version", ""))
            if key not in seen:
                seen.add(key)
                unique.append(ext)

        assert len(unique) == 2

    def test_same_id_same_path_deduped(self):
        """Exact same (path, id, version) must collapse to one."""
        extensions = [
            {"source_path": "/p", "extension_id": "x", "version": "1.0"},
        ] * 5

        seen: set = set()
        unique = []
        for ext in extensions:
            key = (ext.get("source_path", ""),
                   ext.get("extension_id", ""),
                   ext.get("version", ""))
            if key not in seen:
                seen.add(key)
                unique.append(ext)

        assert len(unique) == 1


# ---------------------------------------------------------------------------
# Workstream B: Path Normalization
# ---------------------------------------------------------------------------

class TestPathNormalization:
    """Workstream B: Source path normalization."""

    def test_normalize_adds_leading_slash(self):
        from extractors._shared.path_utils import normalize_evidence_path
        assert normalize_evidence_path("Users/test/file.db") == "/Users/test/file.db"

    def test_normalize_converts_backslashes(self):
        from extractors._shared.path_utils import normalize_evidence_path
        assert normalize_evidence_path("Users\\test\\file.db") == "/Users/test/file.db"

    def test_normalize_already_has_slash(self):
        from extractors._shared.path_utils import normalize_evidence_path
        assert normalize_evidence_path("/Users/test/file.db") == "/Users/test/file.db"

    def test_normalize_collapses_double_slashes(self):
        from extractors._shared.path_utils import normalize_evidence_path
        assert normalize_evidence_path("//Users//test//file.db") == "/Users/test/file.db"

    def test_normalize_none_passthrough(self):
        from extractors._shared.path_utils import normalize_evidence_path
        assert normalize_evidence_path(None) is None

    def test_normalize_empty_passthrough(self):
        from extractors._shared.path_utils import normalize_evidence_path
        assert normalize_evidence_path("") == ""

    def test_normalize_path_object(self):
        from pathlib import Path
        from extractors._shared.path_utils import normalize_evidence_path
        result = normalize_evidence_path(Path("Users/john/Documents"))
        assert result == "/Users/john/Documents"

    def test_normalize_mixed_separators(self):
        from extractors._shared.path_utils import normalize_evidence_path
        assert normalize_evidence_path("Users\\test/mixed\\path") == "/Users/test/mixed/path"


# ---------------------------------------------------------------------------
# Workstream C: Latest Inventory
# ---------------------------------------------------------------------------

class TestLatestInventory:
    """Workstream C: Separate current inventory from run audit."""

    def test_latest_inventory_collapses_reruns(self, evidence_db):
        """Re-running an extractor should not inflate inventory counts."""
        from core.database.helpers.browser_inventory import (
            insert_browser_inventory,
            get_latest_browser_inventory,
        )
        conn = evidence_db
        eid = 1

        insert_browser_inventory(
            conn, eid, "chrome", "history", "run_1", "/extracted/History",
            "ok", "2024-01-01T00:00:00Z", "/Users/test/Chrome/History",
        )
        insert_browser_inventory(
            conn, eid, "chrome", "history", "run_2", "/extracted/History_v2",
            "ok", "2024-02-01T00:00:00Z", "/Users/test/Chrome/History",
        )

        result = get_latest_browser_inventory(conn, eid)
        # Should return 1 row (latest run), not 2
        assert len(result) == 1
        assert result[0]["run_id"] == "run_2"

    def test_latest_inventory_keeps_different_artifacts(self, evidence_db):
        """Different artifact types are not collapsed."""
        from core.database.helpers.browser_inventory import (
            insert_browser_inventory,
            get_latest_browser_inventory,
        )
        conn = evidence_db
        eid = 1

        insert_browser_inventory(
            conn, eid, "chrome", "history", "run_1", "/extracted/History",
            "ok", "2024-01-01T00:00:00Z", "/Users/test/Chrome/History",
        )
        insert_browser_inventory(
            conn, eid, "chrome", "cookies", "run_1", "/extracted/Cookies",
            "ok", "2024-01-01T00:00:00Z", "/Users/test/Chrome/Cookies",
        )

        result = get_latest_browser_inventory(conn, eid)
        assert len(result) == 2

    def test_latest_inventory_prefers_ok_ingestion(self, evidence_db):
        """A run with ingestion_status='ok' should be preferred over a later
        run that has not been ingested, when logical_path matches."""
        from core.database.helpers.browser_inventory import (
            insert_browser_inventory,
            get_latest_browser_inventory,
            update_inventory_ingestion_status,
        )
        conn = evidence_db
        eid = 1

        row_id_1 = insert_browser_inventory(
            conn, eid, "chrome", "history", "run_1", "/extracted/History",
            "ok", "2024-01-01T00:00:00Z", "/Users/test/Chrome/History",
        )
        # Mark run_1 as successfully ingested
        update_inventory_ingestion_status(
            conn, row_id_1, "ok",
            urls_parsed=100, records_parsed=100,
        )

        # run_2 is newer but still pending ingestion
        insert_browser_inventory(
            conn, eid, "chrome", "history", "run_2", "/extracted/History_v2",
            "ok", "2024-02-01T00:00:00Z", "/Users/test/Chrome/History",
        )

        result = get_latest_browser_inventory(conn, eid)
        assert len(result) == 1
        # Should prefer the ingested row
        assert result[0]["run_id"] == "run_1"

    def test_latest_inventory_filter_by_browser(self, evidence_db):
        """Browser filter should restrict results."""
        from core.database.helpers.browser_inventory import (
            insert_browser_inventory,
            get_latest_browser_inventory,
        )
        conn = evidence_db
        eid = 1

        insert_browser_inventory(
            conn, eid, "chrome", "history", "run_1", "/extracted/History",
            "ok", "2024-01-01T00:00:00Z", "/Users/test/Chrome/History",
        )
        insert_browser_inventory(
            conn, eid, "firefox", "history", "run_1", "/extracted/places.sqlite",
            "ok", "2024-01-01T00:00:00Z", "/Users/test/Firefox/places.sqlite",
        )

        result = get_latest_browser_inventory(conn, eid, browser="chrome")
        assert len(result) == 1
        assert result[0]["browser"] == "chrome"


# ---------------------------------------------------------------------------
# Workstream D: Tor Indicators
# ---------------------------------------------------------------------------

class TestTorIndicators:
    """Workstream D: Tor residual indicator coverage."""

    def test_aggregate_tor_from_os_indicators(self, evidence_db):
        """UserAssist Tor references should be aggregated."""
        conn = evidence_db
        eid = 1

        conn.execute(
            """INSERT INTO os_indicators
                   (evidence_id, type, name, value, path, run_id)
               VALUES (?, 'user_assist', 'Tor Browser',
                       'C:\\Users\\test\\Desktop\\Tor Browser\\Browser\\firefox.exe',
                       NULL, 'run_1')""",
            (eid,),
        )
        conn.commit()

        from core.database.helpers.browser_indicators import aggregate_tor_indicators

        indicators = aggregate_tor_indicators(conn, eid, "scan_run_1")
        assert len(indicators) > 0
        assert indicators[0]["browser"] == "tor"
        assert indicators[0]["confidence"] == "high"

    def test_aggregate_tor_from_urls(self, evidence_db):
        """Tor-related URLs should be aggregated."""
        conn = evidence_db
        eid = 1

        conn.execute(
            """INSERT INTO urls (evidence_id, url, discovered_by, run_id)
               VALUES (?, 'https://www.torproject.org/download/',
                       'bulk_extractor', 'run_1')""",
            (eid,),
        )
        conn.commit()

        from core.database.helpers.browser_indicators import aggregate_tor_indicators

        indicators = aggregate_tor_indicators(conn, eid, "scan_run_1")
        assert len(indicators) > 0
        assert any("torproject.org" in i["indicator_value"] for i in indicators)

    def test_aggregate_tor_from_jump_lists(self, evidence_db):
        """Jump list references to Tor Browser should be detected."""
        conn = evidence_db
        eid = 1

        conn.execute(
            """INSERT INTO jump_list_entries
                   (evidence_id, appid, jumplist_path, target_path,
                    arguments, lnk_access_time, source_path, run_id)
               VALUES (?, 'tor_app', '/jumplists/auto.jl',
                       'C:\\Tor Browser\\Browser\\firefox.exe', '-profile',
                       '2024-01-15T10:00:00Z', '/jumplists/auto.jl', 'run_1')""",
            (eid,),
        )
        conn.commit()

        from core.database.helpers.browser_indicators import aggregate_tor_indicators

        indicators = aggregate_tor_indicators(conn, eid, "scan_run_1")
        assert len(indicators) > 0
        assert indicators[0]["source_table"] == "jump_list_entries"

    def test_aggregate_tor_onion_urls_high_confidence(self, evidence_db):
        """.onion URLs should yield high confidence indicators."""
        conn = evidence_db
        eid = 1

        conn.execute(
            """INSERT INTO urls (evidence_id, url, discovered_by, run_id)
               VALUES (?, 'http://exampleonion.onion/page', 'history', 'run_1')""",
            (eid,),
        )
        conn.commit()

        from core.database.helpers.browser_indicators import aggregate_tor_indicators

        indicators = aggregate_tor_indicators(conn, eid, "scan_run_1")
        # .onion URL may match multiple LIKE patterns; all should be high confidence
        assert len(indicators) >= 1
        assert all(i["confidence"] == "high" for i in indicators)

    def test_no_false_positives(self, evidence_db):
        """Non-Tor URLs should not be picked up."""
        conn = evidence_db
        eid = 1

        conn.execute(
            """INSERT INTO urls (evidence_id, url, discovered_by, run_id)
               VALUES (?, 'https://www.google.com/', 'bulk_extractor', 'run_1')""",
            (eid,),
        )
        conn.commit()

        from core.database.helpers.browser_indicators import aggregate_tor_indicators

        indicators = aggregate_tor_indicators(conn, eid, "scan_run_1")
        assert len(indicators) == 0


# ---------------------------------------------------------------------------
# Workstream E: Tor State Discovery
# ---------------------------------------------------------------------------

class TestTorStateDiscovery:
    """Workstream E: Fix tor_state discovery scope."""

    def test_pt_state_pattern_no_global_match(self):
        """pt_state/* should not produce a bare wildcard filename pattern."""
        from extractors.browser.firefox.tor_state._patterns import TOR_ARTIFACT_PATTERNS

        assert "pt_state/*" in TOR_ARTIFACT_PATTERNS
        assert "keys/*" in TOR_ARTIFACT_PATTERNS

        # Simulate the FIXED discovery logic: slash-containing patterns are
        # "nested" and must not be split into bare filename wildcards.
        filename_patterns = []
        nested_patterns = []
        for artifact in TOR_ARTIFACT_PATTERNS:
            if "/" in artifact:
                nested_patterns.append(artifact)
            elif "*" in artifact:
                filename_patterns.append(artifact)
            else:
                filename_patterns.append(artifact)

        # A bare "*" must never appear in filename_patterns
        assert "*" not in filename_patterns
        # "cached-*" SHOULD appear
        assert "cached-*" in filename_patterns
        # Nested bucket should contain the directory patterns
        assert "pt_state/*" in nested_patterns
        assert "keys/*" in nested_patterns

    def test_tor_config_files_defined(self):
        """Config/state/cache classification groups should be populated."""
        from extractors.browser.firefox.tor_state._patterns import (
            TOR_CONFIG_FILES,
            TOR_STATE_FILES,
            TOR_CACHE_FILES,
        )
        assert "torrc" in TOR_CONFIG_FILES
        assert "state" in TOR_STATE_FILES
        assert len(TOR_CACHE_FILES) >= 3

    def test_get_all_tor_patterns_combines_roots_and_artifacts(self):
        """get_all_tor_patterns() should return root×artifact combinations."""
        from extractors.browser.firefox.tor_state._patterns import (
            TOR_DATA_ROOTS,
            TOR_ARTIFACT_PATTERNS,
            get_all_tor_patterns,
        )
        patterns = get_all_tor_patterns()
        expected_count = len(TOR_DATA_ROOTS) * len(TOR_ARTIFACT_PATTERNS)
        assert len(patterns) == expected_count
        # Spot-check one combination
        assert any(p.endswith("/torrc") for p in patterns)


# ---------------------------------------------------------------------------
# Workstream F: Unsupported Artifacts
# ---------------------------------------------------------------------------

class TestUnsupportedArtifacts:
    """Workstream F: Chrome auxiliary artifact coverage."""

    def test_unsupported_artifact_constants(self):
        """Known unsupported artifacts should be defined."""
        from extractors._shared.unsupported_artifacts import (
            CHROMIUM_UNSUPPORTED_ARTIFACTS,
            CHROMIUM_SW_PATTERNS,
        )

        names = [a[0] for a in CHROMIUM_UNSUPPORTED_ARTIFACTS]
        assert "Visited Links" in names
        assert "Shortcuts" in names
        assert "Network Action Predictor" in names
        assert len(CHROMIUM_SW_PATTERNS) > 0

    def test_unsupported_tuples_have_three_elements(self):
        """Each entry should be (filename, description, forensic_value)."""
        from extractors._shared.unsupported_artifacts import CHROMIUM_UNSUPPORTED_ARTIFACTS

        for entry in CHROMIUM_UNSUPPORTED_ARTIFACTS:
            assert len(entry) == 3, f"Unexpected entry length: {entry}"
            assert isinstance(entry[0], str)
            assert isinstance(entry[1], str)
            assert entry[2] in ("low", "medium", "high")

    def test_sw_patterns_are_sql_like(self):
        """Service Worker patterns should be SQL LIKE patterns."""
        from extractors._shared.unsupported_artifacts import CHROMIUM_SW_PATTERNS

        for pattern in CHROMIUM_SW_PATTERNS:
            assert "%" in pattern, f"Expected LIKE wildcard in: {pattern}"


# ---------------------------------------------------------------------------
# Workstream G: Browser Config Parser
# ---------------------------------------------------------------------------

class TestBrowserConfigParser:
    """Workstream G: Browser config ingestion."""

    def test_parse_preferences_config(self):
        """Parse Chrome Preferences for config fields."""
        from extractors.browser.chromium.config._parser import (
            parse_preferences_config,
        )

        prefs = {
            "profile": {"name": "Default", "exit_type": "Normal"},
            "default_search_provider_data": {
                "short_name": "Google",
                "keyword": "google.com",
            },
            "homepage": "https://www.google.com",
            "homepage_is_newtabpage": True,
            "download": {"default_directory": "/Users/test/Downloads"},
        }

        records = parse_preferences_config(
            prefs, "chrome", "Default",
            "/Users/test/Chrome/Preferences",
            run_id="test_run",
        )

        assert len(records) > 0
        keys = {r["config_key"] for r in records}
        assert "profile_name" in keys
        assert "default_search_engine" in keys
        assert "homepage_url" in keys
        assert "download_directory" in keys

    def test_empty_preferences(self):
        """Empty Preferences should return no records."""
        from extractors.browser.chromium.config._parser import (
            parse_preferences_config,
        )

        records = parse_preferences_config(
            {}, "chrome", "Default", "/path", run_id="test",
        )
        assert records == []

    def test_startup_urls_extracted(self):
        """session.startup_urls list should produce a startup_urls record."""
        from extractors.browser.chromium.config._parser import (
            parse_preferences_config,
        )

        prefs = {
            "session": {
                "startup_urls": ["https://example.com", "https://news.example.com"],
            },
        }

        records = parse_preferences_config(
            prefs, "chrome", "Default", "/Users/test/Preferences",
            run_id="test_run",
        )

        startup = [r for r in records if r["config_key"] == "startup_urls"]
        assert len(startup) == 1
        assert startup[0]["value_count"] == 2
        parsed = json.loads(startup[0]["config_value"])
        assert len(parsed) == 2

    def test_record_metadata_fields(self):
        """Each record should carry browser, profile, source_path, run_id."""
        from extractors.browser.chromium.config._parser import (
            parse_preferences_config,
        )

        prefs = {"homepage": "https://example.com"}

        records = parse_preferences_config(
            prefs, "edge", "Profile 1", "/Users/test/Edge/Preferences",
            run_id="run_42",
        )

        assert len(records) == 1
        r = records[0]
        assert r["browser"] == "edge"
        assert r["profile"] == "Profile 1"
        assert r["source_path"] == "/Users/test/Edge/Preferences"
        assert r["run_id"] == "run_42"

    def test_structured_values_are_serialized_stably(self):
        """Lists and dictionaries should be stored deterministically."""
        from extractors.browser.chromium.config._parser import (
            parse_local_state_config,
            parse_preferences_config,
        )

        pref_records = parse_preferences_config(
            {"session": {"startup_urls": ["https://b.example", "https://a.example"]}},
            "chrome", "Default", "/Preferences", run_id="run_42",
        )
        startup = next(r for r in pref_records if r["config_key"] == "startup_urls")
        assert startup["config_value"] == '["https://b.example","https://a.example"]'
        assert startup["value_count"] == 2

        local_state_records = parse_local_state_config(
            {"profile": {"info_cache": {"Profile 1": {"name": "Work"}, "Default": {"name": "Person 1"}}}},
            "chrome", "/Local State", run_id="run_42",
        )
        info_cache = next(r for r in local_state_records if r["config_key"] == "profile_info_cache")
        assert info_cache["profile"] is None
        assert info_cache["config_type"] == "local_state"
        assert info_cache["config_value"] == '{"Default":{"name":"Person 1"},"Profile 1":{"name":"Work"}}'

    def test_type_mismatch_records_warning(self):
        """Unexpected allowlisted value types should be warned and skipped."""
        from unittest.mock import Mock

        from extractors.browser.chromium.config._parser import parse_preferences_config

        warning_collector = Mock()
        records = parse_preferences_config(
            {"session": {"startup_urls": "https://not-a-list.example"}},
            "chrome", "Default", "/Preferences", run_id="run_42",
            warning_collector=warning_collector,
        )

        assert records == []
        warning_collector.add_warning.assert_called_once()

    def test_numeric_values_are_serialized(self):
        """Numeric allowlisted values should be stored as deterministic strings."""
        from extractors.browser.chromium.config._parser import parse_preferences_config

        records = parse_preferences_config(
            {
                "profile": {"creation_time": 13300000000000000},
                "session": {"restore_on_startup": 4},
            },
            "chrome", "Default", "/Preferences", run_id="run_42",
        )

        values = {record["config_key"]: record["config_value"] for record in records}
        assert values["creation_time"] == "13300000000000000"
        assert values["restore_on_startup"] == "4"

    def test_null_values_are_preserved(self):
        """Explicit null allowlisted values should be distinct from absent keys."""
        from extractors.browser.chromium.config._parser import parse_preferences_config

        records = parse_preferences_config(
            {"homepage": None},
            "chrome", "Default", "/Preferences", run_id="run_42",
        )

        assert len(records) == 1
        assert records[0]["config_key"] == "homepage_url"
        assert records[0]["config_value"] is None

    def test_duplicate_alias_disagreement_records_warning(self):
        """Modern and legacy aliases mapping to one key should warn when they disagree."""
        from unittest.mock import Mock

        from extractors.browser.chromium.config._parser import parse_preferences_config

        warning_collector = Mock()
        records = parse_preferences_config(
            {"extensions": {"ui": {"developer_mode": True}, "developer_mode": False}},
            "chrome", "Default", "/Preferences", run_id="run_42",
            warning_collector=warning_collector,
        )

        assert [record["config_key"] for record in records] == ["extensions_developer_mode"]
        warning_collector.add_warning.assert_called_once()


# ---------------------------------------------------------------------------
# Workstream H: Coverage Report
# ---------------------------------------------------------------------------

class TestCoverageReport:
    """Workstream H: File-list coverage report."""

    def test_report_generation_empty_db(self, evidence_db):
        """Coverage report should run without errors on empty DB."""
        from core.coverage_report import generate_coverage_report, format_coverage_report

        report = generate_coverage_report(evidence_db, 1)
        assert "items" in report
        assert "summary" in report
        assert "warnings" in report

        text = format_coverage_report(report)
        assert "Coverage Report" in text

    def test_report_with_file_list_artifact(self, evidence_db):
        """Coverage report should detect known artifacts in file_list."""
        conn = evidence_db
        eid = 1

        conn.execute(
            """INSERT INTO file_list
                   (evidence_id, file_path, file_name, import_timestamp, run_id)
               VALUES (?, '/Users/test/Chrome/Default/History', 'History',
                       '2024-01-01T00:00:00Z', 'run_1')""",
            (eid,),
        )
        conn.commit()

        from core.coverage_report import generate_coverage_report

        report = generate_coverage_report(conn, eid)
        assert report["total_items"] > 0
        assert any(i["file_name"] == "History" for i in report["items"])

    def test_report_unsupported_artifact_status(self, evidence_db):
        """Items with extraction_warnings of type 'unsupported_artifact'
        should be flagged as present_unsupported."""
        conn = evidence_db
        eid = 1

        # Insert an unsupported artifact name in file_list and a matching warning
        conn.execute(
            """INSERT INTO file_list
                   (evidence_id, file_path, file_name, import_timestamp, run_id)
               VALUES (?, '/Users/test/Chrome/Default/Visited Links',
                       'Visited Links', '2024-01-01T00:00:00Z', 'run_1')""",
            (eid,),
        )
        conn.execute(
            """INSERT INTO extraction_warnings
                   (evidence_id, run_id, extractor_name, warning_type,
                    item_name, item_value)
               VALUES (?, 'run_1', 'chromium_extensions', 'unsupported_artifact',
                       'Visited Links', 'Bloom filter of visited URLs')""",
            (eid,),
        )
        conn.commit()

        from core.coverage_report import (
            generate_coverage_report,
            STATUS_PRESENT_UNSUPPORTED,
        )

        report = generate_coverage_report(conn, eid)
        visited = [i for i in report["items"] if i["file_name"] == "Visited Links"]
        assert len(visited) == 1
        assert visited[0]["status"] == STATUS_PRESENT_UNSUPPORTED

    def test_format_coverage_report_structure(self, evidence_db):
        """Formatted report should include header and summary lines."""
        from core.coverage_report import generate_coverage_report, format_coverage_report

        report = generate_coverage_report(evidence_db, 1)
        text = format_coverage_report(report)

        assert "Coverage Report" in text
        assert "Total items:" in text
