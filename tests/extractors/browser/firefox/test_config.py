"""Tests for Firefox browser configuration extractor and parser."""

from __future__ import annotations

import json
from unittest.mock import Mock

import pytest

from core.database.helpers.browser_config import get_browser_configs, insert_browser_config
from extractors._shared.extraction_warnings import ExtractionWarningCollector
from extractors.browser.firefox._patterns import get_artifact_patterns
from extractors.browser.firefox.config import FirefoxConfigExtractor, parse_firefox_pref_file
from extractors.browser.firefox.config.extractor import MAX_CONFIG_FILE_BYTES


@pytest.fixture
def extractor():
    return FirefoxConfigExtractor()


@pytest.fixture
def mock_callbacks():
    callbacks = Mock()
    callbacks.on_step = Mock()
    callbacks.on_progress = Mock()
    callbacks.on_log = Mock()
    callbacks.on_error = Mock()
    callbacks.is_cancelled = Mock(return_value=False)
    return callbacks


class TestMetadataAndRegistry:
    def test_metadata(self, extractor):
        assert extractor.metadata.name == "firefox_config"
        assert extractor.metadata.category == "browser"
        assert extractor.metadata.can_extract is True
        assert extractor.metadata.can_ingest is True

    def test_registry_discovers_extractor(self, extractor_registry_names):
        assert "firefox_config" in extractor_registry_names


class TestPatternsAndParser:
    def test_config_patterns_include_prefs_and_user_js(self):
        patterns = get_artifact_patterns("firefox", "config")

        assert any(pattern.endswith("/prefs.js") for pattern in patterns)
        assert any(pattern.endswith("/user.js") for pattern in patterns)

    def test_parser_handles_comments_whitespace_bools_ints_strings(self):
        raw = """
            // startup settings
            user_pref("browser.startup.page", 3);
            user_pref("browser.privatebrowsing.autostart", false);
            # ignored comment
            user_pref("browser.startup.homepage", "https://example.org");
        """

        records = parse_firefox_pref_file(
            raw,
            browser="firefox",
            profile="abc.default-release",
            source_path="Users/alice/AppData/Roaming/Mozilla/Firefox/Profiles/abc.default-release/prefs.js",
            run_id="run_test",
            config_type="prefs_js",
        )

        assert len(records) == 3
        values = {record["config_key"]: record["config_value"] for record in records}
        assert values["startup_page"] == "3"
        assert values["private_browsing_autostart"] == "false"
        assert values["startup_homepage"] == "https://example.org"

    def test_parser_handles_escaped_strings(self):
        raw = r'user_pref("browser.download.dir", "C:\\Users\\alice\\Downloads");'

        records = parse_firefox_pref_file(
            raw,
            browser="firefox",
            profile="abc.default-release",
            source_path="/home/alice/.mozilla/firefox/abc.default-release/prefs.js",
            run_id="run_test",
            config_type="prefs_js",
        )

        assert len(records) == 1
        assert records[0]["config_key"] == "download_directory"
        assert records[0]["config_value"] == r"C:\Users\alice\Downloads"

    def test_parser_preserves_duplicate_keys_with_value_count(self):
        raw = """
            user_pref("network.proxy.type", 5);
            user_pref("network.proxy.type", 1);
        """

        records = parse_firefox_pref_file(
            raw,
            browser="firefox",
            profile="abc.default-release",
            source_path="/home/alice/.mozilla/firefox/abc.default-release/user.js",
            run_id="run_test",
            config_type="user_js",
        )

        assert len(records) == 2
        assert [record["config_value"] for record in records] == ["5", "1"]
        assert all(record["value_count"] == 2 for record in records)

    def test_parser_records_warning_for_unsupported_and_malformed_statements(self, evidence_db):
        collector = ExtractionWarningCollector(
            extractor_name="firefox_config",
            run_id="run_warn",
            evidence_id=1,
        )
        raw = """
            pref("browser.startup.page", 1);
            user_pref("network.proxy.type", );
        """

        records = parse_firefox_pref_file(
            raw,
            browser="firefox",
            profile="abc.default-release",
            source_path="/home/alice/.mozilla/firefox/abc.default-release/prefs.js",
            run_id="run_warn",
            config_type="prefs_js",
            warning_collector=collector,
        )

        assert records == []
        assert collector.warning_count == 2

        inserted = collector.flush_to_database(evidence_db)
        assert inserted == 2

        warnings = evidence_db.execute(
            "SELECT warning_type, item_value FROM extraction_warnings WHERE run_id = ? ORDER BY id",
            ("run_warn",),
        ).fetchall()
        assert warnings[0][0] == "schema_mismatch"
        assert warnings[0][1] == "pref"


class TestIngestion:
    def test_ingests_prefs_and_user_js_with_override_note(
        self,
        extractor,
        tmp_path,
        evidence_db,
        mock_callbacks,
    ):
        prefs_path = tmp_path / "firefox_default_prefs.js"
        prefs_path.write_text(
            '\n'.join([
                'user_pref("browser.startup.homepage", "https://prefs.example");',
                'user_pref("network.proxy.type", 5);',
            ])
        )
        user_path = tmp_path / "firefox_default_user.js"
        user_path.write_text(
            '\n'.join([
                'user_pref("browser.startup.homepage", "https://user.example");',
                'user_pref("browser.download.dir", "C:/Downloads");',
            ])
        )

        manifest = {
            "run_id": "run_firefox_config_1",
            "extraction_timestamp_utc": "2026-05-27T00:00:00+00:00",
            "extraction_tool": "pytsk3:test",
            "files": [
                {
                    "copy_status": "ok",
                    "extracted_path": str(prefs_path),
                    "browser": "firefox",
                    "profile": "abc.default-release",
                    "file_type": "prefs_js",
                    "logical_path": "Users/alice/AppData/Roaming/Mozilla/Firefox/Profiles/abc.default-release/prefs.js",
                    "partition_index": 2,
                    "fs_type": "NTFS",
                    "forensic_path": "p2:Users/alice/AppData/Roaming/Mozilla/Firefox/Profiles/abc.default-release/prefs.js",
                    "file_size_bytes": prefs_path.stat().st_size,
                    "md5": "d41d8cd98f00b204e9800998ecf8427e",
                    "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                },
                {
                    "copy_status": "ok",
                    "extracted_path": str(user_path),
                    "browser": "firefox",
                    "profile": "abc.default-release",
                    "file_type": "user_js",
                    "logical_path": "Users/alice/AppData/Roaming/Mozilla/Firefox/Profiles/abc.default-release/user.js",
                    "partition_index": 2,
                    "fs_type": "NTFS",
                    "forensic_path": "p2:Users/alice/AppData/Roaming/Mozilla/Firefox/Profiles/abc.default-release/user.js",
                    "file_size_bytes": user_path.stat().st_size,
                    "md5": "d41d8cd98f00b204e9800998ecf8427e",
                    "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                },
            ],
        }
        (tmp_path / "manifest.json").write_text(json.dumps(manifest))

        result = extractor.run_ingestion(tmp_path, evidence_db, 1, {}, mock_callbacks)

        assert result["config_records"] == 4
        rows = get_browser_configs(evidence_db, 1, run_id="run_firefox_config_1", limit=100)
        assert len(rows) == 4
        assert {row["config_type"] for row in rows} == {"prefs_js", "user_js"}
        assert all(row["partition_index"] == 2 for row in rows)
        assert all(row["logical_path"] for row in rows)
        assert all(row["forensic_path"] for row in rows)

        homepage_rows = [row for row in rows if row["config_key"] == "startup_homepage"]
        assert len(homepage_rows) == 2
        assert any("user.js may override" in (row["notes"] or "") for row in homepage_rows)

    def test_ingestion_does_not_delete_unrelated_tor_rows(
        self,
        extractor,
        tmp_path,
        evidence_db,
        mock_callbacks,
    ):
        insert_browser_config(
            evidence_db,
            evidence_id=1,
            browser="tor",
            config_type="torrc",
            config_key="SocksPort",
            config_value="9050",
            source_path="/TorBrowser/Data/Tor/torrc",
            run_id="tor_run_existing",
            profile="tor_data",
        )

        prefs_path = tmp_path / "prefs.js"
        prefs_path.write_text('user_pref("browser.startup.page", 1);')

        manifest = {
            "run_id": "run_firefox_config_2",
            "extraction_timestamp_utc": "2026-05-27T00:00:00+00:00",
            "extraction_tool": "pytsk3:test",
            "files": [
                {
                    "copy_status": "ok",
                    "extracted_path": str(prefs_path),
                    "browser": "firefox",
                    "profile": "abc.default-release",
                    "file_type": "prefs_js",
                    "logical_path": "home/alice/.mozilla/firefox/abc.default-release/prefs.js",
                    "partition_index": 1,
                }
            ],
        }
        (tmp_path / "manifest.json").write_text(json.dumps(manifest))

        result = extractor.run_ingestion(tmp_path, evidence_db, 1, {}, mock_callbacks)

        assert result["config_records"] == 1
        tor_rows = get_browser_configs(evidence_db, 1, run_id="tor_run_existing", limit=10)
        assert len(tor_rows) == 1

    def test_oversized_copied_file_records_warning(self, extractor, tmp_path, evidence_db, mock_callbacks):
        large_path = tmp_path / "large_prefs.js"
        large_path.write_text('user_pref("browser.startup.page", 1);')
        manifest = {
            "run_id": "run_large_prefs",
            "extraction_timestamp_utc": "2026-05-27T00:00:00+00:00",
            "extraction_tool": "pytsk3:test",
            "files": [
                {
                    "copy_status": "ok",
                    "extracted_path": str(large_path),
                    "browser": "firefox",
                    "profile": "Default",
                    "file_type": "prefs_js",
                    "logical_path": "home/alice/.mozilla/firefox/default/prefs.js",
                    "partition_index": 0,
                    "file_size_bytes": MAX_CONFIG_FILE_BYTES + 1,
                }
            ],
        }
        (tmp_path / "manifest.json").write_text(json.dumps(manifest))

        result = extractor.run_ingestion(tmp_path, evidence_db, 1, {}, mock_callbacks)

        assert result == {"records": 0, "config_records": 0}
        warning_type = evidence_db.execute(
            "SELECT warning_type FROM extraction_warnings WHERE run_id = ?",
            ("run_large_prefs",),
        ).fetchone()[0]
        assert warning_type == "file_corrupt"
