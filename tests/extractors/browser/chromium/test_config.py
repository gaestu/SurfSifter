"""Tests for the Chromium browser configuration extractor."""

from __future__ import annotations

import json
from unittest.mock import Mock

import pytest

from core.database.helpers.browser_config import get_browser_configs
from extractors.browser.chromium._patterns import get_artifact_patterns
from extractors.browser.chromium.config import ChromiumConfigExtractor
from extractors.browser.chromium.config.extractor import MAX_CONFIG_FILE_BYTES


@pytest.fixture
def extractor():
    """Create a Chromium config extractor."""
    return ChromiumConfigExtractor()


@pytest.fixture
def mock_callbacks():
    """Create mock callbacks for extraction/ingestion."""
    callbacks = Mock()
    callbacks.on_step = Mock()
    callbacks.on_progress = Mock()
    callbacks.on_log = Mock()
    callbacks.on_error = Mock()
    callbacks.is_cancelled = Mock(return_value=False)
    return callbacks


class TestMetadataAndRegistry:
    """Test extractor metadata and registry discovery."""

    def test_metadata(self, extractor):
        assert extractor.metadata.name == "chromium_config"
        assert extractor.metadata.category == "browser"
        assert extractor.metadata.can_extract is True
        assert extractor.metadata.can_ingest is True

    def test_registry_discovers_extractor(self, extractor_registry_names):
        assert "chromium_config" in extractor_registry_names


class TestPatternsAndDiscovery:
    """Test config file path handling."""

    def test_local_state_patterns_are_user_data_root_scoped(self):
        patterns = get_artifact_patterns("chrome", "local_state")

        assert any(pattern.endswith("Google/Chrome/User Data/Local State") for pattern in patterns)
        assert not any("/Default/Local State" in pattern for pattern in patterns)
        assert not any("/Profile */Local State" in pattern for pattern in patterns)

    def test_single_partition_discovery_finds_preferences_and_local_state(self, extractor, mock_callbacks):
        paths = {
            "Preferences": [
                "Users/alice/AppData/Local/Google/Chrome/User Data/Default/Preferences",
            ],
            "Local State": [
                "Users/alice/AppData/Local/Google/Chrome/User Data/Local State",
            ],
        }

        evidence_fs = Mock()
        evidence_fs.partition_index = 1
        evidence_fs.fs_type = "NTFS"

        def iter_paths(pattern):
            if pattern.endswith("/Preferences"):
                return iter(paths["Preferences"])
            if pattern.endswith("/Local State"):
                return iter(paths["Local State"])
            return iter([])

        evidence_fs.iter_paths.side_effect = iter_paths

        discovered = extractor._discover_config_files(evidence_fs, ["chrome"], mock_callbacks)

        assert {item["file_type"] for item in discovered} == {"preferences", "local_state"}
        preferences = next(item for item in discovered if item["file_type"] == "preferences")
        local_state = next(item for item in discovered if item["file_type"] == "local_state")
        assert preferences["profile"] == "Default"
        assert preferences["fs_type"] == "NTFS"
        assert preferences["forensic_path"].startswith("p1:")
        assert local_state["profile"] is None

    def test_extract_file_uses_collision_safe_name(self, extractor, tmp_path, mock_callbacks):
        evidence_fs = Mock()
        evidence_fs.fs_type = "NTFS"
        evidence_fs.read_file.return_value = b'{"homepage":"https://example.com"}'
        file_info = {
            "logical_path": "Users/alice/AppData/Local/Google/Chrome/User Data/Profile 1/Preferences",
            "browser": "chrome",
            "profile": "Profile 1",
            "file_type": "preferences",
            "partition_index": 3,
        }

        result = extractor._extract_file(evidence_fs, file_info, tmp_path, mock_callbacks)

        assert result["copy_status"] == "ok"
        assert "chrome_Profile_1_p3_" in result["extracted_path"]
        assert result["extracted_path"].endswith("_preferences.json")
        assert result["fs_type"] == "NTFS"
        assert result["forensic_path"].startswith("p3:")
        assert result["md5"]
        assert result["sha256"]

    def test_run_extraction_falls_back_without_file_list(self, extractor, tmp_path, evidence_db, mock_callbacks):
        evidence_fs = Mock()
        evidence_fs.partition_index = 0
        evidence_fs.fs_type = "NTFS"
        evidence_fs.source_path = "/evidence/test.E01"
        evidence_fs.ewf_paths = None
        evidence_fs.read_file.side_effect = lambda path: b'{"homepage":"https://example.com"}'

        def iter_paths(pattern):
            if pattern.endswith("/Default/Preferences"):
                return iter(["Users/alice/AppData/Local/Google/Chrome/User Data/Default/Preferences"])
            return iter([])

        evidence_fs.iter_paths.side_effect = iter_paths

        assert extractor.run_extraction(
            evidence_fs,
            tmp_path,
            {"evidence_conn": evidence_db, "evidence_id": 1, "selected_browsers": ["chrome"]},
            mock_callbacks,
        ) is True

        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert len(manifest["files"]) == 1
        assert manifest["files"][0]["logical_path"].endswith("Default/Preferences")

    def test_extract_file_skips_oversized_config(self, extractor, tmp_path, mock_callbacks):
        evidence_fs = Mock()
        evidence_fs.read_file = Mock(return_value=b"{}")
        file_info = {
            "logical_path": "Users/alice/AppData/Local/Google/Chrome/User Data/Default/Preferences",
            "browser": "chrome",
            "profile": "Default",
            "file_type": "preferences",
            "partition_index": 0,
            "size_bytes": MAX_CONFIG_FILE_BYTES + 1,
        }

        result = extractor._extract_file(evidence_fs, file_info, tmp_path, mock_callbacks)

        assert result["copy_status"] == "error"
        assert result["error"] == result["error_message"]
        evidence_fs.read_file.assert_not_called()


class TestIngestion:
    """Test browser_config ingestion."""

    def test_ingests_preferences_and_local_state_with_provenance(
        self,
        extractor,
        tmp_path,
        evidence_db,
        mock_callbacks,
    ):
        preferences_path = tmp_path / "chrome_Default_preferences.json"
        preferences_path.write_text(json.dumps({
            "profile": {"name": "Person 1", "exit_type": "Normal"},
            "homepage": "https://example.com",
            "session": {"startup_urls": ["https://a.example", "https://b.example"]},
            "download": {"prompt_for_download": True},
        }))
        local_state_path = tmp_path / "chrome_user_data_local_state.json"
        local_state_path.write_text(json.dumps({
            "profile": {
                "last_used": "Default",
                "last_active_profiles": ["Default", "Profile 1"],
            }
        }))

        manifest = {
            "run_id": "run_config_1",
            "extraction_timestamp_utc": "2026-05-27T00:00:00+00:00",
            "extraction_tool": "pytsk3:test",
            "files": [
                {
                    "copy_status": "ok",
                    "extracted_path": str(preferences_path),
                    "browser": "chrome",
                    "profile": "Default",
                    "file_type": "preferences",
                    "logical_path": "Users/alice/AppData/Local/Google/Chrome/User Data/Default/Preferences",
                    "partition_index": 2,
                    "fs_type": "NTFS",
                    "forensic_path": "tsk://2/Preferences",
                    "file_size_bytes": preferences_path.stat().st_size,
                    "md5": "d41d8cd98f00b204e9800998ecf8427e",
                    "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                },
                {
                    "copy_status": "ok",
                    "extracted_path": str(local_state_path),
                    "browser": "chrome",
                    "profile": None,
                    "file_type": "local_state",
                    "logical_path": "Users/alice/AppData/Local/Google/Chrome/User Data/Local State",
                    "partition_index": 2,
                    "fs_type": "NTFS",
                    "forensic_path": "tsk://2/Local State",
                    "file_size_bytes": local_state_path.stat().st_size,
                    "md5": "d41d8cd98f00b204e9800998ecf8427e",
                    "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                },
            ],
        }
        (tmp_path / "manifest.json").write_text(json.dumps(manifest))

        result = extractor.run_ingestion(tmp_path, evidence_db, 1, {}, mock_callbacks)

        assert result["config_records"] >= 6
        rows = get_browser_configs(evidence_db, 1, run_id="run_config_1", limit=100)
        keys = {row["config_key"] for row in rows}
        assert "profile_name" in keys
        assert "homepage_url" in keys
        assert "startup_urls" in keys
        assert "last_used_profile" in keys
        assert "last_active_profiles" in keys

        local_state_rows = [row for row in rows if row["config_type"] == "local_state"]
        assert local_state_rows
        assert all(row["profile"] is None for row in local_state_rows)
        assert all(row["partition_index"] == 2 for row in rows)
        assert all(row["logical_path"] for row in rows)
        assert all(row["forensic_path"] for row in rows)

    def test_malformed_json_records_warning(self, extractor, tmp_path, evidence_db, mock_callbacks):
        bad_path = tmp_path / "bad_preferences.json"
        bad_path.write_text('{"homepage":')
        manifest = {
            "run_id": "run_bad_json",
            "extraction_timestamp_utc": "2026-05-27T00:00:00+00:00",
            "extraction_tool": "pytsk3:test",
            "files": [
                {
                    "copy_status": "ok",
                    "extracted_path": str(bad_path),
                    "browser": "chrome",
                    "profile": "Default",
                    "file_type": "preferences",
                    "logical_path": "Users/alice/AppData/Local/Google/Chrome/User Data/Default/Preferences",
                    "partition_index": 0,
                }
            ],
        }
        (tmp_path / "manifest.json").write_text(json.dumps(manifest))

        result = extractor.run_ingestion(tmp_path, evidence_db, 1, {}, mock_callbacks)

        assert result == {"records": 0, "config_records": 0}
        warnings = evidence_db.execute(
            "SELECT warning_type, source_file FROM extraction_warnings WHERE run_id = ?",
            ("run_bad_json",),
        ).fetchall()
        assert [(row[0], row[1]) for row in warnings] == [
            ("json_parse_error", "Users/alice/AppData/Local/Google/Chrome/User Data/Default/Preferences")
        ]

    def test_oversized_copied_file_records_warning(self, extractor, tmp_path, evidence_db, mock_callbacks):
        large_path = tmp_path / "large_preferences.json"
        large_path.write_text("{}")
        manifest = {
            "run_id": "run_large_json",
            "extraction_timestamp_utc": "2026-05-27T00:00:00+00:00",
            "extraction_tool": "pytsk3:test",
            "files": [
                {
                    "copy_status": "ok",
                    "extracted_path": str(large_path),
                    "browser": "chrome",
                    "profile": "Default",
                    "file_type": "preferences",
                    "logical_path": "Users/alice/AppData/Local/Google/Chrome/User Data/Default/Preferences",
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
            ("run_large_json",),
        ).fetchone()[0]
        assert warning_type == "file_corrupt"