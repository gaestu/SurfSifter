"""
Tests for Firefox Sync Data Extractor.

Tests the extractor metadata, parsers, schema warnings, and structure.
"""
from __future__ import annotations

import json
import pytest
from pathlib import Path
from typing import Dict, Any

from extractors.browser.firefox.sync_data import (
    FirefoxSyncDataExtractor,
    parse_firefox_sync,
    KNOWN_ROOT_KEYS,
    KNOWN_ACCOUNT_DATA_KEYS,
    KNOWN_DEVICE_KEYS,
)
from extractors.browser.firefox.sync_data._schemas import (
    KNOWN_DEVICE_TYPES,
    KNOWN_AVAILABLE_COMMANDS,
    SYNCED_TYPE_INDICATORS,
)
from extractors._shared.extraction_warnings import ExtractionWarningCollector


# =============================================================================
# Extractor Metadata Tests
# =============================================================================

class TestFirefoxSyncDataMetadata:
    """Test extractor metadata and structure."""

    def test_metadata_name(self):
        """Test extractor name is firefox_sync_data."""
        extractor = FirefoxSyncDataExtractor()
        assert extractor.metadata.name == "firefox_sync_data"

    def test_metadata_display_name(self):
        """Test display name is human readable."""
        extractor = FirefoxSyncDataExtractor()
        assert "Firefox" in extractor.metadata.display_name
        assert "Sync" in extractor.metadata.display_name

    def test_metadata_version(self):
        """Test version is populated from pyproject.toml."""
        extractor = FirefoxSyncDataExtractor()
        assert extractor.metadata.version
        assert "." in extractor.metadata.version

    def test_metadata_category(self):
        """Test category is browser."""
        extractor = FirefoxSyncDataExtractor()
        assert extractor.metadata.category == "browser"

    def test_can_extract_and_ingest(self):
        """Test both extraction and ingestion are supported."""
        extractor = FirefoxSyncDataExtractor()
        assert extractor.metadata.can_extract is True
        assert extractor.metadata.can_ingest is True

    def test_supported_browsers(self):
        """Test supported browsers include Firefox family."""
        extractor = FirefoxSyncDataExtractor()
        assert "firefox" in extractor.SUPPORTED_BROWSERS
        assert "tor" in extractor.SUPPORTED_BROWSERS


class TestFirefoxSyncDataMethods:
    """Test extractor method availability."""

    def test_can_run_extraction_requires_evidence(self):
        """Test can_run_extraction returns False without evidence."""
        extractor = FirefoxSyncDataExtractor()
        can_run, reason = extractor.can_run_extraction(None)
        assert can_run is False
        assert "evidence" in reason.lower()

    def test_can_run_ingestion_requires_manifest(self, tmp_path):
        """Test can_run_ingestion returns False without manifest."""
        extractor = FirefoxSyncDataExtractor()
        can_run, reason = extractor.can_run_ingestion(tmp_path)
        assert can_run is False
        assert "manifest" in reason.lower()

    def test_can_run_ingestion_with_manifest(self, tmp_path):
        """Test can_run_ingestion returns True with manifest."""
        extractor = FirefoxSyncDataExtractor()
        manifest = tmp_path / "manifest.json"
        manifest.write_text(json.dumps({"run_id": "test"}))
        can_run, _ = extractor.can_run_ingestion(tmp_path)
        assert can_run is True

    def test_get_output_dir(self, tmp_path):
        """Test output directory path format."""
        extractor = FirefoxSyncDataExtractor()
        output_dir = extractor.get_output_dir(tmp_path, "evidence_1")
        assert "evidences" in str(output_dir)
        assert "evidence_1" in str(output_dir)
        assert "firefox_sync_data" in str(output_dir)


# =============================================================================
# Parser Tests
# =============================================================================

class TestParseFirefoxSync:
    """Test the signedInUser.json parser."""

    @pytest.fixture
    def sample_signedinuser(self) -> Dict[str, Any]:
        """Return sample signedInUser.json data."""
        return {
            "version": 1,
            "accountData": {
                "email": "user@example.com",
                "uid": "abcdef123456",
                "displayName": "Test User",
                "verified": True,
                "sessionToken": "session_token_value",
                "keyFetchToken": "key_fetch_token_value",
                "unwrapBKey": "unwrap_key_value",
                "device": {
                    "id": "device-uuid-1234",
                    "name": "Firefox on Linux",
                    "type": "desktop",
                    "pushEndpointExpired": False,
                    "availableCommands": {
                        "https://identity.mozilla.com/cmd/open-uri": {}
                    },
                },
            },
        }

    def test_parse_extracts_account(self, sample_signedinuser):
        """Test account extraction."""
        result = parse_firefox_sync(sample_signedinuser)
        assert len(result["accounts"]) == 1

        account = result["accounts"][0]
        assert account["email"] == "user@example.com"
        assert account["account_id"] == "abcdef123456"
        assert account["display_name"] == "Test User"
        assert account["sync_enabled"] is True

    def test_parse_extracts_device(self, sample_signedinuser):
        """Test device extraction."""
        result = parse_firefox_sync(sample_signedinuser)
        assert len(result["devices"]) == 1

        device = result["devices"][0]
        assert device["device_id"] == "device-uuid-1234"
        assert device["device_name"] == "Firefox on Linux"
        assert device["device_type"] == "desktop"
        assert device["push_expired"] is False

    def test_parse_synced_types_from_tokens(self, sample_signedinuser):
        """Test synced_types are derived from token presence."""
        result = parse_firefox_sync(sample_signedinuser)
        account = result["accounts"][0]

        # Should have these based on tokens in sample
        assert "active_session" in account["synced_types"]
        assert "key_sync" in account["synced_types"]
        assert "encryption_enabled" in account["synced_types"]
        assert "remote_commands" in account["synced_types"]

    def test_parse_empty_accountdata(self):
        """Test handling of missing/empty accountData."""
        result = parse_firefox_sync({"version": 1})
        assert result["accounts"] == []
        assert result["devices"] == []

        result2 = parse_firefox_sync({"version": 1, "accountData": None})
        assert result2["accounts"] == []
        assert result2["devices"] == []

    def test_parse_minimal_account(self):
        """Test parsing minimal account data (just email)."""
        data = {
            "accountData": {
                "email": "minimal@test.com",
            }
        }
        result = parse_firefox_sync(data)
        assert len(result["accounts"]) == 1
        assert result["accounts"][0]["email"] == "minimal@test.com"
        assert result["accounts"][0]["synced_types"] == []  # No tokens


# =============================================================================
# Schema Warning Tests
# =============================================================================

class TestSchemaWarnings:
    """Test schema warning collection for unknown keys."""

    def test_unknown_root_key_warning(self):
        """Test warning for unknown root-level key."""
        data = {
            "version": 1,
            "accountData": {"email": "test@test.com"},
            "unknownRootKey": "value",
        }

        collector = ExtractionWarningCollector("firefox_sync_data", "run_1", 1)
        parse_firefox_sync(data, warning_collector=collector, source_file="test.json")

        warnings = [w for w in collector._warnings if "root.unknownRootKey" in w.item_name]
        assert len(warnings) == 1
        assert warnings[0].warning_type == "json_unknown_key"

    def test_unknown_accountdata_key_warning(self):
        """Test warning for unknown accountData key."""
        data = {
            "accountData": {
                "email": "test@test.com",
                "unknownAccountField": "value",
            }
        }

        collector = ExtractionWarningCollector("firefox_sync_data", "run_1", 1)
        parse_firefox_sync(data, warning_collector=collector, source_file="test.json")

        warnings = [w for w in collector._warnings if "accountData.unknownAccountField" in w.item_name]
        assert len(warnings) == 1

    def test_unknown_device_key_warning(self):
        """Test warning for unknown device key."""
        data = {
            "accountData": {
                "email": "test@test.com",
                "device": {
                    "id": "dev-1",
                    "name": "Test Device",
                    "unknownDeviceField": "value",
                },
            }
        }

        collector = ExtractionWarningCollector("firefox_sync_data", "run_1", 1)
        parse_firefox_sync(data, warning_collector=collector, source_file="test.json")

        warnings = [w for w in collector._warnings if "accountData.device.unknownDeviceField" in w.item_name]
        assert len(warnings) == 1

    def test_no_warnings_for_known_keys(self):
        """Test that known keys don't generate warnings."""
        data = {
            "version": 1,
            "accountData": {
                "email": "test@test.com",
                "uid": "uid123",
                "displayName": "Test",
                "verified": True,
                "sessionToken": "token",
            }
        }

        collector = ExtractionWarningCollector("firefox_sync_data", "run_1", 1)
        parse_firefox_sync(data, warning_collector=collector, source_file="test.json")

        assert len(collector._warnings) == 0


# =============================================================================
# Schema Definitions Tests
# =============================================================================

class TestSchemaDefinitions:
    """Test schema definitions are comprehensive."""

    def test_known_root_keys_defined(self):
        """Test root keys are defined."""
        assert "accountData" in KNOWN_ROOT_KEYS
        assert "version" in KNOWN_ROOT_KEYS
        assert "profilePath" in KNOWN_ROOT_KEYS

    def test_known_account_data_keys_defined(self):
        """Test accountData keys include core fields."""
        assert "email" in KNOWN_ACCOUNT_DATA_KEYS
        assert "uid" in KNOWN_ACCOUNT_DATA_KEYS
        assert "displayName" in KNOWN_ACCOUNT_DATA_KEYS
        assert "verified" in KNOWN_ACCOUNT_DATA_KEYS
        assert "device" in KNOWN_ACCOUNT_DATA_KEYS
        # Auth tokens
        assert "sessionToken" in KNOWN_ACCOUNT_DATA_KEYS
        assert "keyFetchToken" in KNOWN_ACCOUNT_DATA_KEYS
        assert "unwrapBKey" in KNOWN_ACCOUNT_DATA_KEYS

    def test_known_device_keys_defined(self):
        """Test device keys include core fields."""
        assert "id" in KNOWN_DEVICE_KEYS
        assert "name" in KNOWN_DEVICE_KEYS
        assert "type" in KNOWN_DEVICE_KEYS
        assert "availableCommands" in KNOWN_DEVICE_KEYS
        assert "pushEndpointExpired" in KNOWN_DEVICE_KEYS

    def test_known_device_types_defined(self):
        """Test device types cover common cases."""
        assert "desktop" in KNOWN_DEVICE_TYPES
        assert "mobile" in KNOWN_DEVICE_TYPES
        assert "tablet" in KNOWN_DEVICE_TYPES

    def test_synced_type_indicators_defined(self):
        """Test sync type indicators map tokens to types."""
        assert "sessionToken" in SYNCED_TYPE_INDICATORS
        assert "keyFetchToken" in SYNCED_TYPE_INDICATORS
        assert "unwrapBKey" in SYNCED_TYPE_INDICATORS


# =============================================================================
# Multi-Partition Support Tests
# =============================================================================

class TestMultiPartitionSupport:
    """Test multi-partition discovery methods exist."""

    def test_discover_files_multi_partition_method_exists(self):
        """Test _discover_files_multi_partition method exists."""
        extractor = FirefoxSyncDataExtractor()
        assert hasattr(extractor, "_discover_files_multi_partition")

    def test_discover_sync_files_filesystem_method_exists(self):
        """Test fallback filesystem discovery method exists."""
        extractor = FirefoxSyncDataExtractor()
        assert hasattr(extractor, "_discover_sync_files_filesystem")

    def test_extract_file_accepts_partition_index(self):
        """Test _extract_file accepts partition_index parameter."""
        import inspect
        extractor = FirefoxSyncDataExtractor()
        sig = inspect.signature(extractor._extract_file)
        params = list(sig.parameters.keys())
        assert "partition_index" in params
