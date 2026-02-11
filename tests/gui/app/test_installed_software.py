"""
Tests for Installed Software feature.

Tests:
- Registry rules for software extraction
- Parser software entry processing
- InstalledSoftwareModel
- Forensic interest detection
"""

import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

# =============================================================================
# Registry Rules Tests
# =============================================================================


class TestSoftwareRegistryRules:
    """Test enhanced registry rules for installed software."""

    def test_software_key_has_extract_software_entry_flag(self):
        """Verify Uninstall key uses extract_software_entry mode."""
        from extractors.system.registry.rules import SYSTEM_INFO_SOFTWARE

        # Find the Uninstall key definition
        uninstall_keys = []
        for action in SYSTEM_INFO_SOFTWARE.actions:
            for key in action.keys:
                if "Uninstall" in key.path:
                    uninstall_keys.append(key)

        assert len(uninstall_keys) >= 1, "Should have at least one Uninstall key"

        for key in uninstall_keys:
            assert key.extract_software_entry is True, f"Key {key.path} should have extract_software_entry=True"
            assert key.indicator == "system:installed_software"

    def test_wow6432node_path_included(self):
        """Verify WOW6432Node path is included for 32-bit software."""
        from extractors.system.registry.rules import SYSTEM_INFO_SOFTWARE

        wow_paths = []
        for action in SYSTEM_INFO_SOFTWARE.actions:
            for key in action.keys:
                if "WOW6432Node" in key.path:
                    wow_paths.append(key.path)

        assert len(wow_paths) >= 1, "Should have WOW6432Node path for 32-bit software"
        assert any("Uninstall" in p for p in wow_paths)

    def test_registry_key_dataclass_has_extract_software_entry(self):
        """Verify RegistryKey dataclass has the new field."""
        from extractors.system.registry.rules import RegistryKey

        key = RegistryKey(
            path="Test\\Path",
            extract_software_entry=True,
            indicator="test:indicator"
        )
        assert key.extract_software_entry is True
        assert key.indicator == "test:indicator"


# =============================================================================
# Parser Tests
# =============================================================================


class TestSoftwareEntryProcessing:
    """Test parser software entry extraction."""

    def test_software_fields_constant(self):
        """Verify SOFTWARE_FIELDS contains expected registry values."""
        from extractors.system.registry.parser import SOFTWARE_FIELDS

        expected_fields = [
            "DisplayName",
            "Publisher",
            "DisplayVersion",
            "InstallDate",
            "InstallLocation",
            "InstallSource",
            "UninstallString",
            "EstimatedSize",
        ]

        for field in expected_fields:
            assert field in SOFTWARE_FIELDS, f"Missing field: {field}"

    def test_forensic_software_patterns(self):
        """Verify forensic software patterns include Deep Freeze."""
        from extractors.system.registry.parser import FORENSIC_SOFTWARE_PATTERNS

        assert "deep freeze" in FORENSIC_SOFTWARE_PATTERNS
        assert "faronics" in FORENSIC_SOFTWARE_PATTERNS
        assert "ccleaner" in FORENSIC_SOFTWARE_PATTERNS
        assert "bleachbit" in FORENSIC_SOFTWARE_PATTERNS

    def test_process_software_entry_with_mocked_key(self):
        """Test _process_software_entry with mocked registry key."""
        from extractors.system.registry.parser import _process_software_entry, RegistryFinding

        # Create mock registry key
        mock_key = MagicMock()
        mock_key.name = "Google Chrome"
        mock_key.header.last_modified = "2024-01-15T10:30:00"

        # Mock get_value to return software metadata
        def mock_get_value(name):
            values = {
                "DisplayName": "Google Chrome",
                "Publisher": "Google LLC",
                "DisplayVersion": "120.0.6099.130",
                "InstallDate": "20240115",
                "InstallLocation": "C:\\Program Files\\Google\\Chrome\\Application",
                "EstimatedSize": 150000,
            }
            return values.get(name)

        mock_key.get_value = mock_get_value

        key_def = {"indicator": "system:installed_software", "confidence": 1.0}
        target = {"name": "test_target"}
        action = {"provenance": "registry_test"}
        findings = []

        _process_software_entry(
            mock_key, key_def, target, action, "/test/hive", findings, "SOFTWARE\\Uninstall\\Google Chrome"
        )

        assert len(findings) == 1
        finding = findings[0]
        assert finding.value == "Google Chrome"
        assert finding.name == "system:installed_software"

        # Check extra_json
        extra = json.loads(finding.extra_json)
        assert extra["name"] == "Google Chrome"
        assert extra["publisher"] == "Google LLC"
        assert extra["version"] == "120.0.6099.130"
        assert extra["install_date"] == "20240115"
        assert extra["install_date_formatted"] == "2024-01-15"
        assert extra["size_kb"] == 150000

    def test_process_software_entry_deep_freeze_detection(self):
        """Test that Deep Freeze is flagged as forensic interest."""
        from extractors.system.registry.parser import _process_software_entry

        # Create mock Deep Freeze key
        mock_key = MagicMock()
        mock_key.name = "Deep Freeze Enterprise"
        mock_key.header.last_modified = "2024-01-15T10:30:00"

        def mock_get_value(name):
            values = {
                "DisplayName": "Deep Freeze Enterprise",
                "Publisher": "Faronics Corporation",
                "DisplayVersion": "8.70.020.5687",
            }
            return values.get(name)

        mock_key.get_value = mock_get_value

        findings = []
        _process_software_entry(
            mock_key,
            {"indicator": "system:installed_software"},
            {"name": "test"},
            {"provenance": "registry"},
            "/test/hive",
            findings,
            "SOFTWARE\\Uninstall\\Deep Freeze Enterprise"
        )

        assert len(findings) == 1
        extra = json.loads(findings[0].extra_json)

        assert extra.get("forensic_interest") is True
        assert extra.get("forensic_category") == "system_restore"

    def test_process_software_entry_ccleaner_detection(self):
        """Test that CCleaner is flagged as anti-forensic."""
        from extractors.system.registry.parser import _process_software_entry

        mock_key = MagicMock()
        mock_key.name = "CCleaner"
        mock_key.header.last_modified = "2024-01-15T10:30:00"

        def mock_get_value(name):
            return {"DisplayName": "CCleaner", "Publisher": "Piriform"}.get(name)

        mock_key.get_value = mock_get_value

        findings = []
        _process_software_entry(
            mock_key,
            {"indicator": "system:installed_software"},
            {"name": "test"},
            {"provenance": "registry"},
            "/test/hive",
            findings,
            "SOFTWARE\\Uninstall\\CCleaner"
        )

        assert len(findings) == 1
        extra = json.loads(findings[0].extra_json)

        assert extra.get("forensic_interest") is True
        assert extra.get("forensic_category") == "anti_forensic"

    def test_process_software_entry_skips_no_displayname(self):
        """Test that entries without DisplayName are skipped."""
        from extractors.system.registry.parser import _process_software_entry

        mock_key = MagicMock()
        mock_key.name = "SomeKey"
        mock_key.header.last_modified = "2024-01-15T10:30:00"
        mock_key.get_value = lambda x: None  # No DisplayName

        findings = []
        _process_software_entry(
            mock_key,
            {"indicator": "system:installed_software"},
            {"name": "test"},
            {"provenance": "registry"},
            "/test/hive",
            findings,
            "SOFTWARE\\Uninstall\\SomeKey"
        )

        assert len(findings) == 0, "Should skip entries without DisplayName"

    def test_wow6432node_architecture_detection(self):
        """Test that WOW6432Node paths are marked as 32-bit."""
        from extractors.system.registry.parser import _process_software_entry

        mock_key = MagicMock()
        mock_key.name = "7-Zip"
        mock_key.header.last_modified = "2024-01-15T10:30:00"
        mock_key.get_value = lambda x: {"DisplayName": "7-Zip 23.01 (x86)"}.get(x)

        findings = []
        _process_software_entry(
            mock_key,
            {"indicator": "system:installed_software"},
            {"name": "test"},
            {"provenance": "registry"},
            "/test/hive",
            findings,
            "WOW6432Node\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\7-Zip"
        )

        assert len(findings) == 1
        extra = json.loads(findings[0].extra_json)
        assert extra.get("architecture") == "32-bit"


# =============================================================================
# Model Tests
# =============================================================================


class TestInstalledSoftwareModel:
    """Test InstalledSoftwareModel Qt model."""

    def test_model_columns(self):
        """Verify model has expected columns."""
        from app.features.os_artifacts.models.installed_software_model import InstalledSoftwareModel

        expected_headers = [
            "Software Name",
            "Publisher",
            "Version",
            "Install Date",
            "Install Location",
            "Size (KB)",
            "Forensic",
        ]

        assert InstalledSoftwareModel.HEADERS == expected_headers

    def test_model_forensic_colors(self):
        """Verify forensic interest color coding."""
        from app.features.os_artifacts.models.installed_software_model import InstalledSoftwareModel

        # Colors should be defined
        assert InstalledSoftwareModel.COLOR_FORENSIC_RESTORE is not None
        assert InstalledSoftwareModel.COLOR_FORENSIC_ANTI is not None
        assert InstalledSoftwareModel.COLOR_FORENSIC_OTHER is not None


# =============================================================================
# Integration Tests
# =============================================================================


class TestSoftwareExtractionIntegration:
    """Integration tests for software extraction workflow."""

    def test_key_to_dict_includes_extract_software_entry(self):
        """Verify key_to_dict includes extract_software_entry field."""
        from extractors.system.registry.rules import RegistryKey, _key_to_dict

        key = RegistryKey(
            path="Test\\Uninstall\\*",
            extract_software_entry=True,
            indicator="system:installed_software",
        )

        result = _key_to_dict(key)

        assert result["path"] == "Test\\Uninstall\\*"
        assert result.get("extract_software_entry") is True
        assert result["indicator"] == "system:installed_software"
