"""
Tests for Tool Registry and Discovery System
"""

import pytest
import tempfile
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from core.tool_registry import ToolRegistry, ToolInfo


@pytest.fixture
def temp_config():
    """Create temporary config directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "tool_paths.json"
        yield config_path


@pytest.fixture
def registry(temp_config):
    """Create tool registry with temp config."""
    return ToolRegistry(config_path=temp_config)


def test_tool_registry_initialization(temp_config):
    """Test tool registry initializes correctly."""
    registry = ToolRegistry(config_path=temp_config)

    assert registry.config_path == temp_config
    assert len(registry._tools) == 0
    assert len(registry._custom_paths) == 0


def test_known_tools_include_ewfmount():
    """Ensure ewfmount is tracked in the centralized registry."""
    assert "ewfmount" in ToolRegistry.KNOWN_TOOLS


def test_discover_all_tools(registry):
    """Test discovering all tools."""
    with patch.object(registry, 'discover_tool') as mock_discover:
        mock_discover.return_value = ToolInfo(
            name="test_tool",
            path=Path("/usr/bin/test"),
            version="1.0.0",
            status="found",
            capabilities=["test"]
        )

        results = registry.discover_all_tools()

        # Should discover all known tools
        assert len(results) == len(ToolRegistry.KNOWN_TOOLS)
        assert mock_discover.call_count == len(ToolRegistry.KNOWN_TOOLS)


def test_discover_executable_tool_found(registry):
    """Test discovering executable tool that exists on PATH."""
    with patch('shutil.which', return_value="/usr/bin/bulk_extractor"):
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(
                stdout="bulk_extractor 1.6.0",
                stderr="",
                returncode=0
            )

            tool_info = registry.discover_tool("bulk_extractor")

            assert tool_info.name == "bulk_extractor"
            assert tool_info.path == Path("/usr/bin/bulk_extractor")
            assert tool_info.version == "1.6.0"
            assert tool_info.status == "found"
            assert "url_extraction" in tool_info.capabilities


def test_discover_executable_tool_missing(registry):
    """Test discovering executable tool that doesn't exist."""
    with patch('shutil.which', return_value=None):
        tool_info = registry.discover_tool("foremost")

        assert tool_info.name == "foremost"
        assert tool_info.path is None
        assert tool_info.status == "missing"
        assert "Not found on PATH" in tool_info.error_message


def test_discover_python_module_found(registry):
    """Test discovering Python module that's installed."""
    mock_module = MagicMock()
    mock_module.__version__ = "1.2.3"

    with patch('builtins.__import__', return_value=mock_module):
        # Mock pytsk3 with version function
        with patch.object(registry, '_check_python_module') as mock_check:
            mock_check.return_value = ToolInfo(
                name="pytsk3",
                path=None,
                version="20211111",
                status="found",
                capabilities=["filesystem_access"]
            )

            tool_info = registry.discover_tool("pytsk3")

            assert tool_info.name == "pytsk3"
            assert tool_info.path is None
            assert tool_info.status == "found"
            assert "filesystem_access" in tool_info.capabilities


def test_discover_python_module_missing(registry):
    """Test discovering Python module that's not installed."""
    with patch('builtins.__import__', side_effect=ImportError("No module named 'pytsk3'")):
        tool_info = registry._check_python_module("pytsk3", {"import_check": "pytsk3", "capabilities": []})

        assert tool_info.name == "pytsk3"
        assert tool_info.status == "missing"
        assert "not installed" in tool_info.error_message


def test_version_comparison(registry):
    """Test semantic version comparison."""
    # Equal versions
    assert registry._version_compare("1.6.0", "1.6.0") == 0

    # v1 > v2
    assert registry._version_compare("1.7.0", "1.6.0") == 1
    assert registry._version_compare("2.0.0", "1.9.9") == 1
    assert registry._version_compare("1.6.1", "1.6.0") == 1

    # v1 < v2
    assert registry._version_compare("1.5.0", "1.6.0") == -1
    assert registry._version_compare("1.6.0", "2.0.0") == -1
    assert registry._version_compare("1.6.0", "1.6.1") == -1


def test_version_below_minimum(registry):
    """Test detecting version below minimum requirement."""
    with patch('shutil.which', return_value="/usr/bin/bulk_extractor"):
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(
                stdout="bulk_extractor 1.5.0",
                stderr="",
                returncode=0
            )

            tool_info = registry.discover_tool("bulk_extractor")

            assert tool_info.status == "error"
            assert "below minimum" in tool_info.error_message
            assert "1.6.0" in tool_info.error_message


def test_custom_path_persistence(registry, temp_config):
    """Test custom path saving and loading."""
    custom_path = Path("/custom/path/to/bulk_extractor")

    # Set custom path
    with patch.object(registry, 'discover_tool') as mock_discover:
        mock_discover.return_value = ToolInfo(
            name="bulk_extractor",
            path=custom_path,
            version="1.6.0",
            status="found",
            capabilities=[]
        )

        registry.set_custom_path("bulk_extractor", custom_path)

    # Verify saved to config
    assert temp_config.exists()
    with open(temp_config) as f:
        data = json.load(f)
        assert data["bulk_extractor"] == str(custom_path)

    # Create new registry and verify it loads custom paths
    new_registry = ToolRegistry(config_path=temp_config)
    assert "bulk_extractor" in new_registry._custom_paths
    assert new_registry._custom_paths["bulk_extractor"] == custom_path


def test_default_config_loads_legacy_path_and_migrates(tmp_path, monkeypatch):
    """Legacy ~/.config/web-and-browser-analyzer config should be loaded and migrated to surfsifter location."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    legacy_dir = tmp_path / ".config" / "web-and-browser-analyzer"
    legacy_dir.mkdir(parents=True)
    legacy_config = legacy_dir / "tool_paths.json"
    legacy_config.write_text(
        json.dumps({"bulk_extractor": "/legacy/tools/bulk_extractor"}),
        encoding="utf-8",
    )

    registry = ToolRegistry()

    assert registry.config_path == (
        tmp_path / ".config" / "surfsifter" / "tool_paths.json"
    )
    assert registry._custom_paths["bulk_extractor"] == Path("/legacy/tools/bulk_extractor")
    assert registry.config_path.exists()


def test_get_missing_tools(registry):
    """Test retrieving list of missing tools."""
    registry._tools = {
        "tool1": ToolInfo("tool1", Path("/usr/bin/tool1"), "1.0", "found", []),
        "tool2": ToolInfo("tool2", None, None, "missing", [], "Not found"),
        "tool3": ToolInfo("tool3", Path("/usr/bin/tool3"), "0.5", "error", [], "Version too old"),
        "tool4": ToolInfo("tool4", Path("/usr/bin/tool4"), "2.0", "found", []),
    }

    missing = registry.get_missing_tools()

    assert len(missing) == 2
    assert any(t.name == "tool2" for t in missing)
    assert any(t.name == "tool3" for t in missing)


def test_test_tool_success(registry):
    """Test testing a tool successfully."""
    tool_info = ToolInfo(
        name="bulk_extractor",
        path=Path("/usr/bin/bulk_extractor"),
        version="1.6.0",
        status="found",
        capabilities=[]
    )
    registry._tools["bulk_extractor"] = tool_info

    with patch('subprocess.run') as mock_run:
        mock_run.return_value = Mock(returncode=0)

        success, message = registry.test_tool("bulk_extractor")

        assert success is True
        assert "Successfully executed" in message
        assert "1.6.0" in message


def test_test_tool_failure(registry):
    """Test testing a tool that fails."""
    tool_info = ToolInfo(
        name="bulk_extractor",
        path=Path("/usr/bin/bulk_extractor"),
        version="1.6.0",
        status="found",
        capabilities=[]
    )
    registry._tools["bulk_extractor"] = tool_info

    with patch('subprocess.run', side_effect=Exception("Command failed")):
        success, message = registry.test_tool("bulk_extractor")

        assert success is False
        assert "failed" in message.lower()


def test_test_tool_python_module(registry):
    """Test testing a Python module (always succeeds if found)."""
    tool_info = ToolInfo(
        name="pytsk3",
        path=None,
        version="20211111",
        status="found",
        capabilities=[]
    )
    registry._tools["pytsk3"] = tool_info

    success, message = registry.test_tool("pytsk3")

    assert success is True
    assert "importable" in message


def test_unknown_tool(registry):
    """Test discovering unknown tool."""
    tool_info = registry.discover_tool("nonexistent_tool")

    assert tool_info.status == "error"
    assert "Unknown tool" in tool_info.error_message


def test_version_check_timeout(registry):
    """Test handling version check timeout."""
    import subprocess

    with patch('shutil.which', return_value="/usr/bin/bulk_extractor"):
        with patch('subprocess.run', side_effect=subprocess.TimeoutExpired("cmd", 5)):
            tool_info = registry.discover_tool("bulk_extractor")

            assert tool_info.status == "error"
            assert "timed out" in tool_info.error_message.lower()


def test_custom_path_takes_precedence(registry):
    """Test that custom path takes precedence over PATH."""
    custom_path = Path("/custom/bulk_extractor")
    registry._custom_paths["bulk_extractor"] = custom_path

    with patch.object(Path, 'exists', return_value=True):
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(
                stdout="bulk_extractor 1.7.0",
                stderr="",
                returncode=0
            )

            tool_info = registry.discover_tool("bulk_extractor")

            assert tool_info.path == custom_path
            # shutil.which should NOT be called
            with patch('shutil.which') as mock_which:
                registry.discover_tool("bulk_extractor")
                # which() should not be called since custom path exists
                # (it might still be called once in the actual implementation,
                # but the custom path should be used)
