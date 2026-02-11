"""Tests for SurfSifter rebrand: database fallback, config fallback, branding assets."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from core.database import (
    find_case_database,
    init_db,
    CASE_DB_SUFFIX,
    LEGACY_CASE_DB_SUFFIX,
    CASE_DB_GLOB,
    LEGACY_CASE_DB_GLOB,
)


# ---------------------------------------------------------------------------
# Case DB suffix constants
# ---------------------------------------------------------------------------

def test_case_db_suffix_values():
    """Verify the case DB suffix constants have the expected values."""
    assert CASE_DB_SUFFIX == "_surfsifter.sqlite"
    assert LEGACY_CASE_DB_SUFFIX == "_browser.sqlite"
    assert CASE_DB_GLOB == "*_surfsifter.sqlite"
    assert LEGACY_CASE_DB_GLOB == "*_browser.sqlite"


# ---------------------------------------------------------------------------
# find_case_database — new name preferred
# ---------------------------------------------------------------------------

def test_find_case_database_prefers_surfsifter(tmp_path):
    """When both _surfsifter.sqlite and _browser.sqlite exist, prefer surfsifter."""
    surfsifter_db = tmp_path / f"CASE-001{CASE_DB_SUFFIX}"
    legacy_db = tmp_path / f"CASE-001{LEGACY_CASE_DB_SUFFIX}"
    surfsifter_db.write_bytes(b"surfsifter")
    legacy_db.write_bytes(b"legacy")

    result = find_case_database(tmp_path)
    assert result is not None
    assert result.name.endswith(CASE_DB_SUFFIX)


def test_find_case_database_falls_back_to_browser(tmp_path):
    """When only _browser.sqlite exists, fall back to it."""
    legacy_db = tmp_path / f"CASE-001{LEGACY_CASE_DB_SUFFIX}"
    legacy_db.write_bytes(b"legacy")

    result = find_case_database(tmp_path)
    assert result is not None
    assert result.name == f"CASE-001{LEGACY_CASE_DB_SUFFIX}"


def test_find_case_database_returns_none_when_empty(tmp_path):
    """Returns None when no case DB exists."""
    assert find_case_database(tmp_path) is None


def test_find_case_database_returns_none_for_missing_dir(tmp_path):
    """Returns None when the folder doesn't exist."""
    assert find_case_database(tmp_path / "nonexistent") is None


# ---------------------------------------------------------------------------
# New cases create _surfsifter.sqlite
# ---------------------------------------------------------------------------

def test_new_case_creates_surfsifter_db(tmp_path):
    """New cases should produce a _surfsifter.sqlite file."""
    case_number = "CASE-2026-001"
    db_path = tmp_path / f"{case_number}{CASE_DB_SUFFIX}"
    conn = init_db(tmp_path, db_path=db_path)
    conn.close()

    assert db_path.exists()
    assert db_path.name == f"{case_number}{CASE_DB_SUFFIX}"

    # And find_case_database should find it
    found = find_case_database(tmp_path)
    assert found == db_path


# ---------------------------------------------------------------------------
# Config directory fallback — ToolRegistry
# ---------------------------------------------------------------------------

def test_tool_registry_config_uses_surfsifter(tmp_path):
    """ToolRegistry should use surfsifter config dir by default."""
    from core.tool_registry import ToolRegistry

    config_file = tmp_path / "surfsifter" / "tool_paths.json"
    config_file.parent.mkdir(parents=True, exist_ok=True)

    registry = ToolRegistry(config_path=config_file)
    assert registry.config_path == config_file
    assert "surfsifter" in str(registry.config_path)


def test_tool_registry_has_legacy_fallback():
    """ToolRegistry created without args sets a legacy fallback path."""
    from core.tool_registry import ToolRegistry

    # Use explicit path to avoid filesystem side-effects
    # Just verify the class sets _legacy_config_path when config_path is None
    # by checking the code path via a tmp dir
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        with patch("core.tool_registry.Path.home", return_value=Path(td)):
            registry = ToolRegistry()
            assert registry._legacy_config_path is not None
            assert "web-and-browser-analyzer" in str(registry._legacy_config_path)
            assert "surfsifter" in str(registry.config_path)


# ---------------------------------------------------------------------------
# Config directory fallback — ReferenceListManager
# ---------------------------------------------------------------------------

def test_reference_list_manager_fallback(tmp_path):
    """ReferenceListManager should fall back to web-and-browser-analyzer if surfsifter doesn't exist."""
    from core.matching.manager import ReferenceListManager

    # Create legacy directory with content
    legacy_dir = tmp_path / ".config" / "web-and-browser-analyzer" / "reference_lists"
    legacy_urllists = legacy_dir / "urllists"
    legacy_urllists.mkdir(parents=True)
    (legacy_urllists / "test_list.txt").write_text("example.com\n")

    with patch("core.matching.manager.Path.home", return_value=tmp_path):
        manager = ReferenceListManager()
    # Should use legacy path since surfsifter doesn't exist
    assert "web-and-browser-analyzer" in str(manager.base_path)


def test_reference_list_manager_prefers_surfsifter(tmp_path):
    """ReferenceListManager should prefer surfsifter dir when it exists."""
    from core.matching.manager import ReferenceListManager

    # Create both directories
    surfsifter_dir = tmp_path / ".config" / "surfsifter" / "reference_lists"
    surfsifter_dir.mkdir(parents=True)
    legacy_dir = tmp_path / ".config" / "web-and-browser-analyzer" / "reference_lists"
    legacy_dir.mkdir(parents=True)

    with patch("core.matching.manager.Path.home", return_value=tmp_path):
        manager = ReferenceListManager()
    # Should prefer surfsifter
    assert "surfsifter" in str(manager.base_path)


def test_reference_list_manager_explicit_path(tmp_path):
    """ReferenceListManager should use explicitly provided path."""
    from core.matching.manager import ReferenceListManager

    custom_path = tmp_path / "custom_lists"
    manager = ReferenceListManager(base_path=custom_path)
    assert manager.base_path == custom_path.resolve()


# ---------------------------------------------------------------------------
# Branding assets exist
# ---------------------------------------------------------------------------

def test_branding_assets_exist():
    """Verify that branding assets are present in the repository."""
    repo_root = Path(__file__).resolve().parent.parent.parent
    branding_dir = repo_root / "config" / "branding"

    assert (branding_dir / "logo.jpg").exists(), "logo.jpg is missing"
    assert (branding_dir / "surfsifter.png").exists(), "surfsifter.png is missing"
    assert (branding_dir / "surfsifter.ico").exists(), "surfsifter.ico is missing"


# ---------------------------------------------------------------------------
# install_predefined_lists installs demo content only
# ---------------------------------------------------------------------------

def test_install_predefined_lists_only_installs_demo(tmp_path):
    """install_predefined_lists should only install sanitized demo content."""
    from core.matching.manager import install_predefined_lists

    installed = install_predefined_lists(dest_base_path=tmp_path)

    # Should have installed at least the sample_demo list
    urllist_names = [n for n in installed if n.startswith("urllist/")]
    assert any("sample_demo" in n for n in urllist_names), (
        f"sample_demo not found in installed lists: {urllist_names}"
    )

    # No gambling lists should be present
    for name in installed:
        assert "bet" not in name.lower(), f"Gambling list installed: {name}"
        assert "casino" not in name.lower(), f"Gambling list installed: {name}"


def test_install_predefined_lists_does_not_overwrite(tmp_path):
    """install_predefined_lists should not overwrite existing files."""
    from core.matching.manager import install_predefined_lists

    # Install once
    install_predefined_lists(dest_base_path=tmp_path)

    # Write custom content to sample_demo
    demo_file = tmp_path / "urllists" / "sample_demo.txt"
    demo_file.write_text("custom-content\n")

    # Install again — should not overwrite
    install_predefined_lists(dest_base_path=tmp_path)
    assert demo_file.read_text() == "custom-content\n"
