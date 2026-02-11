"""Tests for lightweight tool discovery helpers."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from core.tool_discovery import TOOL_CANDIDATES, discover_tools


def _write_registry_config(home_dir: Path, payload: dict[str, str]) -> None:
    config_path = home_dir / ".config" / "surfsifter" / "tool_paths.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps(payload), encoding="utf-8")


def test_discover_tools_uses_tool_registry_custom_paths(tmp_path, monkeypatch):
    """discover_tools() should honor custom paths saved by ToolRegistry."""
    tool_path = tmp_path / "bin" / "bulk_extractor"
    tool_path.parent.mkdir(parents=True, exist_ok=True)
    tool_path.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    tool_path.chmod(0o755)

    _write_registry_config(tmp_path, {"bulk_extractor": str(tool_path)})
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))

    with patch("core.tool_discovery.get_tool_version", return_value="bulk_extractor 1.6.0"):
        tools = discover_tools()

    assert tools["bulk_extractor"].path == tool_path
    assert tools["bulk_extractor"].available is True


def test_discover_tools_explicit_overrides_take_precedence(tmp_path, monkeypatch):
    """Explicit overrides should win over ToolRegistry persisted paths."""
    registry_path = tmp_path / "bin" / "bulk_extractor_registry"
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    registry_path.chmod(0o755)

    explicit_path = tmp_path / "bin" / "bulk_extractor_cli"
    explicit_path.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    explicit_path.chmod(0o755)

    _write_registry_config(tmp_path, {"bulk_extractor": str(registry_path)})
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))

    with patch("core.tool_discovery.get_tool_version", return_value="bulk_extractor 1.6.0"):
        tools = discover_tools(overrides={"bulk_extractor": explicit_path})

    assert tools["bulk_extractor"].path == explicit_path


def test_tool_candidates_include_firejail():
    """firejail should be discoverable in the lightweight discovery path."""
    assert "firejail" in TOOL_CANDIDATES

