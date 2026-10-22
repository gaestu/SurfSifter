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



# ---------------------------------------------------------------------------
# Version probe cache (issue #70)
# ---------------------------------------------------------------------------


def _make_tool(path: Path, body: str = "exit 0") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"#!/bin/sh\n{body}\n", encoding="utf-8")
    path.chmod(0o755)
    return path


def _probes_of(probe_calls, path: Path) -> int:
    return sum(1 for cmd in probe_calls if cmd[0] == str(path))


def _recording_probe(probe_calls, version="bulk_extractor 1.6.0"):
    def fake_probe(cmd):
        probe_calls.append(cmd)
        return version

    return fake_probe


def test_discover_tools_probes_each_executable_once(tmp_path, monkeypatch):
    """Repeated discovery reuses the cached version (no new subprocess)."""
    tool_path = _make_tool(tmp_path / "bin" / "bulk_extractor")
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    probe_calls: list = []

    with patch("core.tool_discovery.get_tool_version", side_effect=_recording_probe(probe_calls)):
        first = discover_tools(overrides={"bulk_extractor": tool_path})
        second = discover_tools(overrides={"bulk_extractor": tool_path})

    assert _probes_of(probe_calls, tool_path) == 1
    assert first["bulk_extractor"].version == second["bulk_extractor"].version == "bulk_extractor 1.6.0"


def test_discover_tools_cache_survives_without_subprocess(tmp_path, monkeypatch):
    """A cached discovery spawns no ``--version`` subprocess at all."""
    import subprocess

    tool_path = _make_tool(tmp_path / "bin" / "bulk_extractor", 'echo "bulk_extractor 2.1.1"')
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    first = discover_tools(overrides={"bulk_extractor": tool_path})

    with patch.object(subprocess, "run", side_effect=AssertionError("unexpected subprocess")):
        second = discover_tools(overrides={"bulk_extractor": tool_path})

    assert first["bulk_extractor"].version == "bulk_extractor 2.1.1"
    assert second["bulk_extractor"].version == "bulk_extractor 2.1.1"


def test_discover_tools_reprobes_on_path_change(tmp_path, monkeypatch):
    """A changed tool path (Preferences → Tools) is probed at once."""
    old_path = _make_tool(tmp_path / "old" / "bulk_extractor")
    new_path = _make_tool(tmp_path / "new" / "bulk_extractor")
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    probe_calls: list = []

    with patch("core.tool_discovery.get_tool_version", side_effect=_recording_probe(probe_calls)):
        _write_registry_config(tmp_path, {"bulk_extractor": str(old_path)})
        assert discover_tools()["bulk_extractor"].path == old_path
        _write_registry_config(tmp_path, {"bulk_extractor": str(new_path)})
        assert discover_tools()["bulk_extractor"].path == new_path

    assert _probes_of(probe_calls, old_path) == 1
    assert _probes_of(probe_calls, new_path) == 1


def test_discover_tools_reprobes_replaced_binary(tmp_path, monkeypatch):
    """A binary replaced at the same path is probed again (stat signature)."""
    tool_path = _make_tool(tmp_path / "bin" / "bulk_extractor", 'echo "bulk_extractor 1.6.0"')
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    assert discover_tools(overrides={"bulk_extractor": tool_path})["bulk_extractor"].version == "bulk_extractor 1.6.0"

    _make_tool(tool_path, 'echo "bulk_extractor 2.1.1 (upgraded)"')
    assert discover_tools(overrides={"bulk_extractor": tool_path})["bulk_extractor"].version == (
        "bulk_extractor 2.1.1 (upgraded)"
    )


def test_discover_tools_refresh_and_clear_force_probe(tmp_path, monkeypatch):
    """``refresh=True`` and ``clear_tool_version_cache()`` probe again."""
    from core.tool_discovery import clear_tool_version_cache

    tool_path = _make_tool(tmp_path / "bin" / "bulk_extractor")
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    probe_calls: list = []

    with patch("core.tool_discovery.get_tool_version", side_effect=_recording_probe(probe_calls)):
        discover_tools(overrides={"bulk_extractor": tool_path})
        discover_tools(overrides={"bulk_extractor": tool_path}, refresh=True)
        assert _probes_of(probe_calls, tool_path) == 2
        discover_tools(overrides={"bulk_extractor": tool_path})
        assert _probes_of(probe_calls, tool_path) == 2  # refresh updated the cache
        clear_tool_version_cache()
        discover_tools(overrides={"bulk_extractor": tool_path})
        assert _probes_of(probe_calls, tool_path) == 3


def test_discover_tools_caches_failed_probe(tmp_path, monkeypatch):
    """A tool without version output is not probed again on every call."""
    tool_path = _make_tool(tmp_path / "bin" / "bulk_extractor")
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    probe_calls: list = []

    with patch("core.tool_discovery.get_tool_version", side_effect=_recording_probe(probe_calls, None)):
        discover_tools(overrides={"bulk_extractor": tool_path})
        tools = discover_tools(overrides={"bulk_extractor": tool_path})

    assert _probes_of(probe_calls, tool_path) == 1
    assert tools["bulk_extractor"].available is True
    assert tools["bulk_extractor"].version is None
