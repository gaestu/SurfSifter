from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from .logging import get_logger

LOGGER = get_logger("core.tool_discovery")

TOOL_CANDIDATES: Dict[str, Iterable[str]] = {
    "bulk_extractor": ("bulk_extractor",),
    "foremost": ("foremost",),
    "scalpel": ("scalpel",),
    "exiftool": ("exiftool",),
    "firejail": ("firejail",),
    "ewfmount": ("ewfmount",),
}


@dataclass(slots=True)
class ToolInfo:
    """Description of an external tool present on the system."""

    name: str
    path: Optional[Path]
    version: Optional[str]

    @property
    def available(self) -> bool:
        return self.path is not None


def discover_tools(overrides: Optional[Dict[str, Path]] = None) -> Dict[str, ToolInfo]:
    """Discover supported external tools, optionally using user-provided overrides."""
    resolved_overrides = _load_registry_overrides()
    if overrides:
        resolved_overrides.update(overrides)
    tools: Dict[str, ToolInfo] = {}
    for name, candidates in TOOL_CANDIDATES.items():
        path: Optional[Path] = resolved_overrides.get(name)
        if path and path.exists():
            LOGGER.debug("Using override for tool %s: %s", name, path)
        else:
            path = _which(candidates)
        version = None
        if path:
            version = get_tool_version([str(path)])
        tools[name] = ToolInfo(name=name, path=path, version=version)
    return tools


def get_tool_version(cmd: List[str]) -> Optional[str]:
    """Attempt to retrieve the version string for an external tool."""
    try:
        process = subprocess.run(
            cmd + ["--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        LOGGER.warning("Unable to determine version for %s: %s", cmd, exc)
        return None

    output = (process.stdout or "").strip()
    if not output:
        return None
    first_line = output.splitlines()[0]
    LOGGER.debug("Detected tool version for %s: %s", cmd[0], first_line)
    return first_line


def _which(candidates: Iterable[str]) -> Optional[Path]:
    for candidate in candidates:
        found = shutil.which(candidate)
        if found:
            return Path(found)
    return None


def _load_registry_overrides() -> Dict[str, Path]:
    """Load user-defined tool path overrides from ToolRegistry config."""
    try:
        from .tool_registry import ToolRegistry

        return ToolRegistry().get_custom_paths()
    except Exception as exc:
        LOGGER.debug("Unable to load ToolRegistry overrides for tool discovery: %s", exc)
        return {}
