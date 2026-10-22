from __future__ import annotations

import os
import shutil
import subprocess
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from .logging import get_logger
from .subprocess_env import clean_subprocess_env

LOGGER = get_logger("core.tool_discovery")

TOOL_CANDIDATES: Dict[str, Iterable[str]] = {
    "bulk_extractor": ("bulk_extractor",),
    "swiftbeaver": ("swiftbeaver",),
    "foremost": ("foremost",),
    "scalpel": ("scalpel",),
    "exiftool": ("exiftool",),
    "firejail": ("firejail",),
    "ewfmount": ("ewfmount",),
}


# Version probe cache (issue #70): ``--version`` output per executable, keyed
# by resolved path and stat signature so a changed path or a replaced binary
# is probed again. Shared by the GUI and worker threads.
_ProbeKey = Tuple[str, int, int, int, int]
_VERSION_CACHE: Dict[_ProbeKey, Optional[str]] = {}
_VERSION_CACHE_LOCK = threading.Lock()


@dataclass(slots=True)
class ToolInfo:
    """Description of an external tool present on the system."""

    name: str
    path: Optional[Path]
    version: Optional[str]

    @property
    def available(self) -> bool:
        return self.path is not None


def discover_tools(
    overrides: Optional[Dict[str, Path]] = None,
    *,
    refresh: bool = False,
) -> Dict[str, ToolInfo]:
    """Discover supported external tools, optionally using user-provided overrides.

    Tool paths are resolved on every call. Versions come from a per-process
    probe cache (one ``--version`` subprocess per executable) unless
    ``refresh`` is set: then every tool is probed again and the cache updated.
    Use ``refresh=True`` when the version is recorded for a run.
    """
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
            version = _probe_version(path, refresh=refresh)
        tools[name] = ToolInfo(name=name, path=path, version=version)
    return tools


def clear_tool_version_cache() -> None:
    """Forget all cached tool versions (next discovery probes again)."""
    with _VERSION_CACHE_LOCK:
        _VERSION_CACHE.clear()


def _probe_version(path: Path, *, refresh: bool) -> Optional[str]:
    """Return the version of ``path``, probing only on cache miss or refresh."""
    key = _probe_key(path)
    if key is None:
        return get_tool_version([str(path)])
    if not refresh:
        with _VERSION_CACHE_LOCK:
            if key in _VERSION_CACHE:
                return _VERSION_CACHE[key]
    version = get_tool_version([str(path)])
    with _VERSION_CACHE_LOCK:
        _VERSION_CACHE[key] = version
    return version


def _probe_key(path: Path) -> Optional[_ProbeKey]:
    """Cache key of an executable: resolved path and stat signature."""
    try:
        resolved = os.path.realpath(path)
        st = os.stat(resolved)
    except OSError:
        return None
    return (resolved, st.st_mtime_ns, st.st_size, st.st_ino, st.st_dev)


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
            env=clean_subprocess_env(),
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
