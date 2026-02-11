"""
Helpers for locating bundled SleuthKit binaries.
"""
from __future__ import annotations

import os
import platform
import shutil
import sys
from pathlib import Path
from typing import List, Optional, Tuple

__all__ = ["get_sleuthkit_bin"]


def _platform_subdir(name: str) -> Tuple[Optional[str], str]:
    system = platform.system()
    machine = platform.machine().lower()

    if system == "Windows":
        return "win64", f"{name}.exe"
    if system == "Linux":
        if machine in ("x86_64", "amd64"):
            return "linux-x86_64", name
        if machine in ("aarch64", "arm64"):
            return "linux-arm64", name
        return None, name
    if system == "Darwin":
        if machine in ("x86_64", "amd64"):
            return "macos-x86_64", name
        if machine in ("arm64", "aarch64"):
            return "macos-arm64", name
        return None, name
    return None, name


def _candidate_vendor_dirs(base_dir: Optional[Path]) -> List[Path]:
    if base_dir is not None:
        return [Path(base_dir) / "vendor" / "sleuthkit"]

    roots: List[Path] = []

    if getattr(sys, "frozen", False):
        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            roots.append(Path(meipass))
        if sys.executable:
            roots.append(Path(sys.executable).resolve().parent)

    try:
        roots.append(Path(__file__).resolve().parents[4])
    except IndexError:
        roots.append(Path(__file__).resolve().parent)

    roots.append(Path.cwd())

    seen = set()
    vendor_dirs: List[Path] = []
    for root in roots:
        vendor_dir = root / "vendor" / "sleuthkit"
        key = str(vendor_dir)
        if key in seen:
            continue
        seen.add(key)
        vendor_dirs.append(vendor_dir)
    return vendor_dirs


def _ensure_executable(path: Path) -> None:
    if platform.system() == "Windows":
        return
    if os.access(path, os.X_OK):
        return
    try:
        path.chmod(path.stat().st_mode | 0o111)
    except OSError:
        pass


def get_sleuthkit_bin(name: str, base_dir: Optional[Path] = None) -> Optional[str]:
    """
    Find SleuthKit binary: bundled first, then system PATH.

    Args:
        name: Binary name without extension (e.g., "fls", "mmls", "icat")
        base_dir: Optional project root override for tests.

    Returns:
        Full path to binary, or None if not found.
    """
    subdir, binary = _platform_subdir(name)

    if subdir:
        for vendor_dir in _candidate_vendor_dirs(base_dir):
            candidate = vendor_dir / subdir / binary
            if candidate.is_file():
                _ensure_executable(candidate)
                return str(candidate)

    return shutil.which(name)
