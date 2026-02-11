"""Application version helpers sourced from ``pyproject.toml``."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import re


@lru_cache(maxsize=1)
def get_app_version() -> str:
    """Return the project version from ``pyproject.toml``."""
    import sys
    if getattr(sys, 'frozen', False):
        # PyInstaller bundle — pyproject.toml is bundled alongside the executable
        pyproject_path = Path(sys._MEIPASS) / "pyproject.toml"
    else:
        pyproject_path = Path(__file__).resolve().parents[2] / "pyproject.toml"
    try:
        content = pyproject_path.read_text(encoding="utf-8")
    except OSError:
        return "0.0.0"

    match = re.search(r'^\s*version\s*=\s*"([^"]+)"\s*$', content, flags=re.MULTILINE)
    if match:
        return match.group(1)
    return "0.0.0"

