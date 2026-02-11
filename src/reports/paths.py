"""
Path resolution utilities for reports module.

Handles path resolution in both development and PyInstaller bundle environments.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional


def _get_meipass_base() -> Optional[Path]:
    """Get PyInstaller MEIPASS base directory if running frozen."""
    if getattr(sys, "frozen", False):
        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            return Path(meipass) / "src" / "reports"
    return None


def get_reports_dir() -> Path:
    """Get the reports package root directory.

    Returns:
        Path to reports directory, handling PyInstaller bundles.
    """
    meipass_base = _get_meipass_base()
    if meipass_base:
        return meipass_base
    return Path(__file__).parent


def get_templates_dir() -> Path:
    """Get the templates directory.

    Returns:
        Path to templates directory, handling PyInstaller bundles.
    """
    return get_reports_dir() / "templates"


def get_modules_dir() -> Path:
    """Get the report modules directory.

    Returns:
        Path to modules directory, handling PyInstaller bundles.
    """
    return get_reports_dir() / "modules"


def get_appendix_dir() -> Path:
    """Get the appendix modules directory.

    Returns:
        Path to appendix directory, handling PyInstaller bundles.
    """
    return get_reports_dir() / "appendix"


def get_module_template_dir(module_file: str) -> Path:
    """Get template directory for a specific module.

    When running from source, uses the module's __file__ location.
    When running frozen, maps to the bundled location.

    Args:
        module_file: The __file__ attribute of the calling module.

    Returns:
        Path to the module's directory containing template.html.
    """
    meipass_base = _get_meipass_base()
    if meipass_base:
        # In a PyInstaller bundle, we need to map the module path
        # Extract the relative path from the module file
        # e.g., reports/modules/images/module.py -> images
        source_path = Path(module_file)
        # Find the module folder name (parent of module.py)
        module_folder = source_path.parent.name
        # Determine if this is a modules or appendix module
        grandparent = source_path.parent.parent.name
        if grandparent == "appendix":
            return meipass_base / "appendix" / module_folder
        else:
            return meipass_base / "modules" / module_folder

    # Running from source - use the file's parent directory
    return Path(module_file).parent
