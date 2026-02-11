"""
Screenshots feature package.

Provides the Screenshots tab for investigator documentation.

Initial implementation.
"""
from .tab import ScreenshotsTab
from .models import ScreenshotsTableModel
from .storage import (
    ScreenshotMetadata,
    save_screenshot,
    import_screenshot,
    get_screenshots_dir,
)

__all__ = [
    "ScreenshotsTab",
    "ScreenshotsTableModel",
    "ScreenshotMetadata",
    "save_screenshot",
    "import_screenshot",
    "get_screenshots_dir",
]
