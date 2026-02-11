"""
Modular extractor system for forensic analysis.

Each extractor is a self-contained module with:
- Configuration UI
- Extraction logic (write files)
- Ingestion logic (load database)
- Status reporting

Folder Structure:
- browser/         Browser family extractors (chromium/, firefox/, safari/)
- system/          Windows system artifacts (registry, jump_lists, file_list)
- media/           Image carving (filesystem_images, foremost_carver, scalpel)
- carvers/         Forensic carving tools (bulk_extractor, browser_carver)
- _shared/         Shared utilities (timestamps, sqlite_helpers, path_utils)
"""

from .base import BaseExtractor, ExtractorMetadata
from .callbacks import ExtractorCallbacks
from .extractor_registry import ExtractorRegistry
from .workers import ExtractionWorker, IngestionWorker, WorkerCallbacks
from .exceptions import ExtractorError, ExtractionFailedError, IngestionFailedError
from .browser_patterns import (
    BROWSER_PATTERNS,
    get_browser_paths,
    get_browsers_for_artifact,
    get_all_browsers,
    get_browser_display_name,
    get_browser_engine,
    get_legacy_browser_patterns,
    get_cache_patterns,
)
from .widgets import BrowserSelectionWidget

# New folder structure exports
from . import system
from . import media
from . import carvers
from . import browser

__all__ = [
    'BaseExtractor',
    'ExtractorMetadata',
    'ExtractorCallbacks',
    'ExtractorRegistry',
    'ExtractionWorker',
    'IngestionWorker',
    'WorkerCallbacks',
    'ExtractorError',
    'ExtractionFailedError',
    'IngestionFailedError',
    # Browser patterns
    'BROWSER_PATTERNS',
    'get_browser_paths',
    'get_browsers_for_artifact',
    'get_all_browsers',
    'get_browser_display_name',
    'get_browser_engine',
    'get_legacy_browser_patterns',
    'get_cache_patterns',
    # Widgets
    'BrowserSelectionWidget',
    # New folder structure
    'system',
    'media',
    'carvers',
    'browser',
]
