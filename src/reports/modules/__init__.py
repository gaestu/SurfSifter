"""
Report modules package.

Provides plugin-based report modules that can be added to custom sections.
Each module lives in its own subfolder with code and HTML template.

Usage:
    from reports.modules import ModuleRegistry, BaseReportModule

    # Get all available modules
    registry = ModuleRegistry()
    modules = registry.get_all_modules()

    # Instantiate and render a module
    module = registry.get_module("tagged_urls")
    html = module.render(db_conn, evidence_id, config)
"""

from .base import BaseReportModule, FilterField, FilterType, ModuleMetadata
from .registry import ModuleRegistry

# Built-in module exports
from .activity_summary import ActivitySummaryModule
from .autofill import AutofillModule
from .autofill_form_data import AutofillFormDataModule
from .bookmarks import BookmarksModule
from .browser_history import BrowserHistoryModule
from .credentials import CredentialsModule
from .downloaded_images import DownloadedImagesModule
from .images import ImagesModule
from .screenshots import ScreenshotsModule
from .site_engagement import SiteEngagementModule
from .system_summary import SystemSummaryModule
from .tagged_file_list import TaggedFileListModule
from .url_activity_timeline import UrlActivityTimelineModule
from .url_summary import UrlSummaryModule
from .web_storage_details import WebStorageDetailsModule

__all__ = [
    "BaseReportModule",
    "FilterField",
    "FilterType",
    "ModuleMetadata",
    "ModuleRegistry",
    # Built-in modules
    "ActivitySummaryModule",
    "AutofillModule",
    "AutofillFormDataModule",
    "BookmarksModule",
    "BrowserHistoryModule",
    "CredentialsModule",
    "DownloadedImagesModule",
    "ImagesModule",
    "ScreenshotsModule",
    "SiteEngagementModule",
    "SystemSummaryModule",
    "TaggedFileListModule",
    "UrlActivityTimelineModule",
    "UrlSummaryModule",
    "WebStorageDetailsModule",
]
