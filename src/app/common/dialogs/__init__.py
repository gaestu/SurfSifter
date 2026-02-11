"""
Common dialogs package - re-exports all dialog classes.

This module provides a unified interface for importing dialogs:
    from app.common.dialogs import CreateCaseDialog, ImagePreviewDialog, ...
"""
from __future__ import annotations

# Utility functions
from .utils import show_error_dialog

# Case management dialogs
from .case import (
    CreateCaseDialog,
    PartitionSelectionDialog,
)

# Evidence removal dialog
from .remove_evidence import RemoveEvidenceDialog

# Progress and validation dialogs
from .progress import (
    EnhancedProgressDialog,
    ValidationDialog,
    ValidationWorker,
)

# Import/Export dialogs
from .import_export import (
    ExportDialog,
    ExportWorker,
    ImportDialog,
    ImportWorker,
    SizeEstimateWorker,
)

# Reference list dialogs
from .reference_lists import (
    HashListSelectorDialog,
    ReferenceListSelectorDialog,
)

# Tagging dialog
from .tagging import TagArtifactsDialog

# Image preview dialog
from .image_preview import (
    ImageLoaderThread,
    ImagePreviewDialog,
    MAX_IMAGE_AUTO_LOAD_SIZE,
)

# Batch import dialogs
from .batch_import import (
    BatchHashListImportDialog,
    BatchImportProgressDialog,
)

# Extraction dialogs
from .extraction import (
    BulkExtractorReuseDialog,
    CaseWideExtractIngestDialog,
)

# Sandbox browser
from .sandbox_browser import (
    SandboxBrowserDialog,
    SandboxSettings as DialogSandboxSettings,  # Alias to avoid conflict with config
    open_url_sandboxed,
    open_url_external_sandboxed,
    get_sandbox_availability,
    has_firejail,
    has_firejail_compatible_browser,
    detect_browser,
)

# Screenshot dialog
from .screenshot_dialog import ScreenshotCaptureDialog

# URL deduplication dialog
from .deduplicate_urls import DeduplicateUrlsDialog

__all__ = [
    # Utilities
    "show_error_dialog",
    # Case
    "CreateCaseDialog",
    "PartitionSelectionDialog",
    # Evidence removal
    "RemoveEvidenceDialog",
    # Progress
    "EnhancedProgressDialog",
    "ValidationDialog",
    "ValidationWorker",
    # Import/Export
    "ExportDialog",
    "ExportWorker",
    "ImportDialog",
    "ImportWorker",
    "SizeEstimateWorker",
    # Reference lists
    "HashListSelectorDialog",
    "ReferenceListSelectorDialog",
    # Tagging
    "TagArtifactsDialog",
    # Image preview
    "ImageLoaderThread",
    "ImagePreviewDialog",
    "MAX_IMAGE_AUTO_LOAD_SIZE",
    # Batch import
    "BatchHashListImportDialog",
    "BatchImportProgressDialog",
    # Extraction
    "BulkExtractorReuseDialog",
    "CaseWideExtractIngestDialog",
    # Sandbox browser
    "SandboxBrowserDialog",
    "DialogSandboxSettings",
    "open_url_sandboxed",
    "open_url_external_sandboxed",
    "get_sandbox_availability",
    "has_firejail",
    "has_firejail_compatible_browser",
    "detect_browser",
    # Screenshot dialog
    "ScreenshotCaptureDialog",
    # URL deduplication
    "DeduplicateUrlsDialog",
]

