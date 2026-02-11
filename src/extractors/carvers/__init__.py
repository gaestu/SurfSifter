"""
Carver extractors - Forensic data carving tools.

This module provides extractors that use external carving tools:
- Bulk Extractor: URL/email/IP discovery and image carving
- Browser Carver: Unallocated space carving for browser artifacts

Note: _shared.carving/ remains at extractors/ root due to extensive imports.

Usage:
    from extractors.carvers import (
        BulkExtractorExtractor,
        BrowserCarverExtractor,
    )
"""

from __future__ import annotations

# Import from nested locations within carvers/
from .bulk_extractor import BulkExtractorExtractor
from .browser_carver import BrowserCarverExtractor

__all__ = [
    "BulkExtractorExtractor",
    "BrowserCarverExtractor",
]
