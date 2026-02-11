"""
Chromium Autofill Extractor

Extracts autofill data (form entries, profiles, credentials, credit cards)
from Chromium-based browsers (Chrome, Edge, Opera, Brave).

This is the canonical location for Chromium autofill extraction.
For backward compatibility, the unified extractor is available at:
- extractors.autofill (handles both Chromium and Firefox)
"""

from .extractor import ChromiumAutofillExtractor

__all__ = ["ChromiumAutofillExtractor"]
