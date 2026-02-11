"""
Chromium Media History Extractor

Extracts browser media playback history from Chromium-based browsers
(Chrome, Edge, Opera, Brave) with full forensic provenance.

Note: Only Chromium-based browsers maintain a Media History database.
Firefox stores media state in session storage (not a dedicated database).

This is the canonical location for media history extraction.
For backward compatibility, also available at:
- extractors.media_history (re-export)
"""

from .extractor import MediaHistoryExtractor

# Also export as ChromiumMediaHistoryExtractor for new code
ChromiumMediaHistoryExtractor = MediaHistoryExtractor

__all__ = [
    "MediaHistoryExtractor",
    "ChromiumMediaHistoryExtractor",
]
