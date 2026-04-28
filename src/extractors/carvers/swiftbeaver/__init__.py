"""SwiftBeaver carver extractor.

Runs SwiftBeaver tool to carve images and extract URLs from evidence.
Supports dual-phase workflow:
- Extraction: Run SwiftBeaver subprocess (reads E01 natively via libewf)
- Ingestion: Parse JSONL metadata and load into database (images + URLs)
"""

from .extractor import SwiftbeaverExtractor

# Registry-compatible alias (follows {Group}{Extractor}Extractor convention)
CarversSwiftbeaverExtractor = SwiftbeaverExtractor

__all__ = ["SwiftbeaverExtractor", "CarversSwiftbeaverExtractor"]
