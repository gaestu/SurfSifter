"""bulk_extractor modular extractor.

Runs bulk_extractor forensic tool to extract URLs, emails, IPs, and other
artifacts from evidence images. Supports dual-phase workflow:
- Extraction: Run bulk_extractor subprocess (slow, overnight batch)
- Ingestion: Parse output files and load into database (fast, selective)
"""

from .extractor import BulkExtractorExtractor

# Registry-compatible alias (follows {Group}{Extractor}Extractor convention)
CarversBulkExtractorExtractor = BulkExtractorExtractor

__all__ = ["BulkExtractorExtractor", "CarversBulkExtractorExtractor"]
