"""
Firefox Cache2 Extractor (Modular)

Extracts and ingests Firefox HTTP cache with full forensic provenance.
Parses cache2 format (body-first layout), carves images, generates perceptual hashes.

Architecture:
- parser.py: Cache2 format parsing (URL, metadata, elements)
- manifest.py: Incremental JSONL part-file manifest writer
- strategies/: Pluggable extraction strategies (icat, concurrent, sequential)
- ingestion.py: Database ingestion with image carving
- image_carver.py: Image extraction and perceptual hashing

This is the canonical location for Firefox cache parsing.
For backward compatibility, also available at:
- extractors.cache_firefox (re-export)
- extractors.cache.CacheFirefoxExtractor (re-export)
"""

# Main extractor class
from .extractor import CacheFirefoxExtractor, FirefoxCacheExtractor

# Parser module exports
from .parser import (
    parse_cache2_entry,
    parse_elements,
    extract_http_metadata,
    extract_url_from_key,
    Cache2ParseResult,
    HttpMetadata,
    CACHE2_VERSIONS,
    CACHE2_CHUNK_SIZE,
)

# Manifest module exports
from .manifest import (
    ManifestWriter,
    load_manifest,
    create_extraction_summary,
    PART_FILE_MAX_ENTRIES,
)

# Strategy exports
from .strategies import (
    ExtractionResult,
    DiscoveredFile,
    ExtractionContext,
    ExtractionStrategy,
    IcatExtractionStrategy,
    ConcurrentExtractionStrategy,
    SequentialExtractionStrategy,
    icat_available,
    extract_profile_from_path,
    stream_copy_with_hash,
    compute_file_hashes,
)

# Ingestion handler
from .ingestion import CacheIngestionHandler

# Index parser (Firefox cache2 binary index)
from ._index import (
    parse_cache_index,
    parse_journal,
    CacheIndex,
    CacheIndexEntry,
    INDEX_VERSION_9,
    INDEX_VERSION_A,
    KNOWN_VERSIONS,
    RECORD_SIZE,
    CONTENT_TYPES,
    FLAG_INITIALIZED,
    FLAG_ANONYMOUS,
    FLAG_REMOVED,
    FLAG_PINNED,
    FLAG_HAS_ALT_DATA,
    FLAG_FILE_SIZE_MASK,
)

# Doomed/trash recovery
from ._recovery import (
    discover_all_cache_entries,
    correlate_index_with_files,
)

# Image carving (existing module)
from .image_carver import (
    extract_body,
    detect_image_type,
    carve_image_from_cache_entry,
    save_carved_image,
    compute_hashes,
)

__all__ = [
    # Main extractor
    "CacheFirefoxExtractor",
    "FirefoxCacheExtractor",
    # Parser
    "parse_cache2_entry",
    "parse_elements",
    "extract_http_metadata",
    "extract_url_from_key",
    "Cache2ParseResult",
    "HttpMetadata",
    "CACHE2_VERSIONS",
    "CACHE2_CHUNK_SIZE",
    # Manifest
    "ManifestWriter",
    "load_manifest",
    "create_extraction_summary",
    "PART_FILE_MAX_ENTRIES",
    # Strategies
    "ExtractionResult",
    "DiscoveredFile",
    "ExtractionContext",
    "ExtractionStrategy",
    "IcatExtractionStrategy",
    "ConcurrentExtractionStrategy",
    "SequentialExtractionStrategy",
    "icat_available",
    "extract_profile_from_path",
    "stream_copy_with_hash",
    "compute_file_hashes",
    # Ingestion
    "CacheIngestionHandler",
    # Image carving
    "extract_body",
    "detect_image_type",
    "carve_image_from_cache_entry",
    "save_carved_image",
    "compute_hashes",
]
