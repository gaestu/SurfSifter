"""
Firefox Cache Strategies Package

Provides extraction strategies for different scenarios:
- IcatExtractionStrategy: Uses SleuthKit icat (fastest for E01 with inodes)
- ConcurrentExtractionStrategy: ThreadPool with per-worker handles
- SequentialExtractionStrategy: Simple sequential fallback
"""

from .base import (
    CHUNK_SIZE,
    WORK_QUEUE_SIZE,
    ExtractionResult,
    IcatResult,
    DiscoveredFile,
    ExtractionContext,
    ExtractionStrategy,
    stream_copy_with_hash,
    stream_copy_with_hash_from_iterator,
    compute_file_hashes,
    extract_profile_from_path,
    sanitize_filename,
    generate_output_filename,
)
from .icat import IcatExtractionStrategy, icat_available
from .concurrent import ConcurrentExtractionStrategy
from .sequential import SequentialExtractionStrategy

__all__ = [
    # Constants
    "CHUNK_SIZE",
    "WORK_QUEUE_SIZE",
    # Data classes
    "ExtractionResult",
    "IcatResult",
    "DiscoveredFile",
    "ExtractionContext",
    # Strategy base
    "ExtractionStrategy",
    # Strategies
    "IcatExtractionStrategy",
    "ConcurrentExtractionStrategy",
    "SequentialExtractionStrategy",
    # Utilities
    "stream_copy_with_hash",
    "stream_copy_with_hash_from_iterator",
    "compute_file_hashes",
    "extract_profile_from_path",
    "sanitize_filename",
    "generate_output_filename",
    "icat_available",
]
