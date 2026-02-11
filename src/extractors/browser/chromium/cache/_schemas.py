"""
Chromium Cache Format Schema Definitions.

Centralizes known constants, magic numbers, and version information for
both simple cache and blockfile cache formats. Used for parsing validation
and extraction warnings.

References:
- Simple Cache: net/disk_cache/simple/simple_entry_format.h
- Blockfile: net/disk_cache/blockfile/disk_format.h
- Index: net/disk_cache/simple/simple_index_file.h
"""
from __future__ import annotations

from typing import Dict, FrozenSet, Set

# =============================================================================
# Simple Cache Format (Modern, post-2013)
# =============================================================================

# Magic numbers
SIMPLE_INITIAL_MAGIC = 0xfcfb6d1ba7725c30  # Header magic
SIMPLE_FINAL_MAGIC = 0xf4fa6f45970d41d8    # EOF magic

# Version tracking
SIMPLE_ENTRY_VERSION = 5  # Current entry version on disk
SIMPLE_ENTRY_MIN_VERSION = 5  # Minimum we support
SIMPLE_ENTRY_MAX_VERSION = 6  # Maximum we've seen (for future-proofing)

# Known versions with notes
SIMPLE_ENTRY_VERSIONS: Dict[int, str] = {
    5: "Current stable version (Chrome 40+)",
    6: "Future version (not yet released)",
}

# Header/EOF sizes
SIMPLE_FILE_HEADER_SIZE = 24  # 8+4+4+4+4 = 24 bytes
SIMPLE_FILE_EOF_SIZE = 24     # 8+4+4+4+4 = 24 bytes

# EOF flags (from simple_entry_format.h)
FLAG_HAS_CRC32 = 0x01
FLAG_HAS_KEY_SHA256 = 0x02

# Known EOF flags
KNOWN_EOF_FLAGS: FrozenSet[int] = frozenset({
    0x00,  # No flags
    FLAG_HAS_CRC32,  # 0x01
    FLAG_HAS_KEY_SHA256,  # 0x02
    FLAG_HAS_CRC32 | FLAG_HAS_KEY_SHA256,  # 0x03
})

# Struct formats (little-endian)
SIMPLE_FILE_HEADER_FORMAT = '<QIIII'  # magic(Q), version(I), key_length(I), key_hash(I), padding(I)
SIMPLE_FILE_EOF_FORMAT = '<QIIII'      # magic(Q), flags(I), crc32(I), stream_size(I), padding(I)


# =============================================================================
# Simple Cache Index Format
# =============================================================================

SIMPLE_INDEX_MAGIC = 0x656e74657220796f  # "enter yo" in ASCII
SIMPLE_INDEX_VERSION = 9  # Current index version
SIMPLE_INDEX_MIN_VERSION = 7  # Minimum supported version
SIMPLE_INDEX_MAX_VERSION = 10  # Maximum we've seen

# Known index versions
SIMPLE_INDEX_VERSIONS: Dict[int, str] = {
    7: "Legacy version",
    8: "Added cache_last_modified",
    9: "Current stable version",
    10: "Future version (not yet released)",
}


# =============================================================================
# Blockfile Cache Format (Legacy, pre-2015)
# =============================================================================

# Magic numbers
BLOCKFILE_INDEX_MAGIC = 0xC103CAC3
BLOCKFILE_BLOCK_MAGIC = 0xC104CAC3

# Sizes
BLOCK_HEADER_SIZE = 8192  # 8KB header per block file
DEFAULT_TABLE_SIZE = 0x10000  # 65536 default buckets
INDEX_HEADER_SIZE = 256  # Approximate header size before hash table

# Block sizes by file type (from addr.h)
# Key: file_type bits (28-30), Value: block size in bytes
BLOCK_SIZES: Dict[int, int] = {
    1: 36,    # RANKINGS (data_0) - RankingsNode
    2: 256,   # BLOCK_256 (data_1) - EntryStore
    3: 1024,  # BLOCK_1K (data_2) - small data
    4: 4096,  # BLOCK_4K (data_3) - medium data
    5: 8,     # BLOCK_FILES
    6: 104,   # BLOCK_ENTRIES
    7: 48,    # BLOCK_EVICTED
}

# Known file types
KNOWN_BLOCK_FILE_TYPES: FrozenSet[int] = frozenset(BLOCK_SIZES.keys())

# EntryStore constants
ENTRY_STORE_SIZE = 256
ENTRY_STORE_KEY_OFFSET = 96
ENTRY_STORE_KEY_SIZE = 160  # Inline key storage
MAX_INTERNAL_KEY_LENGTH = 160 - 1  # Null-terminated

# Entry states (from disk_format.h)
ENTRY_NORMAL = 0
ENTRY_EVICTED = 1
ENTRY_DOOMED = 2

# Known entry states
KNOWN_ENTRY_STATES: FrozenSet[int] = frozenset({
    ENTRY_NORMAL,
    ENTRY_EVICTED,
    ENTRY_DOOMED,
})

# Entry state display names
ENTRY_STATE_NAMES: Dict[int, str] = {
    ENTRY_NORMAL: "normal",
    ENTRY_EVICTED: "evicted",
    ENTRY_DOOMED: "doomed",
}

# EntryStore flags (from disk_format.h)
PARENT_ENTRY = 1
CHILD_ENTRY = 2

KNOWN_ENTRY_FLAGS: FrozenSet[int] = frozenset({
    0,
    PARENT_ENTRY,
    CHILD_ENTRY,
    PARENT_ENTRY | CHILD_ENTRY,
})

# Windows epoch offset (1601-01-01 to 1970-01-01 in microseconds)
WINDOWS_EPOCH_OFFSET_MICROSECONDS = 11644473600 * 1000000


# =============================================================================
# Cache Key Format (per http_cache.cc)
# =============================================================================

# Cache key format: credential_key/upload_data_identifier/[isolation_key]url
#
# Where isolation_key (if present) contains:
# - "_dk_" prefix (double-key marker)
# - Optional "s_" prefix for subframe document resources
# - Optional "cn_" prefix for cross-site main frame navigation
# - NetworkIsolationKey string (top-level site + frame site)
# - " " (space) separator
# - The actual resource URL

DOUBLE_KEY_PREFIX = "_dk_"
DOUBLE_KEY_SEPARATOR = " "

# Known cache key prefixes (after the N/N/ part)
KNOWN_CACHE_KEY_PREFIXES: FrozenSet[str] = frozenset({
    "_dk_",   # Double-keying for privacy
    "s_",     # Subframe document resource (within _dk_)
    "cn_",    # Cross-site navigation (within _dk_)
})


# =============================================================================
# HTTP Content-Encoding Values (for decompression)
# =============================================================================

KNOWN_CONTENT_ENCODINGS: FrozenSet[str] = frozenset({
    "gzip",
    "deflate",
    "br",       # Brotli
    "zstd",     # Zstandard
    "identity", # No encoding
    "",         # Empty (same as identity)
})


# =============================================================================
# Cache File Types (for categorization)
# =============================================================================

# Simple cache file patterns
SIMPLE_CACHE_FILE_TYPES: Dict[str, str] = {
    "entry": "Primary cache entry files (hash_0)",
    "sparse": "Sparse/stream files (hash_1, hash_s)",
    "index": "Index files (index, the-real-index)",
}

# Blockfile cache file patterns
BLOCKFILE_FILE_TYPES: Dict[str, str] = {
    "index": "Hash table index file",
    "data_block": "Block storage files (data_0/1/2/3)",
    "external": "External data files (f_NNNNNN)",
}


# =============================================================================
# Validation Helpers
# =============================================================================

def is_known_simple_version(version: int) -> bool:
    """Check if simple cache entry version is known."""
    return SIMPLE_ENTRY_MIN_VERSION <= version <= SIMPLE_ENTRY_MAX_VERSION


def is_known_index_version(version: int) -> bool:
    """Check if simple cache index version is known."""
    return SIMPLE_INDEX_MIN_VERSION <= version <= SIMPLE_INDEX_MAX_VERSION


def is_known_entry_state(state: int) -> bool:
    """Check if blockfile entry state is known."""
    return state in KNOWN_ENTRY_STATES


def is_known_block_file_type(file_type: int) -> bool:
    """Check if blockfile file type is known."""
    return file_type in KNOWN_BLOCK_FILE_TYPES


def is_known_eof_flags(flags: int) -> bool:
    """Check if EOF flags combination is known."""
    # Check if only known flag bits are set
    known_bits = FLAG_HAS_CRC32 | FLAG_HAS_KEY_SHA256
    return (flags & ~known_bits) == 0


def is_known_content_encoding(encoding: str) -> bool:
    """Check if content encoding is known."""
    if not encoding:
        return True
    return encoding.lower().strip() in KNOWN_CONTENT_ENCODINGS


def get_unknown_eof_flags(flags: int) -> int:
    """Return the unknown flag bits (if any)."""
    known_bits = FLAG_HAS_CRC32 | FLAG_HAS_KEY_SHA256
    return flags & ~known_bits
