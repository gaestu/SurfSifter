"""
Chromium Blockfile Cache Parser

Parses legacy Chromium disk cache (blockfile format) used pre-2015.
This format uses data_0/1/2/3 block files + index file + f_* external files.

Format specification (per Chromium net/disk_cache/blockfile/):
- Index file: 256-byte header + hash table of CacheAddr entries
- Block files (data_N): 8KB header + N*block_size data blocks
- External files (f_XXXXXX): Raw data > 16KB

Block file mapping:
- data_0: 36-byte blocks (RankingsNode - LRU tracking)
- data_1: 256-byte blocks (EntryStore - entry metadata)
- data_2: 1024-byte blocks (small data streams)
- data_3: 4096-byte blocks (medium data streams)

References:
- net/disk_cache/blockfile/disk_format.h
- net/disk_cache/blockfile/addr.h
- net/disk_cache/blockfile/disk_format_base.h
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Tuple, Any

from core.logging import get_logger

# Import schema constants
from ._schemas import (
    # Blockfile constants
    BLOCKFILE_INDEX_MAGIC,
    BLOCKFILE_BLOCK_MAGIC,
    BLOCK_HEADER_SIZE,
    DEFAULT_TABLE_SIZE,
    INDEX_HEADER_SIZE,
    BLOCK_SIZES,
    ENTRY_STORE_SIZE,
    ENTRY_STORE_KEY_OFFSET,
    ENTRY_STORE_KEY_SIZE,
    MAX_INTERNAL_KEY_LENGTH,
    ENTRY_NORMAL,
    ENTRY_EVICTED,
    ENTRY_DOOMED,
    WINDOWS_EPOCH_OFFSET_MICROSECONDS,
    # Cache key constants
    DOUBLE_KEY_PREFIX,
    DOUBLE_KEY_SEPARATOR,
    # Validation helpers
    is_known_entry_state,
    is_known_block_file_type,
    KNOWN_ENTRY_STATES,
    KNOWN_BLOCK_FILE_TYPES,
)

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector

LOGGER = get_logger("extractors.cache_simple.blockfile")

# =============================================================================
# Chromium Cache Key Format (per http_cache.cc)
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
#
# Code Cache key format (V8 compiled code):
# - "_key" prefix + script URL + " \n" + top-level site URL
# - Or a pure SHA-256 hash (64 uppercase hex chars) for inline scripts
#
# GPU Cache key format:
# - base64-encoded hash pair: "hash1:hash2"
#
# Examples:
# - Simple: "1/0/https://example.com/image.png"
# - Double-keyed: "1/0/_dk_https://toplevel.com https://frame.com https://cdn.example.com/image.png"
# - Code Cache: "_keyhttps://cdn.example.com/script.js \nhttps://example.com/"
# - Code Cache hash: "AB2A39155883DE91B9EFDA1DBD40620716B544996393D10FCD674C426DA1A250"
# - GPU Cache: "HYN18Fl2bqKa6GEqZSRCWOIq1vc=:L4uLPxDuFfI2ovodjJe3qn+JmNw="
#
# The rightmost space after "_dk_" marks the start of the actual URL.

DOUBLE_KEY_PREFIX = "_dk_"
DOUBLE_KEY_SEPARATOR = " "
CODE_CACHE_KEY_PREFIX = "_key"


def extract_url_from_cache_key(cache_key: str) -> str:
    """
    Extract the actual resource URL from a Chromium cache key.

    Chromium cache keys may contain network isolation prefixes for privacy.
    This function extracts the actual URL being cached.

    Key format: credential_key/upload_data_identifier/[isolation_key]url

    The credential_key and upload_data_identifier are single digits (0 or 1),
    so the pattern is: "N/N/..." where N is a digit.

    Args:
        cache_key: The raw cache key (URL field from EntryStore)

    Returns:
        The extracted resource URL, or the original key if no URL found

    Examples:
        "1/0/_dk_https://msn.com https://msn.com https://cdn.msn.com/image.jpg"
        -> "https://cdn.msn.com/image.jpg"

        "1/0/https://example.com/page.html"
        -> "https://example.com/page.html"

        "https://example.com/simple.html"
        -> "https://example.com/simple.html"

        "_keyhttps://cdn.example.com/script.js \\nhttps://example.com/"
        -> "https://cdn.example.com/script.js"
    """
    if not cache_key:
        return cache_key

    # Chromium cache keys have the format: N/N/url where N is a digit
    # Check if the key starts with this pattern (e.g., "1/0/", "0/0/", "1/1/")
    remaining = cache_key
    if len(cache_key) >= 4 and cache_key[0].isdigit() and cache_key[1] == '/' and cache_key[2].isdigit() and cache_key[3] == '/':
        # Strip the credential_key/upload_data_identifier/ prefix
        remaining = cache_key[4:]

    # Check if the remaining part starts with _dk_ (double-keyed)
    if remaining.startswith(DOUBLE_KEY_PREFIX):
        # Find the rightmost space separator - everything after is the URL
        # We use rfind because there may be multiple spaces in the isolation key
        # (e.g., "https://toplevel.com https://frame.com https://actual-url.com")
        last_space = remaining.rfind(DOUBLE_KEY_SEPARATOR)
        if last_space != -1:
            return remaining[last_space + 1:]

    # Check for _key prefix (Code Cache / V8 compiled code cache)
    # Format: _key<script_url> \n<top_level_site>
    # or:     _key<script_url>\n<top_level_site>
    if remaining.startswith(CODE_CACHE_KEY_PREFIX):
        key_content = remaining[len(CODE_CACHE_KEY_PREFIX):]
        # URLs never contain literal newlines — use \n as separator
        newline_idx = key_content.find('\n')
        if newline_idx > 0:
            return key_content[:newline_idx].rstrip()
        # No separator; return everything after _key
        return key_content

    # Not double-keyed, return the URL part (after credential/upload prefix)
    return remaining


# Typical URL schemes seen in browser caches
_CACHE_URL_SCHEMES = frozenset({
    "http", "https", "ftp", "ftps", "ws", "wss",
    "chrome", "chrome-extension", "edge", "blob", "data",
})


def is_cache_url(value: str) -> bool:
    """
    Check whether *value* looks like a URL rather than an opaque cache key.

    Opaque keys include SHA-256 hashes (Code Cache compiled entries) and
    base64 hash pairs (GPUCache shader keys).  These are legitimate cache
    keys but do **not** represent network resources and should not be
    inserted into the ``urls`` table.

    Args:
        value: Result from :func:`extract_url_from_cache_key`.

    Returns:
        ``True`` if *value* appears to be a URL with a recognised scheme.
    """
    if not value:
        return False
    colon = value.find(':')
    if colon <= 0:
        return False
    scheme = value[:colon].lower()
    return scheme in _CACHE_URL_SCHEMES


# =============================================================================
# Data Structures
# =============================================================================

@dataclass
class CacheAddr:
    """
    Decoded Chromium cache address (32-bit).

    Address encoding:
    - Bit 31: Initialized flag (must be 1)
    - Bits 30-28: File type (0=EXTERNAL, 1-7=block types)
    - If EXTERNAL: Bits 27-0 = file number for f_XXXXXX
    - If block type: Bits 25-24 = num_blocks-1, 23-16 = file_selector, 15-0 = start_block
    """
    raw: int

    @property
    def is_initialized(self) -> bool:
        """Check if address is valid (bit 31 set)."""
        return bool(self.raw & 0x80000000)

    @property
    def file_type(self) -> int:
        """Get file type (0=EXTERNAL, 1-7=block types)."""
        return (self.raw >> 28) & 0x7

    @property
    def is_external(self) -> bool:
        """Check if address points to external file (f_XXXXXX)."""
        return self.file_type == 0

    @property
    def file_number(self) -> int:
        """
        Get file number.

        For EXTERNAL: the hex number in f_XXXXXX
        For blocks: the file selector (data_N)
        """
        if self.is_external:
            return self.raw & 0x0FFFFFFF
        return (self.raw >> 16) & 0xFF

    @property
    def start_block(self) -> int:
        """Get starting block number (0-65535) within block file."""
        return self.raw & 0xFFFF

    @property
    def num_blocks(self) -> int:
        """Get number of contiguous blocks (1-4)."""
        return ((self.raw >> 24) & 0x3) + 1

    @property
    def block_size(self) -> int:
        """Get block size in bytes for this file type."""
        return BLOCK_SIZES.get(self.file_type, 0)

    def offset(self) -> int:
        """Calculate byte offset within block file (after 8KB header)."""
        return BLOCK_HEADER_SIZE + self.start_block * self.block_size

    def total_size(self) -> int:
        """Calculate total size in bytes for this address."""
        return self.num_blocks * self.block_size

    def data_file_name(self) -> str:
        """Get the data file name for this address."""
        if self.is_external:
            return f"f_{self.file_number:06x}"
        return f"data_{self.file_number}"

    def __repr__(self) -> str:
        if not self.is_initialized:
            return f"CacheAddr(uninitialized=0x{self.raw:08x})"
        if self.is_external:
            return f"CacheAddr(external=f_{self.file_number:06x})"
        return f"CacheAddr(type={self.file_type}, file={self.file_number}, block={self.start_block}, count={self.num_blocks})"


@dataclass
class EntryStore:
    """
    Parsed EntryStore structure (256 bytes).

    Contains cache entry metadata, key (URL), and pointers to data streams.
    """
    hash: int                    # Full hash of the key (URL)
    next: CacheAddr              # Next entry in hash chain
    rankings_node: CacheAddr     # Pointer to RankingsNode in data_0
    reuse_count: int             # How often entry is used
    refetch_count: int           # Network refetch count
    state: int                   # ENTRY_NORMAL=0, ENTRY_EVICTED=1, ENTRY_DOOMED=2
    creation_time: int           # base::Time internal value (microseconds since 1601)
    key_len: int                 # Length of URL/key
    long_key: CacheAddr          # Address of long key (if key_len > 160)
    data_size: List[int]         # Size of each data stream [0..3]
    data_addr: List[CacheAddr]   # Address of each data stream [0..3]
    flags: int                   # PARENT_ENTRY=1, CHILD_ENTRY=2
    self_hash: int               # CRC of this EntryStore
    key_inline: bytes            # Inline key storage (up to 160 bytes)

    def get_key(self) -> Optional[str]:
        """
        Get the cache key (URL) from inline storage.

        Note: Long keys (>160 bytes) require separate lookup via long_key address.
        """
        if self.key_len <= 0:
            return None

        if self.key_len <= MAX_INTERNAL_KEY_LENGTH:
            # Key is stored inline
            try:
                key_bytes = self.key_inline[:self.key_len]
                return key_bytes.decode('utf-8', errors='replace')
            except Exception:
                return None

        # Long key - would need separate lookup
        return None

    def get_creation_datetime(self) -> Optional[datetime]:
        """Convert creation_time to datetime."""
        if self.creation_time <= 0:
            return None
        try:
            # Convert from Windows epoch (microseconds since 1601)
            unix_microseconds = self.creation_time - WINDOWS_EPOCH_OFFSET_MICROSECONDS
            if unix_microseconds > 0:
                return datetime.fromtimestamp(
                    unix_microseconds / 1000000,
                    tz=timezone.utc
                )
        except (ValueError, OSError):
            pass
        return None

    @property
    def is_valid(self) -> bool:
        """Check if entry appears valid."""
        return (
            self.state in (ENTRY_NORMAL, ENTRY_EVICTED, ENTRY_DOOMED) and
            self.key_len > 0 and
            self.key_len < 10 * 1024 * 1024  # Sanity: < 10MB
        )


@dataclass
class RankingsNode:
    """
    Parsed RankingsNode structure (36 bytes).

    Contains LRU tracking data for cache eviction.
    """
    last_used: int       # LRU timestamp (base::Time internal)
    last_modified: int   # Formerly used LRU info
    next: CacheAddr      # Next in LRU list
    prev: CacheAddr      # Previous in LRU list
    contents: CacheAddr  # Back-pointer to EntryStore
    dirty: int           # Modification flag
    self_hash: int       # CRC of this node

    def get_last_used_datetime(self) -> Optional[datetime]:
        """Convert last_used to datetime."""
        if self.last_used <= 0:
            return None
        try:
            unix_microseconds = self.last_used - WINDOWS_EPOCH_OFFSET_MICROSECONDS
            if unix_microseconds > 0:
                return datetime.fromtimestamp(
                    unix_microseconds / 1000000,
                    tz=timezone.utc
                )
        except (ValueError, OSError):
            pass
        return None


@dataclass
class IndexHeader:
    """Parsed blockfile index header."""
    magic: int
    version: int
    num_entries: int
    last_file: int          # Last external file number created
    this_id: int            # Dirty flag identifier
    table_len: int          # Actual table size
    create_time: int        # Creation timestamp
    num_bytes: int          # Total cache size in bytes


@dataclass
class BlockFileHeader:
    """Parsed block file header (data_N)."""
    magic: int
    version: int
    this_file: int          # File index (0-3 for data_0-3)
    next_file: int          # Next file when full
    entry_size: int         # Block size for this file
    num_entries: int        # Current stored entries
    max_entries: int        # Max possible entries


@dataclass
class BlockfileCacheEntry:
    """
    Complete parsed cache entry with all metadata.

    Combines EntryStore with resolved key and data stream info.

    Note: The `url` field contains the extracted resource URL (after stripping
    the cache key prefix). The `raw_cache_key` field contains the original
    cache key for forensic provenance (includes network isolation prefixes).
    """
    url: str                         # The extracted resource URL
    creation_time: Optional[datetime]
    last_used_time: Optional[datetime]
    state: int
    data_sizes: List[int]            # Size of each stream [0..3]
    data_addrs: List[CacheAddr]      # Address of each stream [0..3]
    entry_hash: int                  # Hash of the key
    source_file: str                 # Which data_N file contained EntryStore
    block_offset: int                # Offset within data file
    raw_cache_key: Optional[str] = None  # Original cache key (includes prefixes)


# =============================================================================
# Parser Functions
# =============================================================================

def parse_cache_addr(raw: int) -> CacheAddr:
    """Create CacheAddr from raw 32-bit value."""
    return CacheAddr(raw=raw)


def parse_entry_store(data: bytes) -> Optional[EntryStore]:
    """
    Parse a 256-byte EntryStore from data.

    EntryStore layout:
    - Offset 0:   hash (4), next (4), rankings_node (4) = 12 bytes
    - Offset 12:  reuse_count (4), refetch_count (4), state (4) = 12 bytes
    - Offset 24:  creation_time (8) = 8 bytes
    - Offset 32:  key_len (4), long_key (4) = 8 bytes
    - Offset 40:  data_size[4] (16) = 16 bytes
    - Offset 56:  data_addr[4] (16) = 16 bytes
    - Offset 72:  flags (4), pad[4] (16), self_hash (4) = 24 bytes
    - Offset 96:  key[160] = 160 bytes
    Total: 256 bytes
    """
    if len(data) < ENTRY_STORE_SIZE:
        return None

    try:
        # Unpack fixed fields
        (entry_hash, next_addr, rankings_addr,
         reuse_count, refetch_count, state,
         creation_time, key_len, long_key_addr) = struct.unpack_from(
            '<III iii Qii', data, 0
        )

        # data_size[4] and data_addr[4]
        data_sizes = list(struct.unpack_from('<4i', data, 40))
        data_addrs = [CacheAddr(a) for a in struct.unpack_from('<4I', data, 56)]

        # flags, pad, self_hash
        flags = struct.unpack_from('<I', data, 72)[0]
        self_hash = struct.unpack_from('<I', data, 92)[0]

        # Inline key (null-terminated, up to 160 bytes)
        key_data = data[ENTRY_STORE_KEY_OFFSET:ENTRY_STORE_KEY_OFFSET + ENTRY_STORE_KEY_SIZE]
        null_idx = key_data.find(b'\x00')
        if null_idx >= 0:
            key_data = key_data[:null_idx]

        return EntryStore(
            hash=entry_hash,
            next=CacheAddr(next_addr),
            rankings_node=CacheAddr(rankings_addr),
            reuse_count=reuse_count,
            refetch_count=refetch_count,
            state=state,
            creation_time=creation_time,
            key_len=key_len,
            long_key=CacheAddr(long_key_addr),
            data_size=data_sizes,
            data_addr=data_addrs,
            flags=flags,
            self_hash=self_hash,
            key_inline=key_data,
        )
    except Exception as e:
        LOGGER.debug("Failed to parse EntryStore: %s", e)
        return None


def parse_rankings_node(data: bytes) -> Optional[RankingsNode]:
    """
    Parse a 36-byte RankingsNode from data.

    RankingsNode layout:
    - last_used (8), last_modified (8)
    - next (4), prev (4), contents (4)
    - dirty (4), self_hash (4)
    """
    if len(data) < 36:
        return None

    try:
        (last_used, last_modified,
         next_addr, prev_addr, contents_addr,
         dirty, self_hash) = struct.unpack_from(
            '<QQ III II', data, 0
        )

        return RankingsNode(
            last_used=last_used,
            last_modified=last_modified,
            next=CacheAddr(next_addr),
            prev=CacheAddr(prev_addr),
            contents=CacheAddr(contents_addr),
            dirty=dirty,
            self_hash=self_hash,
        )
    except Exception as e:
        LOGGER.debug("Failed to parse RankingsNode: %s", e)
        return None


def parse_index_header(data: bytes) -> Optional[IndexHeader]:
    """
    Parse blockfile index header.

    IndexHeader layout (simplified):
    - magic (4), version (4)
    - num_entries (4), old_v2_num_bytes (4)
    - last_file (4), this_id (4), stats (4), table_len (4)
    - crash (4), experiment (4), create_time (8)
    - num_bytes (8) [v3.0 only]
    """
    if len(data) < 56:  # Minimum for core fields
        return None

    try:
        (magic, version, num_entries, old_num_bytes,
         last_file, this_id, stats_addr, table_len,
         crash, experiment, create_time) = struct.unpack_from(
            '<II ii ii Ii ii Q', data, 0
        )

        if magic != BLOCKFILE_INDEX_MAGIC:
            LOGGER.debug("Invalid index magic: 0x%x (expected 0x%x)", magic, BLOCKFILE_INDEX_MAGIC)
            return None

        # Handle v3.0 num_bytes field
        num_bytes = old_num_bytes
        if version >= 0x30000 and len(data) >= 64:
            num_bytes = struct.unpack_from('<q', data, 56)[0]

        # Validate table_len against file size.
        # The hash table starts at INDEX_HEADER_SIZE (256 bytes) and each
        # bucket is 4 bytes.  Some older Chromium builds (e.g. CefSharp /
        # embedded Chromium ≤50) write a bogus table_len (like 0 or 1) while
        # the file is actually sized for DEFAULT_TABLE_SIZE buckets.
        # Chromium itself falls back to kIndexTablesize (65536) when
        # table_len is 0; we extend this to any value < 256 (no legitimate
        # Chromium cache has fewer than 256 buckets).
        # Corrupted headers may also report absurdly *large* values (e.g.
        # 0xA1010000); cap at the maximum the file can physically hold.
        max_from_file = (len(data) - INDEX_HEADER_SIZE) // 4 if len(data) > INDEX_HEADER_SIZE else 0
        effective_table_len = table_len
        if effective_table_len <= 0 or effective_table_len < 256 or effective_table_len > max_from_file:
            if max_from_file >= DEFAULT_TABLE_SIZE:
                effective_table_len = DEFAULT_TABLE_SIZE
            elif max_from_file > 0:
                # Round down to nearest power of 2
                effective_table_len = 1 << (max_from_file.bit_length() - 1)
            else:
                effective_table_len = DEFAULT_TABLE_SIZE
            LOGGER.debug(
                "table_len=%d seems invalid, using %d (file can hold %d buckets)",
                table_len, effective_table_len, max_from_file,
            )

        return IndexHeader(
            magic=magic,
            version=version,
            num_entries=num_entries,
            last_file=last_file,
            this_id=this_id,
            table_len=effective_table_len,
            create_time=create_time,
            num_bytes=num_bytes,
        )
    except Exception as e:
        LOGGER.debug("Failed to parse index header: %s", e)
        return None


def parse_block_file_header(data: bytes) -> Optional[BlockFileHeader]:
    """
    Parse block file header (data_N).

    BlockFileHeader layout:
    - magic (4), version (4)
    - this_file (2), next_file (2)
    - entry_size (4), num_entries (4), max_entries (4)
    """
    if len(data) < 24:
        return None

    try:
        (magic, version,
         this_file, next_file,
         entry_size, num_entries, max_entries) = struct.unpack_from(
            '<II hh iii', data, 0
        )

        if magic != BLOCKFILE_BLOCK_MAGIC:
            LOGGER.debug("Invalid block file magic: 0x%x (expected 0x%x)", magic, BLOCKFILE_BLOCK_MAGIC)
            return None

        return BlockFileHeader(
            magic=magic,
            version=version,
            this_file=this_file,
            next_file=next_file,
            entry_size=entry_size,
            num_entries=num_entries,
            max_entries=max_entries,
        )
    except Exception as e:
        LOGGER.debug("Failed to parse block file header: %s", e)
        return None


def read_block_data_from_dict(
    data_files: Dict[str, bytes],
    addr: CacheAddr,
    expected_size: Optional[int] = None
) -> Optional[bytes]:
    """
    Read data from block file or external file at given address (dict-based).

    Args:
        data_files: Dict mapping filename (e.g., "data_1", "f_000001") to file contents
        addr: Cache address to read from
        expected_size: Expected data size (for external files)

    Returns:
        Raw bytes or None if read failed
    """
    if not addr.is_initialized:
        return None

    filename = addr.data_file_name()
    file_data = data_files.get(filename)

    if file_data is None:
        LOGGER.debug("Data file not found: %s", filename)
        return None

    if addr.is_external:
        # External file: raw data, no header
        if expected_size is not None and expected_size <= len(file_data):
            return file_data[:expected_size]
        return file_data

    # Block file: calculate offset after header
    offset = addr.offset()
    size = addr.total_size()

    if offset + size > len(file_data):
        LOGGER.debug("Block read out of bounds: offset=%d, size=%d, file_size=%d",
                    offset, size, len(file_data))
        return None

    return file_data[offset:offset + size]


def read_block_data(
    cache_dir: Path,
    addr: CacheAddr,
    expected_size: Optional[int] = None,
    _file_cache: Optional[Dict[str, bytes]] = None,
) -> Optional[bytes]:
    """
    Read data from block file or external file at given address (lazy loading).

    Uses on-demand file reads instead of loading all files into memory.
    Only block files (data_0-3) are cached; external f_* files are read on demand.

    Args:
        cache_dir: Path to cache directory
        addr: Cache address to read from
        expected_size: Expected data size (for external files)
        _file_cache: Optional internal cache for block files (data_0-3 only)

    Returns:
        Raw bytes or None if read failed
    """
    if not addr.is_initialized:
        return None

    filename = addr.data_file_name()
    file_path = cache_dir / filename

    # For external files (f_*), always read on demand - don't cache
    if addr.is_external:
        if not file_path.exists():
            LOGGER.debug("External file not found: %s", file_path)
            return None
        try:
            file_data = file_path.read_bytes()
            if expected_size is not None and expected_size <= len(file_data):
                return file_data[:expected_size]
            return file_data
        except Exception as e:
            LOGGER.debug("Failed to read external file %s: %s", file_path, e)
            return None

    # For block files (data_0-3), use cache if provided
    file_data = None
    if _file_cache is not None:
        file_data = _file_cache.get(filename)

    if file_data is None:
        if not file_path.exists():
            LOGGER.debug("Block file not found: %s", file_path)
            return None
        try:
            file_data = file_path.read_bytes()
            # Cache block files for reuse
            if _file_cache is not None:
                _file_cache[filename] = file_data
        except Exception as e:
            LOGGER.debug("Failed to read block file %s: %s", file_path, e)
            return None

    # Block file: calculate offset after header
    offset = addr.offset()
    size = addr.total_size()

    if offset + size > len(file_data):
        LOGGER.debug("Block read out of bounds: offset=%d, size=%d, file_size=%d",
                    offset, size, len(file_data))
        return None

    return file_data[offset:offset + size]


def read_long_key(
    cache_dir: Path,
    entry_store: 'EntryStore',
    _file_cache: Optional[Dict[str, bytes]] = None,
) -> Optional[str]:
    """
    Read a long key (URL > 160 bytes) from external storage.

    Long keys are stored at the address indicated by entry_store.long_key.
    The key_len field indicates the actual length of the key.

    Args:
        cache_dir: Path to cache directory
        entry_store: Parsed EntryStore containing long_key address
        _file_cache: Optional internal cache for block files

    Returns:
        Decoded key string or None if read failed
    """
    if entry_store.key_len <= MAX_INTERNAL_KEY_LENGTH:
        # Key is inline, not long
        return entry_store.get_key()

    if not entry_store.long_key.is_initialized:
        LOGGER.debug("Long key address not initialized")
        return None

    try:
        key_data = read_block_data(
            cache_dir,
            entry_store.long_key,
            expected_size=entry_store.key_len,
            _file_cache=_file_cache,
        )
        if not key_data:
            LOGGER.debug("Failed to read long key data")
            return None

        # Truncate to actual key length
        key_bytes = key_data[:entry_store.key_len]
        return key_bytes.decode('utf-8', errors='replace')
    except Exception as e:
        LOGGER.debug("Failed to decode long key: %s", e)
        return None


def iter_index_entries(
    index_data: bytes,
    data_files: Dict[str, bytes],
    rankings_files: Optional[Dict[str, bytes]] = None
) -> Iterator[BlockfileCacheEntry]:
    """
    Iterate all cache entries from blockfile index (dict-based, legacy).

    Note: This function loads all data files into memory upfront.
    For large caches, prefer iter_index_entries_lazy() instead.

    Args:
        index_data: Contents of the index file
        data_files: Dict mapping data file names to their contents
        rankings_files: Optional dict for data_0 (RankingsNode) lookups

    Yields:
        BlockfileCacheEntry for each valid cache entry
    """
    header = parse_index_header(index_data)
    if not header:
        return

    LOGGER.debug("Parsing index: version=0x%x, entries=%d, table_len=%d",
                header.version, header.num_entries, header.table_len)

    # Hash table starts after header
    table_offset = INDEX_HEADER_SIZE
    table_size = header.table_len

    # Track visited entries to avoid infinite loops (hash chain cycles)
    visited = set()
    entries_found = 0

    for bucket in range(table_size):
        addr_offset = table_offset + bucket * 4
        if addr_offset + 4 > len(index_data):
            break

        addr_raw = struct.unpack_from('<I', index_data, addr_offset)[0]
        addr = CacheAddr(addr_raw)

        # Follow chain of entries in this bucket
        chain_depth = 0
        max_chain_depth = 1000  # Prevent infinite loops

        while addr.is_initialized and addr.raw not in visited and chain_depth < max_chain_depth:
            visited.add(addr.raw)
            chain_depth += 1

            # EntryStore is in data_1 (BLOCK_256, file_type=2)
            if addr.file_type != 2:
                LOGGER.debug("Unexpected file type %d for EntryStore at addr 0x%08x",
                            addr.file_type, addr.raw)
                break

            # Read EntryStore data
            entry_data = read_block_data_from_dict(data_files, addr)
            if not entry_data:
                break

            entry_store = parse_entry_store(entry_data)
            if not entry_store or not entry_store.is_valid:
                # Try next in chain
                if entry_store:
                    addr = entry_store.next
                else:
                    break
                continue

            # Get the key (URL) - inline only for dict-based version
            url = entry_store.get_key()
            if not url:
                # Skip entries with no key (long keys need lazy version)
                addr = entry_store.next
                continue

            # Get creation time
            creation_time = entry_store.get_creation_datetime()

            # Try to get last_used from RankingsNode
            last_used_time = None
            if rankings_files and entry_store.rankings_node.is_initialized:
                rankings_data = read_block_data_from_dict(
                    rankings_files or data_files,
                    entry_store.rankings_node
                )
                if rankings_data:
                    rankings = parse_rankings_node(rankings_data)
                    if rankings:
                        last_used_time = rankings.get_last_used_datetime()

            # Extract the actual resource URL from the cache key
            # (strips network isolation prefixes like "1/0/_dk_...")
            extracted_url = extract_url_from_cache_key(url)

            # Create entry
            cache_entry = BlockfileCacheEntry(
                url=extracted_url,
                creation_time=creation_time,
                last_used_time=last_used_time,
                state=entry_store.state,
                data_sizes=entry_store.data_size,
                data_addrs=entry_store.data_addr,
                entry_hash=entry_store.hash,
                source_file=addr.data_file_name(),
                block_offset=addr.offset(),
                raw_cache_key=url if url != extracted_url else None,
            )

            entries_found += 1
            yield cache_entry

            # Move to next in chain
            addr = entry_store.next

    LOGGER.debug("Parsed %d cache entries from index", entries_found)


def iter_index_entries_lazy(
    cache_dir: Path,
    *,
    warning_collector: Optional['ExtractionWarningCollector'] = None,
) -> Iterator[BlockfileCacheEntry]:
    """
    Iterate all cache entries from blockfile index with lazy file loading.

    This version:
    - Loads block files (data_0-3) on demand and caches them
    - Reads external f_* files on demand without caching (memory efficient)
    - Supports long keys (>160 bytes) via long_key address lookup
    - Reports unknown entry states and file types via warning_collector

    Args:
        cache_dir: Path to cache directory containing index/data_* files
        warning_collector: Optional collector for extraction warnings

    Yields:
        BlockfileCacheEntry for each valid cache entry
    """
    index_path = cache_dir / "index"
    if not index_path.exists():
        LOGGER.warning("Index file not found: %s", index_path)
        return

    try:
        index_data = index_path.read_bytes()
    except Exception as e:
        LOGGER.error("Failed to read index file: %s", e)
        return

    header = parse_index_header(index_data)
    if not header:
        return

    LOGGER.debug("Parsing index (lazy): version=0x%x, entries=%d, table_len=%d",
                header.version, header.num_entries, header.table_len)

    # Cache for block files only (data_0-3), not external files
    file_cache: Dict[str, bytes] = {}

    # Track unknown values to avoid duplicate warnings (report each unique value once)
    seen_unknown_states: set = set()
    seen_unknown_file_types: set = set()

    # Hash table starts after header
    table_offset = INDEX_HEADER_SIZE
    table_size = header.table_len

    # Track visited entries to avoid infinite loops (hash chain cycles)
    visited: set = set()
    entries_found = 0
    long_keys_found = 0

    for bucket in range(table_size):
        addr_offset = table_offset + bucket * 4
        if addr_offset + 4 > len(index_data):
            break

        addr_raw = struct.unpack_from('<I', index_data, addr_offset)[0]
        addr = CacheAddr(addr_raw)

        # Follow chain of entries in this bucket
        chain_depth = 0
        max_chain_depth = 1000  # Prevent infinite loops

        while addr.is_initialized and addr.raw not in visited and chain_depth < max_chain_depth:
            visited.add(addr.raw)
            chain_depth += 1

            # EntryStore is in data_1 (BLOCK_256, file_type=2)
            if addr.file_type != 2:
                LOGGER.debug("Unexpected file type %d for EntryStore at addr 0x%08x",
                            addr.file_type, addr.raw)
                # Report unknown file type (once per unique type)
                if warning_collector and addr.file_type not in seen_unknown_file_types:
                    if not is_known_block_file_type(addr.file_type):
                        seen_unknown_file_types.add(addr.file_type)
                        warning_collector.add_warning(
                            warning_type="unknown_enum_value",
                            item_name="block_file_type",
                            item_value=str(addr.file_type),
                            severity="info",
                            category="binary",
                            artifact_type="cache_blockfile",
                            source_file=str(cache_dir / "index"),
                            context_json={"address": f"0x{addr.raw:08x}"},
                        )
                break

            # Read EntryStore data
            entry_data = read_block_data(cache_dir, addr, _file_cache=file_cache)
            if not entry_data:
                break

            entry_store = parse_entry_store(entry_data)
            if not entry_store or not entry_store.is_valid:
                # Try next in chain
                if entry_store:
                    addr = entry_store.next
                else:
                    break
                continue

            # Check for unknown entry states (once per unique state)
            if entry_store.state not in KNOWN_ENTRY_STATES:
                if warning_collector and entry_store.state not in seen_unknown_states:
                    seen_unknown_states.add(entry_store.state)
                    warning_collector.add_warning(
                        warning_type="unknown_enum_value",
                        item_name="entry_state",
                        item_value=str(entry_store.state),
                        severity="info",
                        category="binary",
                        artifact_type="cache_blockfile",
                        source_file=str(cache_dir / addr.data_file_name()),
                        context_json={"known_states": list(KNOWN_ENTRY_STATES)},
                    )

            # Get the key (URL) - supports long keys
            if entry_store.key_len <= MAX_INTERNAL_KEY_LENGTH:
                url = entry_store.get_key()
            else:
                # Long key - read from external storage
                url = read_long_key(cache_dir, entry_store, _file_cache=file_cache)
                if url:
                    long_keys_found += 1

            if not url:
                # Skip entries with no key
                addr = entry_store.next
                continue

            # Get creation time
            creation_time = entry_store.get_creation_datetime()

            # Try to get last_used from RankingsNode
            last_used_time = None
            if entry_store.rankings_node.is_initialized:
                rankings_data = read_block_data(
                    cache_dir,
                    entry_store.rankings_node,
                    _file_cache=file_cache,
                )
                if rankings_data:
                    rankings = parse_rankings_node(rankings_data)
                    if rankings:
                        last_used_time = rankings.get_last_used_datetime()

            # Extract the actual resource URL from the cache key
            # (strips network isolation prefixes like "1/0/_dk_...")
            extracted_url = extract_url_from_cache_key(url)

            # Create entry
            cache_entry = BlockfileCacheEntry(
                url=extracted_url,
                creation_time=creation_time,
                last_used_time=last_used_time,
                state=entry_store.state,
                data_sizes=entry_store.data_size,
                data_addrs=entry_store.data_addr,
                entry_hash=entry_store.hash,
                source_file=addr.data_file_name(),
                block_offset=addr.offset(),
                raw_cache_key=url if url != extracted_url else None,
            )

            entries_found += 1
            yield cache_entry

            # Move to next in chain
            addr = entry_store.next

    LOGGER.debug("Parsed %d cache entries (%d with long keys)", entries_found, long_keys_found)


def scan_data1_orphan_entries(
    cache_dir: Path,
    *,
    warning_collector: Optional['ExtractionWarningCollector'] = None,
) -> List[BlockfileCacheEntry]:
    """
    Scan data_1 block file for orphaned EntryStore structures.

    When the index hash table is empty or corrupted but data_1 still contains
    valid 256-byte EntryStore blocks, this function recovers them by scanning
    every block in data_1 and validating the structure heuristically.

    This is typical of old CefSharp / embedded Chromium caches where the index
    metadata was cleared (e.g. on exit) but the block/external data files were
    left intact.

    For each recovered entry the function resolves:
    - The cache key (URL) from inline storage or long-key lookup
    - Data stream addresses so callers can read HTTP headers and body data
    - RankingsNode timestamps (last_used) when available

    Args:
        cache_dir: Path to blockfile cache directory.
        warning_collector: Optional collector for extraction warnings.

    Returns:
        List of recovered :class:`BlockfileCacheEntry` objects.
    """
    data1_path = cache_dir / "data_1"
    if not data1_path.exists():
        return []

    try:
        data1 = data1_path.read_bytes()
    except Exception as e:
        LOGGER.warning("Failed to read data_1 for block scan: %s", e)
        return []

    # Verify block file header
    bf_header = parse_block_file_header(data1)
    if not bf_header:
        LOGGER.debug("data_1 has invalid block file header, skipping scan")
        return []

    if bf_header.entry_size != ENTRY_STORE_SIZE:
        LOGGER.debug(
            "data_1 entry_size=%d (expected %d), skipping scan",
            bf_header.entry_size, ENTRY_STORE_SIZE,
        )
        return []

    total_blocks = (len(data1) - BLOCK_HEADER_SIZE) // ENTRY_STORE_SIZE
    if total_blocks <= 0:
        return []

    LOGGER.info(
        "Scanning %d blocks in data_1 for orphaned entries in %s",
        total_blocks, cache_dir,
    )

    file_cache: Dict[str, bytes] = {}
    entries: List[BlockfileCacheEntry] = []

    for block_idx in range(total_blocks):
        block_offset = BLOCK_HEADER_SIZE + block_idx * ENTRY_STORE_SIZE
        block_data = data1[block_offset:block_offset + ENTRY_STORE_SIZE]

        # Skip empty blocks quickly
        if all(b == 0 for b in block_data):
            continue

        entry_store = parse_entry_store(block_data)
        if entry_store is None:
            continue

        # Heuristic validation for orphaned entries:
        # - key_len must be positive and reasonable (<= inline key size for
        #   most entries, or < 10 KB for long-key entries)
        # - state is often 0 (ENTRY_NORMAL) for orphaned entries whose state
        #   field wasn't fully written, but we also accept known states.
        #   However, very large or negative states indicate garbage data.
        if entry_store.key_len <= 0 or entry_store.key_len > 10 * 1024 * 1024:
            continue

        # Validate data_size values are non-negative and reasonable
        if any(s < 0 or s > 500_000_000 for s in entry_store.data_size):
            continue

        # Resolve key (URL)
        if entry_store.key_len <= MAX_INTERNAL_KEY_LENGTH:
            url = entry_store.get_key()
        else:
            url = read_long_key(cache_dir, entry_store, _file_cache=file_cache)

        if not url:
            continue

        # Require a URL-like key to avoid false positives from garbage blocks
        extracted_url = extract_url_from_cache_key(url)
        if not is_cache_url(extracted_url):
            continue

        # Get creation time
        creation_time = entry_store.get_creation_datetime()

        # Try to get last_used from RankingsNode
        last_used_time = None
        if entry_store.rankings_node.is_initialized:
            rankings_data = read_block_data(
                cache_dir,
                entry_store.rankings_node,
                _file_cache=file_cache,
            )
            if rankings_data:
                rankings = parse_rankings_node(rankings_data)
                if rankings:
                    last_used_time = rankings.get_last_used_datetime()

        cache_entry = BlockfileCacheEntry(
            url=extracted_url,
            creation_time=creation_time,
            last_used_time=last_used_time,
            state=entry_store.state,
            data_sizes=entry_store.data_size,
            data_addrs=entry_store.data_addr,
            entry_hash=entry_store.hash,
            source_file="data_1",
            block_offset=block_offset,
            raw_cache_key=url if url != extracted_url else None,
        )
        entries.append(cache_entry)

    LOGGER.info(
        "Block scan recovered %d orphaned entries from %d blocks in data_1",
        len(entries), total_blocks,
    )

    return entries


def detect_blockfile_cache(cache_dir: Path) -> bool:
    """
    Detect if directory contains a blockfile cache.

    A blockfile cache has:
    - index file with magic 0xC103CAC3
    - data_0/1/2/3 block files

    Returns:
        True if blockfile cache detected
    """
    index_path = cache_dir / "index"
    if not index_path.exists():
        return False

    # Check for at least data_1 (EntryStore blocks)
    data_1_path = cache_dir / "data_1"
    if not data_1_path.exists():
        return False

    # Verify index magic
    try:
        with open(index_path, 'rb') as f:
            magic_bytes = f.read(4)
            if len(magic_bytes) < 4:
                return False
            magic = struct.unpack('<I', magic_bytes)[0]
            return magic == BLOCKFILE_INDEX_MAGIC
    except Exception:
        return False


def get_cache_format(cache_dir: Path) -> str:
    """
    Detect cache format type.

    Returns:
        'blockfile': Legacy blockfile format (data_*, index)
        'simple': Modern simple cache format ({hash}_0, index-dir)
        'unknown': Cannot determine format
    """
    # Check for blockfile indicators
    if detect_blockfile_cache(cache_dir):
        return 'blockfile'

    # Check for simple cache indicators
    index_dir = cache_dir / "index-dir"
    if index_dir.exists() and (index_dir / "the-real-index").exists():
        return 'simple'

    # Check for simple cache entry files ({16-hex}_0)
    import re
    for f in cache_dir.iterdir():
        if re.match(r'^[0-9a-f]{16}_0$', f.name):
            return 'simple'

    return 'unknown'


def load_cache_files(cache_dir: Path) -> Dict[str, bytes]:
    """
    Load all cache files from a blockfile cache directory.

    WARNING: This loads ALL files into memory including external f_* files.
    For large caches, prefer using iter_index_entries_lazy() directly.

    Loads: index, data_0, data_1, data_2, data_3, f_*

    Returns:
        Dict mapping filename to file contents
    """
    files = {}

    # Load index
    index_path = cache_dir / "index"
    if index_path.exists():
        files["index"] = index_path.read_bytes()

    # Load data files
    for i in range(4):
        data_path = cache_dir / f"data_{i}"
        if data_path.exists():
            files[f"data_{i}"] = data_path.read_bytes()

    # Load external files (f_XXXXXX)
    for f in cache_dir.iterdir():
        if f.name.startswith('f_') and f.is_file():
            files[f.name] = f.read_bytes()

    return files


def parse_blockfile_cache(
    cache_dir: Path,
    lazy: bool = True,
    *,
    warning_collector: Optional['ExtractionWarningCollector'] = None,
) -> List[BlockfileCacheEntry]:
    """
    Parse all entries from a blockfile cache directory.

    Args:
        cache_dir: Path to cache directory containing index/data_* files
        lazy: If True (default), use lazy loading for memory efficiency.
              If False, load all files into memory first (legacy behavior).
        warning_collector: Optional collector for extraction warnings

    Returns:
        List of BlockfileCacheEntry objects
    """
    if lazy:
        # Memory-efficient: lazy loading, supports long keys
        return list(iter_index_entries_lazy(cache_dir, warning_collector=warning_collector))

    # Legacy: load all files into memory (no long key support)
    files = load_cache_files(cache_dir)

    if "index" not in files:
        LOGGER.warning("No index file found in %s", cache_dir)
        return []

    # Separate data_0 for rankings lookups
    rankings_files = {"data_0": files.get("data_0", b"")}

    # Parse entries
    entries = list(iter_index_entries(
        index_data=files["index"],
        data_files=files,
        rankings_files=rankings_files,
    ))

    return entries


def read_stream_data(
    cache_dir: Path,
    entry: BlockfileCacheEntry,
    stream_index: int
) -> Optional[bytes]:
    """
    Read a specific data stream from cache entry.

    Args:
        cache_dir: Path to cache directory
        entry: Parsed cache entry
        stream_index: Stream to read (0=headers, 1=body, 2=metadata, 3=unused)

    Returns:
        Stream data or None if not available
    """
    if stream_index < 0 or stream_index >= 4:
        return None

    size = entry.data_sizes[stream_index]
    if size <= 0:
        return None

    addr = entry.data_addrs[stream_index]
    if not addr.is_initialized:
        return None

    # Load the file containing this stream
    filename = addr.data_file_name()
    file_path = cache_dir / filename

    if not file_path.exists():
        LOGGER.debug("Stream file not found: %s", file_path)
        return None

    try:
        file_data = file_path.read_bytes()

        if addr.is_external:
            # External file: raw data
            return file_data[:size] if size <= len(file_data) else file_data

        # Block file: read from offset
        offset = addr.offset()
        if offset + size > len(file_data):
            LOGGER.debug("Stream data out of bounds")
            return None

        return file_data[offset:offset + size]

    except Exception as e:
        LOGGER.debug("Failed to read stream %d: %s", stream_index, e)
        return None
