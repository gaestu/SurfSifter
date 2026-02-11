"""Firefox cache2 index file parser.

Parses the binary ``cache2/index`` file that Firefox maintains for its HTTP cache.
Supports both version 0x9 (Firefox ≤ 78 ESR, 12-byte header) and 0xA
(Firefox ≥ 91 ESR, 16-byte header).  All multi-byte integers are big-endian
(``NetworkEndian``) as defined in ``netwerk/cache2/CacheIndex.h``.

The index contains a ``CacheIndexRecord`` (41 bytes, ``#pragma pack(push, 1)``)
for every cache entry — including entries whose content has been evicted or
marked for deletion.  This metadata can prove site visits even when the
cached content is gone.

Reference:
    - CacheIndex.h  (mozilla-central): header/record struct, flag masks
    - CacheIndex.cpp (mozilla-central): kIndexVersion, serialisation logic
"""

from __future__ import annotations

import struct
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Index version constants (CacheIndex.cpp: ``#define kIndexVersion``)
# ---------------------------------------------------------------------------
INDEX_VERSION_9 = 0x00000009  # Firefox ≤ 78 ESR (12-byte header)
INDEX_VERSION_A = 0x0000000A  # Firefox ≥ 91 ESR (16-byte header, current)
KNOWN_VERSIONS = {INDEX_VERSION_9, INDEX_VERSION_A}

# Header sizes by version
HEADER_SIZE = {INDEX_VERSION_9: 12, INDEX_VERSION_A: 16}

# Record size: 20(hash) + 4 + 8 + 2 + 2 + 1 + 4 = 41 bytes
RECORD_SIZE = 41

# Trailing CRC hash size
CRC_SIZE = 4

# ---------------------------------------------------------------------------
# mFlags bitmasks (CacheIndex.h)
# ---------------------------------------------------------------------------
FLAG_INITIALIZED = 0x80000000
FLAG_ANONYMOUS = 0x40000000
FLAG_REMOVED = 0x20000000
FLAG_DIRTY = 0x10000000  # Cleared on disk writes
FLAG_FRESH = 0x08000000  # Cleared on disk writes
FLAG_PINNED = 0x04000000
FLAG_HAS_ALT_DATA = 0x02000000
FLAG_RESERVED = 0x01000000
FLAG_FILE_SIZE_MASK = 0x00FFFFFF  # File size in KB (lower 24 bits)

# All known flag bits (for detecting unknown flags)
ALL_KNOWN_FLAGS = (
    FLAG_INITIALIZED | FLAG_ANONYMOUS | FLAG_REMOVED | FLAG_DIRTY
    | FLAG_FRESH | FLAG_PINNED | FLAG_HAS_ALT_DATA | FLAG_RESERVED
    | FLAG_FILE_SIZE_MASK
)

# ---------------------------------------------------------------------------
# Content type enum (nsICacheEntry.idl)
# ---------------------------------------------------------------------------
MAX_KNOWN_CONTENT_TYPE = 6
CONTENT_TYPES: Dict[int, str] = {
    0: "unknown",
    1: "other",
    2: "javascript",
    3: "image",
    4: "media",
    5: "stylesheet",
    6: "wasm",
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class CacheIndexEntry:
    """Represents an entry in the Firefox cache2 index."""

    hash: str               # SHA1 hash (hex, uppercase)
    frecency: int           # Frecency score
    origin_attrs_hash: int  # uint64 origin attributes hash
    on_start_time: int      # Response start (uint16, ms resolution)
    on_stop_time: int       # Response end (uint16, ms resolution)
    content_type: int       # Content type enum (0-6)
    flags: int              # Full mFlags uint32

    @property
    def is_initialized(self) -> bool:
        return bool(self.flags & FLAG_INITIALIZED)

    @property
    def is_anonymous(self) -> bool:
        return bool(self.flags & FLAG_ANONYMOUS)

    @property
    def is_removed(self) -> bool:
        return bool(self.flags & FLAG_REMOVED)

    @property
    def is_pinned(self) -> bool:
        return bool(self.flags & FLAG_PINNED)

    @property
    def has_alt_data(self) -> bool:
        return bool(self.flags & FLAG_HAS_ALT_DATA)

    @property
    def file_size_kb(self) -> int:
        """File size in KB (from lower 24 bits of flags)."""
        return self.flags & FLAG_FILE_SIZE_MASK

    @property
    def content_type_name(self) -> str:
        return CONTENT_TYPES.get(self.content_type, f"unknown({self.content_type})")


@dataclass
class CacheIndex:
    """Represents a parsed Firefox cache2 index."""

    version: int
    timestamp: int              # Unix timestamp (seconds)
    is_dirty: bool
    kb_written: Optional[int]   # None for version 9
    entries: List[CacheIndexEntry]
    crc_valid: Optional[bool]   # None if not checked


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def parse_cache_index(
    index_path: Path,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> Tuple[Optional[CacheIndex], List[str]]:
    """Parse Firefox cache2 index file.

    Supports both version 0x9 (Firefox ≤ 78) and 0xA (Firefox ≥ 91).
    All multi-byte integers are big-endian (NetworkEndian).

    Args:
        index_path: Path to the cache2 index file.
        warning_collector: Optional structured warning collector.  When
            provided, format anomalies are persisted to the
            ``extraction_warnings`` table for forensic review in the
            Audit tab.

    Returns:
        Tuple of ``(CacheIndex | None, list[str])`` — the parsed index
        (or ``None`` on fatal errors) and a list of human-readable
        warning strings.
    """
    warnings: List[str] = []
    source = str(index_path)

    def _warn(
        warning_type: str,
        severity: str,
        message: str,
        context: Optional[dict] = None,
    ) -> None:
        """Emit both a text warning and a structured audit warning."""
        warnings.append(message)
        LOGGER.warning("%s: %s", source, message)
        if warning_collector:
            from extractors._shared.extraction_warnings import (
                CATEGORY_BINARY,
            )
            warning_collector.add_warning(
                warning_type=warning_type,
                item_name="firefox_cache2_index",
                severity=severity,
                category=CATEGORY_BINARY,
                artifact_type="cache_index",
                source_file=source,
                item_value=message,
                context_json=context,
            )

    # ------------------------------------------------------------------
    # Read raw bytes
    # ------------------------------------------------------------------
    try:
        with open(index_path, "rb") as f:
            data = f.read()
    except Exception as e:
        _warn("file_corrupt", "error", f"Failed to read index: {e}")
        return None, warnings

    if len(data) < 12:
        _warn(
            "file_corrupt",
            "error",
            f"Index file too small for header ({len(data)} bytes)",
        )
        return None, warnings

    # ------------------------------------------------------------------
    # Parse header
    # ------------------------------------------------------------------
    version = struct.unpack("!I", data[0:4])[0]

    if version not in KNOWN_VERSIONS:
        _warn(
            "version_unsupported",
            "error",
            f"Unknown index version: 0x{version:08X} "
            f"(expected 0x{INDEX_VERSION_9:08X} or 0x{INDEX_VERSION_A:08X})",
            {
                "found_version": f"0x{version:08X}",
                "known_versions": [
                    f"0x{v:08X}" for v in sorted(KNOWN_VERSIONS)
                ],
            },
        )
        return None, warnings

    header_size = HEADER_SIZE[version]

    if len(data) < header_size:
        _warn(
            "binary_format_error",
            "error",
            f"Index file too small for v0x{version:X} header "
            f"(need {header_size}, got {len(data)})",
        )
        return None, warnings

    # All big-endian uint32
    timestamp = struct.unpack("!I", data[4:8])[0]
    is_dirty = struct.unpack("!I", data[8:12])[0] != 0
    kb_written: Optional[int] = None

    if version == INDEX_VERSION_A:
        kb_written = struct.unpack("!I", data[12:16])[0]

    # ------------------------------------------------------------------
    # Validate record region
    # ------------------------------------------------------------------
    entries_data_size = len(data) - header_size - CRC_SIZE
    if entries_data_size < 0:
        _warn(
            "file_corrupt",
            "error",
            f"Index file too small (no room for CRC, {len(data)} bytes)",
        )
        return None, warnings

    if entries_data_size % RECORD_SIZE != 0:
        remainder = entries_data_size % RECORD_SIZE
        _warn(
            "binary_format_error",
            "warning",
            f"Record region size ({entries_data_size}) is not a multiple "
            f"of record size ({RECORD_SIZE}), {remainder} trailing bytes "
            f"will be ignored — index may be truncated",
            {
                "region_size": entries_data_size,
                "record_size": RECORD_SIZE,
                "remainder": remainder,
            },
        )

    # ------------------------------------------------------------------
    # Parse records
    # ------------------------------------------------------------------
    entries: List[CacheIndexEntry] = []
    offset = header_size
    entry_count = entries_data_size // RECORD_SIZE
    unknown_content_types_seen: set = set()
    unknown_flag_bits_seen: set = set()

    for i in range(entry_count):
        try:
            entry = _parse_index_record(data[offset : offset + RECORD_SIZE])
            if entry is not None:
                entries.append(entry)
                # Track unknown content types
                if entry.content_type > MAX_KNOWN_CONTENT_TYPE:
                    unknown_content_types_seen.add(entry.content_type)
                # Track unknown flag bits
                unknown_bits = entry.flags & ~ALL_KNOWN_FLAGS
                if unknown_bits:
                    unknown_flag_bits_seen.add(f"0x{unknown_bits:08X}")
        except Exception as e:
            _warn(
                "binary_format_error",
                "warning",
                f"Failed to parse record {i} at offset {offset}: {e}",
                {"record_index": i, "offset": offset},
            )
        offset += RECORD_SIZE

    # Emit aggregated warnings (once, not per-record)
    if unknown_content_types_seen:
        _warn(
            "binary_format_error",
            "info",
            f"Unknown mContentType values encountered: "
            f"{sorted(unknown_content_types_seen)} "
            f"(known range: 0-{MAX_KNOWN_CONTENT_TYPE})",
            {"unknown_content_types": sorted(unknown_content_types_seen)},
        )

    if unknown_flag_bits_seen:
        _warn(
            "binary_format_error",
            "info",
            f"Unknown bits set in mFlags: {sorted(unknown_flag_bits_seen)} "
            f"— Firefox may have added new flags since this parser was written",
            {"unknown_flag_bits": sorted(unknown_flag_bits_seen)},
        )

    # ------------------------------------------------------------------
    # Trailing CRC
    # ------------------------------------------------------------------
    crc_valid: Optional[bool] = None
    crc_offset = header_size + entry_count * RECORD_SIZE
    if crc_offset + CRC_SIZE <= len(data):
        expected_crc = struct.unpack("!I", data[crc_offset : crc_offset + CRC_SIZE])[0]
        # CacheHash::Hash32_t algorithm is not yet implemented — log only
        crc_valid = None
        LOGGER.debug(
            "Trailing CRC: 0x%08X (validation not yet implemented)",
            expected_crc,
        )

    # ------------------------------------------------------------------
    # Build result
    # ------------------------------------------------------------------
    index = CacheIndex(
        version=version,
        timestamp=timestamp,
        is_dirty=is_dirty,
        kb_written=kb_written,
        entries=entries,
        crc_valid=crc_valid,
    )

    LOGGER.info(
        "Parsed cache index v0x%X: %d entries, %d removed, dirty=%s",
        version,
        len(entries),
        sum(1 for e in entries if e.is_removed),
        is_dirty,
    )

    return index, warnings


def parse_journal(
    journal_path: Path,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> Tuple[List[CacheIndexEntry], List[str]]:
    """Parse Firefox cache2 journal file (``index.log``).

    The journal uses the same 41-byte ``CacheIndexRecord`` format as the
    main index, followed by a trailing 4-byte CRC, but has **no**
    ``CacheIndexHeader``.

    Args:
        journal_path: Path to the ``cache2/index.log`` file.
        warning_collector: Optional warning collector.

    Returns:
        Tuple of ``(list[CacheIndexEntry], list[str])``.
    """
    warnings: List[str] = []
    source = str(journal_path)

    def _warn(
        warning_type: str,
        severity: str,
        message: str,
        context: Optional[dict] = None,
    ) -> None:
        warnings.append(message)
        LOGGER.warning("%s: %s", source, message)
        if warning_collector:
            from extractors._shared.extraction_warnings import CATEGORY_BINARY
            warning_collector.add_warning(
                warning_type=warning_type,
                item_name="firefox_cache2_journal",
                severity=severity,
                category=CATEGORY_BINARY,
                artifact_type="cache_index",
                source_file=source,
                item_value=message,
                context_json=context,
            )

    try:
        with open(journal_path, "rb") as f:
            data = f.read()
    except Exception as e:
        _warn("file_corrupt", "error", f"Failed to read journal: {e}")
        return [], warnings

    if len(data) < CRC_SIZE:
        _warn(
            "file_corrupt",
            "error",
            f"Journal file too small ({len(data)} bytes)",
        )
        return [], warnings

    entries_data_size = len(data) - CRC_SIZE

    if entries_data_size % RECORD_SIZE != 0:
        remainder = entries_data_size % RECORD_SIZE
        _warn(
            "binary_format_error",
            "warning",
            f"Journal record region ({entries_data_size}) not aligned to "
            f"{RECORD_SIZE} bytes, {remainder} trailing bytes ignored",
            {"region_size": entries_data_size, "remainder": remainder},
        )

    entries: List[CacheIndexEntry] = []
    entry_count = entries_data_size // RECORD_SIZE
    offset = 0

    for i in range(entry_count):
        try:
            entry = _parse_index_record(data[offset : offset + RECORD_SIZE])
            if entry is not None:
                entries.append(entry)
        except Exception as e:
            _warn(
                "binary_format_error",
                "warning",
                f"Failed to parse journal record {i} at offset {offset}: {e}",
                {"record_index": i, "offset": offset},
            )
        offset += RECORD_SIZE

    LOGGER.info("Parsed cache journal: %d entries", len(entries))
    return entries, warnings


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _parse_index_record(data: bytes) -> Optional[CacheIndexEntry]:
    """Parse a single 41-byte CacheIndexRecord.

    Wire format (all big-endian)::

        [0:20]   SHA1 hash (raw bytes)
        [20:24]  uint32 mFrecency
        [24:32]  uint64 mOriginAttrsHash
        [32:34]  uint16 mOnStartTime
        [34:36]  uint16 mOnStopTime
        [36]     uint8  mContentType
        [37:41]  uint32 mFlags
    """
    if len(data) < RECORD_SIZE:
        return None

    hash_hex = data[0:20].hex().upper()
    frecency = struct.unpack("!I", data[20:24])[0]
    origin_attrs_hash = struct.unpack("!Q", data[24:32])[0]
    on_start_time = struct.unpack("!H", data[32:34])[0]
    on_stop_time = struct.unpack("!H", data[34:36])[0]
    content_type = data[36]
    flags = struct.unpack("!I", data[37:41])[0]

    return CacheIndexEntry(
        hash=hash_hex,
        frecency=frecency,
        origin_attrs_hash=origin_attrs_hash,
        on_start_time=on_start_time,
        on_stop_time=on_stop_time,
        content_type=content_type,
        flags=flags,
    )
