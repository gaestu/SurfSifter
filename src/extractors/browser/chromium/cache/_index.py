"""
Simple cache index file parser.

Parses Chromium simple cache index files to extract entry metadata
(hash, last_used_time, entry_size) for forensic timeline correlation.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

from core.logging import get_logger
from ._schemas import (
    SIMPLE_INDEX_ACCEPTED_MAGICS,
    SIMPLE_INDEX_VERSION,
    SIMPLE_INDEX_MIN_VERSION,
    SIMPLE_INDEX_V9_ENTRY_FORMAT_VERSION,
    WINDOWS_EPOCH_OFFSET_MICROSECONDS,
)

LOGGER = get_logger("extractors.cache_simple.index")

# Seconds between Windows FILETIME epoch (1601) and Unix epoch (1970)
_WINDOWS_EPOCH_OFFSET_SECONDS = 11644473600


@dataclass
class IndexEntry:
    """Parsed entry from the cache index file."""
    entry_hash: int  # 64-bit hash of the cache key (URL)
    last_used_time: datetime  # When the entry was last accessed
    entry_size: int  # Size in bytes (stored as 256-byte chunks in file)


@dataclass
class IndexMetadata:
    """Parsed metadata from the cache index file header."""
    magic: int
    version: int
    entry_count: int
    cache_size: int
    write_reason: int
    cache_last_modified: Optional[datetime] = None


def parse_index_file(file_path: Path) -> Tuple[Optional[IndexMetadata], List[IndexEntry]]:
    """
    Parse a Chromium simple cache index file.

    The index file format (per simple_index_file.cc):
    1. Pickle header: payload_size (uint32) + CRC (uint32)
    2. Index metadata: magic (uint64), version (uint32), entry_count (uint64),
       cache_size (uint64), write_reason (uint32)
    3. Entries (format depends on version):
       V7-V8 (16 bytes): hash_key (uint64) + seconds_since_1601 (uint32)
                          + size_chunks_and_flags (uint32)
       V9+   (24 bytes): hash_key (uint64) + last_used_us_since_1601 (int64)
                          + size_chunks (uint32) + in_memory_data (uint32)
    4. Final data: cache_last_modified (int64)

    Args:
        file_path: Path to index file (either 'index' or 'the-real-index')

    Returns:
        Tuple of (IndexMetadata, list of IndexEntry) or (None, []) on failure
    """
    entries = []

    try:
        data = file_path.read_bytes()
        if len(data) < 8:  # Minimum: pickle header
            LOGGER.debug("Index file too small: %s (%d bytes)", file_path, len(data))
            return None, []

        # Parse Pickle header
        # The Pickle format: uint32 payload_size, then payload bytes
        # For SimpleIndexPickle, header also contains uint32 crc after payload_size
        offset = 0

        # Skip pickle header (payload_size)
        payload_size = struct.unpack_from('<I', data, offset)[0]
        offset += 4

        # After payload_size comes the CRC (part of custom PickleHeader)
        if len(data) < offset + 4:
            LOGGER.debug("Index file missing CRC header: %s", file_path)
            return None, []

        crc_read = struct.unpack_from('<I', data, offset)[0]
        offset += 4

        # Now parse index metadata
        # magic (uint64), version (uint32), entry_count (uint64), cache_size (uint64), reason (uint32)
        if len(data) < offset + 8 + 4 + 8 + 8 + 4:
            LOGGER.debug("Index file too small for metadata: %s", file_path)
            return None, []

        magic = struct.unpack_from('<Q', data, offset)[0]
        offset += 8

        version = struct.unpack_from('<I', data, offset)[0]
        offset += 4

        entry_count = struct.unpack_from('<Q', data, offset)[0]
        offset += 8

        cache_size = struct.unpack_from('<Q', data, offset)[0]
        offset += 8

        write_reason = struct.unpack_from('<I', data, offset)[0]
        offset += 4

        # Validate magic and version
        if magic not in SIMPLE_INDEX_ACCEPTED_MAGICS:
            LOGGER.debug("Invalid index magic: 0x%x in %s (not in accepted set)",
                        magic, file_path)
            return None, []

        if version < SIMPLE_INDEX_MIN_VERSION or version > SIMPLE_INDEX_VERSION:
            LOGGER.warning("Unsupported index version %d (supported: %d-%d) in %s",
                          version, SIMPLE_INDEX_MIN_VERSION, SIMPLE_INDEX_VERSION, file_path)
            # Continue anyway - format is usually stable

        if entry_count > 1000000:  # Sanity check
            LOGGER.warning("Suspicious entry count %d in %s", entry_count, file_path)
            return None, []

        metadata = IndexMetadata(
            magic=magic,
            version=version,
            entry_count=entry_count,
            cache_size=cache_size,
            write_reason=write_reason,
        )

        # Parse entries — format depends on version.
        # V7-V8 (16 bytes per entry):
        #   hash_key (uint64) + last_used_seconds_since_1601 (uint32)
        #   + size_chunks_and_flags (uint32)
        # V9+ (24 bytes per entry):
        #   hash_key (uint64) + last_used_us_since_1601 (int64)
        #   + size_chunks (uint32) + in_memory_data (uint32)
        is_v9_format = version >= SIMPLE_INDEX_V9_ENTRY_FORMAT_VERSION
        entry_size_on_disk = 24 if is_v9_format else 16

        for i in range(entry_count):
            if len(data) < offset + entry_size_on_disk:
                LOGGER.warning("Index file truncated at entry %d/%d: %s", i, entry_count, file_path)
                break

            entry_hash = struct.unpack_from('<Q', data, offset)[0]
            offset += 8

            if is_v9_format:
                # V9+: 8-byte Chrome timestamp (microseconds since 1601-01-01)
                last_used_us = struct.unpack_from('<q', data, offset)[0]
                offset += 8

                size_chunks = struct.unpack_from('<I', data, offset)[0]
                offset += 4
                # in_memory_data (uint32) — not needed for forensics
                offset += 4

                entry_size_chunks = size_chunks & 0x3FFFFFFF
                entry_size_bytes = entry_size_chunks * 256

                # Convert Chrome timestamp to datetime
                if last_used_us > WINDOWS_EPOCH_OFFSET_MICROSECONDS:
                    try:
                        unix_us = last_used_us - WINDOWS_EPOCH_OFFSET_MICROSECONDS
                        last_used_time = datetime.fromtimestamp(
                            unix_us / 1_000_000, tz=timezone.utc
                        )
                    except (ValueError, OSError):
                        last_used_time = datetime.fromtimestamp(0, tz=timezone.utc)
                elif last_used_us > 0:
                    # Positive but before Unix epoch — still valid forensic data
                    last_used_time = datetime.fromtimestamp(0, tz=timezone.utc)
                else:
                    last_used_time = datetime.fromtimestamp(0, tz=timezone.utc)
            else:
                # V7-V8: 4-byte timestamp (seconds since 1601-01-01)
                last_used_seconds_since_1601 = struct.unpack_from('<I', data, offset)[0]
                offset += 4

                size_and_flags = struct.unpack_from('<I', data, offset)[0]
                offset += 4

                entry_size_chunks = size_and_flags & 0x3FFFFFFF
                entry_size_bytes = entry_size_chunks * 256

                # Convert from seconds since Windows FILETIME epoch (1601)
                if last_used_seconds_since_1601 > _WINDOWS_EPOCH_OFFSET_SECONDS:
                    try:
                        unix_seconds = last_used_seconds_since_1601 - _WINDOWS_EPOCH_OFFSET_SECONDS
                        last_used_time = datetime.fromtimestamp(
                            unix_seconds, tz=timezone.utc
                        )
                    except (ValueError, OSError):
                        last_used_time = datetime.fromtimestamp(0, tz=timezone.utc)
                else:
                    last_used_time = datetime.fromtimestamp(0, tz=timezone.utc)

            entries.append(IndexEntry(
                entry_hash=entry_hash,
                last_used_time=last_used_time,
                entry_size=entry_size_bytes,
            ))

        # Parse final data: cache_last_modified (int64)
        if len(data) >= offset + 8:
            cache_last_modified_raw = struct.unpack_from('<q', data, offset)[0]
            # This is base::Time::ToInternalValue() — microseconds since 1601
            try:
                unix_microseconds = cache_last_modified_raw - WINDOWS_EPOCH_OFFSET_MICROSECONDS
                if unix_microseconds > 0:
                    metadata.cache_last_modified = datetime.fromtimestamp(
                        unix_microseconds / 1_000_000, tz=timezone.utc
                    )
            except (ValueError, OSError):
                pass

        LOGGER.debug("Parsed index file %s: %d entries, cache_size=%d bytes",
                    file_path.name, len(entries), cache_size)

        return metadata, entries

    except Exception as e:
        LOGGER.warning("Failed to parse index file %s: %s", file_path, e)
        return None, []


# Backward compatibility alias
_parse_index_file = parse_index_file
