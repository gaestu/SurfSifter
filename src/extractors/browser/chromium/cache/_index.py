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
from ._schemas import SIMPLE_INDEX_MAGIC, SIMPLE_INDEX_VERSION, SIMPLE_INDEX_MIN_VERSION

LOGGER = get_logger("extractors.cache_simple.index")


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
    3. Entries: For each entry - hash_key (uint64), metadata (2x uint32)
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
        if magic != SIMPLE_INDEX_MAGIC:
            LOGGER.debug("Invalid index magic: 0x%x (expected 0x%x) in %s",
                        magic, SIMPLE_INDEX_MAGIC, file_path)
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

        # Parse entries
        # Each entry: hash_key (uint64), then EntryMetadata serialized
        # EntryMetadata on disk:
        #   - uint32: last_used_time_seconds_since_epoch (or trailer_prefetch_size for APP_CACHE)
        #   - uint32: entry_size_256b_chunks (30 bits) + in_memory_data (2 bits)
        # Total: 16 bytes per entry (8 + 4 + 4)

        ENTRY_SIZE_ON_DISK = 16  # 8 (hash) + 4 + 4 (metadata)

        for i in range(entry_count):
            if len(data) < offset + ENTRY_SIZE_ON_DISK:
                LOGGER.warning("Index file truncated at entry %d/%d: %s", i, entry_count, file_path)
                break

            entry_hash = struct.unpack_from('<Q', data, offset)[0]
            offset += 8

            # EntryMetadata: last_used_time (or trailer_prefetch) + size_chunks_and_flags
            last_used_seconds = struct.unpack_from('<I', data, offset)[0]
            offset += 4

            size_and_flags = struct.unpack_from('<I', data, offset)[0]
            offset += 4

            # Extract entry size (30 bits) and in_memory_data (2 bits)
            entry_size_chunks = size_and_flags & 0x3FFFFFFF  # Lower 30 bits
            # in_memory_data = (size_and_flags >> 30) & 0x03  # Upper 2 bits (not needed for forensics)

            # Convert size from 256-byte chunks to bytes
            entry_size_bytes = entry_size_chunks * 256

            # Convert timestamp: seconds since Unix epoch
            if last_used_seconds > 0:
                try:
                    last_used_time = datetime.fromtimestamp(last_used_seconds, tz=timezone.utc)
                except (ValueError, OSError):
                    # Invalid timestamp
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
            # This is base::Time::ToInternalValue() - microseconds since Windows epoch (1601)
            # Convert to Unix timestamp
            try:
                # Windows epoch to Unix epoch offset in microseconds
                WINDOWS_EPOCH_OFFSET = 11644473600 * 1000000
                unix_microseconds = cache_last_modified_raw - WINDOWS_EPOCH_OFFSET
                if unix_microseconds > 0:
                    metadata.cache_last_modified = datetime.fromtimestamp(
                        unix_microseconds / 1000000, tz=timezone.utc
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
