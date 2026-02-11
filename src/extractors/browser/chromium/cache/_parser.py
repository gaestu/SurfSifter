"""
Simple cache entry parser.

Parses Chromium simple cache entry files (_0 files) to extract:
- Cache key (URL)
- Stream offsets and sizes
- HTTP headers from stream 0
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional

from core.logging import get_logger
from ._schemas import (
    SIMPLE_INITIAL_MAGIC,
    SIMPLE_FINAL_MAGIC,
    SIMPLE_ENTRY_VERSION,
    SIMPLE_FILE_HEADER_SIZE,
    SIMPLE_FILE_EOF_SIZE,
    SIMPLE_FILE_HEADER_FORMAT,
    SIMPLE_FILE_EOF_FORMAT,
    FLAG_HAS_KEY_SHA256,
    is_known_simple_version,
    is_known_eof_flags,
    get_unknown_eof_flags,
)
from .blockfile import extract_url_from_cache_key

if TYPE_CHECKING:
    from ...._shared.extraction_warnings import ExtractionWarningCollector

LOGGER = get_logger("extractors.cache_simple.parser")


@dataclass
class CacheEntry:
    """Parsed cache entry with stream locations and metadata."""
    url: str  # The extracted resource URL
    version: int
    key_length: int
    key_hash: int
    stream0_offset: int  # HTTP headers
    stream0_size: int
    stream1_offset: int  # Response body
    stream1_size: int
    has_key_sha256: bool = False
    file_path: Optional[Path] = None
    raw_cache_key: Optional[str] = None  # Original cache key (includes prefixes)
    # Parsed from stream 0
    http_info: Dict[str, Any] = field(default_factory=dict)


def parse_cache_entry(
    file_path: Path,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> Optional[CacheEntry]:
    """
    Parse a Chromium simple cache entry file (_0 file).

    File layout (per simple_entry_format.h):
        [SimpleFileHeader - 24 bytes]
        [Key - key_length bytes]
        [Stream 1 data - body]
        [SimpleFileEOF for Stream 1 - 24 bytes]
        [Stream 0 data - HTTP headers]
        [Optional SHA256 of key - 32 bytes]
        [SimpleFileEOF for Stream 0 - 24 bytes]

    Args:
        file_path: Path to cache entry file (typically {hash}_0)
        warning_collector: Optional collector for extraction warnings

    Returns:
        CacheEntry with parsed metadata, or None if parsing failed
    """
    try:
        file_size = file_path.stat().st_size

        # Minimum size: header + EOF0 + EOF1 = 24 + 24 + 24 = 72 bytes
        if file_size < SIMPLE_FILE_HEADER_SIZE + 2 * SIMPLE_FILE_EOF_SIZE:
            LOGGER.debug("File too small for cache entry: %s (%d bytes)", file_path, file_size)
            return None

        with open(file_path, 'rb') as f:
            # Read and validate header
            header_bytes = f.read(SIMPLE_FILE_HEADER_SIZE)
            if len(header_bytes) < SIMPLE_FILE_HEADER_SIZE:
                return None

            magic, version, key_length, key_hash, _ = struct.unpack(
                SIMPLE_FILE_HEADER_FORMAT, header_bytes
            )

            # Validate magic number
            if magic != SIMPLE_INITIAL_MAGIC:
                LOGGER.debug("Invalid header magic in %s: 0x%x", file_path, magic)
                if warning_collector:
                    warning_collector.add_file_corrupt(
                        filename=str(file_path),
                        error=f"Invalid header magic: 0x{magic:x} (expected 0x{SIMPLE_INITIAL_MAGIC:x})",
                        artifact_type="cache_simple",
                    )
                return None

            # Warn on version mismatch but proceed (format is stable)
            if version != SIMPLE_ENTRY_VERSION:
                LOGGER.debug("Entry version %d (expected %d) in %s", version, SIMPLE_ENTRY_VERSION, file_path)
                if warning_collector and not is_known_simple_version(version):
                    from ...._shared.extraction_warnings import (
                        WARNING_TYPE_VERSION_UNSUPPORTED,
                        SEVERITY_WARNING,
                        CATEGORY_BINARY,
                    )
                    warning_collector.add_warning(
                        warning_type=WARNING_TYPE_VERSION_UNSUPPORTED,
                        item_name="simple_cache_version",
                        item_value=str(version),
                        severity=SEVERITY_WARNING,
                        category=CATEGORY_BINARY,
                        artifact_type="cache_simple",
                        source_file=str(file_path),
                        context_json={"expected_version": SIMPLE_ENTRY_VERSION},
                    )

            # Sanity check key length
            max_key_length = file_size - SIMPLE_FILE_HEADER_SIZE - 2 * SIMPLE_FILE_EOF_SIZE
            if key_length > max_key_length or key_length > 1024 * 1024:  # 1MB max URL
                LOGGER.debug("Invalid key length %d in %s", key_length, file_path)
                return None

            # Read key (the raw cache key - may include network isolation prefixes)
            key_bytes = f.read(key_length)
            if len(key_bytes) < key_length:
                LOGGER.debug("Truncated key in %s", file_path)
                return None

            try:
                raw_cache_key = key_bytes.decode('utf-8', errors='replace')
            except Exception:
                raw_cache_key = key_bytes.decode('latin-1', errors='replace')

            # Extract the actual resource URL from the cache key
            # (strips network isolation prefixes like "1/0/_dk_...")
            url = extract_url_from_cache_key(raw_cache_key)

            # Read EOF0 (at very end of file)
            f.seek(-SIMPLE_FILE_EOF_SIZE, 2)
            eof0_bytes = f.read(SIMPLE_FILE_EOF_SIZE)

            eof0_magic, eof0_flags, eof0_crc32, stream0_size, _ = struct.unpack(
                SIMPLE_FILE_EOF_FORMAT, eof0_bytes
            )

            if eof0_magic != SIMPLE_FINAL_MAGIC:
                LOGGER.debug("Invalid EOF0 magic in %s: 0x%x", file_path, eof0_magic)
                if warning_collector:
                    warning_collector.add_file_corrupt(
                        filename=str(file_path),
                        error=f"Invalid EOF0 magic: 0x{eof0_magic:x}",
                        artifact_type="cache_simple",
                    )
                return None

            # Check for unknown EOF flags
            if warning_collector and not is_known_eof_flags(eof0_flags):
                from ...._shared.extraction_warnings import (
                    WARNING_TYPE_UNKNOWN_ENUM_VALUE,
                    SEVERITY_INFO,
                    CATEGORY_BINARY,
                )
                unknown_bits = get_unknown_eof_flags(eof0_flags)
                warning_collector.add_warning(
                    warning_type=WARNING_TYPE_UNKNOWN_ENUM_VALUE,
                    item_name="eof_flags",
                    item_value=f"0x{eof0_flags:02x}",
                    severity=SEVERITY_INFO,
                    category=CATEGORY_BINARY,
                    artifact_type="cache_simple",
                    source_file=str(file_path),
                    context_json={"unknown_bits": f"0x{unknown_bits:02x}"},
                )

            has_key_sha256 = bool(eof0_flags & FLAG_HAS_KEY_SHA256)
            sha256_size = 32 if has_key_sha256 else 0

            # Calculate stream 0 location
            stream0_end = file_size - SIMPLE_FILE_EOF_SIZE
            stream0_start = stream0_end - sha256_size - stream0_size

            # Read EOF1 (just before stream 0 / sha256)
            eof1_end = stream0_start
            eof1_start = eof1_end - SIMPLE_FILE_EOF_SIZE

            if eof1_start < SIMPLE_FILE_HEADER_SIZE + key_length:
                LOGGER.debug("Invalid EOF1 position in %s", file_path)
                return None

            f.seek(eof1_start)
            eof1_bytes = f.read(SIMPLE_FILE_EOF_SIZE)

            eof1_magic, eof1_flags, eof1_crc32, eof1_stream_size, _ = struct.unpack(
                SIMPLE_FILE_EOF_FORMAT, eof1_bytes
            )

            if eof1_magic != SIMPLE_FINAL_MAGIC:
                LOGGER.debug("Invalid EOF1 magic in %s: 0x%x", file_path, eof1_magic)
                if warning_collector:
                    warning_collector.add_file_corrupt(
                        filename=str(file_path),
                        error=f"Invalid EOF1 magic: 0x{eof1_magic:x}",
                        artifact_type="cache_simple",
                    )
                return None

            # Stream 1 (body) is between header+key and EOF1
            stream1_start = SIMPLE_FILE_HEADER_SIZE + key_length
            stream1_end = eof1_start
            stream1_size = stream1_end - stream1_start

            # Sanity checks
            if stream0_size < 0 or stream1_size < 0 or stream0_size > file_size or stream1_size > file_size:
                LOGGER.debug("Invalid stream sizes in %s: s0=%d, s1=%d", file_path, stream0_size, stream1_size)
                return None

            return CacheEntry(
                url=url,
                version=version,
                key_length=key_length,
                key_hash=key_hash,
                stream0_offset=stream0_start,
                stream0_size=stream0_size,
                stream1_offset=stream1_start,
                stream1_size=stream1_size,
                has_key_sha256=has_key_sha256,
                file_path=file_path,
                raw_cache_key=raw_cache_key if raw_cache_key != url else None,
            )

    except Exception as e:
        LOGGER.debug("Failed to parse cache entry %s: %s", file_path, e)
        return None


def read_stream(file_path: Path, offset: int, size: int) -> bytes:
    """Read a stream from cache file at given offset."""
    if size <= 0:
        return b''
    try:
        with open(file_path, 'rb') as f:
            f.seek(offset)
            return f.read(size)
    except Exception as e:
        LOGGER.debug("Failed to read stream at offset %d from %s: %s", offset, file_path, e)
        return b''


def parse_http_headers(stream0_bytes: bytes) -> Dict[str, Any]:
    """
    Parse serialized HTTP response info from stream 0.

    Note: Stream 0 is actually a base::Pickle containing HttpResponseInfo.
    For MVP, we scan for text headers which usually works for status/content-type,
    but ignores binary preamble (timestamps, flags).

    Args:
        stream0_bytes: Raw stream 0 data

    Returns:
        Dict with response_code, content_type, content_encoding, headers
    """
    result = {
        'headers': {},
        'raw_headers_text': None,  # Preserve original header text with order/duplicates
        'response_code': None,
        'content_type': None,
        'content_encoding': None,
    }

    if not stream0_bytes:
        return result

    # Decode with latin-1 to preserve all bytes (binary preamble + text headers)
    text = stream0_bytes.decode('latin-1', errors='replace')
    lines = text.split('\n')

    # Find start of HTTP headers (skip binary preamble)
    start_idx = 0
    for i, line in enumerate(lines):
        if line.startswith('HTTP/'):
            start_idx = i
            break

    if start_idx < len(lines):
        # Capture raw headers text from HTTP/ line to blank line
        raw_lines = []
        for line in lines[start_idx:]:
            raw_lines.append(line)
            if line.strip() == '':
                break
        result['raw_headers_text'] = '\n'.join(raw_lines)

        # Parse status line: HTTP/1.1 200 OK
        status_line = lines[start_idx]
        parts = status_line.split(' ', 2)
        if len(parts) >= 2:
            try:
                result['response_code'] = int(parts[1])
            except ValueError:
                pass

        # Parse headers (also store in dict for convenience)
        for line in lines[start_idx + 1:]:
            line = line.strip()
            if ':' in line:
                name, value = line.split(':', 1)
                header_name = name.strip().lower()
                header_value = value.strip()
                result['headers'][header_name] = header_value

                # Extract key headers
                if header_name == 'content-type':
                    # Strip charset/parameters
                    result['content_type'] = header_value.split(';')[0].strip()
                elif header_name == 'content-encoding':
                    result['content_encoding'] = header_value
            elif line == '':
                break  # End of headers

    return result


# Backward compatibility aliases
_parse_cache_entry = parse_cache_entry
_read_stream = read_stream
_parse_http_headers = parse_http_headers
