"""Parser for Safari WebKitCache / NetworkCache binary record files.

Safari's WebKit NetworkProcess stores cached resources as flat binary files in:
  - WebKitCache/Version N/Records/<partition_hash>/Resource/<sha1>
  - WebKitCache/Version N/Records/<partition_hash>/SubResources/<sha1>
  - WebKit/NetworkCache/Version N/Records/<partition_hash>/Resource/<sha1>

Each record file is a binary structure containing:
  1. A record header with magic, version, URL, and body/header offsets
  2. HTTP response headers
  3. Optionally an inline body (or reference to Blobs/<sha1>-blob)

Companion "-blob" files contain the response body when not stored inline.

Reference: WebKit source — NetworkCacheStorage.cpp / NetworkCacheCoders.cpp
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlparse

from core.logging import get_logger

LOGGER = get_logger("extractors.browser.safari.cache.record_parser")

# WebKitCache record magic bytes (little-endian uint32)
_RECORD_MAGIC = 0x4F524543  # "CERO" reversed — "ORECR" / cache record

# Minimum file size to be a valid record (key_length + short URL)
_MIN_RECORD_SIZE = 12


@dataclass
class WebKitCacheRecord:
    """Parsed WebKitCache record metadata."""

    record_hash: str  # filename / SHA1 hash
    url: str
    partition: str
    record_type: str  # "Resource" or "SubResources"
    timestamp_utc: Optional[str]
    http_status: Optional[int]
    content_type: Optional[str]
    content_length: Optional[int]
    headers: Dict[str, str]
    has_blob: bool  # True if a companion -blob file exists
    body_offset: Optional[int]  # offset of inline body within record
    body_size: Optional[int]  # size of inline body
    source_path: str  # original evidence path
    raw_url: Optional[str] = None  # URL before normalization
    all_header_fields: Dict[str, str] = field(default_factory=dict)


def parse_webkit_cache_record(
    record_data: bytes,
    record_hash: str,
    partition: str,
    record_type: str,
    source_path: str,
    has_blob_companion: bool = False,
) -> Optional[WebKitCacheRecord]:
    """Parse a WebKitCache binary record file into structured metadata.

    The binary format (from WebKit source) is roughly:
      - uint32 key_size
      - key_size bytes of URL string (UTF-8)
      - uint64 timestamp (Cocoa epoch, microseconds or seconds)
      - HTTP header data (length-prefixed strings)

    Multiple format versions exist across macOS/iOS versions. This parser
    tries a flexible approach: scan for the URL and headers.
    """
    if not record_data or len(record_data) < _MIN_RECORD_SIZE:
        return None

    try:
        return _parse_record_flexible(
            record_data,
            record_hash=record_hash,
            partition=partition,
            record_type=record_type,
            source_path=source_path,
            has_blob_companion=has_blob_companion,
        )
    except Exception as exc:
        LOGGER.debug("Failed to parse WebKitCache record %s: %s", record_hash, exc)
        return None


def _parse_record_flexible(
    data: bytes,
    *,
    record_hash: str,
    partition: str,
    record_type: str,
    source_path: str,
    has_blob_companion: bool,
) -> Optional[WebKitCacheRecord]:
    """Flexible parser that handles multiple WebKitCache format versions.

    Strategy:
    1. Try structured parsing (key_length + key followed by header block)
    2. Fall back to scanning for URL patterns and HTTP header patterns
    """
    # Try structured approach first
    result = _try_structured_parse(data)
    if result is None:
        # Fall back to scanning
        result = _try_scan_parse(data)

    if result is None:
        return None

    url, headers, timestamp_utc, body_offset, body_size = result

    http_status = _extract_status_from_headers(headers)
    content_type = _header_ci(headers, "content-type")
    content_length = _parse_int(_header_ci(headers, "content-length"))

    return WebKitCacheRecord(
        record_hash=record_hash,
        url=url,
        partition=partition,
        record_type=record_type,
        timestamp_utc=timestamp_utc,
        http_status=http_status,
        content_type=content_type,
        content_length=content_length,
        headers=headers,
        has_blob=has_blob_companion,
        body_offset=body_offset,
        body_size=body_size,
        source_path=source_path,
        raw_url=url,
        all_header_fields=headers,
    )


def _try_structured_parse(
    data: bytes,
) -> Optional[tuple[str, Dict[str, str], Optional[str], Optional[int], Optional[int]]]:
    """Try to parse using the known WebKit record binary layout.

    Layout (Version 12+):
      [0:4]   uint32 key_length  (length of URL string)
      [4:4+N] URL string (UTF-8)
      ... timestamp and metadata fields ...
      ... HTTP headers block ...
    """
    if len(data) < 8:
        return None

    # Read key (URL) length — little-endian uint32
    key_len = struct.unpack_from("<I", data, 0)[0]

    # Sanity check: URL should be reasonable length
    if key_len == 0 or key_len > 8192 or 4 + key_len > len(data):
        return None

    try:
        url = data[4 : 4 + key_len].decode("utf-8", errors="ignore").strip("\x00").strip()
    except Exception:
        return None

    if not _looks_like_url(url):
        return None

    offset = 4 + key_len

    # After URL, there may be timestamp data and header blocks
    # Try to find the HTTP headers section
    timestamp_utc = _try_extract_timestamp(data, offset)

    headers, body_offset, body_size = _extract_headers_from_offset(data, offset)

    return url, headers, timestamp_utc, body_offset, body_size


def _try_scan_parse(
    data: bytes,
) -> Optional[tuple[str, Dict[str, str], Optional[str], Optional[int], Optional[int]]]:
    """Scan binary data for URL and HTTP header patterns."""
    url = _scan_for_url(data)
    if not url:
        return None

    headers = _scan_for_headers(data)
    timestamp_utc = None  # scanning doesn't reliably find timestamps

    return url, headers, timestamp_utc, None, None


def _scan_for_url(data: bytes) -> Optional[str]:
    """Scan for http:// or https:// URL in binary data."""
    for prefix in (b"https://", b"http://"):
        idx = data.find(prefix)
        if idx < 0:
            continue
        # Read until null byte or non-printable character
        end = idx
        while end < len(data) and end < idx + 8192:
            b = data[end]
            if b == 0 or b < 0x20 or b > 0x7E:
                break
            end += 1
        if end > idx + 8:
            candidate = data[idx:end].decode("ascii", errors="ignore").strip()
            if _looks_like_url(candidate):
                return candidate
    return None


def _scan_for_headers(data: bytes) -> Dict[str, str]:
    """Scan for HTTP header patterns in binary data."""
    headers: Dict[str, str] = {}

    # Common header names to look for
    known_headers = [
        b"Content-Type", b"content-type",
        b"Content-Length", b"content-length",
        b"Content-Encoding", b"content-encoding",
        b"Cache-Control", b"cache-control",
        b"ETag", b"etag",
        b"Last-Modified", b"last-modified",
        b"Server", b"server",
        b"Date", b"date",
        b"Expires", b"expires",
        b"Set-Cookie", b"set-cookie",
        b"Access-Control-Allow-Origin", b"access-control-allow-origin",
        b"Vary", b"vary",
        b"X-Content-Type-Options", b"x-content-type-options",
        b"Strict-Transport-Security", b"strict-transport-security",
    ]

    for header_name in known_headers:
        # Look for "Header-Name" followed by some separator and value
        idx = data.find(header_name)
        if idx < 0:
            continue

        name_str = header_name.decode("ascii")
        # Look for the value after the header name
        value_start = idx + len(header_name)

        # Skip separator bytes (null bytes, colons, spaces)
        while value_start < len(data) and data[value_start] in (0, ord(":"), ord(" ")):
            value_start += 1

        if value_start >= len(data):
            continue

        # Read value until null or non-printable
        value_end = value_start
        while value_end < len(data) and value_end < value_start + 1024:
            b = data[value_end]
            if b == 0 or (b < 0x20 and b not in (0x09,)):  # allow tab
                break
            value_end += 1

        value = data[value_start:value_end].decode("utf-8", errors="ignore").strip()
        if value and name_str not in headers:
            headers[name_str] = value

    return headers


def _extract_headers_from_offset(
    data: bytes, start_offset: int,
) -> tuple[Dict[str, str], Optional[int], Optional[int]]:
    """Try to extract headers from a structured header block after the URL.

    WebKit stores headers as length-prefixed key-value pairs. The exact
    format varies but commonly:
      uint32 header_count
      for each header:
        uint32 key_length
        key bytes
        uint32 value_length
        value bytes
    """
    headers: Dict[str, str] = {}
    body_offset: Optional[int] = None
    body_size: Optional[int] = None

    # Skip ahead past timestamp/metadata — scan for a plausible header block
    # Try multiple positions after the URL as format varies between versions
    for probe_offset in range(start_offset, min(start_offset + 256, len(data) - 4), 4):
        trial_headers = _try_read_header_block(data, probe_offset)
        if trial_headers and len(trial_headers) >= 2:
            headers = trial_headers
            break

    # If structured parse didn't find headers, fall back to scanning
    if not headers:
        headers = _scan_for_headers(data[start_offset:])

    return headers, body_offset, body_size


def _try_read_header_block(data: bytes, offset: int) -> Optional[Dict[str, str]]:
    """Try to read a length-prefixed header block at the given offset."""
    if offset + 4 > len(data):
        return None

    count = struct.unpack_from("<I", data, offset)[0]
    if count == 0 or count > 100:  # sanity: more than 100 headers is unlikely
        return None

    headers: Dict[str, str] = {}
    pos = offset + 4

    for _ in range(count):
        if pos + 4 > len(data):
            return None

        key_len = struct.unpack_from("<I", data, pos)[0]
        pos += 4
        if key_len == 0 or key_len > 256 or pos + key_len > len(data):
            return None

        try:
            key = data[pos : pos + key_len].decode("utf-8", errors="strict").strip("\x00")
        except (UnicodeDecodeError, ValueError):
            return None
        pos += key_len

        if pos + 4 > len(data):
            return None
        value_len = struct.unpack_from("<I", data, pos)[0]
        pos += 4
        if value_len > 65536 or pos + value_len > len(data):
            return None

        try:
            value = data[pos : pos + value_len].decode("utf-8", errors="ignore").strip("\x00")
        except (UnicodeDecodeError, ValueError):
            value = ""
        pos += value_len

        if _looks_like_header_name(key):
            headers[key] = value

    # Validate: must have at least one recognizable HTTP header
    if not any(_is_known_header(k) for k in headers):
        return None

    return headers


def _try_extract_timestamp(data: bytes, offset: int) -> Optional[str]:
    """Try to extract a Cocoa timestamp from the data after the URL.

    Cocoa epoch is 2001-01-01. Values around 7xx million correspond to 2023-2025.
    """
    from .._parsers import cocoa_to_iso

    # Try reading at several positions (timestamp position varies by version)
    for ts_offset in range(offset, min(offset + 64, len(data) - 8), 8):
        try:
            ts_val = struct.unpack_from("<d", data, ts_offset)[0]
            # Sane range: 2010 to 2030 in Cocoa epoch (~280M to ~940M)
            if 280_000_000 < ts_val < 940_000_000:
                result = cocoa_to_iso(ts_val)
                if result:
                    return result
        except (struct.error, OverflowError):
            continue
    return None


def _looks_like_url(candidate: str) -> bool:
    """Check if a string looks like a valid HTTP(S) URL."""
    if not candidate:
        return False
    try:
        parsed = urlparse(candidate)
        return parsed.scheme in ("http", "https") and bool(parsed.hostname)
    except Exception:
        return False


def _looks_like_header_name(name: str) -> bool:
    """Check if a string looks like an HTTP header name."""
    if not name or len(name) > 256:
        return False
    return all(c.isalpha() or c in "-_" for c in name)


def _is_known_header(name: str) -> bool:
    """Check if header is a commonly known HTTP header."""
    known = {
        "content-type", "content-length", "content-encoding", "cache-control",
        "etag", "last-modified", "server", "date", "expires", "vary",
        "set-cookie", "location", "transfer-encoding", "connection",
        "accept-ranges", "age", "pragma", "x-content-type-options",
        "x-frame-options", "strict-transport-security", "access-control-allow-origin",
    }
    return name.lower() in known


def _header_ci(headers: Dict[str, str], target: str) -> Optional[str]:
    """Case-insensitive header lookup."""
    target_lower = target.lower()
    for k, v in headers.items():
        if k.lower() == target_lower:
            return v
    return None


def _extract_status_from_headers(headers: Dict[str, str]) -> Optional[int]:
    """Extract HTTP status from headers or status line."""
    # Some records include :status pseudo-header (HTTP/2)
    status = _header_ci(headers, ":status")
    if status:
        return _parse_int(status)
    return None


def _parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value.strip())
    except (ValueError, TypeError):
        return None


def parse_webkit_cache_dir(
    cache_dir: Path,
    source_root: str = "",
) -> List[WebKitCacheRecord]:
    """Parse all records in a WebKitCache/Version N/ directory.

    Scans Records/<partition>/Resource/ and Records/<partition>/SubResources/
    for record files (excluding -blob companions).
    """
    records: List[WebKitCacheRecord] = []
    records_dir = cache_dir / "Records"
    if not records_dir.exists():
        return records

    for partition_dir in sorted(records_dir.iterdir()):
        if not partition_dir.is_dir():
            continue
        partition = partition_dir.name

        for type_dir_name in ("Resource", "SubResources"):
            type_dir = partition_dir / type_dir_name
            if not type_dir.exists():
                continue

            blob_names = _collect_blob_names(type_dir)

            for record_file in sorted(type_dir.iterdir()):
                if not record_file.is_file():
                    continue
                if record_file.name.endswith("-blob"):
                    continue  # skip companion blobs, handle via record

                record_hash = record_file.name
                has_blob = record_hash in blob_names

                try:
                    data = record_file.read_bytes()
                except Exception:
                    continue

                if source_root:
                    src_path = f"{source_root}/Records/{partition}/{type_dir_name}/{record_hash}"
                else:
                    src_path = str(record_file)

                record = parse_webkit_cache_record(
                    data,
                    record_hash=record_hash,
                    partition=partition,
                    record_type=type_dir_name,
                    source_path=src_path,
                    has_blob_companion=has_blob,
                )
                if record:
                    records.append(record)

    return records


def get_blob_path(
    cache_dir: Path,
    record: WebKitCacheRecord,
) -> Optional[Path]:
    """Return the path to body data for a record.

    Checks for:
    1. Companion -blob file next to the record
    2. Blob in the shared Blobs/ directory
    """
    # 1. Companion -blob file
    records_dir = cache_dir / "Records" / record.partition / record.record_type
    blob_companion = records_dir / f"{record.record_hash}-blob"
    if blob_companion.exists():
        return blob_companion

    # 2. Shared Blobs directory (keyed by record hash)
    blobs_dir = cache_dir / "Blobs"
    if blobs_dir.exists():
        blob_file = blobs_dir / record.record_hash
        if blob_file.exists():
            return blob_file

    return None


def _collect_blob_names(type_dir: Path) -> Set[str]:
    """Collect record hashes that have companion -blob files."""
    names: Set[str] = set()
    for f in type_dir.iterdir():
        if f.name.endswith("-blob") and f.is_file():
            names.add(f.name[:-5])  # remove "-blob" suffix
    return names
