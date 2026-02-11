"""
Firefox Cache2 Parser

Parses Firefox cache2 entry files (body-first layout, big-endian metadata).
Extracts URLs, HTTP metadata, and elements from cache entry files.

Firefox cache2 layout:
┌─────────────────────────────────────────────────────────────┐
│ Response Body (raw or compressed)                           │  ← Offset 0
├─────────────────────────────────────────────────────────────┤
│ Metadata Checksum (4 bytes, big-endian Jenkins Hash)        │  ← metaOffset
├─────────────────────────────────────────────────────────────┤
│ Hash Array (2 bytes × chunk_count)                          │
├─────────────────────────────────────────────────────────────┤
│ CacheFileMetadataHeader (28-32 bytes, BIG ENDIAN)           │
├─────────────────────────────────────────────────────────────┤
│ Key (cache key/URL) + null terminator                       │
├─────────────────────────────────────────────────────────────┤
│ Elements (key\\0value\\0 pairs)                              │
├─────────────────────────────────────────────────────────────┤
│ Metadata Offset (4 bytes, big-endian uint32)                │  ← LAST 4 bytes
└─────────────────────────────────────────────────────────────┘

Reference:
- https://www.forensicswiki.org/wiki/Mozilla_Cache2
- https://firefox-source-docs.mozilla.org/netwerk/cache2/cache2.html
"""

from __future__ import annotations

import re
import struct
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, Any, Optional

from core.logging import get_logger

LOGGER = get_logger("extractors.cache_firefox.parser")

# Firefox cache2 constants
CACHE2_VERSIONS = {1, 2, 3}  # Supported versions
CACHE2_CHUNK_SIZE = 262144  # 256 KB chunks for hash array


@dataclass
class Cache2ParseResult:
    """Result from parsing a Firefox cache2 entry file."""
    url: Optional[str] = None
    cache_key: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    elements: Dict[str, str] = field(default_factory=dict)
    is_image: bool = False
    content_type: Optional[str] = None
    content_encoding: Optional[str] = None
    response_code: Optional[int] = None
    body_offset: int = 0
    body_size: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for backward compatibility."""
        return {
            "url": self.url,
            "cache_key": self.cache_key,
            "metadata": self.metadata,
            "elements": self.elements,
            "is_image": self.is_image,
            "content_type": self.content_type,
            "content_encoding": self.content_encoding,
            "response_code": self.response_code,
            "body_offset": self.body_offset,
            "body_size": self.body_size,
        }


@dataclass
class HttpMetadata:
    """HTTP metadata extracted from response-head element."""
    response_code: Optional[int] = None
    content_type: Optional[str] = None
    content_encoding: Optional[str] = None
    content_length: Optional[int] = None
    cache_control: Optional[str] = None
    date: Optional[str] = None
    age: Optional[int] = None
    last_modified: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "response_code": self.response_code,
            "content_type": self.content_type,
            "content_encoding": self.content_encoding,
            "content_length": self.content_length,
            "cache_control": self.cache_control,
            "date": self.date,
            "age": self.age,
            "last_modified": self.last_modified,
        }


def parse_cache2_entry(file_path: Path, file_entry: Optional[dict] = None) -> Cache2ParseResult:
    """
    Parse Firefox cache2 entry file (metadata at END, big-endian).

    Args:
        file_path: Path to extracted cache entry file
        file_entry: Optional manifest entry dict (for context)

    Returns:
        Cache2ParseResult with parsed data
    """
    try:
        data = file_path.read_bytes()
        file_size = len(data)

        # Minimum size: at least 4 bytes for meta_offset
        if file_size < 4:
            LOGGER.warning("Cache2 entry too small: %s (%d bytes)", file_path, file_size)
            return Cache2ParseResult()

        # 1. Read metadata offset from LAST 4 bytes (BIG ENDIAN)
        meta_offset = struct.unpack(">I", data[-4:])[0]

        # Validate meta_offset
        if meta_offset == 0:
            # Empty body - metadata starts at 0
            LOGGER.debug("Empty body in cache entry: %s", file_path)
        elif meta_offset >= file_size - 4:
            # meta_offset should point within file before the last 4 bytes
            LOGGER.warning("Invalid meta_offset %d for file size %d: %s",
                          meta_offset, file_size, file_path)
            return Cache2ParseResult()

        # 2. Body is from 0 to meta_offset
        body_size = meta_offset

        # 3. Calculate hash array size
        hash_count = (meta_offset + CACHE2_CHUNK_SIZE - 1) // CACHE2_CHUNK_SIZE if meta_offset > 0 else 0
        hashes_len = hash_count * 2

        # 4. Calculate offsets for metadata components
        checksum_offset = meta_offset
        hashes_offset = checksum_offset + 4
        header_offset = hashes_offset + hashes_len

        # Check if we have enough data for minimum header
        min_required = header_offset + 28  # 28-byte minimum header
        if file_size < min_required + 4:  # +4 for meta_offset at end
            LOGGER.warning("Not enough data for header in %s", file_path)
            return Cache2ParseResult()

        # 5. Parse header (BIG ENDIAN)
        header_data = data[header_offset:header_offset + 32]
        if len(header_data) < 28:
            LOGGER.warning("Incomplete header in %s", file_path)
            return Cache2ParseResult()

        try:
            version = struct.unpack(">I", header_data[0:4])[0]
        except struct.error:
            LOGGER.warning("Failed to read version from %s", file_path)
            return Cache2ParseResult()

        # Version 1: 28-byte header (no flags)
        # Version 2+: 32-byte header (with flags)
        header_size = 28 if version == 1 else 32

        if version not in CACHE2_VERSIONS:
            LOGGER.debug("Unusual cache2 version %d in %s (continuing anyway)", version, file_path)

        try:
            (
                version,
                fetch_count,
                last_fetched,
                last_modified,
                frecency,
                expiration,
                key_size,
            ) = struct.unpack(">7I", header_data[0:28])
        except struct.error as e:
            LOGGER.warning("Failed to parse cache2 header: %s: %s", file_path, e)
            return Cache2ParseResult()

        flags = 0
        if version >= 2 and len(header_data) >= 32:
            flags = struct.unpack(">I", header_data[28:32])[0]

        # 6. Extract key (URL)
        key_offset = header_offset + header_size

        # Validate key_size
        if key_size == 0:
            LOGGER.debug("Zero key_size in %s", file_path)
            return Cache2ParseResult()

        if key_offset + key_size > file_size - 4:
            LOGGER.warning("key_size %d exceeds file bounds in %s", key_size, file_path)
            return Cache2ParseResult()

        key_bytes = data[key_offset:key_offset + key_size]
        cache_key = key_bytes.rstrip(b'\x00').decode('utf-8', errors='replace')

        # Extract URL from cache key
        url = extract_url_from_key(cache_key)

        # 7. Parse elements (key\0value\0 pairs)
        elements_offset = key_offset + key_size + 1  # +1 for null terminator
        elements_end = file_size - 4  # Before the meta_offset field
        elements = parse_elements(data[elements_offset:elements_end])

        # 8. Extract HTTP metadata from elements
        http_meta = extract_http_metadata(elements)

        # Determine if image based on content type or URL extension
        is_image = _is_image_content(http_meta.content_type, url)

        # Convert timestamps to ISO format
        metadata = {
            "version": version,
            "fetch_count": fetch_count,
            "frecency": frecency,
            "flags": flags,
        }

        if last_fetched > 0 and last_fetched < 2000000000:  # Reasonable Unix timestamp
            metadata["last_fetched"] = datetime.fromtimestamp(last_fetched, tz=timezone.utc).isoformat()
            metadata["last_fetched_unix"] = last_fetched
        if last_modified > 0 and last_modified < 2000000000:
            metadata["last_modified"] = datetime.fromtimestamp(last_modified, tz=timezone.utc).isoformat()
            metadata["last_modified_unix"] = last_modified
        if expiration > 0 and expiration != 0xFFFFFFFF and expiration < 2000000000:
            metadata["expiration"] = datetime.fromtimestamp(expiration, tz=timezone.utc).isoformat()
            metadata["expiration_unix"] = expiration

        return Cache2ParseResult(
            url=url,
            cache_key=cache_key,
            metadata=metadata,
            elements=elements,
            is_image=is_image,
            content_type=http_meta.content_type,
            content_encoding=http_meta.content_encoding,
            response_code=http_meta.response_code,
            body_offset=0,
            body_size=body_size,
        )

    except Exception as e:
        LOGGER.error("Failed to parse cache2 entry %s: %s", file_path, e, exc_info=True)
        return Cache2ParseResult()


def parse_elements(data: bytes) -> Dict[str, str]:
    """
    Parse cache2 elements section (key\\0value\\0 pairs).

    Known keys:
    - "request-method" → "GET", "POST", etc.
    - "response-head" → Full HTTP response headers
    - "security-info" → TLS certificate chain (binary, skip decoding)
    - "original-response-headers" → Pre-modification headers

    Args:
        data: Raw bytes from elements section

    Returns:
        Dict mapping element keys to values (text keys only)
    """
    elements = {}
    if not data:
        return elements

    # Binary keys to skip (contain non-text data)
    binary_keys = {"security-info", "alt-data", "alt-data-info"}

    i = 0
    while i < len(data):
        # Find key (null-terminated)
        null_pos = data.find(b'\x00', i)
        if null_pos == -1 or null_pos == i:
            break

        key = data[i:null_pos].decode('utf-8', errors='replace')
        i = null_pos + 1

        # Find value (null-terminated)
        null_pos = data.find(b'\x00', i)
        if null_pos == -1:
            # Last value may not have trailing null
            value_bytes = data[i:]
            i = len(data)
        else:
            value_bytes = data[i:null_pos]
            i = null_pos + 1

        # Skip binary keys
        if key in binary_keys:
            continue

        # Try to decode value as text
        try:
            value = value_bytes.decode('utf-8', errors='replace')
            elements[key] = value
        except Exception:
            # Skip values that can't be decoded
            pass

    return elements


def extract_http_metadata(elements: Dict[str, str]) -> HttpMetadata:
    """
    Parse response-head element for HTTP metadata.

    Args:
        elements: Parsed elements dict

    Returns:
        HttpMetadata with parsed values
    """
    result = HttpMetadata()

    response_head = elements.get("response-head", "")
    if not response_head:
        return result

    lines = response_head.split('\r\n')
    if not lines:
        lines = response_head.split('\n')

    # Parse status line (e.g., "HTTP/1.1 200 OK")
    if lines:
        status_line = lines[0]
        parts = status_line.split()
        if len(parts) >= 2:
            try:
                result.response_code = int(parts[1])
            except ValueError:
                pass

    # Parse headers
    for line in lines[1:]:
        if ':' not in line:
            continue
        header_name, _, header_value = line.partition(':')
        header_name = header_name.strip().lower()
        header_value = header_value.strip()

        if header_name == "content-type":
            # Take only the MIME type, ignore parameters like charset
            result.content_type = header_value.split(';')[0].strip()
        elif header_name == "content-encoding":
            result.content_encoding = header_value
        elif header_name == "content-length":
            try:
                result.content_length = int(header_value)
            except ValueError:
                pass
        elif header_name == "cache-control":
            result.cache_control = header_value
        elif header_name == "date":
            # Parse HTTP date format (e.g., "Thu, 01 Jan 2026 12:00:00 GMT")
            try:
                dt = parsedate_to_datetime(header_value)
                result.date = dt.isoformat()
            except (ValueError, TypeError):
                # Fallback: store raw value
                result.date = header_value
        elif header_name == "age":
            # Age is cache age in seconds
            try:
                result.age = int(header_value)
            except ValueError:
                pass
        elif header_name == "last-modified":
            # Parse HTTP date format
            try:
                dt = parsedate_to_datetime(header_value)
                result.last_modified = dt.isoformat()
            except (ValueError, TypeError):
                # Fallback: store raw value
                result.last_modified = header_value

    return result


def extract_url_from_key(key_str: str) -> Optional[str]:
    """
    Extract URL from Firefox cache2 key string.

    Key formats (Mozilla cache2 origin attributes):
    - Plain URL: "https://example.com/image.jpg"
    - Prefixed: ":/https://example.com/image.jpg"
    - Partition key: "O^partitionKey=...,:https://example.com/..."
    - Anonymous: "a,~1234,:http://example.com/..."
    - Complex: "O^partitionKey=%28https...%29,:https://..."

    Args:
        key_str: Raw key string from cache entry

    Returns:
        Extracted URL or None
    """
    if not key_str:
        return None

    # Strategy 1: Find the last occurrence of ,:http or ,:https
    # This handles partition keys and origin attributes
    for marker in (',:', ':,'):
        if marker + 'http' in key_str:
            idx = key_str.rfind(marker + 'http')
            if idx != -1:
                url_part = key_str[idx + len(marker):]
                # Clean up: take until first whitespace or null
                url = re.split(r'[\s\x00]', url_part)[0]
                if url.startswith(('http://', 'https://')):
                    return url

    # Strategy 2: Handle simple ":/" prefix
    if key_str.startswith(':/'):
        url_part = key_str[2:]
        # Remove any leading digits and comma (e.g., "0,")
        if url_part and url_part[0].isdigit() and ',' in url_part:
            url_part = url_part.split(',', 1)[1] if ',' in url_part else url_part
        url = re.split(r'[\s\x00]', url_part)[0]
        if url.startswith(('http://', 'https://')):
            return url

    # Strategy 3: Handle ":" prefix without slash
    if key_str.startswith(':') and not key_str.startswith(':/'):
        url_part = key_str[1:]
        url = re.split(r'[\s\x00]', url_part)[0]
        if url.startswith(('http://', 'https://')):
            return url

    # Strategy 4: Direct regex search for URL
    url_match = re.search(r'https?://[^\s\x00]+', key_str)
    if url_match:
        return url_match.group(0)

    # Strategy 5: If key looks like a URL directly
    if key_str.startswith(('http://', 'https://')):
        url = re.split(r'[\s\x00]', key_str)[0]
        return url

    LOGGER.debug("Could not extract URL from key: %s", key_str[:100] if len(key_str) > 100 else key_str)
    return None


def _is_image_content(content_type: Optional[str], url: Optional[str]) -> bool:
    """
    Determine if content is an image based on content type or URL extension.

    Args:
        content_type: HTTP Content-Type header value
        url: URL string

    Returns:
        True if content appears to be an image
    """
    if content_type:
        ct_lower = content_type.lower()
        if ct_lower.startswith("image/") or "svg" in ct_lower:
            return True

    if url:
        url_lower = url.lower()
        image_extensions = [".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".svg", ".ico", ".avif", ".heic"]
        if any(url_lower.endswith(ext) or f"{ext}?" in url_lower for ext in image_extensions):
            return True

    return False


# Backward compatibility: dict-based API
def empty_parse_result() -> dict:
    """Return empty parse result dict (for backward compatibility)."""
    return Cache2ParseResult().to_dict()
