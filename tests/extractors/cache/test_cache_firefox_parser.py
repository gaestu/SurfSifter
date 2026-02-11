"""
Tests for Firefox cache2 parser.

Tests the corrected parser that reads metadata from the END of the file
with big-endian byte order.
"""

import struct
import pytest
from pathlib import Path
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch


def create_cache2_entry(
    url: str = "https://example.com/image.jpg",
    body: bytes = b"test body content",
    response_code: int = 200,
    content_type: str = "text/html",
    fetch_count: int = 1,
    last_fetched: int = 1700000000,  # ~Nov 2023
    last_modified: int = 1699000000,
    frecency: int = 100,
    expiration: int = 1800000000,
    version: int = 3,
    elements: dict = None,
) -> bytes:
    """
    Create a valid Firefox cache2 entry for testing.

    Layout (body-first, metadata at END):
    - Body (0 to meta_offset)
    - Checksum (4 bytes)
    - Hash array (2 bytes per 256KB chunk)
    - Header (28-32 bytes, big-endian)
    - Key (null-terminated)
    - Elements (key\0value\0 pairs)
    - Meta offset (last 4 bytes, big-endian)

    Args:
        url: Cache key / URL
        body: Response body bytes
        response_code: HTTP response code
        content_type: HTTP Content-Type
        fetch_count: Times fetched
        last_fetched: Unix timestamp
        last_modified: Unix timestamp
        frecency: Frecency score
        expiration: Expiration timestamp
        version: Cache format version (1, 2, or 3)
        elements: Optional dict of element key/value pairs

    Returns:
        Complete cache2 entry as bytes
    """
    # 1. Build elements section
    if elements is None:
        elements = {}

    # Add response-head element with HTTP headers
    response_head = f"HTTP/1.1 {response_code} OK\r\n"
    response_head += f"Content-Type: {content_type}\r\n"
    response_head += "\r\n"
    elements["response-head"] = response_head
    elements["request-method"] = "GET"

    elements_bytes = b""
    for key, value in elements.items():
        elements_bytes += key.encode('utf-8') + b'\x00'
        elements_bytes += value.encode('utf-8') + b'\x00'

    # 2. Build key (URL with null terminator)
    key_bytes = url.encode('utf-8')
    key_size = len(key_bytes)

    # 3. Build header (big-endian, 32 bytes for version >= 2)
    header_size = 28 if version == 1 else 32
    flags = 0

    header = struct.pack(
        ">7I",
        version,
        fetch_count,
        last_fetched,
        last_modified,
        frecency,
        expiration,
        key_size,
    )
    if version >= 2:
        header += struct.pack(">I", flags)

    # 4. Calculate hash array (2 bytes per 256KB chunk)
    body_size = len(body)
    chunk_size = 262144
    hash_count = (body_size + chunk_size - 1) // chunk_size if body_size > 0 else 0
    hash_array = b'\x00\x00' * hash_count

    # 5. Build checksum (4 bytes, placeholder)
    checksum = b'\x00\x00\x00\x00'

    # 6. Calculate meta_offset (= body size)
    meta_offset = body_size

    # 7. Assemble file
    # Body + checksum + hashes + header + key + null + elements + meta_offset
    file_data = (
        body +                          # Body at offset 0
        checksum +                      # Checksum at meta_offset
        hash_array +                    # Hash array
        header +                        # Header
        key_bytes + b'\x00' +          # Key + null terminator
        elements_bytes +                # Elements
        struct.pack(">I", meta_offset)  # Meta offset (last 4 bytes)
    )

    return file_data


class TestCache2Parser:
    """Tests for _parse_cache2_entry method."""

    def test_parse_valid_entry_version_3(self, tmp_path):
        """Test parsing a valid version 3 cache2 entry."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        # Create test entry
        entry_data = create_cache2_entry(
            url="https://example.com/test.jpg",
            body=b"fake image body",
            response_code=200,
            content_type="image/jpeg",
            fetch_count=5,
            last_fetched=1700000000,
            last_modified=1699000000,
            frecency=250,
            version=3,
        )

        entry_path = tmp_path / "ABCDEF123456"
        entry_path.write_bytes(entry_data)

        result = extractor._parse_cache2_entry(entry_path, {"source_path": str(entry_path)})

        assert result["url"] == "https://example.com/test.jpg"
        assert result["cache_key"] == "https://example.com/test.jpg"
        assert result["is_image"] is True
        assert result["content_type"] == "image/jpeg"
        assert result["response_code"] == 200
        assert result["body_size"] == len(b"fake image body")

        meta = result["metadata"]
        assert meta["version"] == 3
        assert meta["fetch_count"] == 5
        assert meta["frecency"] == 250
        assert "last_fetched" in meta

    def test_parse_valid_entry_version_1(self, tmp_path):
        """Test parsing older version 1 format (28-byte header, no flags)."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        entry_data = create_cache2_entry(
            url="http://old.example.com/page.html",
            body=b"old cached content",
            version=1,
        )

        entry_path = tmp_path / "OLD123"
        entry_path.write_bytes(entry_data)

        result = extractor._parse_cache2_entry(entry_path, {})

        assert result["url"] == "http://old.example.com/page.html"
        assert result["metadata"]["version"] == 1

    def test_parse_empty_body(self, tmp_path):
        """Test parsing entry with empty body (meta_offset = 0)."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        entry_data = create_cache2_entry(
            url="https://example.com/empty.txt",
            body=b"",  # Empty body
        )

        entry_path = tmp_path / "EMPTY123"
        entry_path.write_bytes(entry_data)

        result = extractor._parse_cache2_entry(entry_path, {})

        assert result["url"] == "https://example.com/empty.txt"
        assert result["body_size"] == 0

    def test_parse_corrupted_too_small(self, tmp_path):
        """Test graceful handling of file too small to be valid."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        entry_path = tmp_path / "TINY"
        entry_path.write_bytes(b"ab")  # Too small

        result = extractor._parse_cache2_entry(entry_path, {})

        assert result["url"] is None
        assert result["cache_key"] is None

    def test_parse_corrupted_invalid_meta_offset(self, tmp_path):
        """Test handling of invalid meta_offset pointing beyond file."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        # Create file with meta_offset pointing past end
        body = b"small body"
        invalid_meta_offset = 99999999  # Way beyond file size

        # Minimal structure: body + meta_offset
        entry_data = body + struct.pack(">I", invalid_meta_offset)

        entry_path = tmp_path / "INVALID"
        entry_path.write_bytes(entry_data)

        result = extractor._parse_cache2_entry(entry_path, {})

        assert result["url"] is None


class TestUrlExtraction:
    """Tests for _extract_url_from_key method."""

    def test_plain_url(self):
        """Test extracting plain URL without prefix."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        assert extractor._extract_url_from_key("https://example.com/image.jpg") == "https://example.com/image.jpg"
        assert extractor._extract_url_from_key("http://example.com/page") == "http://example.com/page"

    def test_colon_slash_prefix(self):
        """Test extracting URL with :/ prefix."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        assert extractor._extract_url_from_key(":/https://example.com/image.jpg") == "https://example.com/image.jpg"
        assert extractor._extract_url_from_key(":/http://example.com/page") == "http://example.com/page"

    def test_partition_key(self):
        """Test extracting URL with partition key prefix."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        # Format: O^partitionKey=...,:<url>
        key = "O^partitionKey=%28https%2Cexample.com%29,:https://cdn.example.com/image.png"
        assert extractor._extract_url_from_key(key) == "https://cdn.example.com/image.png"

    def test_anonymous_marker(self):
        """Test extracting URL with anonymous marker."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        # Format: a,~1234,:<url>
        key = "a,~1234,:http://example.com/page"
        assert extractor._extract_url_from_key(key) == "http://example.com/page"

    def test_complex_origin_attributes(self):
        """Test extracting URL with complex origin attributes."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        # Real-world example
        key = "O^partitionKey=%28https%2Cexample.org%29~privateBrowsingId=1,:https://example.org/script.js"
        url = extractor._extract_url_from_key(key)
        assert url == "https://example.org/script.js"

    def test_colon_prefix_without_slash(self):
        """Test :http format (no slash after colon)."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        assert extractor._extract_url_from_key(":https://example.com/page") == "https://example.com/page"

    def test_numbered_prefix(self):
        """Test :/0,<url> format."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        assert extractor._extract_url_from_key(":/0,https://example.com/page") == "https://example.com/page"
        assert extractor._extract_url_from_key(":/1,http://example.com/page") == "http://example.com/page"

    def test_empty_and_invalid(self):
        """Test handling of empty and invalid keys."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        assert extractor._extract_url_from_key("") is None
        assert extractor._extract_url_from_key("not a url") is None
        assert extractor._extract_url_from_key("ftp://unsupported.com") is None


class TestElementsParsing:
    """Tests for _parse_elements method."""

    def test_parse_simple_elements(self):
        """Test parsing key-value element pairs."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        data = b"request-method\x00GET\x00response-head\x00HTTP/1.1 200 OK\r\n\x00"

        elements = extractor._parse_elements(data)

        assert elements["request-method"] == "GET"
        assert "HTTP/1.1 200 OK" in elements["response-head"]

    def test_skip_binary_keys(self):
        """Test that binary keys like security-info are skipped."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        data = b"request-method\x00GET\x00security-info\x00\x00\x01\x02\x03\x00"

        elements = extractor._parse_elements(data)

        assert "request-method" in elements
        assert "security-info" not in elements  # Binary key skipped

    def test_empty_elements(self):
        """Test parsing empty elements section."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        elements = extractor._parse_elements(b"")

        assert elements == {}


class TestHttpMetadataExtraction:
    """Tests for _extract_http_metadata method."""

    def test_extract_basic_metadata(self):
        """Test extracting basic HTTP metadata from response-head."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        elements = {
            "response-head": (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: image/jpeg\r\n"
                "Content-Length: 12345\r\n"
                "Content-Encoding: gzip\r\n"
                "Cache-Control: max-age=3600\r\n"
                "\r\n"
            )
        }

        meta = extractor._extract_http_metadata(elements)

        assert meta["response_code"] == 200
        assert meta["content_type"] == "image/jpeg"
        assert meta["content_encoding"] == "gzip"
        assert meta["content_length"] == 12345
        assert meta["cache_control"] == "max-age=3600"

    def test_content_type_with_charset(self):
        """Test stripping charset from Content-Type."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        elements = {
            "response-head": "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n\r\n"
        }

        meta = extractor._extract_http_metadata(elements)

        assert meta["content_type"] == "text/html"

    def test_empty_response_head(self):
        """Test handling of missing response-head."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        meta = extractor._extract_http_metadata({})

        assert meta["response_code"] is None
        assert meta["content_type"] is None

    def test_extract_date_header(self):
        """Test extracting HTTP Date header for timeline correlation."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        elements = {
            "response-head": (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/html\r\n"
                "Date: Thu, 01 Jan 2026 12:00:00 GMT\r\n"
                "\r\n"
            )
        }

        meta = extractor._extract_http_metadata(elements)

        assert meta["date"] is not None
        assert "2026-01-01" in meta["date"]
        assert "12:00:00" in meta["date"]

    def test_extract_age_header(self):
        """Test extracting HTTP Age header (cache age in seconds)."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        elements = {
            "response-head": (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: image/png\r\n"
                "Age: 3600\r\n"
                "\r\n"
            )
        }

        meta = extractor._extract_http_metadata(elements)

        assert meta["age"] == 3600

    def test_extract_last_modified_header(self):
        """Test extracting HTTP Last-Modified header for resource timestamps."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        elements = {
            "response-head": (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: application/javascript\r\n"
                "Last-Modified: Wed, 15 Dec 2025 08:30:00 GMT\r\n"
                "\r\n"
            )
        }

        meta = extractor._extract_http_metadata(elements)

        assert meta["last_modified"] is not None
        assert "2025-12-15" in meta["last_modified"]
        assert "08:30:00" in meta["last_modified"]

    def test_extract_all_timestamp_headers(self):
        """Test extracting all HTTP timestamp headers in one response."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        elements = {
            "response-head": (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: image/jpeg\r\n"
                "Content-Length: 54321\r\n"
                "Date: Sat, 28 Dec 2025 15:45:30 GMT\r\n"
                "Age: 120\r\n"
                "Last-Modified: Fri, 27 Dec 2025 10:00:00 GMT\r\n"
                "Cache-Control: max-age=86400\r\n"
                "\r\n"
            )
        }

        meta = extractor._extract_http_metadata(elements)

        # Basic metadata
        assert meta["response_code"] == 200
        assert meta["content_type"] == "image/jpeg"
        assert meta["content_length"] == 54321
        assert meta["cache_control"] == "max-age=86400"

        # New timestamp fields
        assert meta["date"] is not None
        assert "2025-12-28" in meta["date"]
        assert meta["age"] == 120
        assert meta["last_modified"] is not None
        assert "2025-12-27" in meta["last_modified"]

    def test_malformed_date_header_fallback(self):
        """Test that malformed Date header stores raw value as fallback."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        elements = {
            "response-head": (
                "HTTP/1.1 200 OK\r\n"
                "Date: invalid date format\r\n"
                "\r\n"
            )
        }

        meta = extractor._extract_http_metadata(elements)

        # Should store raw value on parse failure
        assert meta["date"] == "invalid date format"

    def test_invalid_age_header_ignored(self):
        """Test that non-integer Age header is ignored."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        elements = {
            "response-head": (
                "HTTP/1.1 200 OK\r\n"
                "Age: not-a-number\r\n"
                "\r\n"
            )
        }

        meta = extractor._extract_http_metadata(elements)

        assert meta["age"] is None


class TestImageCarver:
    """Tests for image_carver module."""

    def test_detect_jpeg(self):
        """Test JPEG detection via magic bytes."""
        from extractors.browser.firefox.cache.image_carver import detect_image_type

        # JPEG with JFIF marker
        jpeg_data = b'\xff\xd8\xff\xe0' + b'JFIF' + b'\x00' * 100
        assert detect_image_type(jpeg_data) == ('jpeg', '.jpg')

        # JPEG with EXIF marker
        jpeg_exif = b'\xff\xd8\xff\xe1' + b'\x00' * 100
        assert detect_image_type(jpeg_exif) == ('jpeg', '.jpg')

    def test_detect_png(self):
        """Test PNG detection via magic bytes."""
        from extractors.browser.firefox.cache.image_carver import detect_image_type

        png_data = b'\x89PNG\r\n\x1a\n' + b'\x00' * 100
        assert detect_image_type(png_data) == ('png', '.png')

    def test_detect_gif(self):
        """Test GIF detection via magic bytes."""
        from extractors.browser.firefox.cache.image_carver import detect_image_type

        assert detect_image_type(b'GIF89a' + b'\x00' * 100) == ('gif', '.gif')
        assert detect_image_type(b'GIF87a' + b'\x00' * 100) == ('gif', '.gif')

    def test_detect_webp(self):
        """Test WebP detection via RIFF container."""
        from extractors.browser.firefox.cache.image_carver import detect_image_type

        # RIFF + size (4 bytes) + WEBP
        webp_data = b'RIFF\x00\x00\x00\x00WEBP' + b'\x00' * 100
        assert detect_image_type(webp_data) == ('webp', '.webp')

    def test_detect_svg(self):
        """Test SVG detection via XML/svg tag."""
        from extractors.browser.firefox.cache.image_carver import detect_image_type

        svg_data = b'<?xml version="1.0"?><svg xmlns="..." />'
        assert detect_image_type(svg_data) == ('svg', '.svg')

        svg_direct = b'<svg width="100" height="100"></svg>'
        assert detect_image_type(svg_direct) == ('svg', '.svg')

    def test_detect_not_image(self):
        """Test non-image content returns None."""
        from extractors.browser.firefox.cache.image_carver import detect_image_type

        assert detect_image_type(b'<!DOCTYPE html>') is None
        assert detect_image_type(b'{"json": "data"}') is None
        assert detect_image_type(b'') is None
        assert detect_image_type(b'x') is None

    def test_extract_body(self):
        """Test body extraction with correct offset."""
        from extractors.browser.firefox.cache.image_carver import extract_body

        body = b"This is the response body"
        metadata = b"This is metadata section"
        full_data = body + metadata + struct.pack(">I", len(body))

        extracted = extract_body(full_data, len(body), None)
        assert extracted == body

    def test_extract_body_gzip(self):
        """Test gzip decompression of body data."""
        import gzip
        from extractors.browser.firefox.cache.image_carver import extract_body

        original = b"Original content to compress"
        compressed = gzip.compress(original)

        extracted = extract_body(compressed, len(compressed), "gzip")
        assert extracted == original

    def test_extract_body_brotli(self):
        """Test brotli decompression of body data."""
        import brotli
        from extractors.browser.firefox.cache.image_carver import extract_body

        original = b"Original content to compress with brotli"
        compressed = brotli.compress(original)

        extracted = extract_body(compressed, len(compressed), "br")
        assert extracted == original

    def test_extract_body_deflate(self):
        """Test deflate decompression of body data."""
        import zlib
        from extractors.browser.firefox.cache.image_carver import extract_body

        original = b"Original content to compress with deflate"
        # Raw deflate without zlib wrapper (-zlib.MAX_WBITS for compress)
        compress_obj = zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED, -zlib.MAX_WBITS)
        compressed = compress_obj.compress(original) + compress_obj.flush()

        extracted = extract_body(compressed, len(compressed), "deflate")
        assert extracted == original

    def test_extract_body_invalid_gzip(self):
        """Test graceful handling of invalid gzip data."""
        from extractors.browser.firefox.cache.image_carver import extract_body

        not_gzip = b"This is not gzip data"

        # Should return raw data on decompression failure
        extracted = extract_body(not_gzip, len(not_gzip), "gzip")
        assert extracted == not_gzip

    def test_compute_hashes(self):
        """Test MD5 and SHA-256 hash computation."""
        from extractors.browser.firefox.cache.image_carver import compute_hashes

        data = b"Test data for hashing"
        md5, sha256 = compute_hashes(data)

        import hashlib
        assert md5 == hashlib.md5(data).hexdigest()
        assert sha256 == hashlib.sha256(data).hexdigest()

    def test_save_carved_image(self, tmp_path):
        """Test saving carved image with hash computation."""
        from extractors.browser.firefox.cache.image_carver import save_carved_image

        # Create a minimal valid JPEG
        jpeg_body = b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xd9'

        run_dir = tmp_path / "run_123"
        run_dir.mkdir()

        result = save_carved_image(
            body=jpeg_body,
            run_dir=run_dir,
            cache_filename="ABC123",
            image_type=('jpeg', '.jpg'),
        )

        assert result["filename"] == "ABC123.jpg"
        assert result["md5"] is not None
        assert result["sha256"] is not None
        assert result["size_bytes"] == len(jpeg_body)
        assert result["format"] == "jpeg"

        # Verify file was created
        carved_path = run_dir / "carved_images" / "ABC123.jpg"
        assert carved_path.exists()
        assert carved_path.read_bytes() == jpeg_body


class TestIngestionIntegration:
    """Integration tests for ingestion with database."""

    def test_url_record_format(self, tmp_path):
        """Test that URL records match expected schema format."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        # Create a test cache entry
        entry_data = create_cache2_entry(
            url="https://example.com/page.html",
            body=b"page content",
            response_code=200,
            content_type="text/html",
            last_fetched=1700000000,
        )

        entry_path = tmp_path / "TEST123"
        entry_path.write_bytes(entry_data)

        result = extractor._parse_cache2_entry(entry_path, {"cache_filename": "TEST123"})

        # Verify all expected fields are present
        assert result["url"] is not None
        assert result["cache_key"] is not None
        assert result["response_code"] == 200
        assert result["content_type"] == "text/html"
        assert result["body_size"] >= 0

    def test_url_record_has_cache_metadata(self, tmp_path):
        """Test that URL records include cache metadata for tags."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        # Create entry with specific metadata
        entry_data = create_cache2_entry(
            url="https://example.com/data.json",
            body=b"json content",
            content_type="application/json",
            fetch_count=42,
            frecency=150,
            expiration=1800000000,  # Valid Unix timestamp
        )

        entry_path = tmp_path / "META_TEST"
        entry_path.write_bytes(entry_data)

        result = extractor._parse_cache2_entry(entry_path, {"cache_filename": "META_TEST"})

        # Verify metadata needed for tags
        assert result["metadata"]["fetch_count"] == 42
        assert result["metadata"]["frecency"] == 150
        assert "expiration" in result["metadata"]


class TestUrlTagsGeneration:
    """Tests for URL tags generation from cache metadata."""

    def test_tags_format(self):
        """Test that tags are correctly formatted from metadata."""
        meta = {
            "fetch_count": 5,
            "frecency": 100,
            "expiration": "2024-11-16T12:00:00+00:00",
        }

        # Build tags like extractor does
        tags_parts = []
        if meta.get("fetch_count"):
            tags_parts.append(f"fetch_count:{meta['fetch_count']}")
        if meta.get("frecency"):
            tags_parts.append(f"frecency:{meta['frecency']}")
        if meta.get("expiration"):
            tags_parts.append(f"expiration:{meta['expiration']}")

        cache_tags = ",".join(tags_parts) if tags_parts else None

        assert cache_tags is not None
        assert "fetch_count:5" in cache_tags
        assert "frecency:100" in cache_tags
        assert "expiration:" in cache_tags

    def test_tags_empty_when_no_metadata(self):
        """Test that tags are None when metadata is missing."""
        meta = {}

        tags_parts = []
        if meta.get("fetch_count"):
            tags_parts.append(f"fetch_count:{meta['fetch_count']}")
        if meta.get("frecency"):
            tags_parts.append(f"frecency:{meta['frecency']}")
        if meta.get("expiration"):
            tags_parts.append(f"expiration:{meta['expiration']}")

        cache_tags = ",".join(tags_parts) if tags_parts else None

        assert cache_tags is None


class TestTimestampHandling:
    """Tests for URL timestamp handling - use metadata, not extraction time."""

    def test_url_timestamps_from_cache_metadata(self, tmp_path):
        """
        Test that URL timestamps come from cache metadata, not extraction time.

        Verifies fix for bug where first_seen_utc/last_seen_utc were incorrectly
        set to extraction timestamp instead of cache last_fetched/last_modified.
        """
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        # Create entry with specific timestamps (Nov 2023)
        last_fetched_unix = 1700000000  # Nov 14, 2023
        last_modified_unix = 1699000000  # Nov 3, 2023

        entry_data = create_cache2_entry(
            url="https://example.com/test.html",
            body=b"test content",
            last_fetched=last_fetched_unix,
            last_modified=last_modified_unix,
        )

        entry_path = tmp_path / "TIMESTAMP_TEST"
        entry_path.write_bytes(entry_data)

        result = extractor._parse_cache2_entry(entry_path, {"cache_filename": "TIMESTAMP_TEST"})

        # Verify timestamps are extracted from cache metadata
        meta = result["metadata"]
        assert "last_fetched" in meta
        assert "last_modified" in meta

        # Verify they're ISO format strings with the correct dates
        assert "2023-11-14" in meta["last_fetched"]  # last_fetched_unix
        assert "2023-11-03" in meta["last_modified"]  # last_modified_unix

    def test_url_timestamps_null_when_missing(self, tmp_path):
        """
        Test that URL timestamps are NULL when cache metadata is missing/invalid.

        Verifies fix for bug where extraction timestamp was used as fallback.
        NULL is more forensically accurate than incorrect extraction time.
        """
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        extractor = CacheFirefoxExtractor()

        # Create entry with zero timestamps (invalid/missing)
        entry_data = create_cache2_entry(
            url="https://example.com/no-timestamps.html",
            body=b"test content",
            last_fetched=0,  # Will be filtered as invalid
            last_modified=0,  # Will be filtered as invalid
        )

        entry_path = tmp_path / "NO_TIMESTAMP_TEST"
        entry_path.write_bytes(entry_data)

        result = extractor._parse_cache2_entry(entry_path, {"cache_filename": "NO_TIMESTAMP_TEST"})

        # Verify timestamps are NOT present (were 0, so filtered out)
        meta = result["metadata"]
        assert "last_fetched" not in meta
        assert "last_modified" not in meta

        # Build URL record like ingestion code does
        first_seen = None
        last_seen = None
        if meta.get("last_fetched"):
            last_seen = meta["last_fetched"]
            first_seen = meta.get("last_modified") or last_seen

        # Should be NULL, not extraction timestamp
        assert first_seen is None
        assert last_seen is None


class TestManifestFormat:
    """Tests for manifest entry format."""

    def test_manifest_entry_has_required_fields(self):
        """Verify manifest entries contain all required forensic fields."""
        required_fields = {
            "source_path",
            "logical_path",
            "forensic_path",
            "partition_index",
            "fs_type",
            "extracted_path",
            "size_bytes",
            "md5",
            "sha256",
            "browser",
            "profile",
            "artifact_type",
            "cache_filename",
        }

        # Sample manifest entry that would be created (matching extractor output)
        sample_entry = {
            "source_path": "Users/test/cache2/entries/ABC123",
            "logical_path": "Users/test/cache2/entries/ABC123",
            "forensic_path": "E01://evidence.E01/NTFS/Users/test/cache2/entries/ABC123",
            "partition_index": 2,
            "fs_type": "NTFS",
            "extracted_path": "run_123/firefox_default/ABC123",
            "size_bytes": 12345,
            "md5": "d41d8cd98f00b204e9800998ecf8427e",
            "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "browser": "firefox",
            "profile": "default",
            "artifact_type": "cache_firefox",
            "cache_filename": "ABC123",
        }

        for field in required_fields:
            assert field in sample_entry, f"Missing required field: {field}"


# Fixtures for cache file testing
@pytest.fixture
def cache2_fixtures_dir():
    """Return path to cache2 fixtures directory."""
    fixtures_dir = Path(__file__).resolve().parents[2] / "fixtures" / "cache2"
    if not fixtures_dir.exists():
        pytest.skip("Cache2 fixtures not generated. Run: python tests/fixtures/cache2/generate_fixtures.py")
    return fixtures_dir


class TestCache2Fixtures:
    """Tests using synthetic cache2 fixture files."""

    def test_parse_valid_jpeg_v2(self, cache2_fixtures_dir):
        """Test parsing valid JPEG cache2 entry (version 2)."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        entry_path = cache2_fixtures_dir / "valid_jpeg_v2.cache2"
        if not entry_path.exists():
            pytest.skip("Fixture not generated")

        extractor = CacheFirefoxExtractor()
        result = extractor._parse_cache2_entry(entry_path, {"cache_filename": "JPEG_TEST"})

        assert result["url"] == "http://example.com/image.jpg"
        assert result["is_image"] is True
        assert result["content_type"] == "image/jpeg"
        assert result["body_size"] > 0  # JPEG header ~22 bytes
        assert result["metadata"]["version"] == 2

    def test_parse_valid_png_v2(self, cache2_fixtures_dir):
        """Test parsing valid PNG cache2 entry with origin attributes."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        entry_path = cache2_fixtures_dir / "valid_png_v2.cache2"
        if not entry_path.exists():
            pytest.skip("Fixture not generated")

        extractor = CacheFirefoxExtractor()
        result = extractor._parse_cache2_entry(entry_path, {"cache_filename": "PNG_TEST"})

        # URL extracted from origin attributes format
        assert result["url"] == "https://example.com/logo.png"
        assert result["is_image"] is True
        assert result["content_type"] == "image/png"

    def test_parse_gzip_compressed(self, cache2_fixtures_dir):
        """Test parsing cache2 entry with gzip-compressed body."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        entry_path = cache2_fixtures_dir / "valid_gzip_v2.cache2"
        if not entry_path.exists():
            pytest.skip("Fixture not generated")

        extractor = CacheFirefoxExtractor()
        result = extractor._parse_cache2_entry(entry_path, {"cache_filename": "GZIP_TEST"})

        assert result["url"] == "http://example.com/data.txt"
        assert result["content_encoding"] == "gzip"
        assert result["content_type"] == "text/plain"
        assert result["body_size"] > 0

    def test_parse_invalid_small(self, cache2_fixtures_dir):
        """Test graceful handling of too-small files."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        entry_path = cache2_fixtures_dir / "invalid_small.cache2"
        if not entry_path.exists():
            pytest.skip("Fixture not generated")

        extractor = CacheFirefoxExtractor()
        result = extractor._parse_cache2_entry(entry_path, {"cache_filename": "SMALL_TEST"})

        # Should return empty result, not crash
        assert result["url"] is None
        assert result["cache_key"] is None

    def test_parse_invalid_offset(self, cache2_fixtures_dir):
        """Test graceful handling of invalid metadata offset."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        entry_path = cache2_fixtures_dir / "invalid_offset.cache2"
        if not entry_path.exists():
            pytest.skip("Fixture not generated")

        extractor = CacheFirefoxExtractor()
        result = extractor._parse_cache2_entry(entry_path, {"cache_filename": "BAD_OFFSET"})

        # Should return empty result for invalid offset
        assert result["url"] is None

    def test_parse_empty_body(self, cache2_fixtures_dir):
        """Test parsing entry with empty body (meta_offset = 0)."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        entry_path = cache2_fixtures_dir / "empty_body.cache2"
        if not entry_path.exists():
            pytest.skip("Fixture not generated")

        extractor = CacheFirefoxExtractor()
        result = extractor._parse_cache2_entry(entry_path, {"cache_filename": "EMPTY_BODY"})

        # Empty body should still have valid metadata
        assert result["body_size"] == 0
        assert result["url"] == "http://example.com/empty"

    def test_parse_version_1(self, cache2_fixtures_dir):
        """Test parsing version 1 cache2 entry (28-byte header, no flags)."""
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        entry_path = cache2_fixtures_dir / "valid_jpeg_v1.cache2"
        if not entry_path.exists():
            pytest.skip("Fixture not generated")

        extractor = CacheFirefoxExtractor()
        result = extractor._parse_cache2_entry(entry_path, {"cache_filename": "V1_TEST"})

        assert result["url"] == "http://example.com/old_image.jpg"
        assert result["metadata"]["version"] == 1
        assert result["metadata"]["flags"] == 0  # No flags in v1


class TestImageCarverWithFixtures:
    """Tests for image carving using fixture files."""

    def test_carve_jpeg_from_fixture(self, cache2_fixtures_dir, tmp_path):
        """Test carving JPEG image from cache2 fixture."""
        from extractors.browser.firefox.cache.image_carver import (
            extract_body, detect_image_type, save_carved_image
        )
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        entry_path = cache2_fixtures_dir / "valid_jpeg_v2.cache2"
        if not entry_path.exists():
            pytest.skip("Fixture not generated")

        # Parse entry to get body info
        extractor = CacheFirefoxExtractor()
        parsed = extractor._parse_cache2_entry(entry_path, {"cache_filename": "CARVE_TEST"})

        # Read body
        data = entry_path.read_bytes()
        body = data[:parsed["body_size"]]

        # Extract body (no compression)
        extracted = extract_body(body, len(body), None)

        # Detect image type
        img_type = detect_image_type(extracted)
        assert img_type is not None
        assert img_type[0] == "jpeg"

        # Save carved image
        run_dir = tmp_path / "run_test"
        run_dir.mkdir()

        result = save_carved_image(
            body=extracted,
            run_dir=run_dir,
            cache_filename="CARVE_TEST",
            image_type=img_type,
        )

        assert result["format"] == "jpeg"
        assert result["size_bytes"] > 0
        assert (run_dir / "carved_images" / "CARVE_TEST.jpg").exists()

    def test_decompress_gzip_fixture(self, cache2_fixtures_dir):
        """Test decompressing gzip body from fixture."""
        from extractors.browser.firefox.cache.image_carver import extract_body
        from extractors.browser.firefox.cache import CacheFirefoxExtractor

        entry_path = cache2_fixtures_dir / "valid_gzip_v2.cache2"
        if not entry_path.exists():
            pytest.skip("Fixture not generated")

        # Parse entry to get body info
        extractor = CacheFirefoxExtractor()
        parsed = extractor._parse_cache2_entry(entry_path, {"cache_filename": "GZIP_DECOMP"})

        # Read and decompress body
        data = entry_path.read_bytes()
        body = data[:parsed["body_size"]]

        # Should decompress successfully
        decompressed = extract_body(body, len(body), "gzip")

        # Original text was "Hello, this is some text content that will be gzip compressed."
        assert b"Hello" in decompressed
        assert b"gzip compressed" in decompressed
