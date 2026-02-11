#!/usr/bin/env python3
"""
Generate synthetic Firefox cache2 test fixtures.

Firefox cache2 format (body-first):
- Body starts at offset 0
- Metadata at END of file
- Last 4 bytes: big-endian uint32 meta_offset

Usage:
    python generate_fixtures.py
"""

import gzip
import struct
from pathlib import Path


def create_cache2_entry(
    body: bytes,
    cache_key: str,
    version: int = 2,
    fetch_count: int = 1,
    last_fetched: int = 1700000000,
    last_modified: int = 0,
    frecency: int = 1000,
    expiration: int = 0xFFFFFFFF,
    flags: int = 0,
    elements: dict[str, str] | None = None,
) -> bytes:
    """
    Create a synthetic cache2 entry file.

    Args:
        body: Response body bytes
        cache_key: Cache key (URL or :scheme:host:port:path format)
        version: Cache format version (1, 2, 3)
        fetch_count: Number of fetches
        last_fetched: Unix timestamp
        last_modified: Unix timestamp
        frecency: Firefox frecency score
        expiration: Unix timestamp or 0xFFFFFFFF
        flags: Entry flags (v2+)
        elements: Key-value pairs for metadata section

    Returns:
        Complete cache2 entry as bytes
    """
    if elements is None:
        elements = {}

    # Calculate hash array size (2 bytes per 256KB chunk)
    chunk_size = 262144
    hash_count = (len(body) + chunk_size - 1) // chunk_size if len(body) > 0 else 0
    hashes = b'\x00\x00' * hash_count  # Dummy hashes

    # Build header (28 bytes for v1, 32 bytes for v2+)
    key_bytes = cache_key.encode('utf-8')
    key_size = len(key_bytes)

    header = struct.pack(
        ">7I",  # BIG ENDIAN, 7 uint32s
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

    # Build elements section (key\0value\0 pairs)
    elements_bytes = b''
    for k, v in elements.items():
        elements_bytes += k.encode('utf-8') + b'\x00'
        elements_bytes += v.encode('utf-8') + b'\x00'

    # Assemble metadata section
    checksum = b'\x00\x00\x00\x00'  # Dummy checksum
    key_with_null = key_bytes + b'\x00'
    metadata = checksum + hashes + header + key_with_null + elements_bytes

    # meta_offset points to start of checksum
    meta_offset = len(body)

    # Final assembly: body + metadata + meta_offset
    entry = body + metadata + struct.pack(">I", meta_offset)

    return entry


def main():
    """Generate all test fixtures."""
    output_dir = Path(__file__).parent

    # Minimal valid JPEG (smallest possible)
    jpeg_header = bytes([
        0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10,
        0x4A, 0x46, 0x49, 0x46, 0x00,  # JFIF
        0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00,
        0xFF, 0xD9  # EOI
    ])

    # Minimal PNG (1x1 transparent)
    png_header = bytes([
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A,  # PNG signature
        0x00, 0x00, 0x00, 0x0D,  # IHDR length
        0x49, 0x48, 0x44, 0x52,  # IHDR
        0x00, 0x00, 0x00, 0x01,  # width: 1
        0x00, 0x00, 0x00, 0x01,  # height: 1
        0x08, 0x06,  # bit depth: 8, color type: RGBA
        0x00, 0x00, 0x00,  # compression, filter, interlace
        0x1F, 0x15, 0xC4, 0x89,  # CRC
        0x00, 0x00, 0x00, 0x0A,  # IDAT length
        0x49, 0x44, 0x41, 0x54,  # IDAT
        0x78, 0x9C, 0x62, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01,
        0x48, 0xAF, 0x04, 0x71,  # CRC placeholder
        0x00, 0x00, 0x00, 0x00,  # IEND length
        0x49, 0x45, 0x4E, 0x44,  # IEND
        0xAE, 0x42, 0x60, 0x82,  # IEND CRC
    ])

    # 1. Valid JPEG cache2 entry
    jpeg_entry = create_cache2_entry(
        body=jpeg_header,
        cache_key="http://example.com/image.jpg",
        elements={
            "response-head": "HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\n",
        }
    )
    (output_dir / "valid_jpeg_v2.cache2").write_bytes(jpeg_entry)
    print(f"Created valid_jpeg_v2.cache2 ({len(jpeg_entry)} bytes)")

    # 2. Valid PNG cache2 entry with origin attributes
    png_entry = create_cache2_entry(
        body=png_header,
        cache_key="O^partitionKey=%28https%2Cexample.com%29,:https://example.com/logo.png",
        elements={
            "response-head": "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\n",
        }
    )
    (output_dir / "valid_png_v2.cache2").write_bytes(png_entry)
    print(f"Created valid_png_v2.cache2 ({len(png_entry)} bytes)")

    # 3. Gzip-compressed body
    original_text = b"Hello, this is some text content that will be gzip compressed."
    gzip_body = gzip.compress(original_text)
    gzip_entry = create_cache2_entry(
        body=gzip_body,
        cache_key="http://example.com/data.txt",
        elements={
            "response-head": "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Encoding: gzip\r\n",
        }
    )
    (output_dir / "valid_gzip_v2.cache2").write_bytes(gzip_entry)
    print(f"Created valid_gzip_v2.cache2 ({len(gzip_entry)} bytes)")

    # 4. Invalid: too small
    (output_dir / "invalid_small.cache2").write_bytes(b"\x00\x00")
    print("Created invalid_small.cache2 (2 bytes)")

    # 5. Invalid: bad meta_offset (points past end of file)
    bad_offset = b"Some body content"
    bad_offset += struct.pack(">I", 0xFFFFFF00)  # Invalid offset
    (output_dir / "invalid_offset.cache2").write_bytes(bad_offset)
    print(f"Created invalid_offset.cache2 ({len(bad_offset)} bytes)")

    # 6. Empty body
    empty_entry = create_cache2_entry(
        body=b"",
        cache_key="http://example.com/empty",
    )
    (output_dir / "empty_body.cache2").write_bytes(empty_entry)
    print(f"Created empty_body.cache2 ({len(empty_entry)} bytes)")

    # 7. Version 1 header (no flags field)
    v1_entry = create_cache2_entry(
        body=jpeg_header,
        cache_key="http://example.com/old_image.jpg",
        version=1,
        elements={
            "response-head": "HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\n",
        }
    )
    (output_dir / "valid_jpeg_v1.cache2").write_bytes(v1_entry)
    print(f"Created valid_jpeg_v1.cache2 ({len(v1_entry)} bytes)")

    print("\nAll fixtures generated successfully!")


if __name__ == "__main__":
    main()
