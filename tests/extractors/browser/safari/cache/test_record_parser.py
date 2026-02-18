"""Tests for WebKitCache / NetworkCache record parser."""

from __future__ import annotations

import struct
from pathlib import Path

from extractors.browser.safari.cache._record_parser import (
    WebKitCacheRecord,
    _read_lf_string,
    _status_text_to_code,
    get_blob_path,
    parse_webkit_cache_dir,
    parse_webkit_cache_record,
)


def _build_lf_string(s: str) -> bytes:
    """Build a length-flag-string: [uint32 len] [byte 0x01] [data]."""
    encoded = s.encode("utf-8")
    return struct.pack("<I", len(encoded)) + b"\x01" + encoded


def _build_v12_record(
    url: str,
    *,
    partition: str = "example.com",
    record_type: str = "Resource",
    status_text: str = "OK",
    http_version: str = "HTTP/1.1",
    headers: dict[str, str] | None = None,
) -> bytes:
    """Build a synthetic WebKitCache Version 12 binary record.

    Matches the real on-disk format:
      uint32 version + lf_string partition + lf_string type + lf_string URL
      + 0xFFFFFFFF key_hash_marker + 20-byte SHA1 + metadata padding
      + 0xFFFFFFFF response_marker + lf_string status + lf_string version
      + uint32 header_count + uint32 unknown + [lf_string key + lf_string val] ...
    """
    parts = []

    # Version
    parts.append(struct.pack("<I", 12))

    # Key fields
    parts.append(_build_lf_string(partition))
    parts.append(_build_lf_string(record_type))
    parts.append(_build_lf_string(url))

    # First marker (key hash section)
    parts.append(b"\xff\xff\xff\xff")
    parts.append(b"\x00" * 20)  # fake SHA1
    parts.append(b"\x00" * 40)  # metadata padding

    # Second URL copy (like real records)
    parts.append(_build_lf_string(url))
    parts.append(b"\x01\x01")  # flags
    parts.append(b"\x00" * 32)  # padding/metadata

    # Response section marker
    parts.append(b"\xff\xff\xff\xff")

    # Status text
    parts.append(_build_lf_string(status_text))

    # HTTP version
    parts.append(_build_lf_string(http_version))

    # Headers
    hdrs = headers or {}
    parts.append(struct.pack("<I", len(hdrs)))
    parts.append(struct.pack("<I", 0))  # unknown/padding

    for k, v in hdrs.items():
        parts.append(_build_lf_string(k))
        parts.append(_build_lf_string(v))

    return b"".join(parts)


def _build_record_bytes(url: str, headers: dict[str, str] | None = None) -> bytes:
    """Build a minimal WebKitCache binary record with a URL and optional headers.

    Layout: uint32 key_length + URL bytes + padding + optional header block.
    """
    url_bytes = url.encode("utf-8")
    key_len = struct.pack("<I", len(url_bytes))

    # Add padding (timestamp placeholder + misc metadata)
    padding = b"\x00" * 32

    header_block = b""
    if headers:
        count = struct.pack("<I", len(headers))
        parts = []
        for k, v in headers.items():
            kb = k.encode("utf-8")
            vb = v.encode("utf-8")
            parts.append(struct.pack("<I", len(kb)) + kb + struct.pack("<I", len(vb)) + vb)
        header_block = count + b"".join(parts)

    return key_len + url_bytes + padding + header_block


def test_parse_record_with_url() -> None:
    data = _build_record_bytes("https://www.example.com/page.html")
    record = parse_webkit_cache_record(
        data,
        record_hash="ABCDEF1234",
        partition="PARTITION1",
        record_type="Resource",
        source_path="/some/path",
    )
    assert record is not None
    assert record.url == "https://www.example.com/page.html"
    assert record.record_hash == "ABCDEF1234"
    assert record.partition == "PARTITION1"
    assert record.record_type == "Resource"


def test_parse_record_with_headers() -> None:
    headers = {
        "Content-Type": "text/html; charset=utf-8",
        "Server": "nginx",
        "Cache-Control": "max-age=3600",
    }
    data = _build_record_bytes("https://example.com/styles.css", headers=headers)
    record = parse_webkit_cache_record(
        data,
        record_hash="ABC123",
        partition="P1",
        record_type="Resource",
        source_path="/test",
    )
    assert record is not None
    assert record.url == "https://example.com/styles.css"
    assert record.content_type == "text/html; charset=utf-8"
    assert record.headers.get("Server") == "nginx"


def test_parse_record_invalid_data_returns_none() -> None:
    assert parse_webkit_cache_record(b"", "x", "p", "Resource", "/test") is None
    assert parse_webkit_cache_record(b"\x00" * 10, "x", "p", "Resource", "/test") is None
    assert parse_webkit_cache_record(b"random garbage data here", "x", "p", "Resource", "/test") is None


def test_parse_record_scan_fallback() -> None:
    """When structured parse fails, scanning should find the URL."""
    # Build data where the key_length field is wrong, but URL pattern exists
    garbage = b"\xff\xff\xff\xff" + b"\x00" * 20 + b"https://scan.example.com/found" + b"\x00" * 20
    record = parse_webkit_cache_record(
        garbage,
        record_hash="SCAN1",
        partition="P1",
        record_type="SubResources",
        source_path="/test",
    )
    assert record is not None
    assert record.url == "https://scan.example.com/found"
    assert record.record_type == "SubResources"


def test_parse_record_blob_companion() -> None:
    data = _build_record_bytes("https://example.com/img.png")
    record = parse_webkit_cache_record(
        data,
        record_hash="BLOBTEST",
        partition="P1",
        record_type="Resource",
        source_path="/test",
        has_blob_companion=True,
    )
    assert record is not None
    assert record.has_blob is True


def test_parse_webkit_cache_dir(tmp_path: Path) -> None:
    """Test parsing a full WebKitCache directory structure."""
    version_dir = tmp_path / "Version 12"
    partition = "3A733D0CD0EF90FA8C002DBD"
    records_dir = version_dir / "Records" / partition / "Resource"
    records_dir.mkdir(parents=True)

    # Create a record file
    record_data = _build_record_bytes(
        "https://example.com/test.js",
        headers={"Content-Type": "application/javascript"},
    )
    (records_dir / "ABC123").write_bytes(record_data)

    # Create a companion blob
    (records_dir / "ABC123-blob").write_bytes(b"console.log('hello');")

    # Create another record without blob
    record_data2 = _build_record_bytes("https://example.com/style.css")
    (records_dir / "DEF456").write_bytes(record_data2)

    records = parse_webkit_cache_dir(version_dir)
    assert len(records) >= 1

    # Find the record with blob companion
    rec_with_blob = [r for r in records if r.record_hash == "ABC123"]
    assert len(rec_with_blob) == 1
    assert rec_with_blob[0].has_blob is True
    assert rec_with_blob[0].url == "https://example.com/test.js"


def test_parse_webkit_cache_dir_subresources(tmp_path: Path) -> None:
    """Test parsing SubResources directory."""
    version_dir = tmp_path / "Version 12"
    partition = "DEADBEEF"
    sub_dir = version_dir / "Records" / partition / "SubResources"
    sub_dir.mkdir(parents=True)

    data = _build_record_bytes("https://example.com/sub.html")
    (sub_dir / "SUB001").write_bytes(data)

    records = parse_webkit_cache_dir(version_dir)
    assert len(records) >= 1
    sub_recs = [r for r in records if r.record_type == "SubResources"]
    assert len(sub_recs) >= 1
    assert sub_recs[0].url == "https://example.com/sub.html"


def test_get_blob_path_companion(tmp_path: Path) -> None:
    """Test finding companion -blob file."""
    version_dir = tmp_path / "Version 12"
    partition = "PART1"
    records_dir = version_dir / "Records" / partition / "Resource"
    records_dir.mkdir(parents=True)

    blob_file = records_dir / "HASH123-blob"
    blob_file.write_bytes(b"blob data")

    record = WebKitCacheRecord(
        record_hash="HASH123",
        url="https://example.com/test",
        partition=partition,
        record_type="Resource",
        timestamp_utc=None,
        http_status=None,
        content_type=None,
        content_length=None,
        headers={},
        has_blob=True,
        body_offset=None,
        body_size=None,
        source_path="/test",
    )

    path = get_blob_path(version_dir, record)
    assert path is not None
    assert path == blob_file


def test_get_blob_path_blobs_dir(tmp_path: Path) -> None:
    """Test finding blob in shared Blobs directory."""
    version_dir = tmp_path / "Version 12"
    blobs_dir = version_dir / "Blobs"
    blobs_dir.mkdir(parents=True)

    # Also need the Records dir even if empty
    records_dir = version_dir / "Records" / "PART1" / "Resource"
    records_dir.mkdir(parents=True)

    blob_file = blobs_dir / "HASH456"
    blob_file.write_bytes(b"shared blob data")

    record = WebKitCacheRecord(
        record_hash="HASH456",
        url="https://example.com/img",
        partition="PART1",
        record_type="Resource",
        timestamp_utc=None,
        http_status=None,
        content_type=None,
        content_length=None,
        headers={},
        has_blob=False,
        body_offset=None,
        body_size=None,
        source_path="/test",
    )

    path = get_blob_path(version_dir, record)
    assert path is not None
    assert path == blob_file


def test_get_blob_path_returns_none(tmp_path: Path) -> None:
    """No blob exists for this record."""
    version_dir = tmp_path / "Version 12"
    records_dir = version_dir / "Records" / "PART1" / "Resource"
    records_dir.mkdir(parents=True)

    record = WebKitCacheRecord(
        record_hash="NOBLOB",
        url="https://example.com/test",
        partition="PART1",
        record_type="Resource",
        timestamp_utc=None,
        http_status=None,
        content_type=None,
        content_length=None,
        headers={},
        has_blob=False,
        body_offset=None,
        body_size=None,
        source_path="/test",
    )

    path = get_blob_path(version_dir, record)
    assert path is None


def test_parse_record_with_http_url() -> None:
    """Test that http:// URLs are also accepted."""
    data = _build_record_bytes("http://insecure.example.com/page")
    record = parse_webkit_cache_record(
        data, record_hash="H1", partition="P1",
        record_type="Resource", source_path="/test",
    )
    assert record is not None
    assert record.url == "http://insecure.example.com/page"


# --- Version 12 format tests ---


def test_v12_parse_basic_record() -> None:
    """V12 format: partition + type + URL + response section."""
    data = _build_v12_record(
        "https://example.com/page.html",
        status_text="OK",
        headers={"Content-Type": "text/html", "Server": "nginx"},
    )
    record = parse_webkit_cache_record(
        data, record_hash="V12HASH", partition="P1",
        record_type="Resource", source_path="/test",
    )
    assert record is not None
    assert record.url == "https://example.com/page.html"
    assert record.http_status == 200
    assert record.content_type == "text/html"
    assert record.headers.get("Server") == "nginx"


def test_v12_parse_with_date_header_timestamp() -> None:
    """V12 format: Date header should be used as timestamp_utc."""
    data = _build_v12_record(
        "https://example.com/api",
        status_text="OK",
        headers={
            "Content-Type": "application/json",
            "Date": "Wed, 04 Oct 2023 19:29:50 GMT",
        },
    )
    record = parse_webkit_cache_record(
        data, record_hash="V12TS", partition="P1",
        record_type="Resource", source_path="/test",
    )
    assert record is not None
    assert record.timestamp_utc is not None
    assert "2023-10-04" in record.timestamp_utc


def test_v12_parse_404_status() -> None:
    """V12 format: 'Not Found' status text should map to 404."""
    data = _build_v12_record(
        "https://example.com/missing",
        status_text="Not Found",
        headers={"Content-Type": "text/html"},
    )
    record = parse_webkit_cache_record(
        data, record_hash="V12_404", partition="P1",
        record_type="Resource", source_path="/test",
    )
    assert record is not None
    assert record.http_status == 404


def test_v12_parse_many_headers() -> None:
    """V12 format: full set of HTTP headers."""
    headers = {
        "Content-Type": "image/jpeg",
        "Content-Length": "41899",
        "Server": "cloudflare",
        "Date": "Wed, 04 Oct 2023 19:29:55 GMT",
        "Last-Modified": "Wed, 17 Jan 2018 11:50:26 GMT",
        "Expires": "Sat, 04 Nov 2023 19:29:55 GMT",
        "Cache-Control": "max-age=2592000",
        "Connection": "keep-alive",
        "Accept-Ranges": "bytes",
        "Age": "2075",
        "Vary": "Accept-Encoding",
    }
    data = _build_v12_record(
        "http://example.com/photo.jpg",
        status_text="OK",
        headers=headers,
    )
    record = parse_webkit_cache_record(
        data, record_hash="V12HDRS", partition="P1",
        record_type="Resource", source_path="/test",
    )
    assert record is not None
    assert record.http_status == 200
    assert record.content_type == "image/jpeg"
    assert record.content_length == 41899
    assert len(record.headers) >= 11
    assert record.headers.get("Server") == "cloudflare"
    assert record.headers.get("Age") == "2075"
    assert record.timestamp_utc is not None


def test_v12_parse_http_url() -> None:
    """V12 format: HTTP (not HTTPS) URLs should work."""
    data = _build_v12_record("http://insecure.example.com/style.css")
    record = parse_webkit_cache_record(
        data, record_hash="V12HTTP", partition="P1",
        record_type="Resource", source_path="/test",
    )
    assert record is not None
    assert record.url == "http://insecure.example.com/style.css"


def test_read_lf_string_basic() -> None:
    """Test low-level lf_string reader."""
    data = _build_lf_string("hello world")
    result, new_off = _read_lf_string(data, 0)
    assert result == "hello world"
    assert new_off == len(data)


def test_read_lf_string_at_offset() -> None:
    """Test lf_string reader at non-zero offset."""
    prefix = b"\x00" * 10
    data = prefix + _build_lf_string("test")
    result, new_off = _read_lf_string(data, 10)
    assert result == "test"
    assert new_off == 10 + 5 + 4  # prefix + header + "test"


def test_read_lf_string_invalid_flag() -> None:
    """lf_string with wrong flag byte should fail."""
    data = struct.pack("<I", 5) + b"\x02" + b"hello"
    result, off = _read_lf_string(data, 0)
    assert result is None


def test_status_text_to_code_mapping() -> None:
    """Test HTTP status text to code mapping."""
    assert _status_text_to_code("OK") == 200
    assert _status_text_to_code("Not Found") == 404
    assert _status_text_to_code("Moved Permanently") == 301
    assert _status_text_to_code("Internal Server Error") == 500
    assert _status_text_to_code("Not Modified") == 304
    assert _status_text_to_code("ok") == 200  # case-insensitive
    assert _status_text_to_code("") is None
    assert _status_text_to_code("200") == 200  # numeric fallback
