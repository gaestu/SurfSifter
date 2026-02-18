"""Tests for WebKitCache / NetworkCache record parser."""

from __future__ import annotations

import struct
from pathlib import Path

from extractors.browser.safari.cache._record_parser import (
    WebKitCacheRecord,
    get_blob_path,
    parse_webkit_cache_dir,
    parse_webkit_cache_record,
)


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
