from __future__ import annotations

import plistlib

from extractors.browser.safari.cache._blob_parser import (
    parse_request_object,
    parse_response_object,
)


def test_parse_response_object_full() -> None:
    blob = plistlib.dumps(
        {
            "NSHTTPURLResponse": {
                "statusCode": 200,
                "allHeaderFields": {
                    "Content-Type": "image/png",
                    "Content-Length": "1234",
                    "Server": "nginx",
                    "Set-Cookie": "a=b",
                },
                "MIMEType": "image/png",
                "textEncodingName": "utf-8",
            }
        },
        fmt=plistlib.FMT_BINARY,
    )

    parsed = parse_response_object(blob)
    assert parsed is not None
    assert parsed.http_status == 200
    assert parsed.content_type == "image/png"
    assert parsed.content_length == 1234
    assert parsed.server == "nginx"
    assert parsed.set_cookie == "a=b"
    assert "Content-Type" in parsed.all_headers


def test_parse_request_object() -> None:
    blob = plistlib.dumps(
        {
            "NSURLRequest": {
                "HTTPMethod": "GET",
                "allHTTPHeaderFields": {
                    "User-Agent": "Safari",
                    "Referer": "https://example.com/",
                    "Accept": "text/html",
                },
            }
        },
        fmt=plistlib.FMT_BINARY,
    )
    parsed = parse_request_object(blob)
    assert parsed is not None
    assert parsed.http_method == "GET"
    assert parsed.user_agent == "Safari"
    assert parsed.referer == "https://example.com/"
    assert parsed.accept == "text/html"


def test_parse_response_object_corrupt_blob_returns_none() -> None:
    assert parse_response_object(b"not a plist") is None
    assert parse_request_object(b"") is None


def test_parse_response_object_cfurl_array_format() -> None:
    """Test CFURLCache Version/Array positional format used by many Safari versions."""
    blob = plistlib.dumps(
        {
            "Version": 1,
            "Array": [
                {"_CFURLStringType": 15, "_CFURLString": "https://example.com/page.html"},
                718140558.941747,  # Cocoa timestamp
                0,
                200,  # HTTP status
                {
                    "Content-Type": "text/html; charset=utf-8",
                    "Server": "Apache",
                    "Content-Length": "65536",
                    "Cache-Control": "max-age=3600",
                    "ETag": '"abc123"',
                    "Last-Modified": "Fri, 16 Jun 2023 07:02:03 GMT",
                },
                "__CFURLResponseNullTokenString__",
                "text/html",  # MIME type
            ],
        },
        fmt=plistlib.FMT_BINARY,
    )
    parsed = parse_response_object(blob)
    assert parsed is not None
    assert parsed.http_status == 200
    assert parsed.content_type == "text/html; charset=utf-8"
    assert parsed.content_length == 65536
    assert parsed.mime_type == "text/html"
    assert parsed.server == "Apache"
    assert parsed.cache_control == "max-age=3600"
    assert parsed.etag == '"abc123"'
    assert parsed.last_modified == "Fri, 16 Jun 2023 07:02:03 GMT"
    assert "Content-Type" in parsed.all_headers


def test_parse_request_object_cfurl_array_format() -> None:
    """Test CFURLCache Version/Array positional format for request objects."""
    blob = plistlib.dumps(
        {
            "Version": 9,
            "Array": [
                False,
                {"_CFURLStringType": 15, "_CFURLString": "https://example.com/api"},
                31536000.0,
                0,
                "__CFURLRequestNullTokenString__",
                True,
                260,
                "__CFURLRequestNullTokenString__",
                "__CFURLRequestNullTokenString__",
                True,
                False,
                0,
                0.0,
                0.0,
                0,
                -1,
                "__CFURLRequestNullTokenString__",
                2,
                "GET",
                {
                    "User-Agent": "Safari/605.1.15",
                    "Accept": "*/*",
                    "Accept-Language": "en-us",
                    "Accept-Encoding": "gzip, deflate",
                },
                "__CFURLRequestNullTokenString__",
            ],
        },
        fmt=plistlib.FMT_BINARY,
    )
    parsed = parse_request_object(blob)
    assert parsed is not None
    assert parsed.http_method == "GET"
    assert parsed.user_agent == "Safari/605.1.15"
    assert parsed.accept == "*/*"
    assert "Accept-Encoding" in parsed.all_headers


def test_parse_response_object_cfurl_array_with_content_length_at_index_5() -> None:
    """Test Array format where content length appears at index 5 instead of headers."""
    blob = plistlib.dumps(
        {
            "Version": 1,
            "Array": [
                {"_CFURLStringType": 15, "_CFURLString": "https://api.example.com/data"},
                718140888.261529,
                0,
                200,
                {
                    "Content-Encoding": "deflate",
                    "Content-Type": "application/json; charset=utf-8",
                    "Cache-Control": "private, max-age=3600",
                    "Date": "Wed, 04 Oct 2023 19:34:48 GMT",
                },
                43020,  # Content length at index 5
                "application/json",
            ],
        },
        fmt=plistlib.FMT_BINARY,
    )
    parsed = parse_response_object(blob)
    assert parsed is not None
    assert parsed.http_status == 200
    assert parsed.content_type == "application/json; charset=utf-8"
    assert parsed.content_length == 43020
    assert parsed.mime_type == "application/json"
