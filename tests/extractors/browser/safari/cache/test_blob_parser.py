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
