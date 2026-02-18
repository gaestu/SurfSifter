"""NSKeyedArchiver blob parsing for Safari Cache.db metadata."""

from __future__ import annotations

import plistlib
from dataclasses import dataclass
from typing import Any, Dict, Optional

from core.logging import get_logger
from .._nska import deserialize_nska

LOGGER = get_logger("extractors.browser.safari.cache.blob_parser")


@dataclass
class ResponseMetadata:
    http_status: Optional[int]
    content_type: Optional[str]
    content_length: Optional[int]
    mime_type: Optional[str]
    text_encoding: Optional[str]
    server: Optional[str]
    cache_control: Optional[str]
    etag: Optional[str]
    last_modified: Optional[str]
    set_cookie: Optional[str]
    all_headers: Dict[str, str]
    raw_deserialized: Optional[dict]


@dataclass
class RequestMetadata:
    http_method: Optional[str]
    user_agent: Optional[str]
    referer: Optional[str]
    accept: Optional[str]
    all_headers: Dict[str, str]
    raw_deserialized: Optional[dict]


def parse_response_object(blob: bytes) -> Optional[ResponseMetadata]:
    """Deserialize and extract response metadata from response_object blob."""
    if not blob:
        return None
    try:
        data = deserialize_nska(blob)
        if not data:
            return None

        headers = _extract_headers(data)
        status = _extract_int(data, ("statusCode", "status", "response_code"))
        content_type = _header(headers, "content-type")
        content_length = _extract_int(data, ("expectedContentLength", "contentLength", "length"))
        if content_length is None:
            content_length = _coerce_int(_header(headers, "content-length"))
        mime_type = _extract_str(data, ("MIMEType", "mimeType"))
        if not mime_type:
            mime_type = content_type

        return ResponseMetadata(
            http_status=status,
            content_type=content_type,
            content_length=content_length,
            mime_type=mime_type,
            text_encoding=_extract_str(data, ("textEncodingName", "textEncoding")),
            server=_header(headers, "server"),
            cache_control=_header(headers, "cache-control"),
            etag=_header(headers, "etag"),
            last_modified=_header(headers, "last-modified"),
            set_cookie=_header(headers, "set-cookie"),
            all_headers=headers,
            raw_deserialized=data,
        )
    except (
        plistlib.InvalidFileException,
        ValueError,
        TypeError,
        KeyError,
        IndexError,
        OSError,
        OverflowError,
    ) as exc:
        LOGGER.warning("Failed to parse Safari response_object blob: %s", exc)
        return None


def parse_request_object(blob: bytes) -> Optional[RequestMetadata]:
    """Deserialize and extract request metadata from request_object blob."""
    if not blob:
        return None
    try:
        data = deserialize_nska(blob)
        if not data:
            return None

        headers = _extract_headers(data)
        method = _extract_str(data, ("HTTPMethod", "method"))
        if method:
            method = method.upper()

        return RequestMetadata(
            http_method=method,
            user_agent=_header(headers, "user-agent"),
            referer=_header(headers, "referer"),
            accept=_header(headers, "accept"),
            all_headers=headers,
            raw_deserialized=data,
        )
    except (
        plistlib.InvalidFileException,
        ValueError,
        TypeError,
        KeyError,
        IndexError,
        OSError,
        OverflowError,
    ) as exc:
        LOGGER.warning("Failed to parse Safari request_object blob: %s", exc)
        return None


def _extract_headers(data: Any) -> Dict[str, str]:
    header_map = _find_first_mapping(
        data,
        {
            "allHeaderFields",
            "allHTTPHeaderFields",
            "headers",
            "headerFields",
        },
    )
    if not isinstance(header_map, dict):
        return {}

    normalized: Dict[str, str] = {}
    for key, value in header_map.items():
        key_str = str(key).strip()
        if not key_str:
            continue
        if isinstance(value, bytes):
            val_str = value.decode("utf-8", errors="ignore").strip()
        else:
            val_str = str(value).strip()
        normalized[key_str] = val_str
    return normalized


def _header(headers: Dict[str, str], target: str) -> Optional[str]:
    target_l = target.lower()
    for key, value in headers.items():
        if key.lower() == target_l:
            return value
    return None


def _extract_str(data: Any, keys: tuple[str, ...]) -> Optional[str]:
    found = _find_first_by_keys(data, set(keys))
    if found is None:
        return None
    if isinstance(found, bytes):
        return found.decode("utf-8", errors="ignore").strip() or None
    value = str(found).strip()
    return value or None


def _extract_int(data: Any, keys: tuple[str, ...]) -> Optional[int]:
    found = _find_first_by_keys(data, set(keys))
    return _coerce_int(found)


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _find_first_by_keys(node: Any, keys: set[str]) -> Any:
    keys_lower = {k.lower() for k in keys}
    if isinstance(node, dict):
        for key, value in node.items():
            if isinstance(key, str) and key.lower() in keys_lower:
                return value
        for value in node.values():
            found = _find_first_by_keys(value, keys)
            if found is not None:
                return found
    elif isinstance(node, list):
        for item in node:
            found = _find_first_by_keys(item, keys)
            if found is not None:
                return found
    return None


def _find_first_mapping(node: Any, keys: set[str]) -> Optional[Dict[str, Any]]:
    keys_lower = {k.lower() for k in keys}
    if isinstance(node, dict):
        for key, value in node.items():
            if isinstance(key, str) and key.lower() in keys_lower and isinstance(value, dict):
                return value
        for value in node.values():
            found = _find_first_mapping(value, keys)
            if found is not None:
                return found
    elif isinstance(node, list):
        for item in node:
            found = _find_first_mapping(item, keys)
            if found is not None:
                return found
    return None
