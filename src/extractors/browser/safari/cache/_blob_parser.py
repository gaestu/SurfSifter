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

        # Try CFURLCache Array format first (Version/Array positional structure).
        # In this format the deserialized blob looks like:
        #   {'Version': N, 'Array': [url_dict, cocoa_timestamp, ?, status_int,
        #                             headers_dict, ..., mime_type_str]}
        array_result = _try_parse_response_array(data)
        if array_result is not None:
            return array_result

        # Fall back to NSKeyedArchiver named-key format
        # (e.g. {'NSHTTPURLResponse': {'statusCode': ..., 'allHeaderFields': ...}})
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

        # Try CFURLCache Array format first
        array_result = _try_parse_request_array(data)
        if array_result is not None:
            return array_result

        # Fall back to NSKeyedArchiver named-key format
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


def _try_parse_response_array(data: dict) -> Optional[ResponseMetadata]:
    """Try to parse response metadata from CFURLCache Version/Array format.

    Safari's CFURLResponse serialization (non-NSKeyedArchiver) produces:
        {'Version': N, 'Array': [
            url_dict,           # index 0: {'_CFURLStringType': ..., '_CFURLString': ...}
            cocoa_timestamp,    # index 1: float (Cocoa epoch seconds)
            unknown_int,        # index 2: usually 0
            http_status,        # index 3: int (e.g. 200)
            headers_dict,       # index 4: {'Content-Type': '...', ...}
            token_or_size,      # index 5: string/int
            mime_type_str,      # index 6: string (optional)
        ]}
    Returns None if the data doesn't match this format.
    """
    arr = _get_cfurl_array(data)
    if arr is None or len(arr) < 5:
        return None

    # Validate: Array[3] should be an int (HTTP status) and Array[4] a dict (headers)
    raw_status = arr[3]
    raw_headers = arr[4]
    if not isinstance(raw_headers, dict):
        return None

    status = _coerce_int(raw_status)
    headers = _normalize_header_dict(raw_headers)
    content_type = _header(headers, "content-type")
    content_length = _coerce_int(_header(headers, "content-length"))
    mime_type = None

    # Index 6 may contain MIME type string
    if len(arr) > 6 and isinstance(arr[6], str) and "/" in arr[6]:
        mime_type = arr[6]
    # Index 5 may be content-length if it's numeric
    if content_length is None and len(arr) > 5:
        cl_candidate = _coerce_int(arr[5])
        if cl_candidate is not None and cl_candidate > 0:
            content_length = cl_candidate

    if not mime_type:
        mime_type = content_type

    return ResponseMetadata(
        http_status=status,
        content_type=content_type,
        content_length=content_length,
        mime_type=mime_type,
        text_encoding=None,
        server=_header(headers, "server"),
        cache_control=_header(headers, "cache-control"),
        etag=_header(headers, "etag"),
        last_modified=_header(headers, "last-modified"),
        set_cookie=_header(headers, "set-cookie"),
        all_headers=headers,
        raw_deserialized=data,
    )


def _try_parse_request_array(data: dict) -> Optional[RequestMetadata]:
    """Try to parse request metadata from CFURLCache Version/Array format.

    The request Array has a variable layout but typically contains:
        - HTTP method as a string (e.g. 'GET', 'POST')
        - Headers as a dict

    We scan the Array for the first plain uppercase HTTP method string
    and the first dict that looks like HTTP headers.
    """
    arr = _get_cfurl_array(data)
    if arr is None or len(arr) < 3:
        return None

    method = None
    headers: Dict[str, str] = {}

    http_methods = {"GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH", "CONNECT", "TRACE"}

    for item in arr:
        if isinstance(item, str) and item.upper() in http_methods and method is None:
            method = item.upper()
        elif isinstance(item, dict) and not headers:
            # Check if this dict looks like HTTP headers (has string keys and values)
            if _looks_like_headers(item):
                headers = _normalize_header_dict(item)

    if not method and not headers:
        return None

    return RequestMetadata(
        http_method=method,
        user_agent=_header(headers, "user-agent"),
        referer=_header(headers, "referer"),
        accept=_header(headers, "accept"),
        all_headers=headers,
        raw_deserialized=data,
    )


def _get_cfurl_array(data: Any) -> Optional[list]:
    """Extract the Array from a Version/Array CFURLCache structure."""
    if not isinstance(data, dict):
        return None
    if "Version" not in data or "Array" not in data:
        return None
    arr = data.get("Array")
    if isinstance(arr, list) and len(arr) >= 3:
        return arr
    return None


def _looks_like_headers(mapping: dict) -> bool:
    """Check if a dict looks like HTTP headers (string keys containing '-' or known names)."""
    if not mapping:
        return False
    known_header_prefixes = {
        "content-", "accept", "cache-", "server", "date", "expires",
        "etag", "last-", "set-cookie", "vary", "x-", "user-agent",
        "connection", "strict-", "access-control", "cf-",
    }
    str_key_count = 0
    header_like_count = 0
    for key in mapping:
        if isinstance(key, str):
            str_key_count += 1
            key_lower = key.lower()
            if any(key_lower.startswith(p) for p in known_header_prefixes) or "-" in key:
                header_like_count += 1
    # It looks like headers if most keys are strings and some look like header names
    return str_key_count > 0 and header_like_count >= 1


def _normalize_header_dict(raw: dict) -> Dict[str, str]:
    """Normalize a raw dict into a clean string-string header mapping."""
    normalized: Dict[str, str] = {}
    for key, value in raw.items():
        key_str = str(key).strip()
        if not key_str:
            continue
        if isinstance(value, bytes):
            val_str = value.decode("utf-8", errors="ignore").strip()
        else:
            val_str = str(value).strip()
        normalized[key_str] = val_str
    return normalized


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
