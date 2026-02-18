"""Known Safari cache schema values for warning discovery."""

from __future__ import annotations

from typing import Set

KNOWN_TABLES = {
    "cfurl_cache_response",
    "cfurl_cache_blob_data",
    "cfurl_cache_receiver_data",
    "cfurl_cache_schema_version",
}

KNOWN_COLUMNS_RESPONSE = {
    "entry_ID",
    "version",
    "hash_value",
    "storage_policy",
    "request_key",
    "time_stamp",
    "partition",
}

KNOWN_COLUMNS_BLOB_DATA = {
    "entry_ID",
    "response_object",
    "request_object",
    "proto_props",
    "user_info",
}

KNOWN_COLUMNS_RECEIVER_DATA = {
    "entry_ID",
    "isDataOnFS",
    "receiver_data",
}

KNOWN_STORAGE_POLICIES = {0, 1, 2}

KNOWN_HTTP_RESPONSE_HEADERS: Set[str] = {
    "Content-Type",
    "Content-Length",
    "Content-Encoding",
    "Cache-Control",
    "ETag",
    "Last-Modified",
    "Server",
    "Date",
    "Expires",
    "Vary",
    "Set-Cookie",
    "X-Content-Type-Options",
    "X-Frame-Options",
    "Strict-Transport-Security",
    "Access-Control-Allow-Origin",
    "Content-Security-Policy",
    "Transfer-Encoding",
    "Connection",
    "Accept-Ranges",
    "Age",
    "Location",
    "Pragma",
    "X-Cache",
    "X-Cache-Hits",
    "X-Served-By",
    "X-Timer",
    "CF-Cache-Status",
    "CF-RAY",
    "X-Request-Id",
    "X-Powered-By",
    "X-AspNet-Version",
    "X-Runtime",
}

KNOWN_HTTP_REQUEST_METHODS = {
    "GET",
    "POST",
    "PUT",
    "DELETE",
    "HEAD",
    "OPTIONS",
    "PATCH",
    "CONNECT",
    "TRACE",
}

KNOWN_CONTENT_TYPES: Set[str] = {
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/webp",
    "image/svg+xml",
    "image/avif",
    "image/heic",
    "image/heif",
    "image/bmp",
    "image/x-icon",
    "image/tiff",
    "image/vnd.microsoft.icon",
    "text/html",
    "text/css",
    "text/javascript",
    "application/javascript",
    "application/json",
    "application/xml",
    "text/xml",
    "text/plain",
    "font/woff",
    "font/woff2",
    "font/ttf",
    "font/otf",
    "application/font-woff",
    "application/font-woff2",
    "video/mp4",
    "audio/mpeg",
    "audio/mp4",
    "video/webm",
    "application/octet-stream",
    "application/pdf",
    "application/x-gzip",
    "application/wasm",
}
