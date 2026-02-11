"""
Firefox Cache2 schema constants for extraction warning discovery.

This module defines known values for Firefox cache2 format to enable
tracking of unknown/new fields that may contain forensic data we're
not yet capturing.

Initial implementation for schema warning support

Reference:
- https://www.forensicswiki.org/wiki/Mozilla_Cache2
- https://firefox-source-docs.mozilla.org/netwerk/cache2/cache2.html
"""

from __future__ import annotations

from typing import Dict, Set

# =============================================================================
# Cache2 Format Constants
# =============================================================================

# Known cache2 format versions (Firefox 32+)
# - Version 1: Firefox 32-38 (28-byte header, no flags)
# - Version 2: Firefox 39-89 (32-byte header with flags)
# - Version 3: Firefox 90+ (32-byte header, zstd compression support)
KNOWN_CACHE2_VERSIONS: Set[int] = {1, 2, 3}

# Cache2 chunk size for hash array calculation (256 KB)
CACHE2_CHUNK_SIZE = 262144


# =============================================================================
# Cache2 Element Keys
# =============================================================================
# Elements are key\0value\0 pairs stored after the metadata header.
# These are the known keys we parse; unknown keys may indicate new
# Firefox features or extensions.

# Text-based element keys we parse
KNOWN_ELEMENT_KEYS: Set[str] = {
    # HTTP request/response
    "request-method",           # GET, POST, etc.
    "response-head",            # Full HTTP response headers
    "original-response-headers",  # Headers before modification

    # Security (skipped as binary, but known)
    "security-info",            # TLS certificate chain (binary)

    # Alternative data (Firefox 52+)
    "alt-data",                 # Alternative representation (e.g., JS bytecode)
    "alt-data-info",            # Metadata about alt-data

    # Content negotiation
    "necko:classified",         # Tracking protection classification
    "necko:cache-control",      # Internal cache hints

    # Service worker
    "response-head-from-serviceWorker",  # SW-modified response
}

# Binary element keys (we skip decoding these)
BINARY_ELEMENT_KEYS: Set[str] = {
    "security-info",
    "alt-data",
    "alt-data-info",
}

# Patterns to filter relevant unknown element keys
# (some keys are internal Firefox debugging/testing keys)
ELEMENT_KEY_PATTERNS: list[str] = [
    "response",
    "request",
    "cache",
    "content",
    "security",
    "necko",
]


# =============================================================================
# HTTP Response Headers
# =============================================================================
# Standard and common HTTP headers we extract from response-head element.
# Unknown headers may contain forensic value (custom server headers, etc.)

KNOWN_HTTP_HEADERS: Set[str] = {
    # Standard response headers
    "content-type",
    "content-encoding",
    "content-length",
    "content-language",
    "content-disposition",
    "content-range",
    "content-location",

    # Caching headers
    "cache-control",
    "pragma",
    "expires",
    "age",
    "date",
    "last-modified",
    "etag",
    "vary",

    # Connection headers
    "connection",
    "keep-alive",
    "transfer-encoding",

    # Server headers
    "server",
    "x-powered-by",

    # Security headers
    "strict-transport-security",
    "content-security-policy",
    "content-security-policy-report-only",
    "x-content-type-options",
    "x-frame-options",
    "x-xss-protection",
    "referrer-policy",
    "permissions-policy",
    "cross-origin-embedder-policy",
    "cross-origin-opener-policy",
    "cross-origin-resource-policy",

    # CORS headers
    "access-control-allow-origin",
    "access-control-allow-methods",
    "access-control-allow-headers",
    "access-control-expose-headers",
    "access-control-max-age",
    "access-control-allow-credentials",

    # Cookie headers
    "set-cookie",

    # Redirect headers
    "location",

    # Authentication headers
    "www-authenticate",
    "proxy-authenticate",

    # Compression/encoding
    "accept-ranges",

    # Timing headers
    "timing-allow-origin",

    # Common custom headers
    "x-cache",
    "x-cache-hit",
    "x-served-by",
    "x-request-id",
    "x-correlation-id",
    "x-amz-request-id",
    "x-amz-id-2",
    "cf-ray",
    "cf-cache-status",
    "x-cdn",
    "x-edge-location",
    "x-robots-tag",
    "x-ua-compatible",
    "x-dns-prefetch-control",
    "x-download-options",
    "x-permitted-cross-domain-policies",

    # Rate limiting
    "x-ratelimit-limit",
    "x-ratelimit-remaining",
    "x-ratelimit-reset",
    "retry-after",

    # Link headers
    "link",

    # Alt-Svc for HTTP/3
    "alt-svc",

    # Feature policy (deprecated but still seen)
    "feature-policy",

    # Early hints
    "103-early-hints",

    # Source map
    "sourcemap",
    "x-sourcemap",
}

# Patterns to filter relevant unknown HTTP headers
# (ignore debug/trace headers, focus on forensically relevant)
HTTP_HEADER_PATTERNS: list[str] = [
    "x-",           # Custom headers often start with x-
    "content-",
    "cache-",
    "access-control-",
    "cross-origin-",
    "set-cookie",
    "cookie",
    "auth",
    "location",
    "server",
]


# =============================================================================
# Request Methods
# =============================================================================
# Standard HTTP methods - non-standard methods may indicate API usage

KNOWN_REQUEST_METHODS: Set[str] = {
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


# =============================================================================
# Content Types (MIME types) of Interest
# =============================================================================
# Common content types for forensic analysis - track unknown types

KNOWN_CONTENT_TYPES: Set[str] = {
    # Images
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/webp",
    "image/svg+xml",
    "image/bmp",
    "image/tiff",
    "image/x-icon",
    "image/vnd.microsoft.icon",
    "image/avif",
    "image/heic",
    "image/heif",

    # Web content
    "text/html",
    "text/css",
    "text/javascript",
    "application/javascript",
    "application/json",
    "application/xml",
    "text/xml",
    "text/plain",

    # Documents
    "application/pdf",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",

    # Media
    "video/mp4",
    "video/webm",
    "video/ogg",
    "audio/mpeg",
    "audio/ogg",
    "audio/wav",
    "audio/webm",

    # Fonts
    "font/woff",
    "font/woff2",
    "font/ttf",
    "font/otf",
    "application/font-woff",
    "application/font-woff2",

    # Data
    "application/octet-stream",
    "application/x-www-form-urlencoded",
    "multipart/form-data",

    # Archives
    "application/zip",
    "application/gzip",
    "application/x-tar",
}


# =============================================================================
# HTTP Status Codes
# =============================================================================
# Track unusual status codes that may indicate interesting server behavior

KNOWN_STATUS_CODES: Set[int] = {
    # Success
    200, 201, 202, 203, 204, 205, 206, 207, 208, 226,
    # Redirection
    300, 301, 302, 303, 304, 305, 307, 308,
    # Client errors
    400, 401, 402, 403, 404, 405, 406, 407, 408, 409,
    410, 411, 412, 413, 414, 415, 416, 417, 418,
    421, 422, 423, 424, 425, 426, 428, 429, 431, 451,
    # Server errors
    500, 501, 502, 503, 504, 505, 506, 507, 508, 510, 511,
}


# =============================================================================
# Metadata Field Names
# =============================================================================
# Cache2 metadata header fields - currently fixed structure but track changes

CACHE2_METADATA_FIELDS: Set[str] = {
    "version",
    "fetch_count",
    "last_fetched",
    "last_modified",
    "frecency",
    "expiration",
    "flags",  # Version 2+
}
