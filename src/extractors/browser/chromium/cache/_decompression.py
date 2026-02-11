"""
Cache decompression utilities.

Handles HTTP content decompression for cached responses.
Supports: gzip, deflate, brotli (br), zstandard (zstd)
"""

from __future__ import annotations

import gzip
import zlib
from typing import TYPE_CHECKING, Optional

from core.logging import get_logger
from ._schemas import is_known_content_encoding

if TYPE_CHECKING:
    from ...._shared.extraction_warnings import ExtractionWarningCollector

LOGGER = get_logger("extractors.cache_simple.decompression")


def decompress_body(
    body_bytes: bytes,
    content_encoding: Optional[str],
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
    source_file: Optional[str] = None,
) -> bytes:
    """
    Decompress HTTP response body based on Content-Encoding.

    Supports: gzip, deflate, br (brotli), zstd (zstandard)

    Args:
        body_bytes: Raw (possibly compressed) response body
        content_encoding: Content-Encoding header value (may be comma-separated)
        warning_collector: Optional collector for extraction warnings
        source_file: Optional source file path for warning context

    Returns:
        Decompressed body bytes, or original if decompression fails/not needed
    """
    if not content_encoding:
        return body_bytes

    encoding = content_encoding.lower().strip()

    # Check for unknown encoding
    if warning_collector and not is_known_content_encoding(encoding):
        from ...._shared.extraction_warnings import (
            WARNING_TYPE_UNKNOWN_ENUM_VALUE,
            SEVERITY_INFO,
            CATEGORY_BINARY,
        )
        warning_collector.add_warning(
            warning_type=WARNING_TYPE_UNKNOWN_ENUM_VALUE,
            item_name="content_encoding",
            item_value=encoding,
            severity=SEVERITY_INFO,
            category=CATEGORY_BINARY,
            artifact_type="cache_simple",
            source_file=source_file,
        )

    try:
        if 'gzip' in encoding:
            return gzip.decompress(body_bytes)
        elif 'br' in encoding:
            try:
                import brotli
                return brotli.decompress(body_bytes)
            except ImportError:
                LOGGER.debug("Brotli not installed, returning raw body")
                return body_bytes
        elif 'deflate' in encoding:
            try:
                # Try raw deflate first
                return zlib.decompress(body_bytes, -zlib.MAX_WBITS)
            except zlib.error:
                # Fall back to zlib-wrapped
                return zlib.decompress(body_bytes)
        elif 'zstd' in encoding:
            try:
                import zstandard as zstd
                dctx = zstd.ZstdDecompressor()
                return dctx.decompress(body_bytes)
            except ImportError:
                LOGGER.debug("Zstandard not installed, returning raw body")
                return body_bytes
    except Exception as e:
        LOGGER.debug("Decompression failed (%s): %s", encoding, e)
        if warning_collector:
            from ...._shared.extraction_warnings import (
                WARNING_TYPE_COMPRESSION_ERROR,
                SEVERITY_WARNING,
                CATEGORY_BINARY,
            )
            warning_collector.add_warning(
                warning_type=WARNING_TYPE_COMPRESSION_ERROR,
                item_name=encoding,
                item_value=str(e),
                severity=SEVERITY_WARNING,
                category=CATEGORY_BINARY,
                artifact_type="cache_simple",
                source_file=source_file,
            )
        return body_bytes

    return body_bytes


# Backward compatibility alias
_decompress_body = decompress_body
