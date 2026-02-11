"""
Timestamp conversion utilities for browser extractors.

These are PURE FUNCTIONS with no side effects.
Each browser extractor calls these directly - no abstraction layers.

Note: This is intentionally separate from core/timestamps.py to keep
extractors modular and independent from core infrastructure.

Formats supported:
- WebKit: Microseconds since 1601-01-01 (Chromium browsers)
- PRTime: Microseconds since 1970-01-01 (Firefox)
- Unix: Seconds since 1970-01-01
- Cocoa: Seconds since 2001-01-01 (Safari/macOS)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

# Constants for timestamp epoch calculations
WEBKIT_EPOCH_DIFF = 11644473600  # Seconds between 1601-01-01 and 1970-01-01
COCOA_EPOCH_DIFF = 978307200     # Seconds between 1970-01-01 and 2001-01-01


def webkit_to_datetime(microseconds: int) -> Optional[datetime]:
    """
    Convert WebKit timestamp to datetime.

    WebKit timestamps are microseconds since 1601-01-01 00:00:00 UTC.
    Used by Chromium-based browsers (Chrome, Edge, Opera, Brave).

    Args:
        microseconds: WebKit timestamp (microseconds since 1601)

    Returns:
        datetime in UTC, or None if invalid/zero

    Example:
        >>> webkit_to_datetime(13350000000000000)
        datetime(2023, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    """
    if not microseconds or microseconds <= 0:
        return None

    try:
        # Convert to Unix seconds
        unix_seconds = (microseconds / 1_000_000) - WEBKIT_EPOCH_DIFF
        if unix_seconds < 0 or unix_seconds > 32503680000:  # Beyond year 3000
            return None
        return datetime.fromtimestamp(unix_seconds, tz=timezone.utc)
    except (ValueError, OSError, OverflowError):
        return None


def webkit_to_iso(microseconds: int) -> Optional[str]:
    """
    Convert WebKit timestamp to ISO 8601 string.

    Args:
        microseconds: WebKit timestamp (microseconds since 1601)

    Returns:
        ISO 8601 string or None if invalid

    Example:
        >>> webkit_to_iso(13350000000000000)
        '2023-06-15T12:00:00+00:00'
    """
    dt = webkit_to_datetime(microseconds)
    return dt.isoformat() if dt else None


def prtime_to_datetime(microseconds: int) -> Optional[datetime]:
    """
    Convert PRTime timestamp to datetime.

    PRTime timestamps are microseconds since 1970-01-01 00:00:00 UTC.
    Used by Firefox (moz_* tables).

    Args:
        microseconds: PRTime timestamp (microseconds since 1970)

    Returns:
        datetime in UTC, or None if invalid/zero
    """
    if not microseconds or microseconds <= 0:
        return None

    try:
        unix_seconds = microseconds / 1_000_000
        if unix_seconds > 32503680000:  # Beyond year 3000
            return None
        return datetime.fromtimestamp(unix_seconds, tz=timezone.utc)
    except (ValueError, OSError, OverflowError):
        return None


def prtime_to_iso(microseconds: int) -> Optional[str]:
    """
    Convert PRTime timestamp to ISO 8601 string.

    Args:
        microseconds: PRTime timestamp (microseconds since 1970)

    Returns:
        ISO 8601 string or None if invalid
    """
    dt = prtime_to_datetime(microseconds)
    return dt.isoformat() if dt else None


def unix_to_datetime(timestamp: int | float) -> Optional[datetime]:
    """
    Convert Unix timestamp to datetime.

    Unix timestamps are seconds since 1970-01-01 00:00:00 UTC.

    Args:
        timestamp: Unix timestamp (seconds since 1970)

    Returns:
        datetime in UTC, or None if invalid/zero
    """
    if not timestamp or timestamp <= 0:
        return None

    try:
        if timestamp > 32503680000:  # Beyond year 3000
            return None
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    except (ValueError, OSError, OverflowError):
        return None


def unix_to_iso(timestamp: int | float) -> Optional[str]:
    """
    Convert Unix timestamp to ISO 8601 string.

    Args:
        timestamp: Unix timestamp (seconds since 1970)

    Returns:
        ISO 8601 string or None if invalid
    """
    dt = unix_to_datetime(timestamp)
    return dt.isoformat() if dt else None


def cocoa_to_datetime(seconds: float) -> Optional[datetime]:
    """
    Convert Cocoa/Core Data timestamp to datetime.

    Cocoa timestamps are seconds since 2001-01-01 00:00:00 UTC.
    Used by Safari and macOS applications.

    Args:
        seconds: Cocoa timestamp (seconds since 2001)

    Returns:
        datetime in UTC, or None if invalid/zero
    """
    if not seconds:
        return None

    try:
        # Convert to Unix timestamp by adding the epoch difference
        unix_seconds = seconds + COCOA_EPOCH_DIFF
        if unix_seconds < 0 or unix_seconds > 32503680000:  # Beyond year 3000
            return None
        return datetime.fromtimestamp(unix_seconds, tz=timezone.utc)
    except (ValueError, OSError, OverflowError):
        return None


def cocoa_to_iso(seconds: float) -> Optional[str]:
    """
    Convert Cocoa/Core Data timestamp to ISO 8601 string.

    Args:
        seconds: Cocoa timestamp (seconds since 2001)

    Returns:
        ISO 8601 string or None if invalid
    """
    dt = cocoa_to_datetime(seconds)
    return dt.isoformat() if dt else None


def unix_milliseconds_to_datetime(milliseconds: int) -> Optional[datetime]:
    """
    Convert Unix milliseconds timestamp to datetime.

    Some systems use milliseconds instead of seconds.

    Args:
        milliseconds: Unix timestamp in milliseconds

    Returns:
        datetime in UTC, or None if invalid/zero
    """
    if not milliseconds or milliseconds <= 0:
        return None
    return unix_to_datetime(milliseconds / 1000)


def unix_milliseconds_to_iso(milliseconds: int) -> Optional[str]:
    """
    Convert Unix milliseconds timestamp to ISO 8601 string.

    Args:
        milliseconds: Unix timestamp in milliseconds

    Returns:
        ISO 8601 string or None if invalid
    """
    dt = unix_milliseconds_to_datetime(milliseconds)
    return dt.isoformat() if dt else None
