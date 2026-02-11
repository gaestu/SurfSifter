"""
Timestamp conversion utilities for browser forensics.

Provides centralized conversion functions for various browser timestamp formats
to ISO 8601 UTC strings.

Formats supported:
- WebKit: Microseconds since 1601-01-01 (Chromium browsers)
- PRTime: Microseconds since 1970-01-01 (Firefox)
- Unix: Seconds since 1970-01-01
- Cocoa: Seconds since 2001-01-01 (Safari/macOS)
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional, Union

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


def unix_to_datetime(seconds: Union[int, float]) -> Optional[datetime]:
    """
    Convert Unix timestamp to datetime.

    Unix timestamps are seconds since 1970-01-01 00:00:00 UTC.

    Args:
        seconds: Unix timestamp (seconds since 1970)

    Returns:
        datetime in UTC, or None if invalid/zero
    """
    if not seconds or seconds <= 0:
        return None

    try:
        if seconds > 32503680000:  # Beyond year 3000
            return None
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    except (ValueError, OSError, OverflowError):
        return None


def unix_to_iso(seconds: Union[int, float]) -> Optional[str]:
    """
    Convert Unix timestamp to ISO 8601 string.

    Args:
        seconds: Unix timestamp (seconds since 1970)

    Returns:
        ISO 8601 string or None if invalid
    """
    dt = unix_to_datetime(seconds)
    return dt.isoformat() if dt else None


def cocoa_to_datetime(seconds: Union[int, float]) -> Optional[datetime]:
    """
    Convert Cocoa/Core Data timestamp to datetime.

    Cocoa timestamps are seconds since 2001-01-01 00:00:00 UTC.
    Used by Safari and macOS applications.

    Args:
        seconds: Cocoa timestamp (seconds since 2001)

    Returns:
        datetime in UTC, or None if invalid
    """
    if seconds is None:
        return None

    try:
        # Convert to Unix timestamp by adding epoch difference
        unix_seconds = seconds + COCOA_EPOCH_DIFF
        if unix_seconds < 0 or unix_seconds > 32503680000:
            return None
        return datetime.fromtimestamp(unix_seconds, tz=timezone.utc)
    except (ValueError, OSError, OverflowError, TypeError):
        return None


def cocoa_to_iso(seconds: Union[int, float]) -> Optional[str]:
    """
    Convert Cocoa timestamp to ISO 8601 string.

    Args:
        seconds: Cocoa timestamp (seconds since 2001)

    Returns:
        ISO 8601 string or None if invalid
    """
    dt = cocoa_to_datetime(seconds)
    return dt.isoformat() if dt else None


def utc_now() -> str:
    """
    Get current UTC time as ISO 8601 string.

    Returns:
        Current UTC time in ISO 8601 format without microseconds
    """
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def parse_iso(iso_string: Optional[str]) -> Optional[datetime]:
    """
    Parse ISO 8601 string to datetime.

    Args:
        iso_string: ISO 8601 formatted string

    Returns:
        datetime in UTC, or None if invalid
    """
    if not iso_string:
        return None

    try:
        # Handle various ISO formats
        dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
        # Ensure UTC timezone
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def format_duration(seconds: Union[int, float]) -> str:
    """
    Format duration in seconds to human-readable string.

    Args:
        seconds: Duration in seconds

    Returns:
        Human-readable duration string

    Example:
        >>> format_duration(3661)
        '1h 1m 1s'
    """
    if seconds < 0:
        return "0s"

    hours, remainder = divmod(int(seconds), 3600)
    minutes, secs = divmod(remainder, 60)

    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")

    return " ".join(parts)
