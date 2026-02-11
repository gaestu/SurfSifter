"""
Windows timestamp conversion utilities.

Internet Explorer and Windows use various timestamp formats that differ
from Unix time and WebKit time. This module provides converters for:

1. FILETIME - 100-nanosecond intervals since January 1, 1601
2. OLE Automation Date - Days since December 30, 1899
3. FAT Time - DOS date/time format (16-bit each)

Usage:
    from extractors.browser.ie_legacy._timestamps import (
        filetime_to_datetime,
        filetime_to_iso,
        ole_date_to_datetime,
    )

    # Convert FILETIME (64-bit integer)
    dt = filetime_to_datetime(132456789012345678)

    # Convert OLE date (float)
    dt = ole_date_to_datetime(44197.5)  # 2021-01-01 12:00:00
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional, Union


# ============================================================================
# Constants
# ============================================================================

# Difference between Windows FILETIME epoch (1601-01-01) and Unix epoch (1970-01-01)
# in seconds
FILETIME_UNIX_DIFF = 11644473600

# Difference in 100-nanosecond intervals
FILETIME_UNIX_DIFF_100NS = FILETIME_UNIX_DIFF * 10_000_000

# OLE Automation date epoch: December 30, 1899
OLE_EPOCH = datetime(1899, 12, 30, tzinfo=timezone.utc)

# WebKit epoch: January 1, 1601 (same as FILETIME but in microseconds)
WEBKIT_UNIX_DIFF = 11644473600


# ============================================================================
# FILETIME Conversion
# ============================================================================

def filetime_to_datetime(filetime: int) -> Optional[datetime]:
    """
    Convert Windows FILETIME to datetime.

    FILETIME is a 64-bit value representing the number of 100-nanosecond
    intervals since January 1, 1601 (UTC).

    Args:
        filetime: 64-bit FILETIME value

    Returns:
        datetime object in UTC, or None if conversion fails

    Example:
        >>> filetime_to_datetime(132456789012345678)
        datetime.datetime(2020, 10, 15, 12, 35, 1, 234567, tzinfo=datetime.timezone.utc)
    """
    if filetime is None or filetime == 0:
        return None

    try:
        # Handle negative values (before 1601)
        if filetime < 0:
            return None

        # Convert to Unix timestamp
        # FILETIME is in 100-nanosecond intervals
        unix_timestamp = (filetime - FILETIME_UNIX_DIFF_100NS) / 10_000_000

        # Check for reasonable date range (1970-2100)
        if unix_timestamp < 0 or unix_timestamp > 4102444800:  # 2100-01-01
            # May be in a different format, try as-is
            return None

        return datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)

    except (OSError, OverflowError, ValueError):
        return None


def filetime_to_iso(filetime: int) -> Optional[str]:
    """
    Convert Windows FILETIME to ISO 8601 string.

    Args:
        filetime: 64-bit FILETIME value

    Returns:
        ISO 8601 formatted string, or None if conversion fails

    Example:
        >>> filetime_to_iso(132456789012345678)
        '2020-10-15T12:35:01.234567+00:00'
    """
    dt = filetime_to_datetime(filetime)
    if dt is None:
        return None
    return dt.isoformat()


def datetime_to_filetime(dt: datetime) -> int:
    """
    Convert datetime to Windows FILETIME.

    Args:
        dt: datetime object (should be UTC)

    Returns:
        64-bit FILETIME value
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    unix_timestamp = dt.timestamp()
    filetime = int((unix_timestamp + FILETIME_UNIX_DIFF) * 10_000_000)
    return filetime


# ============================================================================
# OLE Automation Date Conversion
# ============================================================================

def ole_date_to_datetime(ole_date: float) -> Optional[datetime]:
    """
    Convert OLE Automation date to datetime.

    OLE dates are stored as floating-point numbers representing days
    since December 30, 1899. The integer part is days, the fractional
    part is the time of day.

    Args:
        ole_date: OLE Automation date (float)

    Returns:
        datetime object in UTC, or None if conversion fails

    Example:
        >>> ole_date_to_datetime(44197.5)
        datetime.datetime(2021, 1, 1, 12, 0, tzinfo=datetime.timezone.utc)
    """
    if ole_date is None or ole_date == 0:
        return None

    try:
        # OLE dates can be negative (before epoch)
        # but we'll only handle positive reasonable values
        if ole_date < 0 or ole_date > 73050:  # ~2100-01-01
            return None

        return OLE_EPOCH + timedelta(days=ole_date)

    except (OverflowError, ValueError):
        return None


def ole_date_to_iso(ole_date: float) -> Optional[str]:
    """
    Convert OLE Automation date to ISO 8601 string.

    Args:
        ole_date: OLE Automation date (float)

    Returns:
        ISO 8601 formatted string, or None if conversion fails
    """
    dt = ole_date_to_datetime(ole_date)
    if dt is None:
        return None
    return dt.isoformat()


# ============================================================================
# WebKit Timestamp Conversion (for Chromium Edge compatibility)
# ============================================================================

def webkit_to_datetime(webkit_time: int) -> Optional[datetime]:
    """
    Convert WebKit timestamp to datetime.

    WebKit timestamps are microseconds since January 1, 1601 (UTC).
    Used by Chromium-based browsers (including Chromium Edge).

    Args:
        webkit_time: Microseconds since 1601-01-01

    Returns:
        datetime object in UTC, or None if conversion fails
    """
    if webkit_time is None or webkit_time == 0:
        return None

    try:
        # Convert to Unix timestamp
        unix_timestamp = (webkit_time / 1_000_000) - WEBKIT_UNIX_DIFF

        if unix_timestamp < 0 or unix_timestamp > 4102444800:
            return None

        return datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)

    except (OSError, OverflowError, ValueError):
        return None


def webkit_to_iso(webkit_time: int) -> Optional[str]:
    """
    Convert WebKit timestamp to ISO 8601 string.

    Args:
        webkit_time: Microseconds since 1601-01-01

    Returns:
        ISO 8601 formatted string, or None if conversion fails
    """
    dt = webkit_to_datetime(webkit_time)
    if dt is None:
        return None
    return dt.isoformat()


# ============================================================================
# FAT Time Conversion
# ============================================================================

def fat_datetime_to_datetime(fat_date: int, fat_time: int) -> Optional[datetime]:
    """
    Convert FAT date/time to datetime.

    FAT timestamps are stored as two 16-bit values:
    - Date: bits 0-4 = day (1-31), bits 5-8 = month (1-12), bits 9-15 = year (since 1980)
    - Time: bits 0-4 = seconds/2 (0-29), bits 5-10 = minutes (0-59), bits 11-15 = hours (0-23)

    Args:
        fat_date: 16-bit FAT date value
        fat_time: 16-bit FAT time value

    Returns:
        datetime object (local time, no timezone), or None if invalid
    """
    try:
        day = fat_date & 0x1F
        month = (fat_date >> 5) & 0x0F
        year = ((fat_date >> 9) & 0x7F) + 1980

        seconds = (fat_time & 0x1F) * 2
        minutes = (fat_time >> 5) & 0x3F
        hours = (fat_time >> 11) & 0x1F

        return datetime(year, month, day, hours, minutes, seconds)

    except ValueError:
        return None


# ============================================================================
# Auto-Detection and Smart Conversion
# ============================================================================

def auto_convert_timestamp(
    value: Union[int, float],
    hint: Optional[str] = None,
) -> Optional[datetime]:
    """
    Attempt to auto-detect and convert a timestamp.

    Uses heuristics to guess the timestamp format based on value range.

    Args:
        value: Timestamp value (int or float)
        hint: Optional format hint ('filetime', 'ole', 'webkit', 'unix')

    Returns:
        datetime object in UTC, or None if conversion fails
    """
    if value is None or value == 0:
        return None

    # Use hint if provided
    if hint == "filetime":
        return filetime_to_datetime(int(value))
    elif hint == "ole":
        return ole_date_to_datetime(float(value))
    elif hint == "webkit":
        return webkit_to_datetime(int(value))
    elif hint == "unix":
        try:
            return datetime.fromtimestamp(value, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None

    # Auto-detect based on value range
    if isinstance(value, float) and 0 < value < 100000:
        # Likely OLE date (days since 1899)
        return ole_date_to_datetime(value)

    if isinstance(value, int):
        # FILETIME range: ~100 trillion for dates 1601-2100
        if value > 116_444_736_000_000_000:  # > 1970-01-01 in FILETIME
            return filetime_to_datetime(value)

        # WebKit range: ~10 trillion for dates 1601-2100
        if value > 11_644_473_600_000_000:  # > 1970-01-01 in WebKit
            return webkit_to_datetime(value)

        # Unix timestamp range
        if 0 < value < 4_102_444_800:  # 1970-2100 in Unix
            try:
                return datetime.fromtimestamp(value, tz=timezone.utc)
            except (OSError, OverflowError, ValueError):
                pass

        # Unix milliseconds
        if value > 1_000_000_000_000:
            try:
                return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
            except (OSError, OverflowError, ValueError):
                pass

    return None
