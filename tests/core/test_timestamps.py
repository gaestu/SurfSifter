"""
Tests for timestamp conversion utilities.
"""

import pytest
from datetime import datetime, timezone

from core.timestamps import (
    webkit_to_datetime,
    webkit_to_iso,
    prtime_to_datetime,
    prtime_to_iso,
    unix_to_datetime,
    unix_to_iso,
    cocoa_to_datetime,
    cocoa_to_iso,
    utc_now,
    parse_iso,
    format_duration,
    WEBKIT_EPOCH_DIFF,
    COCOA_EPOCH_DIFF,
)


class TestWebKitTimestamps:
    """Tests for WebKit timestamp conversion (Chromium browsers)."""

    def test_webkit_to_datetime_valid(self):
        """Convert valid WebKit timestamp to datetime."""
        # 2020-01-01 00:00:00 UTC
        # Unix timestamp: 1577836800
        # WebKit: (1577836800 + 11644473600) * 1_000_000 = 13222310400000000
        webkit_ts = 13222310400000000
        dt = webkit_to_datetime(webkit_ts)

        assert dt is not None
        assert dt.year == 2020
        assert dt.month == 1
        assert dt.day == 1
        assert dt.tzinfo == timezone.utc

    def test_webkit_to_datetime_zero(self):
        """Return None for zero timestamp."""
        assert webkit_to_datetime(0) is None

    def test_webkit_to_datetime_negative(self):
        """Return None for negative timestamp."""
        assert webkit_to_datetime(-1) is None

    def test_webkit_to_datetime_overflow(self):
        """Return None for overflow timestamp (beyond year 3000)."""
        huge_ts = 99999999999999999999
        assert webkit_to_datetime(huge_ts) is None

    def test_webkit_to_iso_valid(self):
        """Convert valid WebKit timestamp to ISO string."""
        webkit_ts = 13222310400000000  # 2020-01-01 00:00:00
        iso = webkit_to_iso(webkit_ts)

        assert iso is not None
        assert iso.startswith("2020-01-01")

    def test_webkit_to_iso_invalid(self):
        """Return None for invalid timestamp."""
        assert webkit_to_iso(0) is None


class TestPRTimeTimestamps:
    """Tests for PRTime timestamp conversion (Firefox)."""

    def test_prtime_to_datetime_valid(self):
        """Convert valid PRTime timestamp to datetime."""
        # 2020-01-01 00:00:00 UTC in microseconds
        prtime_ts = 1577836800000000
        dt = prtime_to_datetime(prtime_ts)

        assert dt is not None
        assert dt.year == 2020
        assert dt.month == 1
        assert dt.day == 1
        assert dt.tzinfo == timezone.utc

    def test_prtime_to_datetime_zero(self):
        """Return None for zero timestamp."""
        assert prtime_to_datetime(0) is None

    def test_prtime_to_datetime_negative(self):
        """Return None for negative timestamp."""
        assert prtime_to_datetime(-1) is None

    def test_prtime_to_iso_valid(self):
        """Convert valid PRTime timestamp to ISO string."""
        prtime_ts = 1577836800000000  # 2020-01-01 00:00:00
        iso = prtime_to_iso(prtime_ts)

        assert iso is not None
        assert iso.startswith("2020-01-01")


class TestUnixTimestamps:
    """Tests for Unix timestamp conversion."""

    def test_unix_to_datetime_valid(self):
        """Convert valid Unix timestamp to datetime."""
        unix_ts = 1577836800  # 2020-01-01 00:00:00 UTC
        dt = unix_to_datetime(unix_ts)

        assert dt is not None
        assert dt.year == 2020
        assert dt.month == 1
        assert dt.day == 1
        assert dt.tzinfo == timezone.utc

    def test_unix_to_datetime_float(self):
        """Convert Unix timestamp with fractional seconds."""
        unix_ts = 1577836800.5
        dt = unix_to_datetime(unix_ts)

        assert dt is not None
        assert dt.year == 2020

    def test_unix_to_datetime_zero(self):
        """Return None for zero timestamp."""
        assert unix_to_datetime(0) is None

    def test_unix_to_iso_valid(self):
        """Convert valid Unix timestamp to ISO string."""
        unix_ts = 1577836800
        iso = unix_to_iso(unix_ts)

        assert iso is not None
        assert iso.startswith("2020-01-01")


class TestCocoaTimestamps:
    """Tests for Cocoa timestamp conversion (Safari/macOS)."""

    def test_cocoa_to_datetime_valid(self):
        """Convert valid Cocoa timestamp to datetime."""
        # 2020-01-01 00:00:00 UTC
        # Unix: 1577836800
        # Cocoa: 1577836800 - 978307200 = 599529600
        cocoa_ts = 599529600
        dt = cocoa_to_datetime(cocoa_ts)

        assert dt is not None
        assert dt.year == 2020
        assert dt.month == 1
        assert dt.day == 1

    def test_cocoa_to_datetime_none(self):
        """Return None for None input."""
        assert cocoa_to_datetime(None) is None

    def test_cocoa_to_iso_valid(self):
        """Convert valid Cocoa timestamp to ISO string."""
        cocoa_ts = 599529600  # 2020-01-01 00:00:00
        iso = cocoa_to_iso(cocoa_ts)

        assert iso is not None
        assert iso.startswith("2020-01-01")


class TestUtcNow:
    """Tests for UTC now function."""

    def test_utc_now_format(self):
        """utc_now returns valid ISO format."""
        now = utc_now()

        assert now is not None
        assert "T" in now
        assert "+00:00" in now or "Z" in now

    def test_utc_now_parseable(self):
        """utc_now result can be parsed."""
        now = utc_now()
        dt = parse_iso(now)

        assert dt is not None
        assert dt.tzinfo is not None


class TestParseIso:
    """Tests for ISO string parsing."""

    def test_parse_iso_standard(self):
        """Parse standard ISO format."""
        dt = parse_iso("2020-01-01T00:00:00+00:00")

        assert dt is not None
        assert dt.year == 2020

    def test_parse_iso_with_z(self):
        """Parse ISO format with Z suffix."""
        dt = parse_iso("2020-01-01T00:00:00Z")

        assert dt is not None
        assert dt.year == 2020

    def test_parse_iso_none(self):
        """Return None for None input."""
        assert parse_iso(None) is None

    def test_parse_iso_empty(self):
        """Return None for empty string."""
        assert parse_iso("") is None

    def test_parse_iso_invalid(self):
        """Return None for invalid format."""
        assert parse_iso("not-a-date") is None


class TestFormatDuration:
    """Tests for duration formatting."""

    def test_format_duration_seconds(self):
        """Format seconds only."""
        assert format_duration(45) == "45s"

    def test_format_duration_minutes(self):
        """Format minutes and seconds."""
        assert format_duration(90) == "1m 30s"

    def test_format_duration_hours(self):
        """Format hours, minutes, and seconds."""
        assert format_duration(3661) == "1h 1m 1s"

    def test_format_duration_zero(self):
        """Format zero seconds."""
        assert format_duration(0) == "0s"

    def test_format_duration_negative(self):
        """Format negative returns 0s."""
        assert format_duration(-10) == "0s"

    def test_format_duration_exact_hour(self):
        """Format exact hour."""
        assert format_duration(3600) == "1h"

    def test_format_duration_exact_minute(self):
        """Format exact minute."""
        assert format_duration(60) == "1m"
