"""Tests for report date formatting helpers."""

import pytest

from reports.dates import format_datetime, format_date, _try_parse


class TestTryParse:
    """Test the internal _try_parse helper."""

    def test_parse_iso_datetime(self):
        """Parse standard ISO datetime."""
        dt = _try_parse("2024-01-15T10:30:45")
        assert dt is not None
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 15
        assert dt.hour == 10
        assert dt.minute == 30
        assert dt.second == 45

    def test_parse_iso_with_z_suffix(self):
        """Parse ISO datetime with Z timezone suffix."""
        dt = _try_parse("2024-01-15T10:30:45Z")
        assert dt is not None
        assert dt.year == 2024

    def test_parse_iso_with_offset(self):
        """Parse ISO datetime with timezone offset."""
        dt = _try_parse("2024-01-15T10:30:45+02:00")
        assert dt is not None
        assert dt.year == 2024

    def test_parse_european_date(self):
        """Parse European date format dd.mm.yyyy."""
        dt = _try_parse("15.01.2024")
        assert dt is not None
        assert dt.day == 15
        assert dt.month == 1
        assert dt.year == 2024

    def test_parse_us_date(self):
        """Parse US date format mm/dd/yyyy."""
        dt = _try_parse("01/15/2024")
        assert dt is not None
        assert dt.month == 1
        assert dt.day == 15
        assert dt.year == 2024

    def test_parse_datetime_with_microseconds(self):
        """Parse datetime with microseconds."""
        dt = _try_parse("2024-01-15T10:30:45.123456")
        assert dt is not None
        assert dt.microsecond == 123456

    def test_parse_datetime_with_utc_suffix(self):
        """Parse datetime with ' UTC' suffix."""
        dt = _try_parse("2024-01-15 10:30:45 UTC")
        assert dt is not None
        assert dt.year == 2024

    def test_parse_empty_string(self):
        """Empty string returns None."""
        assert _try_parse("") is None
        assert _try_parse("   ") is None

    def test_parse_none(self):
        """None input returns None."""
        assert _try_parse(None) is None

    def test_parse_invalid_string(self):
        """Invalid string returns None."""
        assert _try_parse("not-a-date") is None


class TestFormatDatetime:
    """Test format_datetime function."""

    def test_eu_format_date_only(self):
        """Format as European date only."""
        result = format_datetime("2024-01-15", "eu", include_time=False)
        assert result == "15.01.2024"

    def test_us_format_date_only(self):
        """Format as US date only."""
        result = format_datetime("2024-01-15", "us", include_time=False)
        assert result == "01/15/2024"

    def test_eu_format_with_time(self):
        """Format as European date with time."""
        result = format_datetime("2024-01-15T10:30:45", "eu", include_time=True, include_seconds=True)
        assert result == "15.01.2024 10:30:45"

    def test_us_format_with_time(self):
        """Format as US date with time."""
        result = format_datetime("2024-01-15T10:30:45", "us", include_time=True, include_seconds=True)
        assert result == "01/15/2024 10:30:45"

    def test_eu_format_without_seconds(self):
        """Format without seconds."""
        result = format_datetime("2024-01-15T10:30:45", "eu", include_time=True, include_seconds=False)
        assert result == "15.01.2024 10:30"

    def test_us_format_without_seconds(self):
        """Format without seconds."""
        result = format_datetime("2024-01-15T10:30:45", "us", include_time=True, include_seconds=False)
        assert result == "01/15/2024 10:30"

    def test_none_input_returns_empty(self):
        """None input returns empty string."""
        assert format_datetime(None, "eu") == ""

    def test_empty_input_returns_empty(self):
        """Empty string returns empty string."""
        assert format_datetime("", "eu") == ""
        assert format_datetime("   ", "eu") == ""

    def test_unparseable_returns_original(self):
        """Unparseable string returns original."""
        result = format_datetime("not-a-date", "eu")
        assert result == "not-a-date"

    def test_default_format_is_eu(self):
        """Default date format is EU."""
        result = format_datetime("2024-01-15", include_time=False)
        assert result == "15.01.2024"

    def test_iso_with_timezone(self):
        """ISO datetime with timezone formats correctly."""
        result = format_datetime("2024-01-15T10:30:45+00:00", "eu", include_time=True)
        assert result == "15.01.2024 10:30:45"

    def test_iso_with_z_suffix(self):
        """ISO datetime with Z suffix formats correctly."""
        result = format_datetime("2024-01-15T10:30:45Z", "eu", include_time=True)
        assert result == "15.01.2024 10:30:45"

    def test_space_separated_datetime(self):
        """Space-separated datetime formats correctly."""
        result = format_datetime("2024-01-15 10:30:45", "eu", include_time=True)
        assert result == "15.01.2024 10:30:45"

    def test_date_only_input_with_include_time_true(self):
        """Date-only input with include_time=True returns date only (no colon in input)."""
        result = format_datetime("2024-01-15", "eu", include_time=True)
        assert result == "15.01.2024"


class TestFormatDate:
    """Test format_date convenience function."""

    def test_eu_format(self):
        """Format as European date."""
        result = format_date("2024-01-15", "eu")
        assert result == "15.01.2024"

    def test_us_format(self):
        """Format as US date."""
        result = format_date("2024-01-15", "us")
        assert result == "01/15/2024"

    def test_datetime_input_strips_time(self):
        """Datetime input returns date only."""
        result = format_date("2024-01-15T10:30:45", "eu")
        assert result == "15.01.2024"

    def test_none_returns_empty(self):
        """None returns empty string."""
        assert format_date(None, "eu") == ""

    def test_empty_returns_empty(self):
        """Empty string returns empty string."""
        assert format_date("", "eu") == ""


class TestEdgeCases:
    """Test edge cases and real-world inputs."""

    def test_windows_filetime_style_already_formatted(self):
        """Registry extractor already formats FILETIME values, pass through."""
        # This might come pre-formatted from registry extractor
        result = format_datetime("15.01.2024 10:30:45", "eu", include_time=True)
        assert result == "15.01.2024 10:30:45"

    def test_us_formatted_input_to_eu_output(self):
        """US-formatted input converted to EU output."""
        result = format_datetime("01/15/2024 10:30:45", "eu", include_time=True)
        assert result == "15.01.2024 10:30:45"

    def test_eu_formatted_input_to_us_output(self):
        """EU-formatted input converted to US output."""
        result = format_datetime("15.01.2024 10:30:45", "us", include_time=True)
        assert result == "01/15/2024 10:30:45"

    def test_milliseconds_stripped(self):
        """Datetime with milliseconds formats without them."""
        result = format_datetime("2024-01-15T10:30:45.123", "eu", include_time=True)
        assert result == "15.01.2024 10:30:45"
