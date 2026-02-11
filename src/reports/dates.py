"""
Shared date/time formatting helpers for report output.

Keeps all report modules consistent with the selected date format.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

# Common datetime input formats to try (fallback when ISO parsing fails)
_INPUT_FORMATS = [
    "%Y-%m-%d",
    "%d.%m.%Y",
    "%m/%d/%Y",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%d.%m.%Y %H:%M",
    "%d.%m.%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
]


def _try_parse(value: str) -> Optional[datetime]:
    """Try to parse a date/time string into a datetime."""
    text = (value or "").strip()
    if not text:
        return None

    # Strip a trailing " UTC" if present
    if text.endswith(" UTC"):
        text = text[:-4]

    # Try ISO parsing first (handles offsets)
    iso_candidate = text
    if iso_candidate.endswith("Z"):
        iso_candidate = iso_candidate[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(iso_candidate)
    except ValueError:
        pass

    # If ISO failed, strip timezone offset and try common formats
    tz_split = iso_candidate
    for sep in ("+", "-"):
        idx = tz_split.find(sep, 10)  # after date part
        if idx != -1:
            tz_split = tz_split[:idx]
            break

    for fmt in _INPUT_FORMATS:
        try:
            return datetime.strptime(tz_split, fmt)
        except ValueError:
            continue

    return None


def format_datetime(
    value: str | None,
    date_format: str = "eu",
    *,
    include_time: bool = True,
    include_seconds: bool = True,
) -> str:
    """Format a datetime string according to the selected date format.

    Args:
        value: Input timestamp string.
        date_format: "eu" for dd.mm.yyyy, "us" for mm/dd/yyyy.
        include_time: Whether to include time if present.
        include_seconds: Whether to include seconds when time is shown.

    Returns:
        Formatted date/time string, or original value if parsing fails.
    """
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""

    dt = _try_parse(text)
    if not dt:
        return text

    date_part = "%d.%m.%Y" if date_format == "eu" else "%m/%d/%Y"
    has_time = ":" in text
    if not include_time or not has_time:
        return dt.strftime(date_part)

    time_part = "%H:%M:%S" if include_seconds else "%H:%M"
    return dt.strftime(f"{date_part} {time_part}")


def format_date(value: str | None, date_format: str = "eu") -> str:
    """Format a date-only value using the selected date format."""
    return format_datetime(value, date_format, include_time=False)
