"""
Firefox Transport Security parser functions.

This module contains parser functions for Firefox SiteSecurityServiceState.txt.
Each parser handles timestamp conversion and warning collection.

Initial parser extraction from extractor.py
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING

from ._schemas import (
    KNOWN_ENTRY_TYPES,
    KNOWN_STATE_VALUES,
    STATE_TO_MODE,
    PRTIME_DAYS_TO_SECONDS,
    MIN_LINE_FIELDS,
    EXPECTED_DATA_PARTS,
    WARNING_CATEGORY,
    WARNING_ARTIFACT_TYPE,
)

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector

from core.logging import get_logger

LOGGER = get_logger("extractors.browser.firefox.transport_security._parsers")


# =============================================================================
# Timestamp Conversion Functions
# =============================================================================

def days_since_epoch_to_iso8601(days: int) -> Optional[str]:
    """
    Convert Firefox PRTime days since epoch to ISO 8601.

    Firefox SiteSecurityServiceState.txt stores last_access as integer days
    since Unix epoch (PRTime / 86400000000).

    Args:
        days: Days since Unix epoch

    Returns:
        ISO 8601 timestamp string or None if invalid
    """
    if not days or days <= 0:
        return None
    try:
        unix_seconds = days * PRTIME_DAYS_TO_SECONDS
        # Sanity check: should be between 1970 and 2100
        if unix_seconds < 0 or unix_seconds > 4102444800:  # 2100-01-01
            return None
        dt = datetime.fromtimestamp(unix_seconds, tz=timezone.utc)
        return dt.isoformat()
    except (ValueError, OSError, OverflowError):
        return None


def ms_to_unix_seconds(ms: int) -> Optional[float]:
    """
    Convert milliseconds since epoch to Unix seconds with validation.

    Args:
        ms: Milliseconds since Unix epoch

    Returns:
        Unix timestamp in seconds or None if out of range
    """
    if not ms or ms <= 0:
        return None
    try:
        unix_seconds = ms / 1000
        # Sanity check: should be between 1970 and 2100
        if unix_seconds < 0 or unix_seconds > 4102444800:  # 2100-01-01
            return None
        return unix_seconds
    except (ValueError, OverflowError):
        return None


def unix_to_iso8601(timestamp: float) -> Optional[str]:
    """
    Convert Unix timestamp (seconds) to ISO 8601 with validation.

    Args:
        timestamp: Unix timestamp in seconds

    Returns:
        ISO 8601 timestamp string or None if invalid
    """
    if not timestamp or timestamp <= 0:
        return None
    try:
        # Sanity check: should be between 1970 and 2100
        if timestamp < 0 or timestamp > 4102444800:  # 2100-01-01
            return None
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return dt.isoformat()
    except (ValueError, OSError, OverflowError):
        return None


# =============================================================================
# Line Parsing Functions
# =============================================================================

def parse_line(
    line: str,
    line_num: int,
    source_file: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
    found_entry_types: Optional[Set[str]] = None,
    found_state_values: Optional[Set[int]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Parse a single SiteSecurityServiceState line.

    Firefox format: <host>:<type>\t<score>\t<last_access>\t<expiry_ms>,<state>,<include_subdomains>

    Args:
        line: Raw line from file
        line_num: Line number for error reporting
        source_file: Source file path for warnings
        warning_collector: Optional collector for schema warnings
        found_entry_types: Optional set to track found entry types
        found_state_values: Optional set to track found state values

    Returns:
        Parsed entry dict or None if invalid
    """
    parts = line.split('\t')

    if len(parts) < MIN_LINE_FIELDS:
        if warning_collector and line.strip():
            warning_collector.add_warning(
                warning_type="format_error",
                category=WARNING_CATEGORY,
                severity="warning",
                artifact_type=WARNING_ARTIFACT_TYPE,
                source_file=source_file,
                item_name=f"line_{line_num}",
                item_value=f"insufficient_fields:{len(parts)}",
                context_json={"line": line[:100]},  # Truncate for safety
            )
        LOGGER.debug("Line %d: insufficient parts (%d)", line_num, len(parts))
        return None

    # Parse host:type
    host_type = parts[0]
    match = re.match(r'^(.+):([^:]+)$', host_type)
    if not match:
        if warning_collector:
            warning_collector.add_warning(
                warning_type="format_error",
                category=WARNING_CATEGORY,
                severity="warning",
                artifact_type=WARNING_ARTIFACT_TYPE,
                source_file=source_file,
                item_name=f"line_{line_num}",
                item_value="invalid_host_type_format",
                context_json={"host_type": host_type[:50]},
            )
        LOGGER.debug("Line %d: invalid host:type format: %s", line_num, host_type)
        return None

    host = match.group(1)
    entry_type = match.group(2)

    # Track entry type for warning collection
    if found_entry_types is not None:
        found_entry_types.add(entry_type)

    # Parse score (parts[1]) - usually 0, not forensically significant
    score = None
    try:
        score = int(parts[1])
    except ValueError:
        pass

    # Parse last_access (parts[2]) - days since Unix epoch
    last_access_days = None
    try:
        last_access_days = int(parts[2])
    except ValueError:
        pass

    # Parse data field (parts[3]): expiry_ms,state,include_subdomains
    data_parts = parts[3].split(',') if len(parts) > 3 else []

    expiry_ms = None
    state = None
    include_subdomains = 0

    if len(data_parts) >= 1:
        try:
            expiry_ms = int(data_parts[0])
        except ValueError:
            pass

    if len(data_parts) >= 2:
        try:
            state = int(data_parts[1])
            # Track state value for warning collection
            if found_state_values is not None:
                found_state_values.add(state)
        except ValueError:
            pass

    if len(data_parts) >= 3:
        try:
            include_subdomains = int(data_parts[2])
        except ValueError:
            pass

    # Warn about unexpected data field format
    if warning_collector and len(data_parts) != EXPECTED_DATA_PARTS:
        warning_collector.add_warning(
            warning_type="format_warning",
            category=WARNING_CATEGORY,
            severity="info",
            artifact_type=WARNING_ARTIFACT_TYPE,
            source_file=source_file,
            item_name=f"line_{line_num}_data_parts",
            item_value=f"expected_{EXPECTED_DATA_PARTS}_got_{len(data_parts)}",
        )

    # Convert expiry from milliseconds to seconds with validation
    expiry_seconds = ms_to_unix_seconds(expiry_ms) if expiry_ms else None

    # Convert last_access from days to ISO 8601
    last_access_iso = days_since_epoch_to_iso8601(last_access_days) if last_access_days else None

    # Map state to mode string
    mode = STATE_TO_MODE.get(state) if state is not None else None

    return {
        "host": host,
        "entry_type": entry_type,
        "score": score,
        "last_access_days": last_access_days,
        "last_access_iso": last_access_iso,
        "expiry_ms": expiry_ms,
        "expiry_seconds": expiry_seconds,
        "state": state,
        "mode": mode,
        "include_subdomains": include_subdomains,
    }


def track_unknown_entry_types(
    found_types: Set[str],
    source_file: str,
    warning_collector: Optional["ExtractionWarningCollector"],
) -> None:
    """
    Report unknown entry types to warning collector.

    Args:
        found_types: Set of entry types found during parsing
        source_file: Source file path
        warning_collector: Warning collector instance
    """
    if not warning_collector or not found_types:
        return

    unknown_types = found_types - KNOWN_ENTRY_TYPES
    for entry_type in unknown_types:
        warning_collector.add_warning(
            warning_type="unknown_entry_type",
            category=WARNING_CATEGORY,
            severity="warning",
            artifact_type=WARNING_ARTIFACT_TYPE,
            source_file=source_file,
            item_name="entry_type",
            item_value=entry_type,
        )
        LOGGER.info("Unknown entry type discovered: %s", entry_type)


def track_unknown_state_values(
    found_values: Set[int],
    source_file: str,
    warning_collector: Optional["ExtractionWarningCollector"],
) -> None:
    """
    Report unknown state values to warning collector.

    Args:
        found_values: Set of state values found during parsing
        source_file: Source file path
        warning_collector: Warning collector instance
    """
    if not warning_collector or not found_values:
        return

    unknown_values = found_values - set(KNOWN_STATE_VALUES.keys())
    for value in unknown_values:
        warning_collector.add_warning(
            warning_type="unknown_state_value",
            category=WARNING_CATEGORY,
            severity="warning",
            artifact_type=WARNING_ARTIFACT_TYPE,
            source_file=source_file,
            item_name="state",
            item_value=str(value),
        )
        LOGGER.info("Unknown state value discovered: %d", value)


# =============================================================================
# File Parsing Functions
# =============================================================================

def parse_transport_security_file(
    content: str,
    source_file: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse entire SiteSecurityServiceState.txt content.

    Args:
        content: File content as string
        source_file: Source file path for warnings
        warning_collector: Optional collector for schema warnings

    Returns:
        List of parsed entry dicts
    """
    entries = []
    found_entry_types: Set[str] = set()
    found_state_values: Set[int] = set()

    for line_num, line in enumerate(content.splitlines(), 1):
        line = line.strip()
        if not line or line.startswith('#'):
            continue

        entry = parse_line(
            line,
            line_num,
            source_file,
            warning_collector=warning_collector,
            found_entry_types=found_entry_types,
            found_state_values=found_state_values,
        )

        if entry:
            entries.append(entry)

    # Track unknown types and values after parsing all lines
    track_unknown_entry_types(found_entry_types, source_file, warning_collector)
    track_unknown_state_values(found_state_values, source_file, warning_collector)

    return entries
