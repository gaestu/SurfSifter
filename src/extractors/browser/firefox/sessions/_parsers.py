"""
Firefox Session parsing functions.

This module contains the parsing logic for Firefox session files,
separated from the main extractor for maintainability.

Functions:
- parse_session_data: Parse decompressed session JSON into records
- collect_all_urls: Collect ALL URLs from session data (no deduplication)
- extract_form_data: Extract form field values from session entries
- decompress_session_file: Handle Mozilla LZ4 and legacy formats

Extracted from extractor.py, added form data extraction
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from pathlib import Path
    from extractors._shared.extraction_warnings import ExtractionWarningCollector

from ._schemas import (
    KNOWN_SESSION_KEYS,
    KNOWN_WINDOW_KEYS,
    KNOWN_TAB_KEYS,
    KNOWN_ENTRY_KEYS,
    KNOWN_FORMDATA_KEYS,
    IGNORED_KEY_PATTERNS,
)

LOGGER = logging.getLogger(__name__)


# =============================================================================
# Timestamp Helpers
# =============================================================================

def ms_to_iso8601(ms: Optional[int]) -> Optional[str]:
    """Convert milliseconds since 1970 to ISO 8601 string."""
    if ms is None or ms == 0:
        return None

    try:
        unix_seconds = ms / 1000
        dt = datetime.fromtimestamp(unix_seconds, tz=timezone.utc)
        return dt.isoformat()
    except (ValueError, OSError, OverflowError):
        return None


# =============================================================================
# Decompression
# =============================================================================

def decompress_session_file(
    file_path: "Path",
    file_type: str,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> Optional[Dict[str, Any]]:
    """
    Decompress and parse Firefox session file.

    Handles multiple formats:
    - .jsonlz4/.baklz4: Mozilla LZ4 compressed JSON (Firefox 56+)
    - .js: Uncompressed JSON with optional 'sessionstore =' prefix (Firefox < 56)

    Args:
        file_path: Path to session file
        file_type: Classified file type (e.g., "sessionstore_jsonlz4")
        warning_collector: Optional collector for parse errors

    Returns:
        Parsed session data dict, or None if parsing failed
    """
    try:
        data = file_path.read_bytes()
    except Exception as e:
        LOGGER.error("Failed to read session file %s: %s", file_path, e)
        if warning_collector:
            warning_collector.add_warning(
                warning_type="file_read_error",
                item_name=str(file_path),
                item_value=str(e),
                severity="error",
                category="json",
                artifact_type="sessions",
            )
        return None

    # Detect format based on magic bytes
    if data[:8] == b"mozLz40\x00":
        # Mozilla LZ4 compressed format
        try:
            import lz4.block
            json_data = lz4.block.decompress(data[8:])
            return json.loads(json_data)
        except ImportError:
            LOGGER.error("lz4 module not available for decompression")
            if warning_collector:
                warning_collector.add_warning(
                    warning_type="compression_error",
                    item_name="lz4_module",
                    item_value="lz4 module not installed",
                    severity="error",
                    category="binary",
                    artifact_type="sessions",
                )
            return None
        except Exception as e:
            LOGGER.error("Failed to decompress LZ4 session: %s", e)
            if warning_collector:
                warning_collector.add_json_parse_error(
                    filename=str(file_path),
                    error=f"LZ4 decompression failed: {e}",
                )
            return None

    elif file_type.endswith("_js") or str(file_path).endswith(".js"):
        # Legacy uncompressed format
        try:
            text_data = data.decode("utf-8", errors="replace")
            # Strip legacy "sessionstore = " prefix if present
            if text_data.startswith("sessionstore"):
                json_start = text_data.find("{")
                if json_start != -1:
                    text_data = text_data[json_start:]
            return json.loads(text_data)
        except json.JSONDecodeError as e:
            LOGGER.error("Failed to parse legacy session JSON: %s", e)
            if warning_collector:
                warning_collector.add_json_parse_error(
                    filename=str(file_path),
                    error=str(e),
                )
            return None

    else:
        # Try LZ4 first, fall back to plain JSON
        try:
            import lz4.block
            json_data = lz4.block.decompress(data[8:])
            return json.loads(json_data)
        except Exception:
            try:
                return json.loads(data.decode("utf-8", errors="replace"))
            except Exception as e:
                LOGGER.warning("Unknown session format in %s: %s", file_path, e)
                if warning_collector:
                    warning_collector.add_json_parse_error(
                        filename=str(file_path),
                        error=f"Unknown format: {e}",
                    )
                return None


# =============================================================================
# Schema Warning Discovery
# =============================================================================

def _should_ignore_key(key: str) -> bool:
    """Check if a key should be ignored for unknown key reporting."""
    for pattern in IGNORED_KEY_PATTERNS:
        if pattern in key:
            return True
    return False


def discover_unknown_session_keys(
    session_data: Dict[str, Any],
    source_file: str,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> None:
    """
    Discover unknown keys in session data and report to warning collector.

    Checks top-level session keys, window keys, tab keys, and entry keys
    against known schemas.
    """
    if not warning_collector:
        return

    # Check top-level keys
    for key in session_data.keys():
        if key not in KNOWN_SESSION_KEYS and not _should_ignore_key(key):
            warning_collector.add_warning(
                warning_type="json_unknown_key",
                item_name=f"session.{key}",
                item_value=str(type(session_data[key]).__name__),
                severity="info",
                category="json",
                artifact_type="sessions",
                source_file=source_file,
            )

    # Check window keys (sample first window)
    windows = session_data.get("windows", [])
    if windows:
        for key in windows[0].keys():
            if key not in KNOWN_WINDOW_KEYS and not _should_ignore_key(key):
                warning_collector.add_warning(
                    warning_type="json_unknown_key",
                    item_name=f"window.{key}",
                    item_value=str(type(windows[0][key]).__name__),
                    severity="info",
                    category="json",
                    artifact_type="sessions",
                    source_file=source_file,
                )

        # Check tab keys (sample first tab)
        tabs = windows[0].get("tabs", [])
        if tabs:
            for key in tabs[0].keys():
                if key not in KNOWN_TAB_KEYS and not _should_ignore_key(key):
                    warning_collector.add_warning(
                        warning_type="json_unknown_key",
                        item_name=f"tab.{key}",
                        item_value=str(type(tabs[0][key]).__name__),
                        severity="info",
                        category="json",
                        artifact_type="sessions",
                        source_file=source_file,
                    )

            # Check entry keys (sample first entry of first tab)
            entries = tabs[0].get("entries", [])
            if entries:
                for key in entries[0].keys():
                    if key not in KNOWN_ENTRY_KEYS and not _should_ignore_key(key):
                        warning_collector.add_warning(
                            warning_type="json_unknown_key",
                            item_name=f"entry.{key}",
                            item_value=str(type(entries[0][key]).__name__),
                            severity="info",
                            category="json",
                            artifact_type="sessions",
                            source_file=source_file,
                        )


# =============================================================================
# Form Data Extraction
# =============================================================================

def extract_form_data_from_entry(
    entry: Dict[str, Any],
    tab_url: str,
    browser: str,
    profile: str,
    run_id: str,
    discovered_by: str,
    file_entry: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Extract form field data from a session entry.

    Firefox stores form data in two places:
    1. entry["formdata"]["id"] - Maps element IDs to values
    2. entry["formdata"]["xpath"] - Maps XPath expressions to values

    Args:
        entry: Session history entry dict
        tab_url: URL of the tab containing this entry
        browser: Browser key (e.g., "firefox")
        profile: Profile name
        run_id: Extraction run ID
        discovered_by: Discovery provenance string
        file_entry: File metadata dict

    Returns:
        List of form data records for database insertion
    """
    form_records = []
    formdata = entry.get("formdata")

    if not formdata or not isinstance(formdata, dict):
        return form_records

    entry_url = entry.get("url", tab_url)
    entry_title = entry.get("title", "")

    # Extract ID-based form fields
    id_fields = formdata.get("id", {})
    if isinstance(id_fields, dict):
        for field_id, value in id_fields.items():
            if value:  # Skip empty values
                form_records.append({
                    "browser": browser,
                    "profile": profile,
                    "url": entry_url,
                    "page_title": entry_title,
                    "field_type": "id",
                    "field_name": field_id,
                    "field_value": _stringify_form_value(value),
                    "run_id": run_id,
                    "source_path": file_entry.get("logical_path", ""),
                    "discovered_by": discovered_by,
                    "partition_index": file_entry.get("partition_index"),
                    "fs_type": file_entry.get("fs_type"),
                    "logical_path": file_entry.get("logical_path", ""),
                    "forensic_path": file_entry.get("forensic_path"),
                })

    # Extract XPath-based form fields
    xpath_fields = formdata.get("xpath", {})
    if isinstance(xpath_fields, dict):
        for xpath_expr, value in xpath_fields.items():
            if value:  # Skip empty values
                form_records.append({
                    "browser": browser,
                    "profile": profile,
                    "url": entry_url,
                    "page_title": entry_title,
                    "field_type": "xpath",
                    "field_name": xpath_expr,
                    "field_value": _stringify_form_value(value),
                    "run_id": run_id,
                    "source_path": file_entry.get("logical_path", ""),
                    "discovered_by": discovered_by,
                    "partition_index": file_entry.get("partition_index"),
                    "fs_type": file_entry.get("fs_type"),
                    "logical_path": file_entry.get("logical_path", ""),
                    "forensic_path": file_entry.get("forensic_path"),
                })

    # Extract innerHTML (rich text editor content)
    inner_html = formdata.get("innerHTML")
    if inner_html:
        form_records.append({
            "browser": browser,
            "profile": profile,
            "url": entry_url,
            "page_title": entry_title,
            "field_type": "innerHTML",
            "field_name": "richtext_content",
            "field_value": _stringify_form_value(inner_html),
            "run_id": run_id,
            "source_path": file_entry.get("logical_path", ""),
            "discovered_by": discovered_by,
            "partition_index": file_entry.get("partition_index"),
            "fs_type": file_entry.get("fs_type"),
            "logical_path": file_entry.get("logical_path", ""),
            "forensic_path": file_entry.get("forensic_path"),
        })

    return form_records


def _stringify_form_value(value: Any) -> str:
    """Convert form value to string, handling various types."""
    if isinstance(value, str):
        return value
    elif isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    else:
        return str(value)


# =============================================================================
# Session Data Parsing
# =============================================================================

def parse_session_data(
    session_data: Dict[str, Any],
    file_entry: Dict[str, Any],
    run_id: str,
    evidence_id: int,
    browser: str,
    profile: str,
    discovered_by: str,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parse Firefox session data into database records.

    Args:
        session_data: Parsed session JSON
        file_entry: File metadata from manifest
        run_id: Extraction run ID
        evidence_id: Evidence database ID
        browser: Browser key
        profile: Profile name
        discovered_by: Discovery provenance string
        warning_collector: Optional schema warning collector

    Returns:
        Dict with keys: windows, tabs, history, closed_tabs, form_data
    """
    result = {
        "windows": [],
        "tabs": [],
        "history": [],
        "closed_tabs": [],
        "form_data": [],
    }

    # Discover unknown JSON keys for schema tracking
    if warning_collector:
        discover_unknown_session_keys(
            session_data,
            file_entry.get("logical_path", ""),
            warning_collector,
        )

    # Parse active windows
    windows = session_data.get("windows", [])

    for window_idx, window in enumerate(windows):
        # Window record
        result["windows"].append({
            "browser": browser,
            "profile": profile,
            "window_id": window_idx,
            "selected_tab_index": window.get("selected", 0) - 1,  # Firefox is 1-indexed
            "window_type": "normal",
            "session_type": "current",
            "run_id": run_id,
            "source_path": file_entry.get("logical_path", ""),
            "discovered_by": discovered_by,
            "partition_index": file_entry.get("partition_index"),
            "fs_type": file_entry.get("fs_type"),
            "logical_path": file_entry.get("logical_path", ""),
            "forensic_path": file_entry.get("forensic_path"),
        })

        # Parse tabs in window
        for tab_idx, tab in enumerate(window.get("tabs", [])):
            entries = tab.get("entries", [])
            current_idx = tab.get("index", 1) - 1  # Firefox is 1-indexed

            # Get current entry
            current_entry = entries[current_idx] if 0 <= current_idx < len(entries) else {}
            tab_url = current_entry.get("url", "")

            # Tab-level timestamps
            tab_last_accessed = ms_to_iso8601(tab.get("lastAccessed"))
            tab_created_at = ms_to_iso8601(tab.get("createdAt"))

            result["tabs"].append({
                "browser": browser,
                "profile": profile,
                "window_id": window_idx,
                "tab_index": tab_idx,
                "url": tab_url,
                "title": current_entry.get("title", ""),
                "pinned": 1 if tab.get("pinned", False) else 0,
                "group_id": tab.get("groupId"),
                "last_accessed_utc": tab_last_accessed,
                "created_at_utc": tab_created_at,
                "user_context_id": tab.get("userContextId"),  # Container ID
                "run_id": run_id,
                "source_path": file_entry.get("logical_path", ""),
                "discovered_by": discovered_by,
                "partition_index": file_entry.get("partition_index"),
                "fs_type": file_entry.get("fs_type"),
                "logical_path": file_entry.get("logical_path", ""),
                "forensic_path": file_entry.get("forensic_path"),
            })

            # Parse navigation history for this tab
            for nav_idx, entry in enumerate(entries):
                entry_timestamp = None
                if entry.get("lastAccessed"):
                    entry_timestamp = ms_to_iso8601(entry.get("lastAccessed"))
                elif entry.get("lastModified"):
                    entry_timestamp = ms_to_iso8601(entry.get("lastModified"))

                result["history"].append({
                    "browser": browser,
                    "profile": profile,
                    "tab_id": None,  # Resolved after tab insert
                    "_window_id": window_idx,
                    "_tab_index": tab_idx,
                    "nav_index": nav_idx,
                    "url": entry.get("url", ""),
                    "title": entry.get("title", ""),
                    "transition_type": entry.get("triggeringPrincipal_base64"),
                    "timestamp_utc": entry_timestamp,
                    "run_id": run_id,
                    "source_path": file_entry.get("logical_path", ""),
                    "discovered_by": discovered_by,
                    "partition_index": file_entry.get("partition_index"),
                    "fs_type": file_entry.get("fs_type"),
                    "logical_path": file_entry.get("logical_path", ""),
                    "forensic_path": file_entry.get("forensic_path"),
                })

                # Extract form data from this entry
                form_records = extract_form_data_from_entry(
                    entry, tab_url, browser, profile,
                    run_id, discovered_by, file_entry,
                )
                result["form_data"].extend(form_records)

        # Parse recently closed tabs from this window
        for closed_tab_entry in window.get("_closedTabs", []):
            tab_state = closed_tab_entry.get("state", {})
            closed_at = closed_tab_entry.get("closedAt")
            record = _make_closed_tab_record(
                tab_state, closed_at, browser, profile,
                run_id, discovered_by, file_entry,
            )
            if record:
                result["closed_tabs"].append(record)

    # Parse tabs from closed windows
    for closed_window in session_data.get("_closedWindows", []):
        window_closed_at = closed_window.get("closedAt")
        for tab in closed_window.get("tabs", []):
            tab_closed_at = tab.get("closedAt") or window_closed_at
            record = _make_closed_tab_record(
                tab, tab_closed_at, browser, profile,
                run_id, discovered_by, file_entry,
            )
            if record:
                result["closed_tabs"].append(record)

    return result


def _make_closed_tab_record(
    tab_data: Dict[str, Any],
    closed_at_ms: Optional[int],
    browser: str,
    profile: str,
    run_id: str,
    discovered_by: str,
    file_entry: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Create a closed tab record from tab data."""
    entries = tab_data.get("entries", [])
    if not entries:
        return None

    last_entry = entries[-1]
    closed_at_utc = ms_to_iso8601(closed_at_ms) if closed_at_ms else None

    return {
        "browser": browser,
        "profile": profile,
        "url": last_entry.get("url", ""),
        "title": last_entry.get("title", ""),
        "closed_at_utc": closed_at_utc,
        "original_window_id": None,
        "original_tab_index": tab_data.get("index"),
        "run_id": run_id,
        "source_path": file_entry.get("logical_path", ""),
        "discovered_by": discovered_by,
        "partition_index": file_entry.get("partition_index"),
        "fs_type": file_entry.get("fs_type"),
        "logical_path": file_entry.get("logical_path", ""),
        "forensic_path": file_entry.get("forensic_path"),
    }


# =============================================================================
# URL Collection (No Deduplication)
# =============================================================================

def collect_all_urls(
    tab_records: List[Dict[str, Any]],
    history_records: List[Dict[str, Any]],
    closed_tab_records: List[Dict[str, Any]],
    browser: str,
    profile: str,
    run_id: str,
    discovered_by: str,
    file_entry: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Collect ALL URLs from session data for insertion to urls table.

    NO DEDUPLICATION - every URL occurrence is preserved with its timestamp
    for complete forensic record. Investigators can deduplicate in the UI
    if needed.

    Args:
        tab_records: Parsed tab records
        history_records: Parsed navigation history records
        closed_tab_records: Parsed closed tab records
        browser: Browser key
        profile: Profile name
        run_id: Extraction run ID
        discovered_by: Discovery provenance string
        file_entry: File metadata dict

    Returns:
        List of URL records for database insertion
    """
    url_records = []

    def _make_url_record(
        url: str,
        title: str,
        timestamp: Optional[str],
        source_type: str,
    ) -> Optional[Dict[str, Any]]:
        """Create a URL record if URL is valid."""
        if not url or url.startswith(("about:", "chrome:", "moz-extension:", "resource:")):
            return None

        try:
            parsed = urlparse(url)
            domain = parsed.netloc or ""
            scheme = parsed.scheme or ""
        except Exception:
            domain = ""
            scheme = ""

        return {
            "url": url,
            "domain": domain,
            "scheme": scheme,
            "title": title or "",
            "discovered_by": discovered_by,
            "first_seen_utc": timestamp,
            "last_seen_utc": timestamp,
            "source_path": file_entry.get("logical_path", ""),
            "notes": f"Firefox session ({source_type}, profile: {profile})",
            "run_id": run_id,
        }

    # Collect from current tabs
    for tab in tab_records:
        record = _make_url_record(
            tab.get("url", ""),
            tab.get("title", ""),
            tab.get("last_accessed_utc"),
            "session_tab",
        )
        if record:
            url_records.append(record)

    # Collect from navigation history (every entry!)
    for hist in history_records:
        record = _make_url_record(
            hist.get("url", ""),
            hist.get("title", ""),
            hist.get("timestamp_utc"),
            "session_history",
        )
        if record:
            url_records.append(record)

    # Collect from closed tabs
    for closed in closed_tab_records:
        record = _make_url_record(
            closed.get("url", ""),
            closed.get("title", ""),
            closed.get("closed_at_utc"),
            "closed_tab",
        )
        if record:
            url_records.append(record)

    return url_records
