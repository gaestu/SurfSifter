"""
Firefox Downloads parsing utilities with schema warning support.

This module provides parsing functions for Firefox downloads from places.sqlite,
with integrated schema warning collection to detect unknown formats.

Features:
- Modern annotation-based downloads (Firefox v26+)
- Legacy moz_downloads table (Firefox < v26)
- PRTime timestamp conversion to ISO 8601
- Schema warning collection for unknown tables, annotations, and JSON keys
- Statistics collection for manifest population

Usage:
    from extractors.browser.firefox.downloads._parsers import (
        parse_downloads,
        get_download_stats,
    )
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional, Dict, Set, TYPE_CHECKING
from urllib.parse import unquote

from extractors._shared.timestamps import prtime_to_iso, unix_milliseconds_to_iso
from ._schemas import (
    KNOWN_PLACES_TABLES,
    DOWNLOADS_TABLE_PATTERNS,
    KNOWN_DOWNLOAD_ANNOTATIONS,
    KNOWN_ANNOTATION_ATTRIBUTES,
    KNOWN_METADATA_KEYS,
    KNOWN_LEGACY_DOWNLOADS_COLUMNS,
    KNOWN_REPUTATION_VERDICTS,
    FIREFOX_STATE_MAP,
)

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class FirefoxDownload:
    """Single download record from Firefox."""

    url: str
    target_path: str
    filename: str

    # Timestamps (ISO 8601)
    start_time_utc: Optional[str]
    end_time_utc: Optional[str]

    # Size info
    total_bytes: Optional[int]
    received_bytes: Optional[int]

    # Status
    state: str  # complete, in_progress, cancelled, failed, etc.
    mime_type: Optional[str]
    referrer: Optional[str]

    # Forensic fields
    deleted: bool = False  # File was manually removed (from metaData.deleted)
    danger_type: Optional[str] = None  # reputationCheckVerdict if blocked


@dataclass
class FirefoxDownloadStats:
    """Statistics from a Firefox downloads extraction."""

    download_count: int = 0
    complete_count: int = 0
    failed_count: int = 0
    cancelled_count: int = 0
    blocked_count: int = 0
    total_bytes: int = 0
    earliest_download_utc: Optional[str] = None
    latest_download_utc: Optional[str] = None


# =============================================================================
# Main Parsing Functions
# =============================================================================


def parse_downloads(
    db_path: Path,
    source_file: Optional[str] = None,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> Iterator[FirefoxDownload]:
    """
    Parse Firefox downloads from places.sqlite with schema warning support.

    Modern Firefox (v26+) uses moz_annos annotations.
    Legacy Firefox (< v26) uses moz_downloads table.

    Args:
        db_path: Path to places.sqlite
        source_file: Logical path for warning context (optional)
        warning_collector: Collector for schema warnings (optional)

    Yields:
        FirefoxDownload records
    """
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        if warning_collector:
            warning_collector.add_warning(
                warning_type="file_corrupt",
                category="database",
                severity="error",
                artifact_type="downloads",
                source_file=source_file or str(db_path),
                item_name="places.sqlite",
                item_value=str(e),
            )
        return

    try:
        cursor = conn.cursor()

        # Discover unknown tables if warning collector provided
        if warning_collector:
            _check_unknown_tables(cursor, source_file, warning_collector)

        # Check which download storage is used
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('moz_downloads', 'moz_annos', 'moz_places')"
        )
        tables = {row[0] for row in cursor.fetchall()}

        if "moz_downloads" in tables:
            # Legacy Firefox
            yield from _parse_legacy_downloads(
                conn, source_file, warning_collector
            )
        elif tables.issuperset({'moz_annos', 'moz_places'}):
            # Modern Firefox with annotations
            yield from _parse_annotation_downloads(
                conn, source_file, warning_collector
            )
    finally:
        conn.close()


def get_download_stats(
    db_path: Path,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> FirefoxDownloadStats:
    """
    Get statistics from Firefox places.sqlite downloads without full parsing.

    Useful for manifest population and quick overview.

    Args:
        db_path: Path to places.sqlite
        warning_collector: Optional collector (not used here, for API consistency)

    Returns:
        FirefoxDownloadStats with counts and date range
    """
    stats = FirefoxDownloadStats()

    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error:
        return stats

    try:
        cursor = conn.cursor()

        # Check for moz_downloads (legacy)
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='moz_downloads'"
        )
        if cursor.fetchone():
            _get_legacy_stats(cursor, stats)
        else:
            # Modern annotations
            _get_annotation_stats(cursor, stats)
    except sqlite3.Error:
        pass
    finally:
        conn.close()

    return stats


# =============================================================================
# Schema Warning Helpers
# =============================================================================


def _check_unknown_tables(
    cursor: sqlite3.Cursor,
    source_file: Optional[str],
    warning_collector: "ExtractionWarningCollector",
) -> None:
    """Check for unknown tables that might contain download-related data."""
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )
    all_tables = {row[0] for row in cursor.fetchall()}

    unknown_tables = all_tables - KNOWN_PLACES_TABLES

    for table_name in unknown_tables:
        # Filter to potentially download-related tables
        is_relevant = any(
            pattern.lower() in table_name.lower()
            for pattern in DOWNLOADS_TABLE_PATTERNS
        )
        if is_relevant:
            # Get column info for context
            try:
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [row[1] for row in cursor.fetchall()]
                warning_collector.add_unknown_table(
                    table_name=table_name,
                    columns=columns,
                    source_file=source_file or "places.sqlite",
                    artifact_type="downloads",
                )
            except sqlite3.Error:
                warning_collector.add_unknown_table(
                    table_name=table_name,
                    columns=[],
                    source_file=source_file or "places.sqlite",
                    artifact_type="downloads",
                )


def _check_unknown_annotations(
    cursor: sqlite3.Cursor,
    source_file: Optional[str],
    warning_collector: "ExtractionWarningCollector",
) -> None:
    """Check for unknown annotation attributes that might be download-related."""
    try:
        cursor.execute("SELECT name FROM moz_anno_attributes")
        all_attrs = {row[0] for row in cursor.fetchall()}

        unknown_attrs = all_attrs - KNOWN_ANNOTATION_ATTRIBUTES

        for attr_name in unknown_attrs:
            # Filter to download-related annotations
            if "download" in attr_name.lower():
                warning_collector.add_warning(
                    warning_type="unknown_column",
                    category="database",
                    severity="warning",
                    artifact_type="downloads",
                    source_file=source_file or "places.sqlite",
                    item_name=f"moz_anno_attributes.{attr_name}",
                    item_value="unknown annotation attribute",
                )
    except sqlite3.Error:
        pass


def _check_unknown_metadata_keys(
    metadata: Dict,
    source_file: Optional[str],
    warning_collector: "ExtractionWarningCollector",
    found_keys: Set[str],
) -> None:
    """Track unknown keys in metaData JSON."""
    found_keys.update(metadata.keys())


def _report_unknown_metadata_keys(
    found_keys: Set[str],
    source_file: Optional[str],
    warning_collector: "ExtractionWarningCollector",
) -> None:
    """Report unknown metadata keys after parsing all records."""
    unknown_keys = found_keys - KNOWN_METADATA_KEYS
    for key in unknown_keys:
        warning_collector.add_warning(
            warning_type="json_unknown_key",
            category="json",
            severity="info",
            artifact_type="downloads",
            source_file=source_file or "places.sqlite",
            item_name=f"metaData.{key}",
            item_value="unknown metadata key",
        )


def _track_unknown_state(
    state_code: int,
    found_states: Set[int],
) -> None:
    """Track state codes for later reporting."""
    found_states.add(state_code)


def _report_unknown_states(
    found_states: Set[int],
    source_file: Optional[str],
    warning_collector: "ExtractionWarningCollector",
) -> None:
    """Report unknown state codes after parsing all records."""
    known_states = set(FIREFOX_STATE_MAP.keys())
    unknown_states = found_states - known_states

    for state_code in unknown_states:
        warning_collector.add_unknown_token_type(
            token_type=state_code,
            source_file=source_file or "places.sqlite",
            artifact_type="downloads",
        )


def _track_unknown_verdict(
    verdict: str,
    found_verdicts: Set[str],
) -> None:
    """Track reputation verdicts for later reporting."""
    if verdict:
        found_verdicts.add(verdict)


def _report_unknown_verdicts(
    found_verdicts: Set[str],
    source_file: Optional[str],
    warning_collector: "ExtractionWarningCollector",
) -> None:
    """Report unknown reputation verdicts after parsing."""
    unknown_verdicts = found_verdicts - KNOWN_REPUTATION_VERDICTS
    for verdict in unknown_verdicts:
        warning_collector.add_warning(
            warning_type="unknown_enum_value",
            category="database",
            severity="info",
            artifact_type="downloads",
            source_file=source_file or "places.sqlite",
            item_name="reputationCheckVerdict",
            item_value=verdict,
        )


# =============================================================================
# Annotation-based Parsing (Modern Firefox v26+)
# =============================================================================


def _parse_annotation_downloads(
    conn: sqlite3.Connection,
    source_file: Optional[str],
    warning_collector: Optional["ExtractionWarningCollector"],
) -> Iterator[FirefoxDownload]:
    """
    Parse downloads from modern Firefox moz_annos annotations (v26+).

    Firefox stores download metadata in two annotations:
    - downloads/destinationFileURI: file:/// path to downloaded file
    - downloads/metaData: JSON with state, endTime, fileSize, deleted, reputationCheckVerdict
    """
    cursor = conn.cursor()

    # Check for moz_anno_attributes
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name='moz_anno_attributes'"
    )
    if not cursor.fetchone():
        return

    # Check for unknown annotations if warning collector provided
    if warning_collector:
        _check_unknown_annotations(cursor, source_file, warning_collector)

    # Get annotation attribute IDs
    cursor.execute(
        "SELECT id, name FROM moz_anno_attributes "
        "WHERE name IN ('downloads/destinationFileURI', 'downloads/metaData')"
    )
    attr_map = {row["name"]: row["id"] for row in cursor}

    dest_attr_id = attr_map.get('downloads/destinationFileURI')
    meta_attr_id = attr_map.get('downloads/metaData')

    if not dest_attr_id:
        return

    # Build referrer lookup from moz_historyvisits if available
    referrer_map = _build_referrer_map(conn)

    # Query downloads
    cursor.execute("""
        SELECT
            p.url,
            p.last_visit_date,
            dest.content as dest_uri,
            meta.content as metadata
        FROM moz_places p
        JOIN moz_annos dest ON p.id = dest.place_id AND dest.anno_attribute_id = ?
        LEFT JOIN moz_annos meta ON p.id = meta.place_id AND meta.anno_attribute_id = ?
    """, (dest_attr_id, meta_attr_id))

    # Track unknowns for batch reporting
    found_states: Set[int] = set()
    found_metadata_keys: Set[str] = set()
    found_verdicts: Set[str] = set()
    json_parse_errors = 0

    for row in cursor:
        url = row["url"]
        dest_uri = row["dest_uri"]

        # Parse file path from file:/// URI
        target_path = ""
        if dest_uri and dest_uri.startswith("file:///"):
            target_path = unquote(dest_uri[7:])

        filename = _extract_filename(target_path)

        # Parse metadata JSON
        state = "unknown"
        state_code = -1
        end_time_utc = None
        start_time_utc = None
        total_bytes = None
        deleted = False
        danger_type = None

        if row["metadata"]:
            try:
                meta = json.loads(row["metadata"])

                # Track keys for warning reporting
                if warning_collector:
                    _check_unknown_metadata_keys(
                        meta, source_file, warning_collector, found_metadata_keys
                    )

                state_code = meta.get("state", -1)
                state = FIREFOX_STATE_MAP.get(state_code, f"unknown_{state_code}")

                # Track state for warning reporting
                if warning_collector and state_code >= 0:
                    _track_unknown_state(state_code, found_states)

                # endTime: when download finished/stopped (JavaScript milliseconds)
                if "endTime" in meta:
                    end_time_utc = unix_milliseconds_to_iso(meta["endTime"])

                # fileSize: final size on disk
                if "fileSize" in meta:
                    total_bytes = meta["fileSize"]

                # deleted: file was manually removed via browser UI
                if meta.get("deleted"):
                    deleted = True

                # reputationCheckVerdict: block reason if download was flagged
                if "reputationCheckVerdict" in meta:
                    danger_type = meta["reputationCheckVerdict"]
                    if warning_collector:
                        _track_unknown_verdict(danger_type, found_verdicts)

            except json.JSONDecodeError as e:
                json_parse_errors += 1
                if warning_collector and json_parse_errors == 1:
                    # Report only first error to avoid spam
                    warning_collector.add_json_parse_error(
                        filename=source_file or "places.sqlite",
                        error=f"metaData JSON: {e}",
                    )

        # Start time: prefer last_visit_date (when download was initiated)
        if row["last_visit_date"]:
            start_time_utc = prtime_to_iso(row["last_visit_date"])

        # Referrer from moz_historyvisits from_visit chain
        referrer = referrer_map.get(url)

        yield FirefoxDownload(
            url=url,
            target_path=target_path,
            filename=filename,
            start_time_utc=start_time_utc,
            end_time_utc=end_time_utc,
            total_bytes=total_bytes,
            received_bytes=total_bytes,  # Annotations don't track partial downloads
            state=state,
            mime_type=None,  # Not stored in annotations
            referrer=referrer,
            deleted=deleted,
            danger_type=danger_type,
        )

    # Report collected warnings
    if warning_collector:
        _report_unknown_states(found_states, source_file, warning_collector)
        _report_unknown_metadata_keys(found_metadata_keys, source_file, warning_collector)
        _report_unknown_verdicts(found_verdicts, source_file, warning_collector)

        if json_parse_errors > 1:
            warning_collector.add_warning(
                warning_type="json_parse_error",
                category="json",
                severity="warning",
                artifact_type="downloads",
                source_file=source_file or "places.sqlite",
                item_name="metaData",
                item_value=f"{json_parse_errors} total parse errors",
            )


def _build_referrer_map(conn: sqlite3.Connection) -> Dict[str, str]:
    """
    Build referrer lookup from moz_historyvisits.

    Downloads create a visit with transition=7 (TRANSITION_DOWNLOAD).
    The from_visit points to the page that initiated the download.
    """
    referrer_map: Dict[str, str] = {}
    cursor = conn.cursor()

    try:
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='moz_historyvisits'"
        )
        if not cursor.fetchone():
            return referrer_map

        cursor.execute("""
            SELECT
                p.url AS download_url,
                p2.url AS referrer_url
            FROM moz_historyvisits v
            JOIN moz_places p ON v.place_id = p.id
            JOIN moz_historyvisits v2 ON v.from_visit = v2.id
            JOIN moz_places p2 ON v2.place_id = p2.id
            WHERE v.visit_type = 7
        """)
        for row in cursor:
            referrer_map[row["download_url"]] = row["referrer_url"]
    except sqlite3.Error:
        pass

    return referrer_map


# =============================================================================
# Legacy Parsing (Firefox < v26)
# =============================================================================


def _parse_legacy_downloads(
    conn: sqlite3.Connection,
    source_file: Optional[str],
    warning_collector: Optional["ExtractionWarningCollector"],
) -> Iterator[FirefoxDownload]:
    """Parse downloads from legacy Firefox moz_downloads table."""
    cursor = conn.cursor()

    # Check for unknown columns if warning collector provided
    if warning_collector:
        _check_legacy_columns(cursor, source_file, warning_collector)

    # Track unknown states
    found_states: Set[int] = set()

    cursor.execute("""
        SELECT
            name,
            source,
            target,
            startTime,
            endTime,
            state,
            referrer,
            currBytes,
            maxBytes,
            mimeType
        FROM moz_downloads
    """)

    for row in cursor:
        url = row["source"]
        target_uri = row["target"] or ""

        # Parse file path from file:/// URI
        target_path = ""
        if target_uri.startswith("file:///"):
            target_path = unquote(target_uri[7:])

        filename = row["name"] or _extract_filename(target_path)

        # Convert timestamps (PRTime)
        start_time = prtime_to_iso(row["startTime"]) if row["startTime"] else None
        end_time = prtime_to_iso(row["endTime"]) if row["endTime"] else None

        state_code = row["state"]
        state = FIREFOX_STATE_MAP.get(state_code, f"unknown_{state_code}")

        # Track state for warning reporting
        if warning_collector and state_code is not None:
            _track_unknown_state(state_code, found_states)

        yield FirefoxDownload(
            url=url,
            target_path=target_path,
            filename=filename,
            start_time_utc=start_time,
            end_time_utc=end_time,
            total_bytes=row["maxBytes"],
            received_bytes=row["currBytes"],
            state=state,
            mime_type=row["mimeType"],
            referrer=row["referrer"],
        )

    # Report unknown states
    if warning_collector:
        _report_unknown_states(found_states, source_file, warning_collector)


def _check_legacy_columns(
    cursor: sqlite3.Cursor,
    source_file: Optional[str],
    warning_collector: "ExtractionWarningCollector",
) -> None:
    """Check for unknown columns in legacy moz_downloads table."""
    try:
        cursor.execute("PRAGMA table_info(moz_downloads)")
        all_columns = {row[1] for row in cursor.fetchall()}

        unknown_columns = all_columns - KNOWN_LEGACY_DOWNLOADS_COLUMNS

        for col_name in unknown_columns:
            warning_collector.add_unknown_column(
                table_name="moz_downloads",
                column_name=col_name,
                column_type="unknown",
                source_file=source_file or "places.sqlite",
                artifact_type="downloads",
            )
    except sqlite3.Error:
        pass


# =============================================================================
# Statistics Helpers
# =============================================================================


def _get_legacy_stats(cursor: sqlite3.Cursor, stats: FirefoxDownloadStats) -> None:
    """Get statistics from legacy moz_downloads table."""
    try:
        cursor.execute("SELECT COUNT(*) FROM moz_downloads")
        stats.download_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM moz_downloads WHERE state = 1")
        stats.complete_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM moz_downloads WHERE state = 2")
        stats.failed_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM moz_downloads WHERE state = 3")
        stats.cancelled_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM moz_downloads WHERE state IN (5, 7, 8, 9)")
        stats.blocked_count = cursor.fetchone()[0]

        cursor.execute("SELECT SUM(maxBytes) FROM moz_downloads WHERE state = 1")
        result = cursor.fetchone()[0]
        stats.total_bytes = result if result else 0

        # Date range
        cursor.execute("SELECT MIN(startTime), MAX(startTime) FROM moz_downloads")
        row = cursor.fetchone()
        if row[0]:
            stats.earliest_download_utc = prtime_to_iso(row[0])
        if row[1]:
            stats.latest_download_utc = prtime_to_iso(row[1])
    except sqlite3.Error:
        pass


def _get_annotation_stats(cursor: sqlite3.Cursor, stats: FirefoxDownloadStats) -> None:
    """Get statistics from modern annotation-based downloads."""
    try:
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='moz_anno_attributes'"
        )
        if not cursor.fetchone():
            return

        # Get annotation IDs
        cursor.execute(
            "SELECT id, name FROM moz_anno_attributes "
            "WHERE name IN ('downloads/destinationFileURI', 'downloads/metaData')"
        )
        attr_map = {row[0]: row[1] for row in cursor}

        dest_id = None
        meta_id = None
        for attr_id, name in attr_map.items():
            if name == 'downloads/destinationFileURI':
                dest_id = attr_id
            elif name == 'downloads/metaData':
                meta_id = attr_id

        if not dest_id:
            return

        # Count downloads
        cursor.execute(
            "SELECT COUNT(*) FROM moz_annos WHERE anno_attribute_id = ?",
            (dest_id,)
        )
        stats.download_count = cursor.fetchone()[0]

        # Count by state if metadata available
        if meta_id:
            cursor.execute(
                "SELECT content FROM moz_annos WHERE anno_attribute_id = ?",
                (meta_id,)
            )
            for row in cursor:
                try:
                    meta = json.loads(row[0])
                    state = meta.get("state", -1)
                    if state == 1:
                        stats.complete_count += 1
                    elif state == 2:
                        stats.failed_count += 1
                    elif state == 3:
                        stats.cancelled_count += 1
                    elif state in (5, 7, 8, 9):
                        stats.blocked_count += 1

                    if meta.get("fileSize"):
                        stats.total_bytes += meta["fileSize"]
                except json.JSONDecodeError:
                    pass

        # Date range from moz_places
        cursor.execute("""
            SELECT MIN(p.last_visit_date), MAX(p.last_visit_date)
            FROM moz_places p
            JOIN moz_annos a ON p.id = a.place_id
            WHERE a.anno_attribute_id = ?
        """, (dest_id,))
        row = cursor.fetchone()
        if row[0]:
            stats.earliest_download_utc = prtime_to_iso(row[0])
        if row[1]:
            stats.latest_download_utc = prtime_to_iso(row[1])
    except sqlite3.Error:
        pass


# =============================================================================
# Utility Functions
# =============================================================================


def _extract_filename(path: str) -> str:
    """Extract filename from a path (Windows or Unix)."""
    if not path:
        return ""

    # Handle both Windows backslashes and Unix forward slashes
    if '\\' in path:
        parts = path.split('\\')
    else:
        parts = path.split('/')

    return parts[-1] if parts else ""
