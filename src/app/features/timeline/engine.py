"""
Timeline engine - event mapping, building, and persistence.

Refactored from src/core/timelines.py
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional

from core.logging import get_logger
from .config import TimelineConfig

LOGGER = get_logger("app.features.timeline.engine")

# Progress callback type for build_timeline
ProgressCallback = Optional[Callable[[float, str], None]]


@dataclass(slots=True)
class TimelineEvent:
    """A single timeline event."""
    evidence_id: int
    ts_utc: datetime
    kind: str
    ref_table: str
    ref_id: int
    confidence: str
    note: str = ""
    provenance: str = ""


# =============================================================================
# Timestamp Parsing Utilities
# =============================================================================


def _parse_timestamp(ts_str: Optional[str]) -> Optional[datetime]:
    """Parse ISO-8601 timestamp string to datetime object"""
    if not ts_str:
        return None
    try:
        # Handle both with and without timezone
        if "+" in ts_str or ts_str.endswith("Z"):
            return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        else:
            # Assume UTC if no timezone
            return datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
    except (ValueError, AttributeError) as exc:
        LOGGER.debug("Failed to parse timestamp '%s': %s", ts_str, exc)
        return None


def _unix_to_datetime(unix_ts: Optional[float]) -> Optional[datetime]:
    """Convert Unix timestamp (seconds since 1970) to datetime object in UTC.

    Used for hsts_entries.sts_observed and hsts_entries.expiry which are stored
    as REAL (Unix timestamps) rather than TEXT (ISO-8601).
    """
    if unix_ts is None or unix_ts <= 0:
        return None
    try:
        return datetime.fromtimestamp(unix_ts, tz=timezone.utc)
    except (ValueError, OSError) as exc:
        LOGGER.debug("Failed to convert Unix timestamp %s: %s", unix_ts, exc)
        return None


def _format_note(template: str, context: Dict[str, Any]) -> str:
    """Format note template with context values, safely handling missing keys"""
    try:
        return template.format(**context)
    except KeyError:
        # Fallback: just use available context as string
        return f"{template} | {context}"


# =============================================================================
# Source Mappers
# =============================================================================


def map_browser_history_to_events(
    conn: sqlite3.Connection,
    evidence_id: int,
    config: TimelineConfig
) -> List[TimelineEvent]:
    """Map browser_history records to timeline events"""
    source_config = config.sources.get("browser_history", {})
    confidence = source_config.get("confidence", "medium")
    mappings_list = source_config.get("mappings", [
        {"timestamp_field": "ts_utc", "kind": "browser_visit", "note_template": "{browser} visit: {title}"}
    ])

    query = """
        SELECT id, url, title, ts_utc, browser, profile
        FROM browser_history
        WHERE evidence_id = ?
        AND ts_utc IS NOT NULL
        ORDER BY ts_utc
    """

    events = []
    cursor = conn.execute(query, (evidence_id,))

    for row in cursor:
        try:
            row_data = {
                "browser": row["browser"] or "unknown",
                "title": row["title"] or row["url"],
                "url": row["url"],
                "profile": row["profile"] or ""
            }

            for mapping in mappings_list:
                ts_field = mapping.get("timestamp_field", "ts_utc")
                if not row[ts_field]:
                    continue

                ts = _parse_timestamp(row[ts_field])
                if ts is None:
                    continue

                note = _format_note(
                    mapping.get("note_template", "{browser} visit: {title}"),
                    row_data
                )

                events.append(TimelineEvent(
                    evidence_id=evidence_id,
                    ts_utc=ts,
                    kind=mapping.get("kind", "browser_visit"),
                    ref_table="browser_history",
                    ref_id=row["id"],
                    confidence=confidence,
                    note=note,
                    provenance=f"browser:{row['browser']}"
                ))
        except Exception as exc:
            LOGGER.warning("Failed to map browser_history row %s: %s", row["id"], exc)

    return events


def map_urls_to_events(
    conn: sqlite3.Connection,
    evidence_id: int,
    config: TimelineConfig
) -> List[TimelineEvent]:
    """Map urls records to timeline events"""
    source_config = config.sources.get("urls", {})
    confidence = source_config.get("confidence", "medium")
    mappings_list = source_config.get("mappings", [
        {"timestamp_field": "first_seen_utc", "kind": "url_discovered", "note_template": "URL: {url} (via {discovered_by})"}
    ])

    query = """
        SELECT id, url, domain, discovered_by, first_seen_utc, last_seen_utc
        FROM urls
        WHERE evidence_id = ?
        AND (first_seen_utc IS NOT NULL OR last_seen_utc IS NOT NULL)
        ORDER BY first_seen_utc
    """

    events = []
    cursor = conn.execute(query, (evidence_id,))

    for row in cursor:
        try:
            row_data = {
                "discovered_by": row["discovered_by"] or "unknown",
                "domain": row["domain"] or "unknown",
                "url": row["url"]
            }

            for mapping in mappings_list:
                ts_field = mapping.get("timestamp_field", "first_seen_utc")
                if not row[ts_field]:
                    continue

                ts = _parse_timestamp(row[ts_field])
                if ts is None:
                    continue

                note = _format_note(
                    mapping.get("note_template", "URL: {url} (via {discovered_by})"),
                    row_data
                )

                events.append(TimelineEvent(
                    evidence_id=evidence_id,
                    ts_utc=ts,
                    kind=mapping.get("kind", "url_discovered"),
                    ref_table="urls",
                    ref_id=row["id"],
                    confidence=confidence,
                    note=note,
                    provenance=f"discovered_by:{row['discovered_by']}"
                ))
        except Exception as exc:
            LOGGER.warning("Failed to map urls row %s: %s", row["id"], exc)

    return events


def map_images_to_events(
    conn: sqlite3.Connection,
    evidence_id: int,
    config: TimelineConfig
) -> List[TimelineEvent]:
    """Map images records to timeline events"""
    source_config = config.sources.get("images", {})
    confidence = source_config.get("confidence", "medium")
    mappings_list = source_config.get("mappings", [
        {"timestamp_field": "ts_utc", "kind": "image_extracted", "note_template": "Image: {filename}"}
    ])

    # images table uses first_discovered_by, alias as discovered_by for backward compat
    query = """
        SELECT id, filename, first_discovered_by as discovered_by, ts_utc, rel_path
        FROM images
        WHERE evidence_id = ?
        AND ts_utc IS NOT NULL
        ORDER BY ts_utc
    """

    events = []
    cursor = conn.execute(query, (evidence_id,))

    for row in cursor:
        try:
            row_data = {
                "filename": row["filename"],
                "discovered_by": row["discovered_by"] or "unknown",
                "rel_path": row["rel_path"] or ""
            }

            for mapping in mappings_list:
                ts_field = mapping.get("timestamp_field", "ts_utc")
                if not row[ts_field]:
                    continue

                ts = _parse_timestamp(row[ts_field])
                if ts is None:
                    continue

                note = _format_note(
                    mapping.get("note_template", "Image: {filename}"),
                    row_data
                )

                events.append(TimelineEvent(
                    evidence_id=evidence_id,
                    ts_utc=ts,
                    kind=mapping.get("kind", "image_extracted"),
                    ref_table="images",
                    ref_id=row["id"],
                    confidence=confidence,
                    note=note,
                    provenance=f"discovered_by:{row['discovered_by']}"
                ))
        except Exception as exc:
            LOGGER.warning("Failed to map images row %s: %s", row["id"], exc)

    return events


def map_image_discoveries_to_events(
    conn: sqlite3.Connection,
    evidence_id: int,
    config: TimelineConfig
) -> List[TimelineEvent]:
    """
    Map image_discoveries records to timeline events for per-source provenance.

    New mapper for image discoveries - provides richer timeline with
    per-discovery timestamps (discovered_at, cache_response_time, fs_mtime).
    This supplements map_images_to_events which uses the first discovery timestamp.

    NOTE: Only runs if image_discoveries source is explicitly configured.
    Timestamps are ISO8601 strings (not Unix floats) - use _parse_timestamp().
    """
    source_config = config.sources.get("image_discoveries", {})
    if not source_config:
        return []  # Only enabled if explicitly configured

    confidence = source_config.get("confidence", "medium")
    mappings_list = source_config.get("mappings", [
        {"timestamp_field": "discovered_at", "kind": "image_discovered", "note_template": "Image discovered by {discovered_by}: {filename}"},
        {"timestamp_field": "cache_response_time", "kind": "image_cached", "note_template": "Image cached: {filename} ({cache_url})"},
        {"timestamp_field": "fs_mtime", "kind": "file_modified", "note_template": "File modified: {filename} ({fs_path})"},
    ])

    # Query uses actual schema columns from 0001_evidence_schema.sql
    query = """
        SELECT d.id, d.image_id, d.discovered_by, d.discovered_at, d.run_id,
               d.fs_path, d.fs_mtime, d.carved_offset_bytes,
               d.cache_url, d.cache_response_time, d.cache_key,
               i.filename, i.rel_path, i.md5
        FROM image_discoveries d
        JOIN images i ON d.evidence_id = i.evidence_id AND d.image_id = i.id
        WHERE d.evidence_id = ?
        ORDER BY COALESCE(d.discovered_at, d.cache_response_time, d.fs_mtime)
    """

    events = []
    cursor = conn.execute(query, (evidence_id,))

    for row in cursor:
        try:
            row_data = {
                "filename": row["filename"] or "unknown",
                "discovered_by": row["discovered_by"] or "unknown",
                "fs_path": row["fs_path"] or "",
                "cache_url": row["cache_url"] or "",
                "cache_key": row["cache_key"] or "",
            }

            for mapping in mappings_list:
                ts_field = mapping.get("timestamp_field", "discovered_at")
                ts_value = row[ts_field] if ts_field in row.keys() else None
                if not ts_value:
                    continue

                # All timestamps in image_discoveries are ISO8601 strings
                ts = _parse_timestamp(ts_value)
                if ts is None:
                    continue

                note = _format_note(
                    mapping.get("note_template", "Image: {filename}"),
                    row_data
                )

                events.append(TimelineEvent(
                    evidence_id=evidence_id,
                    ts_utc=ts,
                    kind=mapping.get("kind", "image_discovered"),
                    ref_table="image_discoveries",
                    ref_id=row["id"],
                    confidence=confidence,
                    note=note,
                    provenance=f"discovered_by:{row['discovered_by']}"
                ))
        except Exception as exc:
            LOGGER.warning("Failed to map image_discoveries row %s: %s", row["id"], exc)

    return events


def map_os_indicators_to_events(
    conn: sqlite3.Connection,
    evidence_id: int,
    config: TimelineConfig
) -> List[TimelineEvent]:
    """Map os_indicators records to timeline events"""
    source_config = config.sources.get("os_indicators", {})
    confidence = source_config.get("confidence", "low")
    mappings_list = source_config.get("mappings", [
        {"timestamp_field": "detected_at_utc", "kind": "os_artifact", "note_template": "OS: {type} - {name}"}
    ])

    query = """
        SELECT id, type, name, value, detected_at_utc, provenance, confidence as indicator_confidence
        FROM os_indicators
        WHERE evidence_id = ?
        AND detected_at_utc IS NOT NULL
        ORDER BY detected_at_utc
    """

    events = []
    cursor = conn.execute(query, (evidence_id,))

    for row in cursor:
        try:
            row_data = {
                "type": row["type"],
                "name": row["name"],
                "value": row["value"] or ""
            }

            # Use indicator's own confidence if higher
            event_confidence = row["indicator_confidence"] or confidence

            for mapping in mappings_list:
                ts_field = mapping.get("timestamp_field", "detected_at_utc")
                if not row[ts_field]:
                    continue

                ts = _parse_timestamp(row[ts_field])
                if ts is None:
                    continue

                note = _format_note(
                    mapping.get("note_template", "OS: {type} - {name}"),
                    row_data
                )

                events.append(TimelineEvent(
                    evidence_id=evidence_id,
                    ts_utc=ts,
                    kind=mapping.get("kind", "os_artifact"),
                    ref_table="os_indicators",
                    ref_id=row["id"],
                    confidence=event_confidence,
                    note=note,
                    provenance=row["provenance"] or "os_detector"
                ))
        except Exception as exc:
            LOGGER.warning("Failed to map os_indicators row %s: %s", row["id"], exc)

    return events


# =============================================================================
# Phase 5 Timeline Wiring Mappers
# =============================================================================


def map_cookies_to_events(
    conn: sqlite3.Connection,
    evidence_id: int,
    config: TimelineConfig
) -> List[TimelineEvent]:
    """Map cookies table to timeline events (creation + access)."""
    source_config = config.sources.get("cookies", {})
    confidence = source_config.get("confidence", "medium")
    mappings_list = source_config.get("mappings", [
        {"timestamp_field": "creation_utc", "kind": "cookie_created", "note_template": "Cookie created: {domain} ({name})"},
        {"timestamp_field": "last_access_utc", "kind": "cookie_accessed", "note_template": "Cookie accessed: {domain} ({name})"}
    ])

    query = """
        SELECT id, browser, domain, name, path, creation_utc, last_access_utc
        FROM cookies
        WHERE evidence_id = ?
        AND (creation_utc IS NOT NULL OR last_access_utc IS NOT NULL)
    """
    events = []

    try:
        cursor = conn.execute(query, (evidence_id,))
    except sqlite3.OperationalError as exc:
        LOGGER.debug("Table cookies not found, skipping: %s", exc)
        return events

    for row in cursor:
        try:
            row_data = {
                "browser": row["browser"],
                "domain": row["domain"],
                "name": row["name"],
                "path": row["path"] or ""
            }

            for mapping in mappings_list:
                ts_field = mapping.get("timestamp_field")
                if not ts_field or not row[ts_field]:
                    continue

                ts = _parse_timestamp(row[ts_field])
                if ts is None:
                    continue

                note = _format_note(
                    mapping.get("note_template", f"Cookie: {{domain}} ({{name}})"),
                    row_data
                )

                events.append(TimelineEvent(
                    evidence_id=evidence_id,
                    ts_utc=ts,
                    kind=mapping.get("kind", "cookie_event"),
                    ref_table="cookies",
                    ref_id=row["id"],
                    confidence=confidence,
                    note=note,
                    provenance=f"browser:{row['browser']}"
                ))
        except Exception as exc:
            LOGGER.warning("Failed to map cookies row %s: %s", row["id"], exc)

    return events


def map_bookmarks_to_events(
    conn: sqlite3.Connection,
    evidence_id: int,
    config: TimelineConfig
) -> List[TimelineEvent]:
    """Map bookmarks table to timeline events."""
    source_config = config.sources.get("bookmarks", {})
    confidence = source_config.get("confidence", "high")
    mappings_list = source_config.get("mappings", [
        {"timestamp_field": "date_added_utc", "kind": "bookmark_added", "note_template": "Bookmark: {title}"}
    ])

    query = """
        SELECT id, browser, url, title, folder_path, date_added_utc
        FROM bookmarks
        WHERE evidence_id = ?
        AND date_added_utc IS NOT NULL
    """
    events = []

    try:
        cursor = conn.execute(query, (evidence_id,))
    except sqlite3.OperationalError as exc:
        LOGGER.debug("Table bookmarks not found, skipping: %s", exc)
        return events

    for row in cursor:
        try:
            row_data = {
                "browser": row["browser"],
                "url": row["url"],
                "title": row["title"] or row["url"],
                "folder_path": row["folder_path"] or ""
            }

            for mapping in mappings_list:
                ts_field = mapping.get("timestamp_field")
                if not ts_field or not row[ts_field]:
                    continue

                ts = _parse_timestamp(row[ts_field])
                if ts is None:
                    continue

                note = _format_note(
                    mapping.get("note_template", "Bookmark: {title}"),
                    row_data
                )

                events.append(TimelineEvent(
                    evidence_id=evidence_id,
                    ts_utc=ts,
                    kind=mapping.get("kind", "bookmark_added"),
                    ref_table="bookmarks",
                    ref_id=row["id"],
                    confidence=confidence,
                    note=note,
                    provenance=f"browser:{row['browser']}"
                ))
        except Exception as exc:
            LOGGER.warning("Failed to map bookmarks row %s: %s", row["id"], exc)

    return events


def map_browser_downloads_to_events(
    conn: sqlite3.Connection,
    evidence_id: int,
    config: TimelineConfig
) -> List[TimelineEvent]:
    """Map browser_downloads table to timeline events (start + end)."""
    source_config = config.sources.get("browser_downloads", {})
    confidence = source_config.get("confidence", "high")
    mappings_list = source_config.get("mappings", [
        {"timestamp_field": "start_time_utc", "kind": "download_started", "note_template": "Download started: {filename}"},
        {"timestamp_field": "end_time_utc", "kind": "download_completed", "note_template": "Download completed: {filename}"}
    ])

    query = """
        SELECT id, browser, url, filename, start_time_utc, end_time_utc
        FROM browser_downloads
        WHERE evidence_id = ?
        AND (start_time_utc IS NOT NULL OR end_time_utc IS NOT NULL)
    """
    events = []

    try:
        cursor = conn.execute(query, (evidence_id,))
    except sqlite3.OperationalError as exc:
        LOGGER.debug("Table browser_downloads not found, skipping: %s", exc)
        return events

    for row in cursor:
        try:
            row_data = {
                "browser": row["browser"],
                "url": row["url"],
                "filename": row["filename"] or row["url"]
            }

            for mapping in mappings_list:
                ts_field = mapping.get("timestamp_field")
                if not ts_field or not row[ts_field]:
                    continue

                ts = _parse_timestamp(row[ts_field])
                if ts is None:
                    continue

                note = _format_note(
                    mapping.get("note_template", f"Download: {{filename}}"),
                    row_data
                )

                events.append(TimelineEvent(
                    evidence_id=evidence_id,
                    ts_utc=ts,
                    kind=mapping.get("kind", "download_event"),
                    ref_table="browser_downloads",
                    ref_id=row["id"],
                    confidence=confidence,
                    note=note,
                    provenance=f"browser:{row['browser']}"
                ))
        except Exception as exc:
            LOGGER.warning("Failed to map browser_downloads row %s: %s", row["id"], exc)

    return events


def map_session_tabs_to_events(
    conn: sqlite3.Connection,
    evidence_id: int,
    config: TimelineConfig
) -> List[TimelineEvent]:
    """Map session_tabs table to timeline events."""
    source_config = config.sources.get("session_tabs", {})
    confidence = source_config.get("confidence", "medium")
    mappings_list = source_config.get("mappings", [
        {"timestamp_field": "last_accessed_utc", "kind": "tab_accessed", "note_template": "Tab: {title}"}
    ])

    query = """
        SELECT id, browser, url, title, last_accessed_utc
        FROM session_tabs
        WHERE evidence_id = ?
        AND last_accessed_utc IS NOT NULL
    """
    events = []

    try:
        cursor = conn.execute(query, (evidence_id,))
    except sqlite3.OperationalError as exc:
        LOGGER.debug("Table session_tabs not found, skipping: %s", exc)
        return events

    for row in cursor:
        try:
            row_data = {
                "browser": row["browser"],
                "url": row["url"],
                "title": row["title"] or row["url"]
            }

            for mapping in mappings_list:
                ts_field = mapping.get("timestamp_field")
                if not ts_field or not row[ts_field]:
                    continue

                ts = _parse_timestamp(row[ts_field])
                if ts is None:
                    continue

                note = _format_note(
                    mapping.get("note_template", "Tab: {title}"),
                    row_data
                )

                events.append(TimelineEvent(
                    evidence_id=evidence_id,
                    ts_utc=ts,
                    kind=mapping.get("kind", "tab_accessed"),
                    ref_table="session_tabs",
                    ref_id=row["id"],
                    confidence=confidence,
                    note=note,
                    provenance=f"browser:{row['browser']}"
                ))
        except Exception as exc:
            LOGGER.warning("Failed to map session_tabs row %s: %s", row["id"], exc)

    return events


def map_autofill_to_events(
    conn: sqlite3.Connection,
    evidence_id: int,
    config: TimelineConfig
) -> List[TimelineEvent]:
    """Map autofill table to timeline events (created + used)."""
    source_config = config.sources.get("autofill", {})
    confidence = source_config.get("confidence", "medium")
    mappings_list = source_config.get("mappings", [
        {"timestamp_field": "date_created_utc", "kind": "autofill_created", "note_template": "Autofill saved: {name}"},
        {"timestamp_field": "date_last_used_utc", "kind": "autofill_used", "note_template": "Autofill used: {name}"}
    ])

    query = """
        SELECT id, browser, name, value, date_created_utc, date_last_used_utc
        FROM autofill
        WHERE evidence_id = ?
        AND (date_created_utc IS NOT NULL OR date_last_used_utc IS NOT NULL)
    """
    events = []

    try:
        cursor = conn.execute(query, (evidence_id,))
    except sqlite3.OperationalError as exc:
        LOGGER.debug("Table autofill not found, skipping: %s", exc)
        return events

    for row in cursor:
        try:
            row_data = {
                "browser": row["browser"],
                "name": row["name"],
                "value": row["value"] or ""
            }

            for mapping in mappings_list:
                ts_field = mapping.get("timestamp_field")
                if not ts_field or not row[ts_field]:
                    continue

                ts = _parse_timestamp(row[ts_field])
                if ts is None:
                    continue

                note = _format_note(
                    mapping.get("note_template", "Autofill: {name}"),
                    row_data
                )

                events.append(TimelineEvent(
                    evidence_id=evidence_id,
                    ts_utc=ts,
                    kind=mapping.get("kind", "autofill_event"),
                    ref_table="autofill",
                    ref_id=row["id"],
                    confidence=confidence,
                    note=note,
                    provenance=f"browser:{row['browser']}"
                ))
        except Exception as exc:
            LOGGER.warning("Failed to map autofill row %s: %s", row["id"], exc)

    return events


def map_credentials_to_events(
    conn: sqlite3.Connection,
    evidence_id: int,
    config: TimelineConfig
) -> List[TimelineEvent]:
    """Map credentials table to timeline events (saved + used)."""
    source_config = config.sources.get("credentials", {})
    confidence = source_config.get("confidence", "high")
    mappings_list = source_config.get("mappings", [
        {"timestamp_field": "date_created_utc", "kind": "credential_saved", "note_template": "Credential saved: {origin_url}"},
        {"timestamp_field": "date_last_used_utc", "kind": "credential_used", "note_template": "Credential used: {origin_url}"}
    ])

    query = """
        SELECT id, browser, origin_url, username_value, date_created_utc, date_last_used_utc
        FROM credentials
        WHERE evidence_id = ?
        AND (date_created_utc IS NOT NULL OR date_last_used_utc IS NOT NULL)
    """
    events = []

    try:
        cursor = conn.execute(query, (evidence_id,))
    except sqlite3.OperationalError as exc:
        LOGGER.debug("Table credentials not found, skipping: %s", exc)
        return events

    for row in cursor:
        try:
            row_data = {
                "browser": row["browser"],
                "origin_url": row["origin_url"],
                "username_value": row["username_value"] or ""
            }

            for mapping in mappings_list:
                ts_field = mapping.get("timestamp_field")
                if not ts_field or not row[ts_field]:
                    continue

                ts = _parse_timestamp(row[ts_field])
                if ts is None:
                    continue

                note = _format_note(
                    mapping.get("note_template", "Credential: {origin_url}"),
                    row_data
                )

                events.append(TimelineEvent(
                    evidence_id=evidence_id,
                    ts_utc=ts,
                    kind=mapping.get("kind", "credential_event"),
                    ref_table="credentials",
                    ref_id=row["id"],
                    confidence=confidence,
                    note=note,
                    provenance=f"browser:{row['browser']}"
                ))
        except Exception as exc:
            LOGGER.warning("Failed to map credentials row %s: %s", row["id"], exc)

    return events


def map_media_playback_to_events(
    conn: sqlite3.Connection,
    evidence_id: int,
    config: TimelineConfig
) -> List[TimelineEvent]:
    """Map media_playback table to timeline events."""
    source_config = config.sources.get("media_playback", {})
    confidence = source_config.get("confidence", "medium")
    mappings_list = source_config.get("mappings", [
        {"timestamp_field": "last_played_utc", "kind": "media_played", "note_template": "Media: {url} ({watch_time_seconds}s)"}
    ])

    query = """
        SELECT id, browser, url, origin, watch_time_seconds, last_played_utc
        FROM media_playback
        WHERE evidence_id = ?
        AND last_played_utc IS NOT NULL
    """
    events = []

    try:
        cursor = conn.execute(query, (evidence_id,))
    except sqlite3.OperationalError as exc:
        LOGGER.debug("Table media_playback not found, skipping: %s", exc)
        return events

    for row in cursor:
        try:
            row_data = {
                "browser": row["browser"],
                "url": row["url"],
                "origin": row["origin"] or "",
                "watch_time_seconds": row["watch_time_seconds"] or 0
            }

            for mapping in mappings_list:
                ts_field = mapping.get("timestamp_field")
                if not ts_field or not row[ts_field]:
                    continue

                ts = _parse_timestamp(row[ts_field])
                if ts is None:
                    continue

                note = _format_note(
                    mapping.get("note_template", "Media: {url}"),
                    row_data
                )

                events.append(TimelineEvent(
                    evidence_id=evidence_id,
                    ts_utc=ts,
                    kind=mapping.get("kind", "media_played"),
                    ref_table="media_playback",
                    ref_id=row["id"],
                    confidence=confidence,
                    note=note,
                    provenance=f"browser:{row['browser']}"
                ))
        except Exception as exc:
            LOGGER.warning("Failed to map media_playback row %s: %s", row["id"], exc)

    return events


def map_hsts_to_events(
    conn: sqlite3.Connection,
    evidence_id: int,
    config: TimelineConfig
) -> List[TimelineEvent]:
    """Map hsts_entries table to timeline events with Unix→datetime conversion."""
    source_config = config.sources.get("hsts_entries", {})
    confidence = source_config.get("confidence", "low")
    mappings_list = source_config.get("mappings", [
        {"timestamp_field": "sts_observed", "kind": "hsts_observed", "note_template": "HSTS: {host}"},
        {"timestamp_field": "expiry", "kind": "hsts_expiry", "note_template": "HSTS expiry: {host}"}
    ])

    query = """
        SELECT id, browser, hashed_host, decoded_host, sts_observed, expiry
        FROM hsts_entries
        WHERE evidence_id = ?
    """
    events = []

    try:
        cursor = conn.execute(query, (evidence_id,))
    except sqlite3.OperationalError as exc:
        LOGGER.debug("Table hsts_entries not found, skipping: %s", exc)
        return events

    for row in cursor:
        try:
            # Prefer decoded_host, fall back to hashed_host
            host = row["decoded_host"] or row["hashed_host"]
            row_data = {
                "browser": row["browser"],
                "host": host,
                "hashed_host": row["hashed_host"],
                "decoded_host": row["decoded_host"] or ""
            }

            for mapping in mappings_list:
                ts_field = mapping.get("timestamp_field")
                if not ts_field or not row[ts_field]:
                    continue

                # HSTS uses Unix timestamps (REAL)
                ts = _unix_to_datetime(row[ts_field])
                if ts is None:
                    continue

                note = _format_note(
                    mapping.get("note_template", "HSTS: {host}"),
                    row_data
                )

                events.append(TimelineEvent(
                    evidence_id=evidence_id,
                    ts_utc=ts,
                    kind=mapping.get("kind", "hsts_event"),
                    ref_table="hsts_entries",
                    ref_id=row["id"],
                    confidence=confidence,
                    note=note,
                    provenance=f"browser:{row['browser']}"
                ))
        except Exception as exc:
            LOGGER.warning("Failed to map hsts_entries row %s: %s", row["id"], exc)

    return events


def map_jump_list_to_events(
    conn: sqlite3.Connection,
    evidence_id: int,
    config: TimelineConfig
) -> List[TimelineEvent]:
    """Map jump_list_entries table to timeline events."""
    source_config = config.sources.get("jump_list_entries", {})
    confidence = source_config.get("confidence", "medium")
    mappings_list = source_config.get("mappings", [
        {"timestamp_field": "lnk_access_time", "kind": "jumplist_accessed", "note_template": "Jump list: {url}"},
        {"timestamp_field": "lnk_creation_time", "kind": "jumplist_created", "note_template": "Jump list created: {url}"}
    ])

    query = """
        SELECT id, browser, url, target_path, lnk_access_time, lnk_creation_time
        FROM jump_list_entries
        WHERE evidence_id = ?
        AND (lnk_access_time IS NOT NULL OR lnk_creation_time IS NOT NULL)
    """
    events = []

    try:
        cursor = conn.execute(query, (evidence_id,))
    except sqlite3.OperationalError as exc:
        LOGGER.debug("Table jump_list_entries not found, skipping: %s", exc)
        return events

    for row in cursor:
        try:
            display_url = row["url"] or row["target_path"] or "unknown"
            browser = row["browser"] or "unknown"

            row_data = {
                "browser": browser,
                "url": display_url,
                "target_path": row["target_path"] or ""
            }

            for mapping in mappings_list:
                ts_field = mapping.get("timestamp_field")
                if not ts_field or not row[ts_field]:
                    continue

                ts = _parse_timestamp(row[ts_field])
                if ts is None:
                    continue

                note = _format_note(
                    mapping.get("note_template", "Jump list: {url}"),
                    row_data
                )

                events.append(TimelineEvent(
                    evidence_id=evidence_id,
                    ts_utc=ts,
                    kind=mapping.get("kind", "jumplist_event"),
                    ref_table="jump_list_entries",
                    ref_id=row["id"],
                    confidence=confidence,
                    note=note,
                    provenance=f"browser:{browser}"
                ))
        except Exception as exc:
            LOGGER.warning("Failed to map jump_list_entries row %s: %s", row["id"], exc)

    return events


# =============================================================================
# Timeline Mappers Registry
# =============================================================================

TIMELINE_MAPPERS: Dict[str, Callable[[sqlite3.Connection, int, TimelineConfig], List[TimelineEvent]]] = {
    "browser_history": map_browser_history_to_events,
    "urls": map_urls_to_events,
    "images": map_images_to_events,
    "image_discoveries": map_image_discoveries_to_events,
    "os_indicators": map_os_indicators_to_events,
    "cookies": map_cookies_to_events,
    "bookmarks": map_bookmarks_to_events,
    "browser_downloads": map_browser_downloads_to_events,
    "session_tabs": map_session_tabs_to_events,
    "autofill": map_autofill_to_events,
    "credentials": map_credentials_to_events,
    "media_playback": map_media_playback_to_events,
    "hsts_entries": map_hsts_to_events,
    "jump_list_entries": map_jump_list_to_events,
}


# =============================================================================
# Timeline Building and Persistence
# =============================================================================


def build_timeline(
    conn: sqlite3.Connection,
    evidence_id: int,
    config: TimelineConfig,
    progress_cb: ProgressCallback = None,
) -> List[TimelineEvent]:
    """
    Build complete timeline for an evidence by mapping all configured sources.

    Args:
        conn: SQLite database connection
        evidence_id: Evidence ID to build timeline for
        config: Timeline configuration
        progress_cb: Optional callback (progress_fraction: float, message: str)

    Returns:
        List of TimelineEvent objects sorted by timestamp (deterministic).
    """
    all_events = []
    sources = list(config.sources.keys())
    total = len(sources)

    for i, source_name in enumerate(sources):
        if progress_cb:
            progress_cb(i / total, f"Processing {source_name}...")

        mapper = TIMELINE_MAPPERS.get(source_name)
        if mapper is None:
            LOGGER.warning("No mapper registered for source: %s", source_name)
            continue

        try:
            events = mapper(conn, evidence_id, config)
            all_events.extend(events)
            LOGGER.debug("Mapped %d events from %s", len(events), source_name)
        except Exception as exc:
            LOGGER.warning("Failed to map %s: %s", source_name, exc)
            # Continue with other sources

    if progress_cb:
        progress_cb(1.0, "Sorting events...")

    return coalesce_events(all_events)


def persist_timeline(
    conn: sqlite3.Connection,
    events: Iterable[TimelineEvent],
    evidence_id: Optional[int] = None
) -> int:
    """
    Persist timeline events to the timeline table.

    Args:
        conn: SQLite connection to evidence database
        events: Iterable of TimelineEvent objects to persist
        evidence_id: If provided, clears existing timeline for this evidence
                     even if events is empty (avoids stale data on rebuild)

    Returns:
        Count of inserted events.
    """
    rows = [
        (
            event.evidence_id,
            event.ts_utc.isoformat(),
            event.kind,
            event.ref_table,
            event.ref_id,
            event.confidence,
            event.note
        )
        for event in events
    ]

    with conn:
        # Clear existing timeline for this evidence
        if evidence_id is not None:
            # Explicit evidence_id provided - always delete (even if no new events)
            conn.execute("DELETE FROM timeline WHERE evidence_id = ?", (evidence_id,))
        elif rows:
            # Infer from events (legacy behavior)
            evidence_ids = {row[0] for row in rows}
            for eid in evidence_ids:
                conn.execute("DELETE FROM timeline WHERE evidence_id = ?", (eid,))

        if not rows:
            return 0

        # Insert new events
        conn.executemany(
            """
            INSERT INTO timeline(
                evidence_id, ts_utc, kind, ref_table, ref_id, confidence, note
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows
        )

    return len(rows)


def coalesce_events(events: Iterable[TimelineEvent]) -> List[TimelineEvent]:
    """Return events sorted by timestamp, useful for deterministic reporting."""
    return sorted(events, key=lambda event: (event.ts_utc, event.ref_table, event.ref_id))
