"""
Chromium Favicons & Top Sites Parsers.

This module contains the parsing logic for:
- Favicons SQLite database (favicons, favicon_bitmaps, icon_mapping tables)
- Top Sites SQLite database (top_sites, thumbnails tables)

All parsers accept an optional ExtractionWarningCollector to report:
- Unknown tables in the databases
- Unknown columns in known tables
- Unknown icon_type values
- Parse errors

Extracted from extractor.py with schema warning support
"""
from __future__ import annotations

import hashlib
import io
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

from core.logging import get_logger

from ._schemas import (
    ICON_TYPES,
    KNOWN_FAVICONS_DB_TABLES,
    KNOWN_TOP_SITES_DB_TABLES,
    KNOWN_FAVICON_BITMAPS_COLUMNS,
    KNOWN_ICON_MAPPING_COLUMNS,
    KNOWN_TOP_SITES_TABLE_COLUMNS,
    KNOWN_THUMBNAILS_COLUMNS,
    FAVICON_TABLE_PATTERNS,
    TOP_SITES_TABLE_PATTERNS,
    FAVICONS_DB_COLUMN_MAP,
    TOP_SITES_DB_COLUMN_MAP,
    get_icon_type_name,
)

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector
    from extractors.callbacks import ExtractorCallbacks

LOGGER = get_logger("extractors.browser.chromium.favicons.parsers")

# Maximum icon size to store (1MB)
MAX_ICON_SIZE_BYTES = 1 * 1024 * 1024


# =============================================================================
# WebKit Timestamp Conversion
# =============================================================================

def webkit_to_iso8601(webkit_time: Optional[int]) -> Optional[str]:
    """
    Convert WebKit timestamp (microseconds since 1601) to ISO 8601.

    Args:
        webkit_time: WebKit timestamp in microseconds since January 1, 1601

    Returns:
        ISO 8601 formatted string, or None if invalid/zero
    """
    if webkit_time is None or webkit_time == 0:
        return None
    try:
        # WebKit timestamp is microseconds since Jan 1, 1601
        # Unix epoch is Jan 1, 1970
        # Difference is 11644473600 seconds
        unix_seconds = (webkit_time / 1_000_000) - 11644473600
        dt = datetime.fromtimestamp(unix_seconds, tz=timezone.utc)
        return dt.isoformat()
    except (ValueError, OSError, OverflowError):
        return None


# =============================================================================
# Image Format Detection
# =============================================================================

def detect_image_extension(data: bytes) -> str:
    """
    Detect image format from magic bytes and return appropriate extension.

    Args:
        data: Raw image bytes

    Returns:
        File extension (without dot): png, jpg, gif, webp, bmp, ico, svg, or bin
    """
    if len(data) < 8:
        return "ico"

    # PNG: 89 50 4E 47 0D 0A 1A 0A
    if data[:8] == b'\x89PNG\r\n\x1a\n':
        return "png"
    # JPEG: FF D8 FF
    if data[:3] == b'\xff\xd8\xff':
        return "jpg"
    # GIF: GIF87a or GIF89a
    if data[:6] in (b'GIF87a', b'GIF89a'):
        return "gif"
    # WebP: RIFF....WEBP
    if data[:4] == b'RIFF' and len(data) >= 12 and data[8:12] == b'WEBP':
        return "webp"
    # BMP: BM
    if data[:2] == b'BM':
        return "bmp"
    # ICO: 00 00 01 00
    if data[:4] == b'\x00\x00\x01\x00':
        return "ico"
    # SVG: starts with <svg or <?xml (check first 100 bytes for svg tag)
    text_start = data[:100].lower()
    if b'<svg' in text_start or (b'<?xml' in text_start and b'svg' in text_start):
        return "svg"

    # Default to bin for unknown binary data
    return "bin"


# =============================================================================
# Schema Discovery Utilities
# =============================================================================

def discover_unknown_tables(
    cursor: sqlite3.Cursor,
    known_tables: Set[str],
    patterns: List[str],
    source_file: str,
    warning_collector: Optional["ExtractionWarningCollector"],
    artifact_type: str,
) -> None:
    """
    Discover and report unknown tables in a SQLite database.

    Args:
        cursor: SQLite cursor
        known_tables: Set of table names we know about
        patterns: Patterns to filter relevant unknown tables
        source_file: Source file path for context
        warning_collector: Optional collector for warnings
        artifact_type: Artifact type for warning context (e.g., "favicons")
    """
    if not warning_collector:
        return

    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        all_tables = {row[0] for row in cursor.fetchall()}

        unknown_tables = all_tables - known_tables

        for table_name in unknown_tables:
            # Check if table name matches any pattern (might be relevant)
            table_lower = table_name.lower()
            is_relevant = any(p in table_lower for p in patterns)

            if is_relevant:
                # Get column info for context
                try:
                    cursor.execute(f"PRAGMA table_info({table_name})")
                    columns = [row[1] for row in cursor.fetchall()]
                except Exception:
                    columns = []

                warning_collector.add_unknown_table(
                    table_name=table_name,
                    columns=columns,
                    source_file=source_file,
                    artifact_type=artifact_type,
                )
    except Exception as e:
        LOGGER.debug("Error discovering unknown tables: %s", e)


def discover_unknown_columns(
    cursor: sqlite3.Cursor,
    table_name: str,
    known_columns: Set[str],
    source_file: str,
    warning_collector: Optional["ExtractionWarningCollector"],
    artifact_type: str,
) -> None:
    """
    Discover and report unknown columns in a table.

    Args:
        cursor: SQLite cursor
        table_name: Table to check
        known_columns: Set of column names we parse
        source_file: Source file path for context
        warning_collector: Optional collector for warnings
        artifact_type: Artifact type for warning context
    """
    if not warning_collector:
        return

    try:
        cursor.execute(f"PRAGMA table_info({table_name})")
        column_info = cursor.fetchall()

        for col in column_info:
            col_name = col[1]
            col_type = col[2]

            if col_name not in known_columns:
                warning_collector.add_unknown_column(
                    table_name=table_name,
                    column_name=col_name,
                    column_type=col_type,
                    source_file=source_file,
                    artifact_type=artifact_type,
                )
    except Exception as e:
        LOGGER.debug("Error discovering unknown columns in %s: %s", table_name, e)


def track_unknown_icon_types(
    found_types: Set[int],
    source_file: str,
    warning_collector: Optional["ExtractionWarningCollector"],
) -> None:
    """
    Track and report unknown icon_type values.

    Args:
        found_types: Set of icon_type values found during parsing
        source_file: Source file path for context
        warning_collector: Optional collector for warnings
    """
    if not warning_collector:
        return

    for icon_type in found_types:
        if icon_type not in ICON_TYPES:
            warning_collector.add_unknown_token_type(
                token_type=icon_type,
                source_file=source_file,
                artifact_type="favicons",
            )


# =============================================================================
# Favicon Database Parsing
# =============================================================================

def parse_favicons_database(
    db_path: Path,
    evidence_conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str,
    browser: str,
    profile: str,
    file_info: Dict[str, Any],
    output_dir: Path,
    extractor_name: str,
    extractor_version: str,
    callbacks: Optional["ExtractorCallbacks"] = None,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> Tuple[int, int, List[Dict[str, Any]]]:
    """
    Parse a Chromium Favicons SQLite database.

    Returns:
        Tuple of (favicon_count, mapping_count, url_list)
    """
    from core.database import insert_favicon, insert_favicon_mappings

    favicon_count = 0
    mapping_count = 0
    url_list: List[Dict[str, Any]] = []

    source_file = file_info.get("source_path", str(db_path))

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get table list
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}

        # Discover unknown tables
        discover_unknown_tables(
            cursor, KNOWN_FAVICONS_DB_TABLES, FAVICON_TABLE_PATTERNS,
            source_file, warning_collector, "favicons"
        )

        # Track icon types for warning
        found_icon_types: Set[int] = set()

        # Map icon_id -> favicon DB id
        icon_id_map: Dict[int, int] = {}

        # Parse favicon_bitmaps + favicons tables
        if "favicon_bitmaps" in tables and "favicons" in tables:
            # Discover unknown columns
            discover_unknown_columns(
                cursor, "favicon_bitmaps", KNOWN_FAVICON_BITMAPS_COLUMNS,
                source_file, warning_collector, "favicons"
            )

            # Check for optional columns
            cursor.execute("PRAGMA table_info(favicon_bitmaps)")
            fb_columns = {row[1] for row in cursor.fetchall()}
            has_last_requested = "last_requested" in fb_columns

            # Build query
            select_cols = [
                "fb.id", "fb.icon_id", "fb.last_updated", "fb.image_data",
                "fb.width", "fb.height", "f.url as icon_url", "f.icon_type"
            ]
            if has_last_requested:
                select_cols.append("fb.last_requested")

            try:
                cursor.execute(f"""
                    SELECT {', '.join(select_cols)}
                    FROM favicon_bitmaps fb
                    JOIN favicons f ON fb.icon_id = f.id
                    WHERE fb.image_data IS NOT NULL
                """)

                for row in cursor:
                    icon_data = row["image_data"]
                    if icon_data is None or len(icon_data) > MAX_ICON_SIZE_BYTES:
                        continue

                    icon_sha256 = hashlib.sha256(icon_data).hexdigest()
                    icon_md5 = hashlib.md5(icon_data).hexdigest()

                    last_updated = webkit_to_iso8601(row["last_updated"])
                    last_requested = None
                    if has_last_requested:
                        last_requested = webkit_to_iso8601(row["last_requested"])

                    icon_type = row["icon_type"] if row["icon_type"] else 1
                    found_icon_types.add(icon_type)
                    icon_url = row["icon_url"]

                    # Write icon to disk
                    ext = detect_image_extension(icon_data)
                    icon_dir = output_dir / "icons" / browser / icon_sha256[:2]
                    icon_dir.mkdir(parents=True, exist_ok=True)
                    icon_path = icon_dir / f"{icon_sha256}.{ext}"
                    icon_path.write_bytes(icon_data)

                    # Relative path for storage
                    rel_path = f"chromium_favicons/icons/{browser}/{icon_sha256[:2]}/{icon_sha256}.{ext}"

                    # Insert favicon record (no icon_data, only disk path)
                    favicon_id = insert_favicon(
                        evidence_conn, evidence_id,
                        browser=browser,
                        icon_url=icon_url,
                        profile=profile,
                        icon_md5=icon_md5,
                        icon_sha256=icon_sha256,
                        icon_type=icon_type,
                        width=row["width"],
                        height=row["height"],
                        last_updated_utc=last_updated,
                        last_requested_utc=last_requested,
                        run_id=run_id,
                        source_path=source_file,
                        partition_index=file_info.get("partition_index"),
                        fs_type=file_info.get("fs_type"),
                        logical_path=source_file,
                        forensic_path=source_file,
                        notes=f"Saved to: {rel_path}",
                    )

                    if favicon_id:
                        favicon_count += 1
                        icon_id_map[row["icon_id"]] = favicon_id

                        # Cross-post to images table
                        _cross_post_favicon_to_images(
                            evidence_conn, evidence_id, run_id,
                            icon_data, icon_md5, icon_sha256, icon_url,
                            browser, profile, file_info, output_dir,
                            extractor_name, extractor_version,
                        )

                        # Collect icon URL (no deduplication)
                        if icon_url:
                            parsed = urlparse(icon_url)
                            url_list.append({
                                "url": icon_url,
                                "domain": parsed.netloc or None,
                                "scheme": parsed.scheme or None,
                                "context": f"favicon_icon:{browser}:{profile}",
                                "timestamp": last_updated,
                            })

            except Exception as e:
                LOGGER.debug("Error reading favicon_bitmaps: %s", e)
                if warning_collector:
                    warning_collector.add_warning(
                        warning_type="database_parse_error",
                        category="database",
                        severity="error",
                        artifact_type="favicons",
                        source_file=source_file,
                        item_name="favicon_bitmaps",
                        item_value=str(e),
                    )

        # Report unknown icon types
        track_unknown_icon_types(found_icon_types, source_file, warning_collector)

        # Parse icon_mapping table
        if "icon_mapping" in tables and icon_id_map:
            discover_unknown_columns(
                cursor, "icon_mapping", KNOWN_ICON_MAPPING_COLUMNS,
                source_file, warning_collector, "favicons"
            )

            try:
                cursor.execute("SELECT page_url, icon_id FROM icon_mapping")

                mappings = []
                for row in cursor:
                    icon_id = row["icon_id"]
                    if icon_id in icon_id_map:
                        page_url = row["page_url"]
                        mappings.append({
                            "favicon_id": icon_id_map[icon_id],
                            "page_url": page_url,
                            "browser": browser,
                            "profile": profile,
                            "run_id": run_id,
                        })
                        # Collect page URL (no deduplication)
                        if page_url:
                            parsed = urlparse(page_url)
                            url_list.append({
                                "url": page_url,
                                "domain": parsed.netloc or None,
                                "scheme": parsed.scheme or None,
                                "context": f"favicon_page:{browser}:{profile}",
                                "timestamp": None,
                            })

                if mappings:
                    mapping_count = insert_favicon_mappings(evidence_conn, evidence_id, mappings)

            except Exception as e:
                LOGGER.debug("Error reading icon_mapping: %s", e)
                if warning_collector:
                    warning_collector.add_warning(
                        warning_type="database_parse_error",
                        category="database",
                        severity="error",
                        artifact_type="favicons",
                        source_file=source_file,
                        item_name="icon_mapping",
                        item_value=str(e),
                    )

        conn.close()

    except Exception as e:
        LOGGER.error("Failed to parse favicons from %s: %s", db_path, e)
        if warning_collector:
            warning_collector.add_warning(
                warning_type="file_corrupt",
                category="database",
                severity="error",
                artifact_type="favicons",
                source_file=source_file,
                item_name="Favicons",
                item_value=str(e),
            )

    return favicon_count, mapping_count, url_list


# =============================================================================
# Top Sites Database Parsing
# =============================================================================

def parse_top_sites_database(
    db_path: Path,
    evidence_conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str,
    browser: str,
    profile: str,
    file_info: Dict[str, Any],
    output_dir: Path,
    extractor_name: str,
    extractor_version: str,
    callbacks: Optional["ExtractorCallbacks"] = None,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Parse a Chromium Top Sites SQLite database.

    Returns:
        Tuple of (count, url_list)
    """
    from core.database import insert_top_sites

    count = 0
    url_list: List[Dict[str, Any]] = []

    source_file = file_info.get("source_path", str(db_path))

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get table list
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}

        # Discover unknown tables
        discover_unknown_tables(
            cursor, KNOWN_TOP_SITES_DB_TABLES, TOP_SITES_TABLE_PATTERNS,
            source_file, warning_collector, "top_sites"
        )

        records: List[Dict[str, Any]] = []

        # Parse top_sites table (modern)
        if "top_sites" in tables:
            discover_unknown_columns(
                cursor, "top_sites", KNOWN_TOP_SITES_TABLE_COLUMNS,
                source_file, warning_collector, "top_sites"
            )

            cursor.execute("PRAGMA table_info(top_sites)")
            columns = {row[1] for row in cursor.fetchall()}

            if "url" in columns:
                try:
                    cursor.execute("SELECT url, title, url_rank FROM top_sites")

                    for row in cursor:
                        records.append({
                            "browser": browser,
                            "profile": profile,
                            "url": row["url"],
                            "title": row["title"],
                            "url_rank": row["url_rank"],
                            "run_id": run_id,
                            "source_path": source_file,
                            "partition_index": file_info.get("partition_index"),
                            "fs_type": file_info.get("fs_type"),
                            "logical_path": source_file,
                            "forensic_path": source_file,
                        })

                except Exception as e:
                    LOGGER.debug("Error reading top_sites table: %s", e)
                    if warning_collector:
                        warning_collector.add_warning(
                            warning_type="database_parse_error",
                            category="database",
                            severity="error",
                            artifact_type="top_sites",
                            source_file=source_file,
                            item_name="top_sites",
                            item_value=str(e),
                        )

        # Parse thumbnails table (legacy) - also cross-post to images
        if "thumbnails" in tables and not records:
            discover_unknown_columns(
                cursor, "thumbnails", KNOWN_THUMBNAILS_COLUMNS,
                source_file, warning_collector, "top_sites"
            )

            try:
                cursor.execute("""
                    SELECT url, title, url_rank, thumbnail, last_updated
                    FROM thumbnails
                """)

                for row in cursor:
                    thumbnail_data = row["thumbnail"]
                    thumbnail_path = None

                    # Process thumbnail if present
                    if thumbnail_data and len(thumbnail_data) <= MAX_ICON_SIZE_BYTES:
                        thumb_sha256 = hashlib.sha256(thumbnail_data).hexdigest()
                        thumb_md5 = hashlib.md5(thumbnail_data).hexdigest()
                        ext = detect_image_extension(thumbnail_data)

                        # Write to disk
                        thumb_dir = output_dir / "thumbnails" / browser / thumb_sha256[:2]
                        thumb_dir.mkdir(parents=True, exist_ok=True)
                        thumb_path = thumb_dir / f"{thumb_sha256}.{ext}"
                        thumb_path.write_bytes(thumbnail_data)

                        thumbnail_path = f"chromium_favicons/thumbnails/{browser}/{thumb_sha256[:2]}/{thumb_sha256}.{ext}"

                        # Cross-post thumbnail to images table
                        _cross_post_thumbnail_to_images(
                            evidence_conn, evidence_id, run_id,
                            thumbnail_data, thumb_md5, thumb_sha256,
                            row["url"], browser, profile, file_info,
                            output_dir, extractor_name, extractor_version,
                        )

                    last_updated = webkit_to_iso8601(row["last_updated"]) if row["last_updated"] else None

                    records.append({
                        "browser": browser,
                        "profile": profile,
                        "url": row["url"],
                        "title": row["title"],
                        "url_rank": row["url_rank"],
                        "thumbnail_path": thumbnail_path,
                        "last_forced_time_utc": last_updated,
                        "run_id": run_id,
                        "source_path": source_file,
                        "partition_index": file_info.get("partition_index"),
                        "fs_type": file_info.get("fs_type"),
                        "logical_path": source_file,
                        "forensic_path": source_file,
                    })

            except Exception as e:
                LOGGER.debug("Error reading thumbnails table: %s", e)
                if warning_collector:
                    warning_collector.add_warning(
                        warning_type="database_parse_error",
                        category="database",
                        severity="error",
                        artifact_type="top_sites",
                        source_file=source_file,
                        item_name="thumbnails",
                        item_value=str(e),
                    )

        if records:
            count = insert_top_sites(evidence_conn, evidence_id, records)
            # Collect URLs (no deduplication)
            for rec in records:
                url = rec.get("url")
                if url:
                    parsed = urlparse(url)
                    url_list.append({
                        "url": url,
                        "domain": parsed.netloc or None,
                        "scheme": parsed.scheme or None,
                        "context": f"top_sites:{browser}:{profile}",
                        "timestamp": rec.get("last_forced_time_utc"),
                    })

        conn.close()

    except Exception as e:
        LOGGER.error("Failed to parse top sites from %s: %s", db_path, e)
        if warning_collector:
            warning_collector.add_warning(
                warning_type="file_corrupt",
                category="database",
                severity="error",
                artifact_type="top_sites",
                source_file=source_file,
                item_name="Top Sites",
                item_value=str(e),
            )

    return count, url_list


# =============================================================================
# Cross-posting to Images Table
# =============================================================================

def _cross_post_favicon_to_images(
    evidence_conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str,
    icon_data: bytes,
    icon_md5: str,
    icon_sha256: str,
    icon_url: Optional[str],
    browser: str,
    profile: str,
    file_info: Dict[str, Any],
    output_dir: Path,
    extractor_name: str,
    extractor_version: str,
) -> None:
    """
    Cross-post a favicon icon to the images table for unified image analysis.
    """
    try:
        from core.database import insert_image_with_discovery
        from core.phash import compute_phash, compute_phash_prefix

        ext = detect_image_extension(icon_data)
        rel_path = f"chromium_favicons/icons/{browser}/{icon_sha256[:2]}/{icon_sha256}.{ext}"

        # Compute pHash (skip SVG and unknown formats)
        phash = None
        phash_prefix = None
        if ext not in ("svg", "bin"):
            try:
                phash = compute_phash(io.BytesIO(icon_data))
                if phash:
                    phash_prefix = compute_phash_prefix(phash)
            except Exception as e:
                LOGGER.debug("pHash computation failed for favicon: %s", e)

        image_data = {
            "rel_path": rel_path,
            "filename": f"{icon_sha256}.{ext}",
            "md5": icon_md5,
            "sha256": icon_sha256,
            "phash": phash,
            "phash_prefix": phash_prefix,
            "file_type": ext,
            "size_bytes": len(icon_data),
            "notes": f"Favicon from {browser}/{profile}",
        }

        discovery_data = {
            "discovered_by": extractor_name,
            "run_id": run_id,
            "extractor_version": extractor_version,
            "source_path": file_info.get("source_path"),
            "cache_url": icon_url,
        }

        insert_image_with_discovery(evidence_conn, evidence_id, image_data, discovery_data)

    except Exception as e:
        LOGGER.debug("Failed to cross-post favicon to images table: %s", e)


def _cross_post_thumbnail_to_images(
    evidence_conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str,
    thumbnail_data: bytes,
    thumb_md5: str,
    thumb_sha256: str,
    page_url: Optional[str],
    browser: str,
    profile: str,
    file_info: Dict[str, Any],
    output_dir: Path,
    extractor_name: str,
    extractor_version: str,
) -> None:
    """
    Cross-post a top site thumbnail to the images table for unified image analysis.
    """
    try:
        from core.database import insert_image_with_discovery
        from core.phash import compute_phash, compute_phash_prefix

        ext = detect_image_extension(thumbnail_data)
        rel_path = f"chromium_favicons/thumbnails/{browser}/{thumb_sha256[:2]}/{thumb_sha256}.{ext}"

        # Compute pHash
        phash = None
        phash_prefix = None
        if ext not in ("svg", "bin"):
            try:
                phash = compute_phash(io.BytesIO(thumbnail_data))
                if phash:
                    phash_prefix = compute_phash_prefix(phash)
            except Exception as e:
                LOGGER.debug("pHash computation failed for thumbnail: %s", e)

        image_data = {
            "rel_path": rel_path,
            "filename": f"{thumb_sha256}.{ext}",
            "md5": thumb_md5,
            "sha256": thumb_sha256,
            "phash": phash,
            "phash_prefix": phash_prefix,
            "file_type": ext,
            "size_bytes": len(thumbnail_data),
            "notes": f"Top Sites thumbnail from {browser}/{profile}",
        }

        discovery_data = {
            "discovered_by": extractor_name,
            "run_id": run_id,
            "extractor_version": extractor_version,
            "source_path": file_info.get("source_path"),
            "cache_url": page_url,
        }

        insert_image_with_discovery(evidence_conn, evidence_id, image_data, discovery_data)

    except Exception as e:
        LOGGER.debug("Failed to cross-post thumbnail to images table: %s", e)
