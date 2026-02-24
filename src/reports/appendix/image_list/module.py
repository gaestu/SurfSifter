"""Appendix Image List Module.

Displays a grid of images with tag and hash match filters.
Uses multi-select filters with OR/AND mode like the file list appendix.
"""

from __future__ import annotations

import base64
import json
import sqlite3
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from ..base import BaseAppendixModule, FilterField, FilterType, ModuleMetadata
from ...dates import format_datetime
from ...paths import get_module_template_dir
from core.image_codecs import ensure_pillow_heif_registered
from core.database.manager import slugify_label

# Try to import PIL for thumbnail generation
try:
    from PIL import Image as PILImage
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

# Human-readable display names for extractor identifiers
EXTRACTOR_DISPLAY_NAMES: Dict[str, str] = {
    "bulk_extractor": "Bulk Extractor",
    "bulk_extractor:images": "Bulk Extractor",
    "bulk_extractor_images": "Bulk Extractor",
    "foremost_carver": "Foremost Carver",
    "scalpel": "Scalpel",
    "image_carving": "Image Carving",
    "filesystem_images": "Filesystem",
    "cache_simple": "Chromium Cache",
    "cache_blockfile": "Chromium Cache (Blockfile)",
    "cache_firefox": "Firefox Cache",
    "browser_storage_indexeddb": "Browser Storage (IndexedDB)",
    "safari": "Safari",
    "safari_cache": "Safari Cache",
    "firefox_favicons": "Firefox Favicons",
    "chromium_favicons": "Chromium Favicons",
}


def _humanize_extractor(name: str) -> str:
    """Return a human-readable display name for an extractor identifier."""
    if name in EXTRACTOR_DISPLAY_NAMES:
        return EXTRACTOR_DISPLAY_NAMES[name]
    # Fallback: title-case with underscores replaced
    return name.replace("_", " ").title()


class AppendixImageListModule(BaseAppendixModule):
    """Appendix module for listing images with tag and hash match filters."""

    # Thumbnail size
    THUMB_SIZE = (200, 200)

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="appendix_image_list",
            name="Image List",
            description="Lists images in a grid with tag and hash match filters",
            category="Appendix",
            icon="🖼️",
        )

    def get_filter_fields(self) -> List[FilterField]:
        return [
            FilterField(
                key="tag_filter",
                label="Tags",
                filter_type=FilterType.TAG_SELECT,
                help_text="Filter by one or more tags",
                required=False,
            ),
            FilterField(
                key="match_filter",
                label="Hash Matches",
                filter_type=FilterType.MULTI_SELECT,
                help_text="Filter by one or more hash match lists",
                required=False,
            ),
            FilterField(
                key="filter_mode",
                label="Filter Mode",
                filter_type=FilterType.DROPDOWN,
                default="or",
                options=[
                    ("or", "OR - Any tag or any match"),
                    ("and", "AND - Must have tag AND match"),
                ],
                help_text="OR: Images with any selected tag or match. AND: Images must have a selected tag AND a selected match.",
                required=False,
            ),
            FilterField(
                key="include_filepath",
                label="Include File Path",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the file path under each image",
                required=False,
            ),
            FilterField(
                key="include_url",
                label="Include URLs",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show associated URLs (from cache/browser sources) for each image",
                required=False,
            ),
            FilterField(
                key="sort_by",
                label="Sort By",
                filter_type=FilterType.DROPDOWN,
                default="date_desc",
                options=[
                    ("date_desc", "Date (Newest First)"),
                    ("date_asc", "Date (Oldest First)"),
                    ("filename_asc", "Filename (A-Z)"),
                    ("filename_desc", "Filename (Z-A)"),
                ],
                help_text="Sort order for the images",
                required=False,
            ),
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> Optional[List[tuple]]:
        if key == "tag_filter":
            options: List[tuple] = []
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT t.name
                    FROM tags t
                    JOIN tag_associations ta ON ta.tag_id = t.id
                    WHERE ta.artifact_type = 'image'
                    ORDER BY t.name
                    """
                )
                for (tag_name,) in cursor.fetchall():
                    options.append((tag_name, tag_name))
            except Exception:
                pass
            return options

        if key == "match_filter":
            options: List[tuple] = []
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT db_name
                    FROM hash_matches
                    ORDER BY db_name
                    """
                )
                for (list_name,) in cursor.fetchall():
                    if list_name:
                        options.append((list_name, list_name))
            except Exception:
                pass
            return options

        return None

    def render(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        config: Dict[str, Any],
    ) -> str:
        # Extract config values
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")

        tag_filter = config.get("tag_filter") or []
        match_filter = config.get("match_filter") or []
        filter_mode = config.get("filter_mode", "or")
        include_filepath = bool(config.get("include_filepath", False))
        include_url = bool(config.get("include_url", False))
        sort_by = config.get("sort_by", "date_desc")

        # Get context for path resolution (injected by ReportBuilder)
        case_folder = config.get("_case_folder")
        evidence_label = config.get("_evidence_label")

        query, params = self._build_query(
            evidence_id, tag_filter, match_filter, filter_mode, sort_by
        )

        images: List[Dict[str, Any]] = []
        seen_ids: set[int] = set()
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)
            for row in cursor.fetchall():
                image_id = row["id"]
                if image_id in seen_ids:
                    continue
                seen_ids.add(image_id)

                image_data = self._process_image(
                    dict(row),
                    case_folder,
                    evidence_id,
                    evidence_label,
                    date_format,
                    translations,
                )
                images.append(image_data)
        except Exception as exc:
            return f'<div class="module-error">Error loading images: {exc}</div>'

        # Fetch extractor sources and filesystem dates for all images
        image_ids = [img["id"] for img in images]
        if image_ids:
            image_sources = self._get_image_sources(db_conn, image_ids)
            fs_dates = self._get_filesystem_dates(db_conn, image_ids)
            for img in images:
                raw_sources = image_sources.get(img["id"], [])
                img["found_by"] = [_humanize_extractor(s) for s in raw_sources]

                # Use filesystem creation date when available for fs images
                if img["id"] in fs_dates:
                    img["timestamp"] = format_datetime(
                        fs_dates[img["id"]], date_format,
                        include_time=True, include_seconds=True,
                    )
                    img["timestamp_label"] = translations.get(
                        "creation_date", "Creation Date"
                    )
                elif "filesystem_images" in raw_sources and not img.get("timestamp"):
                    # Filesystem image but no date at all
                    img["timestamp_label"] = translations.get(
                        "creation_date", "Creation Date"
                    )
                else:
                    img["timestamp_label"] = translations.get(
                        "source_date", "Source Date"
                    )

        # Fetch URLs for images if requested
        if include_url and images:
            image_urls = self._get_image_urls(db_conn, image_ids)
            for img in images:
                img["urls"] = image_urls.get(img["id"], [])

        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            images=images,
            total_count=len(images),
            include_filepath=include_filepath,
            include_url=include_url,
            t=translations,
            locale=locale,
        )

    def _get_image_urls(
        self, db_conn: sqlite3.Connection, image_ids: List[int]
    ) -> Dict[int, List[str]]:
        """Fetch unique URLs associated with images from image_discoveries.

        Args:
            db_conn: Database connection
            image_ids: List of image IDs to fetch URLs for

        Returns:
            Dict mapping image_id to list of unique URLs
        """
        if not image_ids:
            return {}

        result: Dict[int, List[str]] = {}
        placeholders = ",".join("?" * len(image_ids))
        try:
            cursor = db_conn.execute(
                f"""
                SELECT image_id, cache_url
                FROM image_discoveries
                WHERE image_id IN ({placeholders})
                  AND cache_url IS NOT NULL
                  AND cache_url != ''
                ORDER BY image_id, discovered_at
                """,
                image_ids,
            )
            for image_id, cache_url in cursor.fetchall():
                if image_id not in result:
                    result[image_id] = []
                # Add only unique URLs per image
                if cache_url not in result[image_id]:
                    result[image_id].append(cache_url)
        except Exception:
            pass
        return result

    def _get_filesystem_dates(
        self, db_conn: sqlite3.Connection, image_ids: List[int]
    ) -> Dict[int, str]:
        """Fetch filesystem creation dates for images from image_discoveries.

        Returns the best available filesystem timestamp (fs_crtime preferred,
        then fs_mtime as fallback) for images discovered by filesystem_images.

        Args:
            db_conn: Database connection
            image_ids: List of image IDs to fetch dates for

        Returns:
            Dict mapping image_id to ISO datetime string
        """
        if not image_ids:
            return {}

        result: Dict[int, str] = {}
        placeholders = ",".join("?" * len(image_ids))
        try:
            cursor = db_conn.execute(
                f"""
                SELECT image_id,
                       COALESCE(fs_crtime, fs_mtime) AS best_date
                FROM image_discoveries
                WHERE image_id IN ({placeholders})
                  AND discovered_by = 'filesystem_images'
                  AND (fs_crtime IS NOT NULL OR fs_mtime IS NOT NULL)
                """,
                image_ids,
            )
            for image_id, best_date in cursor.fetchall():
                if best_date and image_id not in result:
                    result[image_id] = best_date
        except Exception:
            pass
        return result

    def _get_image_sources(
        self, db_conn: sqlite3.Connection, image_ids: List[int]
    ) -> Dict[int, List[str]]:
        """Fetch distinct extractor sources for images using v_image_sources.

        Args:
            db_conn: Database connection
            image_ids: List of image IDs to fetch sources for

        Returns:
            Dict mapping image_id to list of extractor name strings
        """
        if not image_ids:
            return {}

        result: Dict[int, List[str]] = {}
        placeholders = ",".join("?" * len(image_ids))
        try:
            cursor = db_conn.execute(
                f"""
                SELECT image_id, sources
                FROM v_image_sources
                WHERE image_id IN ({placeholders})
                """,
                image_ids,
            )
            for image_id, sources in cursor.fetchall():
                if sources:
                    result[image_id] = [
                        s.strip() for s in sources.split(",") if s.strip()
                    ]
                else:
                    result[image_id] = []
        except Exception:
            pass
        return result

    def _build_query(
        self,
        evidence_id: int,
        tag_filter: List[str],
        match_filter: List[str],
        filter_mode: str,
        sort_by: str,
    ) -> tuple[str, list[Any]]:
        params: list[Any] = [evidence_id]
        conditions: list[str] = ["i.evidence_id = ?"]

        tag_condition = None
        match_condition = None

        if tag_filter:
            placeholders = ", ".join(["?"] * len(tag_filter))
            tag_condition = f"""
                EXISTS (
                    SELECT 1
                    FROM tag_associations ta
                    JOIN tags t ON t.id = ta.tag_id
                    WHERE ta.artifact_id = i.id
                      AND ta.artifact_type = 'image'
                      AND ta.evidence_id = i.evidence_id
                      AND t.name IN ({placeholders})
                )
                """

        if match_filter:
            placeholders = ", ".join(["?"] * len(match_filter))
            match_condition = f"""
                EXISTS (
                    SELECT 1
                    FROM hash_matches hm
                    WHERE hm.image_id = i.id
                      AND hm.db_name IN ({placeholders})
                )
                """

        # Apply filter logic based on mode
        if filter_mode == "and" and tag_filter and match_filter:
            # AND mode: must have a selected tag AND a selected match
            conditions.append(f"({tag_condition})")
            params.extend(tag_filter)
            conditions.append(f"({match_condition})")
            params.extend(match_filter)
        elif tag_condition or match_condition:
            # OR mode (default): any selected tag OR any selected match
            or_parts = []
            if tag_condition:
                or_parts.append(tag_condition)
                params.extend(tag_filter)
            if match_condition:
                or_parts.append(match_condition)
                params.extend(match_filter)
            if or_parts:
                conditions.append(f"({' OR '.join(or_parts)})")

        where_clause = " AND ".join(conditions)

        # Sort order
        order_map = {
            "date_desc": "i.ts_utc DESC NULLS LAST, i.filename ASC",
            "date_asc": "i.ts_utc ASC NULLS FIRST, i.filename ASC",
            "filename_asc": "i.filename ASC",
            "filename_desc": "i.filename DESC",
        }
        order = order_map.get(sort_by, "i.ts_utc DESC NULLS LAST")

        query = f"""
            SELECT DISTINCT
                i.id,
                i.rel_path,
                i.filename,
                i.md5,
                i.sha256,
                i.ts_utc,
                i.exif_json,
                i.size_bytes,
                i.first_discovered_by
            FROM images i
            WHERE {where_clause}
            ORDER BY {order}
        """

        return query, params

    def _process_image(
        self,
        row: Dict[str, Any],
        case_folder: Optional[Path],
        evidence_id: int,
        evidence_label: Optional[str],
        date_format: str,
        t: Dict[str, str],
    ) -> Dict[str, Any]:
        """Process an image row into display data."""
        # Parse EXIF if available
        exif_data = {}
        if row.get("exif_json"):
            try:
                exif_data = json.loads(row["exif_json"])
            except (json.JSONDecodeError, TypeError):
                pass

        # Format EXIF for display (key fields only)
        exif_display = []
        exif_keys = [
            ("DateTimeOriginal", t.get("exif_taken", "Taken")),
            ("Make", t.get("exif_camera", "Camera")),
            ("Model", t.get("exif_model", "Model")),
            ("GPSLatitude", t.get("exif_gps_lat", "GPS Lat")),
            ("GPSLongitude", t.get("exif_gps_lon", "GPS Lon")),
        ]
        for exif_key, label in exif_keys:
            if exif_key in exif_data:
                exif_display.append(f"{label}: {exif_data[exif_key]}")

        # Generate thumbnail as base64
        thumbnail_b64 = self._generate_thumbnail(
            row.get("rel_path"),
            row.get("first_discovered_by"),
            case_folder,
            evidence_id,
            evidence_label,
        )

        return {
            "id": row["id"],
            "rel_path": row.get("rel_path", ""),
            "md5": row.get("md5", ""),
            "sha256": row.get("sha256", ""),
            "timestamp": format_datetime(
                row.get("ts_utc", ""), date_format, include_time=True, include_seconds=True
            ),
            "timestamp_label": t.get("source_date", "Source Date"),  # default; overridden in render()
            "size_bytes": row.get("size_bytes"),
            "exif_display": exif_display,
            "thumbnail_b64": thumbnail_b64,
            "filename": row.get("filename", ""),  # kept for alt text
        }

    def _generate_thumbnail(
        self,
        rel_path: Optional[str],
        discovered_by: Optional[str],
        case_folder: Optional[Path],
        evidence_id: int,
        evidence_label: Optional[str],
    ) -> str:
        """Generate a base64 thumbnail for an image."""
        if not rel_path or not HAS_PIL:
            return ""

        try:
            image_path = self._resolve_image_path(
                rel_path, discovered_by, case_folder, evidence_id, evidence_label
            )

            if not image_path or not image_path.exists():
                return ""

            ensure_pillow_heif_registered()
            # Open and create thumbnail
            with PILImage.open(image_path) as img:
                # Convert to RGB if necessary (for PNG with transparency)
                if img.mode in ("RGBA", "P"):
                    img = img.convert("RGB")

                # Create thumbnail (maintains aspect ratio)
                img.thumbnail(self.THUMB_SIZE, PILImage.Resampling.LANCZOS)

                # Save to bytes
                buffer = BytesIO()
                img.save(buffer, format="JPEG", quality=85)
                buffer.seek(0)

                # Encode as base64
                b64 = base64.b64encode(buffer.read()).decode("utf-8")
                return f"data:image/jpeg;base64,{b64}"

        except Exception:
            return ""

    def _resolve_image_path(
        self,
        rel_path: str,
        discovered_by: Optional[str],
        case_folder: Optional[Path],
        evidence_id: int,
        evidence_label: Optional[str],
    ) -> Optional[Path]:
        """Resolve an image's relative path to its full filesystem path."""
        if not case_folder:
            # Try simple paths as fallback
            simple_path = Path(rel_path)
            if simple_path.exists():
                return simple_path
            return None

        # Build evidence slug using canonical slugify function
        if evidence_label:
            evidence_slug = slugify_label(evidence_label, evidence_id)
        else:
            evidence_slug = f"evidence_{evidence_id}"

        evidence_dir = case_folder / "evidences" / evidence_slug

        # Map discovered_by to the extractor's output subdirectory
        source_map = {
            # Carving extractors
            "bulk_extractor": "bulk_extractor",
            "bulk_extractor:images": "bulk_extractor",
            "bulk_extractor_images": "bulk_extractor",
            "foremost_carver": "foremost_carver",
            "scalpel": "scalpel",
            "image_carving": "",  # Legacy: rel_path is full path
            # Filesystem extractor
            "filesystem_images": "filesystem_images/extracted",
            # Cache/browser extractors
            "cache_simple": "cache_simple",
            "cache_blockfile": "cache_simple",
            "cache_firefox": "cache_firefox",
            "browser_storage_indexeddb": "browser_storage",
            "safari": "safari",
            # Favicon extractors
            "firefox_favicons": "firefox_favicons",
            "chromium_favicons": "chromium_favicons",
        }

        base_subdir = source_map.get(discovered_by or "", "")

        # Try extractor-specific path first
        if base_subdir:
            full_path = (evidence_dir / base_subdir / rel_path).resolve()
            if full_path.exists():
                return full_path

        # Fallback: try direct path under evidence directory
        fallback_path = (evidence_dir / rel_path).resolve()
        if fallback_path.exists():
            return fallback_path

        # Last resort: try from case folder directly
        last_resort = (case_folder / rel_path).resolve()
        if last_resort.exists():
            return last_resort

        return None
