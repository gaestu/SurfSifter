"""Images Report Module.

Displays a grid of images with hash, timestamp, and optional filepath.
Filters by tag and hash match.
"""

from __future__ import annotations

import base64
import json
import sqlite3
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional

from jinja2 import Template

from ...dates import format_datetime
from ..base import (
    BaseReportModule,
    FilterField,
    FilterType,
    ModuleMetadata,
)
from core.image_codecs import ensure_pillow_heif_registered
from core.database.manager import slugify_label
from reports.paths import get_module_template_dir

# Try to import PIL for thumbnail generation
try:
    from PIL import Image as PILImage
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

# Module directory for template resolution
_MODULE_DIR = get_module_template_dir(__file__)


class ImagesModule(BaseReportModule):
    """Module for displaying images as a grid in reports."""

    # Special filter values
    ALL = "all"
    ANY_TAG = "any_tag"
    ANY_MATCH = "any_match"

    # Thumbnail size
    THUMB_SIZE = (200, 200)

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="images",
            name="Images",
            description="Displays images in a grid with hash, timestamp, and filepath",
            category="Images",
            icon="🖼️",
        )

    def get_template_path(self) -> Optional[Path]:
        """Get path to the module's HTML template."""
        template_path = _MODULE_DIR / "template.html"
        if template_path.exists():
            return template_path
        return None

    def get_filter_fields(self) -> List[FilterField]:
        """Return filter fields for tags, hash matches, and sort."""
        return [
            FilterField(
                key="title",
                label="Title",
                filter_type=FilterType.TEXT,
                default="",
                help_text="Optional title displayed above the images",
                required=False,
            ),
            FilterField(
                key="tag_filter",
                label="Tags",
                filter_type=FilterType.DROPDOWN,
                default=self.ALL,
                options=[
                    (self.ALL, "All"),
                    (self.ANY_TAG, "Any Tag"),
                ],
                help_text="Filter by tag (specific tags loaded dynamically)",
                required=False,
            ),
            FilterField(
                key="match_filter",
                label="Hash Match",
                filter_type=FilterType.DROPDOWN,
                default=self.ALL,
                options=[
                    (self.ALL, "All"),
                    (self.ANY_MATCH, "Any Match"),
                ],
                help_text="Filter by hash match (lists loaded dynamically)",
                required=False,
            ),
            FilterField(
                key="include_filename",
                label="Include Filename",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the filename under each image (often a hash)",
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
                label="Include URL",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the source URL(s) under each image (from cache discoveries)",
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
            FilterField(
                key="show_filter_info",
                label="Show Filter Info",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display filter criteria below the images",
                required=False,
            ),
            FilterField(
                key="show_image_count",
                label="Show Image Count",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show 'Showing X of Y images' when a limit is applied",
                required=False,
            ),
            FilterField(
                key="limit",
                label="Max Images",
                filter_type=FilterType.DROPDOWN,
                default="all",
                options=[
                    ("5", "5"),
                    ("9", "9"),
                    ("21", "21"),
                    ("33", "33"),
                    ("60", "60"),
                    ("all", "All"),
                ],
                help_text="Maximum number of images to display",
                required=False,
            ),
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> List[tuple] | None:
        """Load dynamic options for tag and match filters.

        Args:
            key: The filter field key
            db_conn: SQLite connection to evidence database

        Returns:
            List of (value, label) tuples or None if not a dynamic field
        """
        if key == "tag_filter":
            # Get all tags used for images
            options = [
                (self.ALL, "All"),
                (self.ANY_TAG, "Any Tag"),
            ]
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

        elif key == "match_filter":
            # Get all hash lists with matches
            options = [
                (self.ALL, "All"),
                (self.ANY_MATCH, "Any Match"),
            ]
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
        """Render the images grid as HTML.

        Args:
            db_conn: SQLite connection to the evidence database
            evidence_id: Current evidence ID
            config: Dictionary of filter values from user configuration

        Returns:
            HTML string with images grid
        """
        # Get filter values
        title = config.get("title", "")
        tag_filter = config.get("tag_filter", self.ALL)
        match_filter = config.get("match_filter", self.ALL)
        include_filename = config.get("include_filename", True)
        include_filepath = config.get("include_filepath", False)
        include_url = config.get("include_url", False)
        sort_by = config.get("sort_by", "date_desc")
        show_filter_info = config.get("show_filter_info", False)
        show_image_count = config.get("show_image_count", True)
        limit = config.get("limit", "all")

        # Get locale and translations
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")

        # Get context for path resolution (injected by ReportBuilder)
        case_folder = config.get("_case_folder")
        evidence_label = config.get("_evidence_label")

        # Build query
        query, params = self._build_query(
            evidence_id, tag_filter, match_filter, sort_by, limit
        )

        # Get total count (without limit) for display
        count_query, count_params = self._build_query(
            evidence_id, tag_filter, match_filter, sort_by, "all"
        )
        count_cursor = db_conn.execute(
            f"SELECT COUNT(*) FROM ({count_query})", count_params
        )
        total_count = count_cursor.fetchone()[0]

        # Execute query
        cursor = db_conn.execute(query, params)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        # If URLs are requested, fetch them for all images in one query
        image_urls_map: Dict[int, List[str]] = {}
        if include_url:
            image_ids = [dict(zip(columns, row))["id"] for row in rows]
            if image_ids:
                image_urls_map = self._fetch_image_urls(db_conn, image_ids)

        # Process images
        images = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            image_data = self._process_image(
                row_dict,
                case_folder,
                evidence_id,
                evidence_label,
                date_format,
                translations,
                urls=image_urls_map.get(row_dict["id"], []) if include_url else [],
            )
            images.append(image_data)

        # Build filter description
        filter_description = self._build_filter_description(
            tag_filter, match_filter, translations
        )

        # Load and render template
        template_path = self.get_template_path()
        if template_path and template_path.exists():
            template_content = template_path.read_text(encoding="utf-8")
            template = Template(template_content)
            return template.render(
                images=images,
                title=title,
                shown_count=len(images),
                total_count=total_count,
                filter_description=filter_description,
                include_filename=include_filename,
                include_filepath=include_filepath,
                include_url=include_url,
                show_filter_info=show_filter_info,
                show_image_count=show_image_count,
                t=translations,
                locale=locale,
            )

        # Fallback if no template
        return f"<p>Found {len(images)} images matching filters.</p>"

    def _build_query(
        self,
        evidence_id: int,
        tag_filter: str,
        match_filter: str,
        sort_by: str,
        limit: str = "all",
    ) -> tuple[str, list]:
        """Build SQL query based on filters.

        Args:
            evidence_id: Evidence ID to filter by
            tag_filter: Tag filter value
            match_filter: Hash match filter value
            sort_by: Sort option
            limit: Maximum number of images to return

        Returns:
            Tuple of (query_string, parameters)
        """
        params: list = [evidence_id]

        # Base select with distinct to avoid duplicates from joins
        select = """
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
        """

        joins = []
        where = ["i.evidence_id = ?"]

        # Tag filter
        if tag_filter == self.ANY_TAG:
            joins.append(
                """
                JOIN tag_associations ta ON ta.artifact_id = i.id
                    AND ta.artifact_type = 'image'
                """
            )
        elif tag_filter not in (self.ALL, None, ""):
            joins.append(
                """
                JOIN tag_associations ta ON ta.artifact_id = i.id
                    AND ta.artifact_type = 'image'
                JOIN tags t ON t.id = ta.tag_id
                """
            )
            where.append("t.name = ?")
            params.append(tag_filter)

        # Match filter
        if match_filter == self.ANY_MATCH:
            joins.append(
                """
                JOIN hash_matches hm ON hm.image_id = i.id
                """
            )
        elif match_filter not in (self.ALL, None, ""):
            joins.append(
                """
                JOIN hash_matches hm ON hm.image_id = i.id
                """
            )
            where.append("hm.db_name = ?")
            params.append(match_filter)

        # Sort order
        order_map = {
            "date_desc": "i.ts_utc DESC NULLS LAST, i.filename ASC",
            "date_asc": "i.ts_utc ASC NULLS FIRST, i.filename ASC",
            "filename_asc": "i.filename ASC",
            "filename_desc": "i.filename DESC",
        }
        order = order_map.get(sort_by, "i.ts_utc DESC NULLS LAST")

        # Build full query
        query = select + " ".join(joins)
        query += " WHERE " + " AND ".join(where)
        query += f" ORDER BY {order}"

        # Apply limit
        if limit != "all":
            try:
                limit_val = int(limit)
                query += f" LIMIT {limit_val}"
            except ValueError:
                pass

        return query, params

    def _process_image(
        self,
        row: Dict[str, Any],
        case_folder: Optional[Path],
        evidence_id: int,
        evidence_label: Optional[str],
        date_format: str,
        t: Dict[str, str],
        urls: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Process an image row into display data.

        Args:
            row: Database row as dictionary
            case_folder: Path to the case folder
            evidence_id: Evidence ID
            evidence_label: Evidence label for folder resolution
            date_format: Date format preference
            t: Translation dictionary
            urls: List of unique source URLs for this image

        Returns:
            Dictionary with processed image data
        """
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
            "filename": row.get("filename", t.get("unknown", "Unknown")),
            "rel_path": row.get("rel_path", ""),
            "md5": row.get("md5", ""),
            "sha256": row.get("sha256", ""),
            "timestamp": format_datetime(
                row.get("ts_utc", ""), date_format, include_time=True, include_seconds=True
            ),
            "size_bytes": row.get("size_bytes"),
            "exif_display": exif_display,
            "thumbnail_b64": thumbnail_b64,
            "urls": urls or [],
        }

    def _fetch_image_urls(
        self,
        db_conn: sqlite3.Connection,
        image_ids: List[int],
    ) -> Dict[int, List[str]]:
        """Fetch unique source URLs for a list of images.

        Args:
            db_conn: SQLite connection to evidence database
            image_ids: List of image IDs to fetch URLs for

        Returns:
            Dictionary mapping image_id to list of unique URLs
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

    def _generate_thumbnail(
        self,
        rel_path: Optional[str],
        discovered_by: Optional[str],
        case_folder: Optional[Path],
        evidence_id: int,
        evidence_label: Optional[str],
    ) -> str:
        """Generate a base64 thumbnail for an image.

        Args:
            rel_path: Relative path to the image file
            discovered_by: Extractor that discovered the image
            case_folder: Path to the case folder
            evidence_id: Evidence ID
            evidence_label: Evidence label for folder resolution

        Returns:
            Base64 encoded thumbnail or empty string if failed
        """
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
        """Resolve an image's relative path to its full filesystem path.

        Args:
            rel_path: Relative path stored in database
            discovered_by: Source extractor
            case_folder: Path to the case folder
            evidence_id: Evidence ID
            evidence_label: Evidence label

        Returns:
            Full path to the image file, or None if cannot resolve
        """
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

    def _build_filter_description(
        self, tag_filter: str, match_filter: str, t: Dict[str, str] | None = None
    ) -> str:
        """Build human-readable filter description.

        Args:
            tag_filter: Tag filter value
            match_filter: Match filter value

        Returns:
            Description string
        """
        parts = []
        t = t or {}

        if tag_filter == self.ALL:
            parts.append(t.get("filter_all_tags", "All tags"))
        elif tag_filter == self.ANY_TAG:
            parts.append(t.get("filter_any_tagged", "Any tagged"))
        else:
            parts.append(
                t.get("filter_tag_label", "Tag: {tag}").format(tag=tag_filter)
            )

        if match_filter == self.ALL:
            parts.append(t.get("filter_all_matches", "All matches"))
        elif match_filter == self.ANY_MATCH:
            parts.append(t.get("filter_any_hash_match", "Any hash match"))
        else:
            parts.append(
                t.get("filter_match_label", "Match: {match}").format(match=match_filter)
            )

        return " | ".join(parts)
