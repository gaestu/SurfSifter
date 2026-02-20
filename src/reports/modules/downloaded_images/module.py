"""Downloaded Images Report Module.

Displays downloaded images in a grid with URL, hash, and download timestamp.
Filters by domain and tag.
"""

from __future__ import annotations

import base64
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


class DownloadedImagesModule(BaseReportModule):
    """Module for displaying downloaded images in reports."""

    # Special filter values
    ALL = "all"
    ANY_TAG = "any_tag"

    # Thumbnail size
    THUMB_SIZE = (200, 200)

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="downloaded_images",
            name="Downloaded Images",
            description="Displays downloaded images with URL, hash, and download timestamp",
            category="Downloads",
            icon="📥",
        )

    def get_template_path(self) -> Optional[Path]:
        """Get path to the module's HTML template."""
        template_path = _MODULE_DIR / "template.html"
        if template_path.exists():
            return template_path
        return None

    def get_filter_fields(self) -> List[FilterField]:
        """Return filter fields for domain, tags, and sort."""
        return [
            FilterField(
                key="domain_filter",
                label="Domain",
                filter_type=FilterType.DROPDOWN,
                default=self.ALL,
                options=[
                    (self.ALL, "All Domains"),
                ],
                help_text="Filter by source domain (specific domains loaded dynamically)",
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
                key="sort_by",
                label="Sort By",
                filter_type=FilterType.DROPDOWN,
                default="date_desc",
                options=[
                    ("date_desc", "Date (Newest First)"),
                    ("date_asc", "Date (Oldest First)"),
                    ("filename_asc", "Filename (A-Z)"),
                    ("filename_desc", "Filename (Z-A)"),
                    ("url_asc", "URL (A-Z)"),
                    ("url_desc", "URL (Z-A)"),
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
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> List[tuple] | None:
        """Load dynamic options for domain and tag filters.

        Args:
            key: The filter field key
            db_conn: SQLite connection to evidence database

        Returns:
            List of (value, label) tuples or None if not a dynamic field
        """
        if key == "domain_filter":
            # Get all domains from downloaded images
            options = [
                (self.ALL, "All Domains"),
            ]
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT domain
                    FROM downloads
                    WHERE file_type = 'image'
                      AND status = 'completed'
                      AND domain IS NOT NULL
                      AND domain != ''
                    ORDER BY domain
                    """
                )
                for (domain,) in cursor.fetchall():
                    if domain:
                        options.append((domain, domain))
            except Exception:
                pass
            return options

        elif key == "tag_filter":
            # Get all tags used for downloads
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
                    WHERE ta.artifact_type = 'download'
                    ORDER BY t.name
                    """
                )
                for (tag_name,) in cursor.fetchall():
                    options.append((tag_name, tag_name))
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
        """Render the downloaded images grid as HTML.

        Args:
            db_conn: SQLite connection to the evidence database
            evidence_id: Current evidence ID
            config: Dictionary of filter values from user configuration

        Returns:
            HTML string with images grid
        """
        # Get filter values
        domain_filter = config.get("domain_filter", self.ALL)
        tag_filter = config.get("tag_filter", self.ALL)
        sort_by = config.get("sort_by", "date_desc")
        show_filter_info = config.get("show_filter_info", False)

        # Get locale and translations
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")

        # Get context for path resolution (injected by ReportBuilder)
        case_folder = config.get("_case_folder")
        evidence_label = config.get("_evidence_label")

        # Build query
        query, params = self._build_query(
            evidence_id, domain_filter, tag_filter, sort_by
        )

        # Execute query
        cursor = db_conn.execute(query, params)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        # Process images
        images = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            image_data = self._process_download(
                row_dict,
                case_folder,
                evidence_id,
                evidence_label,
                date_format,
                translations,
            )
            images.append(image_data)

        # Build filter description
        filter_description = self._build_filter_description(
            domain_filter, tag_filter, translations
        )

        # Load and render template
        template_path = self.get_template_path()
        if template_path and template_path.exists():
            template_content = template_path.read_text(encoding="utf-8")
            template = Template(template_content)
            return template.render(
                images=images,
                total_count=len(images),
                filter_description=filter_description,
                show_filter_info=show_filter_info,
                t=translations,
                locale=locale,
            )

        # Fallback if no template
        return f"<p>Found {len(images)} downloaded images matching filters.</p>"

    def _build_query(
        self,
        evidence_id: int,
        domain_filter: str,
        tag_filter: str,
        sort_by: str,
    ) -> tuple[str, list]:
        """Build SQL query based on filters.

        Args:
            evidence_id: Evidence ID to filter by
            domain_filter: Domain filter value
            tag_filter: Tag filter value
            sort_by: Sort option

        Returns:
            Tuple of (query_string, parameters)
        """
        params: list = [evidence_id]

        # Base select with distinct to avoid duplicates from joins
        select = """
            SELECT DISTINCT
                d.id,
                d.url,
                d.domain,
                d.filename,
                d.dest_path,
                d.md5,
                d.sha256,
                d.size_bytes,
                d.completed_at_utc,
                d.width,
                d.height
            FROM downloads d
        """

        joins = []
        where = [
            "d.evidence_id = ?",
            "d.file_type = 'image'",
            "d.status = 'completed'",
        ]

        # Domain filter
        if domain_filter not in (self.ALL, None, ""):
            where.append("d.domain = ?")
            params.append(domain_filter)

        # Tag filter
        if tag_filter == self.ANY_TAG:
            joins.append(
                """
                JOIN tag_associations ta ON ta.artifact_id = d.id
                    AND ta.artifact_type = 'download'
                """
            )
        elif tag_filter not in (self.ALL, None, ""):
            joins.append(
                """
                JOIN tag_associations ta ON ta.artifact_id = d.id
                    AND ta.artifact_type = 'download'
                JOIN tags t ON t.id = ta.tag_id
                """
            )
            where.append("t.name = ?")
            params.append(tag_filter)

        # Sort order
        order_map = {
            "date_desc": "d.completed_at_utc DESC NULLS LAST, d.filename ASC",
            "date_asc": "d.completed_at_utc ASC NULLS FIRST, d.filename ASC",
            "filename_asc": "d.filename ASC",
            "filename_desc": "d.filename DESC",
            "url_asc": "d.url ASC",
            "url_desc": "d.url DESC",
        }
        order = order_map.get(sort_by, "d.completed_at_utc DESC NULLS LAST")

        # Build full query
        query = select + " ".join(joins)
        query += " WHERE " + " AND ".join(where)
        query += f" ORDER BY {order}"

        return query, params

    def _process_download(
        self,
        row: Dict[str, Any],
        case_folder: Optional[Path],
        evidence_id: int,
        evidence_label: Optional[str],
        date_format: str,
        t: Dict[str, str],
    ) -> Dict[str, Any]:
        """Process a download row into display data.

        Args:
            row: Database row as dictionary
            case_folder: Path to the case folder
            evidence_id: Evidence ID
            evidence_label: Evidence label for folder resolution

        Returns:
            Dictionary with processed download data
        """
        # Format file size for display
        size_bytes = row.get("size_bytes")
        size_display = self._format_size(size_bytes) if size_bytes else ""

        # Generate thumbnail as base64
        thumbnail_b64 = self._generate_thumbnail(
            row.get("dest_path"),
            case_folder,
            evidence_id,
            evidence_label,
        )

        # Format download date
        completed_at = format_datetime(
            row.get("completed_at_utc", ""),
            date_format,
            include_time=True,
            include_seconds=True,
        )

        return {
            "id": row["id"],
            "filename": row.get("filename", t.get("unknown", "Unknown")),
            "url": row.get("url", ""),
            "domain": row.get("domain", ""),
            "md5": row.get("md5", ""),
            "sha256": row.get("sha256", ""),
            "size_bytes": size_bytes,
            "size_display": size_display,
            "downloaded_at": completed_at,
            "width": row.get("width"),
            "height": row.get("height"),
            "thumbnail_b64": thumbnail_b64,
        }

    def _format_size(self, size_bytes: int) -> str:
        """Format file size for display.

        Args:
            size_bytes: File size in bytes

        Returns:
            Human-readable size string
        """
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        else:
            return f"{size_bytes / (1024 * 1024):.1f} MB"

    def _generate_thumbnail(
        self,
        dest_path: Optional[str],
        case_folder: Optional[Path],
        evidence_id: int,
        evidence_label: Optional[str],
    ) -> str:
        """Generate a base64 thumbnail for a downloaded image.

        Args:
            dest_path: Destination path stored in database
            case_folder: Path to the case folder
            evidence_id: Evidence ID
            evidence_label: Evidence label for folder resolution

        Returns:
            Base64 encoded thumbnail or empty string if failed
        """
        if not dest_path or not HAS_PIL:
            return ""

        try:
            image_path = self._resolve_download_path(
                dest_path, case_folder, evidence_id, evidence_label
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

    def _resolve_download_path(
        self,
        dest_path: str,
        case_folder: Optional[Path],
        evidence_id: int,
        evidence_label: Optional[str],
    ) -> Optional[Path]:
        """Resolve a download's destination path to its full filesystem path.

        Args:
            dest_path: Destination path stored in database
            case_folder: Path to the case folder
            evidence_id: Evidence ID
            evidence_label: Evidence label

        Returns:
            Full path to the downloaded file, or None if cannot resolve
        """
        # Try dest_path directly first (may be absolute)
        direct_path = Path(dest_path)
        if direct_path.is_absolute() and direct_path.exists():
            return direct_path

        if not case_folder:
            # Try as relative path
            if direct_path.exists():
                return direct_path
            return None

        # Build evidence slug using canonical slugify function
        if evidence_label:
            evidence_slug = slugify_label(evidence_label, evidence_id)
        else:
            evidence_slug = f"evidence_{evidence_id}"

        evidence_dir = case_folder / "evidences" / evidence_slug

        # Downloads are stored in _downloads folder
        downloads_dir = evidence_dir / "_downloads"

        # Try with _downloads prefix
        full_path = downloads_dir / dest_path
        if full_path.exists():
            return full_path

        # Try without prefix (maybe dest_path is already relative to downloads)
        relative_path = evidence_dir / dest_path
        if relative_path.exists():
            return relative_path

        # Try from case folder directly
        last_resort = case_folder / dest_path
        if last_resort.exists():
            return last_resort

        return None

    def _build_filter_description(
        self, domain_filter: str, tag_filter: str, t: Dict[str, str] | None = None
    ) -> str:
        """Build human-readable filter description.

        Args:
            domain_filter: Domain filter value
            tag_filter: Tag filter value

        Returns:
            Description string
        """
        parts = []
        t = t or {}

        if domain_filter == self.ALL:
            parts.append(t.get("filter_all_domains", "All domains"))
        else:
            parts.append(
                t.get("filter_domain_label", "Domain: {domain}").format(domain=domain_filter)
            )

        if tag_filter == self.ALL:
            parts.append(t.get("filter_all_tags", "All tags"))
        elif tag_filter == self.ANY_TAG:
            parts.append(t.get("filter_any_tagged", "Any tagged"))
        else:
            parts.append(
                t.get("filter_tag_label", "Tag: {tag}").format(tag=tag_filter)
            )

        return " | ".join(parts)
