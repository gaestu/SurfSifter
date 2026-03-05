"""Appendix Download List Module.

Displays all downloaded images in a grid for the appendix.
Uses parallel thumbnail generation with disk caching for performance.

Performance notes:
- Thumbnails are generated in parallel via ThreadPoolExecutor
- Thumbnails are cached to disk under ``{case_folder}/report_thumbs/downloads/``
- Thumbnails are referenced via file:// URIs to keep HTML small
"""

from __future__ import annotations

import base64
import hashlib
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from ..base import BaseAppendixModule, FilterField, FilterType, ModuleMetadata
from ...dates import format_datetime
from ...paths import get_module_template_dir
from core.image_codecs import ensure_pillow_heif_registered
from core.database.manager import slugify_label

logger = logging.getLogger(__name__)

# Try to import PIL for thumbnail generation
try:
    from PIL import Image as PILImage

    HAS_PIL = True
except ImportError:
    HAS_PIL = False

# Maximum number of SQL parameters per query (conservative, SQLite default is 999)
_SQL_CHUNK_SIZE = 500

# Number of parallel threads for thumbnail generation
_THUMB_WORKERS = 8


class AppendixDownloadListModule(BaseAppendixModule):
    """Appendix module for listing all downloaded images."""

    # Special filter values
    ALL = "all"
    ANY_TAG = "any_tag"

    # Thumbnail size
    THUMB_SIZE = (200, 200)

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="appendix_download_list",
            name="Download List",
            description="Lists all downloaded images with URL, hash, and timestamp",
            category="Appendix",
            icon="📥",
        )

    def get_default_title(self) -> str:
        """Return default title for the appendix section."""
        return "Downloaded Images"

    def get_filter_fields(self) -> List[FilterField]:
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
                key="include_url",
                label="Include URL",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the source URL under each image",
                required=False,
            ),
            FilterField(
                key="include_hash",
                label="Include Hash",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the MD5/SHA256 hash under each image",
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
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> Optional[List[tuple]]:
        """Load dynamic options for domain and tag filters."""
        if key == "domain_filter":
            options: List[tuple] = [
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

        if key == "tag_filter":
            options: List[tuple] = [
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
        """Render the full download list for the appendix."""
        # Extract config values
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")

        domain_filter = config.get("domain_filter", self.ALL)
        tag_filter = config.get("tag_filter", self.ALL)
        include_url = bool(config.get("include_url", True))
        include_hash = bool(config.get("include_hash", True))
        sort_by = config.get("sort_by", "date_desc")

        # Context for path resolution (injected by ReportBuilder)
        case_folder = config.get("_case_folder")
        evidence_label = config.get("_evidence_label")

        # Optional progress / cancellation callbacks
        progress_cb: Optional[Callable[[int, str], None]] = config.get(
            "_progress_callback"
        )
        cancelled_fn: Optional[Callable[[], bool]] = config.get("_cancelled_fn")

        # Build and execute query
        query, params = self._build_query(
            evidence_id, domain_filter, tag_filter, sort_by
        )

        # ── Step 1: Load download rows ─────────────────────────────────
        downloads: List[Dict[str, Any]] = []
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)
            for row in cursor.fetchall():
                downloads.append(dict(row))
        except Exception as exc:
            return (
                f'<div class="module-error">'
                f"Error loading downloaded images: {exc}</div>"
            )

        if progress_cb:
            progress_cb(5, f"Loaded {len(downloads)} downloaded images")

        # ── Step 2: Generate thumbnails in parallel with disk caching ──
        thumb_cache_dir = self._get_thumb_cache_dir(case_folder)
        if HAS_PIL:
            ensure_pillow_heif_registered()

        thumbnail_map = self._generate_thumbnails_batch(
            downloads,
            case_folder,
            evidence_id,
            evidence_label,
            thumb_cache_dir,
            progress_cb=progress_cb,
            cancelled_fn=cancelled_fn,
        )

        if cancelled_fn and cancelled_fn():
            return '<div class="module-error">Report generation was cancelled.</div>'

        # ── Step 3: Build display data ─────────────────────────────────
        processed: List[Dict[str, Any]] = []
        for row in downloads:
            image_data = self._process_download(
                row,
                date_format,
                translations,
                thumbnail_map.get(row["id"], ""),
            )
            processed.append(image_data)

        if progress_cb:
            progress_cb(80, "Rendering template")

        # ── Step 4: Render template ────────────────────────────────────
        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            images=processed,
            total_count=len(processed),
            include_url=include_url,
            include_hash=include_hash,
            t=translations,
            locale=locale,
        )

    # ── Query building ────────────────────────────────────────────────

    def _build_query(
        self,
        evidence_id: int,
        domain_filter: str,
        tag_filter: str,
        sort_by: str,
    ) -> tuple[str, list]:
        """Build SQL query based on filters."""
        params: list = [evidence_id]

        select = """
            SELECT DISTINCT
                d.id,
                d.url,
                d.domain,
                d.filename,
                d.dest_path,
                d.md5,
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

        query = select + " ".join(joins)
        query += " WHERE " + " AND ".join(where)
        query += f" ORDER BY {order}"

        return query, params

    # ── Data processing ───────────────────────────────────────────────

    def _process_download(
        self,
        row: Dict[str, Any],
        date_format: str,
        t: Dict[str, str],
        thumbnail_ref: str,
    ) -> Dict[str, Any]:
        """Process a download row into display data."""
        size_bytes = row.get("size_bytes")
        size_display = self._format_size(size_bytes) if size_bytes else ""

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
            "size_bytes": size_bytes,
            "size_display": size_display,
            "downloaded_at": completed_at,
            "width": row.get("width"),
            "height": row.get("height"),
            "thumbnail_src": thumbnail_ref,
        }

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Format file size for display."""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        else:
            return f"{size_bytes / (1024 * 1024):.1f} MB"

    # ── Thumbnail generation (parallel + cached) ──────────────────────

    @staticmethod
    def _get_thumb_cache_dir(case_folder: Optional[Path]) -> Optional[Path]:
        """Return (and create) thumbnail cache directory under the case folder."""
        if not case_folder:
            return None
        cache_dir = Path(case_folder) / "report_thumbs" / "downloads"
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            return None
        return cache_dir

    @staticmethod
    def _thumb_cache_key(download_id: int, md5: Optional[str]) -> str:
        """Deterministic cache key for a thumbnail."""
        if md5:
            return md5
        raw = f"download:{download_id}"
        return hashlib.md5(raw.encode(), usedforsecurity=False).hexdigest()

    def _generate_single_thumbnail(
        self,
        image_path: Optional[Path],
        cache_path: Optional[Path],
    ) -> str:
        """Generate a thumbnail for one image, using cache when available.

        Returns a ``file://`` URI if cache is available, otherwise inline base64.
        """
        # Check disk cache first
        if cache_path and cache_path.exists() and cache_path.stat().st_size > 100:
            return cache_path.as_uri()

        if not image_path:
            return ""

        try:
            with PILImage.open(image_path) as img:
                if img.mode in ("RGBA", "P"):
                    img = img.convert("RGB")
                img.thumbnail(self.THUMB_SIZE, PILImage.Resampling.LANCZOS)

                buffer = BytesIO()
                img.save(buffer, format="JPEG", quality=85)
                thumb_bytes = buffer.getvalue()

            # Try to write to disk cache
            if cache_path:
                try:
                    cache_path.write_bytes(thumb_bytes)
                    return cache_path.as_uri()
                except OSError:
                    pass

            # Fallback: inline base64
            b64 = base64.b64encode(thumb_bytes).decode("utf-8")
            return f"data:image/jpeg;base64,{b64}"
        except Exception:
            return ""

    def _generate_thumbnails_batch(
        self,
        download_rows: List[Dict[str, Any]],
        case_folder: Optional[Path],
        evidence_id: int,
        evidence_label: Optional[str],
        thumb_cache_dir: Optional[Path],
        progress_cb: Optional[Callable[[int, str], None]] = None,
        cancelled_fn: Optional[Callable[[], bool]] = None,
    ) -> Dict[int, str]:
        """Generate thumbnails for all downloads in parallel with disk caching."""
        result: Dict[int, str] = {}
        if not HAS_PIL or not download_rows:
            return result

        work_items: List[tuple] = []
        for row in download_rows:
            dest_path = row.get("dest_path")
            if not dest_path:
                continue
            download_id = row["id"]

            cache_path: Optional[Path] = None
            if thumb_cache_dir:
                key = self._thumb_cache_key(download_id, row.get("md5"))
                cache_path = thumb_cache_dir / f"{key}.jpg"

                # Cache hit — skip path resolution
                if cache_path.exists() and cache_path.stat().st_size > 100:
                    work_items.append((download_id, None, cache_path))
                    continue

            # Resolve source path
            source_path = self._resolve_download_path(
                dest_path, case_folder, evidence_id, evidence_label
            )
            if not source_path or not source_path.exists():
                continue

            work_items.append((download_id, source_path, cache_path))

        total = len(work_items)
        if total == 0:
            return result

        # Fast path: all cached
        uncached = [item for item in work_items if item[1] is not None]
        if not uncached:
            logger.debug("All %d download thumbnails served from cache", total)
            for download_id, _src, cache_path in work_items:
                result[download_id] = cache_path.as_uri()  # type: ignore[union-attr]
            if progress_cb:
                progress_cb(60, f"All {total} thumbnails loaded from cache")
            return result

        # Parallel generation
        done_count = 0
        workers = min(_THUMB_WORKERS, total)

        def _task(item: tuple) -> tuple:
            download_id, source_path, cache_path = item
            ref = self._generate_single_thumbnail(source_path, cache_path)
            return (download_id, ref)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_task, item): item for item in work_items}
            for future in as_completed(futures):
                if cancelled_fn and cancelled_fn():
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                try:
                    download_id, ref = future.result()
                    result[download_id] = ref
                except Exception:
                    pass
                done_count += 1
                if progress_cb and done_count % 50 == 0:
                    pct = 10 + int(50 * done_count / total)
                    progress_cb(pct, f"Thumbnails: {done_count}/{total}")

        if progress_cb:
            progress_cb(60, f"Generated {len(result)} thumbnails")

        return result

    def _resolve_download_path(
        self,
        dest_path: str,
        case_folder: Optional[Path],
        evidence_id: int,
        evidence_label: Optional[str],
    ) -> Optional[Path]:
        """Resolve a download's destination path to its full filesystem path."""
        # Try dest_path directly first (may be absolute)
        direct_path = Path(dest_path)
        if direct_path.is_absolute() and direct_path.exists():
            return direct_path

        if not case_folder:
            if direct_path.exists():
                return direct_path
            return None

        # Build evidence slug
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

        # Try without prefix
        relative_path = evidence_dir / dest_path
        if relative_path.exists():
            return relative_path

        # Try from case folder directly
        last_resort = case_folder / dest_path
        if last_resort.exists():
            return last_resort

        return None
